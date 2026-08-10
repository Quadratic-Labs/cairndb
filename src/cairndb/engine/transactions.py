"""Layer 2 of the engine, continued: multi-key transactions.

The one thing raw blob CAS cannot do is change several keyed objects
atomically. The engine uses a dedicated system log (``logs/_tx/``) as the
transaction coordinator:

- The appended transaction record is the **commit point**: once it is
  durable in the log, the transaction has happened.
- Applying its mutations to the object store is an **idempotent fold**:
  applies are deterministic and last-writer-wins by log order, so duplicate
  application (concurrent committers, recovery re-runs) is harmless.
- A per-commit marker object (``txapplied/{N:012d}``) records that commit
  N's mutations have reached the object store; recovery re-applies any
  committed record lacking its marker, in log order.

Conflict detection is optimistic and layered:

1. At commit, the transaction's read set is re-read and etag-compared
   (catches applied transactions and direct object writers).
2. The transaction log is scanned from the transaction's begin point for
   committed records whose write set intersects this read set (catches
   committed-but-not-yet-applied transactions).
3. Anything landing between that scan and our winning put forces a lost
   put-if-absent race — the log is dense — where the committer's
   revalidate hook performs the same write-set check.

Isolation therefore holds between transactions; direct ``objects.put``
writers bypass steps 2–3 (they are still never lost — applies are plain
object writes — but they can be overwritten by a committed transaction).
Transactions serialize on the ``_tx`` log: a few tens of tx/s at most.
"""

import asyncio
from typing import Any, Self

import structlog

from cairndb.committer import CommitterConfig
from cairndb.core.exceptions import EventRejectedError, TransactionConflict
from cairndb.core.log import Commit, Event
from cairndb.core.types import EventType, SchemaVersion, SequenceNumber, Timestamp
from cairndb.engine.logs import Log
from cairndb.engine.objects import check_key
from cairndb.storage.base import BlobStorage

logger = structlog.get_logger(__name__)

TX_LOG_NAME = "_tx"
TX_EVENT_TYPE = EventType("cairndb.tx")
TX_SCHEMA_VERSION = SchemaVersion("1.0.0")
APPLIED_MARKER_PREFIX = "txapplied/"


def _marker_key(commit_number: int) -> str:
    return f"{APPLIED_MARKER_PREFIX}{commit_number:012d}"


def _write_set(event: Event) -> set[str]:
    """Keys a transaction record mutates."""
    if event.event_type != TX_EVENT_TYPE:
        return set()
    return {op["key"] for op in event.payload.get("ops", [])}


class Transaction:
    """A staged multi-key transaction. Obtain via ``CairnDB.transact()``.

    Used as an async context manager: exiting without an exception commits;
    an exception (or never entering) discards the staged operations.
    """

    def __init__(self, manager: TransactionManager):
        self._manager = manager
        self._reads: dict[str, str | None] = {}  # key -> etag observed (None = absent)
        self._ops: list[dict[str, Any]] = []
        self._staged: dict[str, bytes | None] = {}  # read-your-writes view
        self._notes: list[dict[str, Any]] = []
        self._begin_tail: int | None = None
        self.sequence: SequenceNumber | None = None  # set after commit

    async def __aenter__(self) -> Self:
        self._begin_tail = await self._manager.log.current_tail()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            await self._manager.commit(self)

    # -- staging -----------------------------------------------------------

    async def get(self, key: str) -> bytes | None:
        """Read `key`, recording it in the read set.

        Returns this transaction's own staged write when there is one
        (read-your-writes); the underlying read is still recorded so the
        transaction conflicts with concurrent writers of `key`.
        """
        check_key(key)
        if key not in self._reads:
            obj = await self._manager.storage.get_object(key)
            self._reads[key] = obj.etag if obj else None
            if key not in self._staged:
                return obj.data if obj else None
        if key in self._staged:
            return self._staged[key]
        obj = await self._manager.storage.get_object(key)
        return obj.data if obj else None

    def put(self, key: str, data: bytes) -> None:
        """Stage a write of `key`."""
        check_key(key)
        self._ops.append({"op": "put", "key": key, "data": data})
        self._staged[key] = data

    def delete(self, key: str) -> None:
        """Stage a deletion of `key`."""
        check_key(key)
        self._ops.append({"op": "delete", "key": key})
        self._staged[key] = None

    def note(self, event_type: str, payload: dict[str, Any]) -> None:
        """Attach an audit event to the transaction record.

        Notes are embedded in the record itself; projections of the ``_tx``
        log can fold them. They are not appended to any other log.
        """
        self._notes.append({"event_type": event_type, "payload": payload})

    # -- record ------------------------------------------------------------

    def to_event(self) -> Event:
        return Event(
            event_type=TX_EVENT_TYPE,
            timestamp=Timestamp.now(),
            payload={
                "reads": dict(self._reads),
                "ops": list(self._ops),
                "notes": list(self._notes),
            },
            schema_version=TX_SCHEMA_VERSION,
        )

    @property
    def read_keys(self) -> set[str]:
        return set(self._reads)

    @property
    def write_keys(self) -> set[str]:
        return {op["key"] for op in self._ops}


class TransactionManager:
    """Coordinates transactions over one engine's ``_tx`` log."""

    def __init__(self, storage: BlobStorage):
        self.storage = storage
        # One event per commit: each transaction record gets its own commit
        # number, so a competing transaction always either appears in the
        # begin-tail scan or forces a lost race into the revalidate hook.
        self.log = Log(
            storage,
            TX_LOG_NAME,
            committer_config=CommitterConfig(max_events_per_commit=1),
            revalidate=self._revalidate,
        )
        self._commit_lock = asyncio.Lock()
        self._pending_reads: dict[int, set[str]] = {}  # id(event) -> read set

    def transaction(self) -> Transaction:
        return Transaction(self)

    # -- commit protocol -----------------------------------------------------

    async def commit(self, tx: Transaction) -> SequenceNumber | None:
        """Run the commit protocol for a staged transaction.

        Returns the transaction record's sequence number, or None for a
        transaction with nothing to write.

        Raises:
            TransactionConflict: If the read set was written concurrently.
        """
        if not tx._ops and not tx._notes:
            return None
        if tx._begin_tail is None:
            raise RuntimeError("transaction was never entered ('async with db.transact()')")

        async with self._commit_lock:
            await self._validate_reads(tx)
            await self._check_committed_since(tx)
            sequence = await self._append(tx)

        await self._apply_commit(sequence.commit)
        tx.sequence = sequence
        logger.info(
            "transaction_committed",
            commit=sequence.commit,
            writes=len(tx._ops),
            reads=len(tx._reads),
        )
        return sequence

    async def _validate_reads(self, tx: Transaction) -> None:
        """Etag-compare the read set against current storage."""
        for key, etag in tx._reads.items():
            obj = await self.storage.get_object(key)
            current = obj.etag if obj else None
            if current != etag:
                raise TransactionConflict(
                    f"read set stale: {key!r} changed since it was read"
                )

    async def _check_committed_since(self, tx: Transaction) -> None:
        """Scan tx records committed since begin for write/read overlap."""
        async for commit in self.log.read_commits(after=tx._begin_tail):
            for event in commit.events:
                overlap = tx.read_keys & _write_set(event)
                if overlap:
                    raise TransactionConflict(
                        f"read set overlaps commit {commit.number}'s writes: {sorted(overlap)}"
                    )

    async def _append(self, tx: Transaction) -> SequenceNumber:
        event = tx.to_event()
        self._pending_reads[id(event)] = tx.read_keys
        try:
            return await self.log.append(event)
        except EventRejectedError as e:
            raise TransactionConflict(str(e)) from e
        finally:
            self._pending_reads.pop(id(event), None)

    async def _revalidate(
        self, events: list[Event], interleaved: list[Commit]
    ) -> list[Event | None]:
        """Committer hook: reject pending tx records that lost a race to a
        conflicting record."""
        interleaved_writes: set[str] = set()
        for commit in interleaved:
            for event in commit.events:
                interleaved_writes |= _write_set(event)

        decisions: list[Event | None] = []
        for event in events:
            reads = self._pending_reads.get(id(event), set(event.payload.get("reads", {})))
            decisions.append(None if reads & interleaved_writes else event)
        return decisions

    # -- apply / recovery -------------------------------------------------

    async def _apply_commit(self, number: int) -> None:
        """Apply every transaction record in commit `number`, then mark it.

        Records may share a commit only in recovery edge cases; applying
        the whole commit before marking keeps the marker truthful even so.
        Applies are unconditional: log order is the authority.
        """
        commit_bytes = await self.log.storage.get_commit(number)
        if commit_bytes is None:
            raise TransactionConflict(f"tx commit {number} vanished before apply")
        commit = Commit.from_msgpack(commit_bytes)

        for event in commit.events:
            if event.event_type != TX_EVENT_TYPE:
                continue
            for op in event.payload.get("ops", []):
                if op["op"] == "put":
                    await self.storage.put_object(op["key"], op["data"])
                elif op["op"] == "delete":
                    await self.storage.delete_object(op["key"])

        await self.storage.put_object(_marker_key(number), b"", if_absent=True)

    async def recover(self) -> int:
        """Re-apply committed transaction records that lack an applied marker.

        Idempotent and safe to run concurrently with live commits (applies
        are unconditional and deterministic; markers are put-if-absent).
        Run at engine start or as a cron, like the snapshot job.

        Returns:
            Number of commits (re-)applied.
        """
        marker_keys = await self.storage.list_objects(APPLIED_MARKER_PREFIX)
        applied = {
            int(k.removeprefix(APPLIED_MARKER_PREFIX))
            for k in marker_keys
            if k.removeprefix(APPLIED_MARKER_PREFIX).isdigit()
        }

        # Skip the contiguous applied prefix of the log.
        start = 0
        while start + 1 in applied:
            start += 1

        recovered = 0
        async for commit in self.log.read_commits(after=start):
            if commit.number in applied:
                continue
            await self._apply_commit(commit.number)
            recovered += 1
            logger.info("transaction_recovered", commit=commit.number)
        return recovered

    async def close(self) -> None:
        await self.log.close()
