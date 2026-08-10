"""Layer 1 of the engine: coordination primitives on conditional writes.

Three primitives, extracted from the patterns Flowlet proved on blob-storage
CAS (its dispatch keys, run-state leases, and hand-rolled read-modify-write
loops):

- claim: a unique constraint — put-if-absent where losers converge on the
  winner's value
- Lease: expiring ownership with epoch fencing — every write through the
  lease is etag-guarded, so a fenced holder can never publish an outcome
- Document: a typed mutable document with a bounded CAS retry loop

All primitives follow the storage layer's convention: the *_sync methods are
the primitive form, the async methods wrap them in a worker thread.
"""

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from cairndb.core.exceptions import CairnDBError, LeaseLost
from cairndb.engine.objects import check_key
from cairndb.storage.base import BlobStorage

logger = structlog.get_logger(__name__)

_ACQUIRE_ATTEMPTS = 8
_UPDATE_ATTEMPTS = 10


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _encode(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def _decode(data: bytes) -> Any:
    return json.loads(data)


# ----------------------------------------------------------------------
# claim — unique constraint
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ClaimResult:
    """Outcome of a claim: exactly one caller wins, everyone converges.

    ``value`` is the claimant's own value when ``won`` is True, and the
    winner's value otherwise — a duplicate or recovered caller reads back
    the winner's result and continues on the identical path.
    """

    won: bool
    key: str
    value: Any
    etag: str


def claim_sync(storage: BlobStorage, key: str, value: Any) -> ClaimResult:
    """
    Claim `key` with a JSON-serializable `value`, put-if-absent.

    Claims are immutable once won; there is no unclaim.

    Raises:
        CairnDBError: If the winner's value could not be read back
            (the object was deleted between the lost put and the read).
    """
    check_key(key)
    data = _encode(value)

    for _ in range(_ACQUIRE_ATTEMPTS):
        etag = storage.put_object_sync(key, data, if_absent=True)
        if etag is not None:
            logger.debug("claim_won", key=key)
            return ClaimResult(won=True, key=key, value=value, etag=etag)

        existing = storage.get_object_sync(key)
        if existing is not None:
            logger.debug("claim_lost", key=key)
            return ClaimResult(
                won=False, key=key, value=_decode(existing.data), etag=existing.etag
            )
        # Lost the put but the object is gone: it was deleted in between.
        # Retry the claim.

    raise CairnDBError(f"claim on {key!r} could not settle after {_ACQUIRE_ATTEMPTS} attempts")


async def claim(storage: BlobStorage, key: str, value: Any) -> ClaimResult:
    """Async twin of :func:`claim_sync`."""
    return await asyncio.to_thread(claim_sync, storage, key, value)


# ----------------------------------------------------------------------
# Lease — expiring ownership with epoch fencing
# ----------------------------------------------------------------------


class Lease:
    """An acquired lease on a key.

    The lease document is ``{epoch, holder, deadline_at, state}`` and is
    only ever replaced by CAS. Acquiring — fresh, after a release, or by
    stealing an expired lease — bumps ``epoch``, the monotonic fence token.

    Every write through the lease is guarded by the etag observed at the
    previous write; when the guard fails and the document's epoch has
    advanced, the holder has been fenced and gets :class:`LeaseLost`.

    Obtain instances via :func:`acquire_sync` / :func:`acquire`, not the
    constructor.
    """

    def __init__(
        self,
        storage: BlobStorage,
        key: str,
        *,
        ttl: float,
        epoch: int,
        holder: str | None,
        deadline_at: datetime,
        state: Any,
        etag: str,
    ):
        self.storage = storage
        self.key = key
        self.ttl = ttl
        self.epoch = epoch
        self.holder = holder
        self.deadline_at = deadline_at
        self.state = state
        self._etag = etag
        self._released = False

    # -- document (de)serialization ------------------------------------

    @staticmethod
    def _doc_bytes(
        epoch: int, holder: str | None, deadline_at: datetime, state: Any
    ) -> bytes:
        return _encode(
            {
                "epoch": epoch,
                "holder": holder,
                "deadline_at": deadline_at.isoformat(),
                "state": state,
            }
        )

    @staticmethod
    def _parse(data: bytes) -> dict[str, Any]:
        doc = _decode(data)
        doc["deadline_at"] = datetime.fromisoformat(doc["deadline_at"])
        return doc

    @staticmethod
    def _is_expired(doc: dict[str, Any], now: datetime) -> bool:
        return doc["holder"] is None or doc["deadline_at"] <= now

    # -- guarded writes -------------------------------------------------

    def _guarded_put_sync(self, holder: str | None, deadline_at: datetime, state: Any) -> None:
        """CAS-replace the lease document; LeaseLost if we were fenced.

        A failed guard is re-read once: if epoch and holder are still ours
        the etag was moved by an out-of-band write to our own document
        (nothing in the engine does this, but a cooperating cancel flag
        might) and the write is retried on the fresh etag; any other
        content means we were fenced.
        """
        if self._released:
            raise LeaseLost(f"lease on {self.key!r} was already released")

        data = self._doc_bytes(self.epoch, holder, deadline_at, state)
        for _ in range(2):
            new_etag = self.storage.put_object_sync(self.key, data, if_match=self._etag)
            if new_etag is not None:
                self._etag = new_etag
                self.holder = holder
                self.deadline_at = deadline_at
                self.state = state
                return

            current = self.storage.get_object_sync(self.key)
            if current is None:
                break
            doc = self._parse(current.data)
            if doc["epoch"] != self.epoch or doc["holder"] != self.holder:
                break
            self._etag = current.etag  # our document, moved etag: retry once

        logger.info("lease_lost", key=self.key, epoch=self.epoch)
        raise LeaseLost(f"lease on {self.key!r} lost (epoch {self.epoch} fenced)")

    def renew_sync(self) -> None:
        """Extend the deadline by ttl. Raises LeaseLost if fenced."""
        self._guarded_put_sync(self.holder, _utcnow() + timedelta(seconds=self.ttl), self.state)
        logger.debug("lease_renewed", key=self.key, epoch=self.epoch)

    def write_sync(self, state: Any) -> None:
        """Update the lease's state payload under the ownership guard."""
        self._guarded_put_sync(self.holder, self.deadline_at, state)

    def release_sync(self, state: Any = None) -> None:
        """Release the lease, optionally recording a final state.

        The document remains (holder None) so the epoch stays monotonic
        across re-acquisitions.
        """
        final_state = state if state is not None else self.state
        self._guarded_put_sync(None, _utcnow(), final_state)
        self._released = True
        logger.info("lease_released", key=self.key, epoch=self.epoch)

    async def renew(self) -> None:
        """Async twin of :meth:`renew_sync`."""
        await asyncio.to_thread(self.renew_sync)

    async def write(self, state: Any) -> None:
        """Async twin of :meth:`write_sync`."""
        await asyncio.to_thread(self.write_sync, state)

    async def release(self, state: Any = None) -> None:
        """Async twin of :meth:`release_sync`."""
        await asyncio.to_thread(self.release_sync, state)


def acquire_sync(
    storage: BlobStorage,
    key: str,
    *,
    ttl: float,
    holder: str | None = None,
    steal_if_expired: bool = True,
) -> Lease | None:
    """
    Acquire the lease on `key` for `ttl` seconds.

    Returns None when the lease is actively held by someone else, or when
    it is expired but ``steal_if_expired`` is False. Expiry is judged
    against the holder-written deadline, so clocks only need to agree to
    within the ttl — choose generous ttls.
    """
    check_key(key)
    if ttl <= 0:
        raise ValueError("ttl must be positive")

    for _ in range(_ACQUIRE_ATTEMPTS):
        now = _utcnow()
        deadline = now + timedelta(seconds=ttl)
        current = storage.get_object_sync(key)

        if current is None:
            etag = storage.put_object_sync(
                key, Lease._doc_bytes(1, holder, deadline, None), if_absent=True
            )
            if etag is None:
                continue  # raced another acquirer; re-read
            logger.info("lease_acquired", key=key, epoch=1, holder=holder)
            return Lease(
                storage, key, ttl=ttl, epoch=1, holder=holder,
                deadline_at=deadline, state=None, etag=etag,
            )

        doc = Lease._parse(current.data)
        if not Lease._is_expired(doc, now):
            return None
        if doc["holder"] is not None and not steal_if_expired:
            return None

        epoch = doc["epoch"] + 1
        etag = storage.put_object_sync(
            key,
            Lease._doc_bytes(epoch, holder, deadline, doc.get("state")),
            if_match=current.etag,
        )
        if etag is None:
            continue  # raced another acquirer; re-read
        logger.info("lease_acquired", key=key, epoch=epoch, holder=holder, stolen=True)
        return Lease(
            storage, key, ttl=ttl, epoch=epoch, holder=holder,
            deadline_at=deadline, state=doc.get("state"), etag=etag,
        )

    raise CairnDBError(f"lease on {key!r} could not settle after {_ACQUIRE_ATTEMPTS} attempts")


async def acquire(
    storage: BlobStorage,
    key: str,
    *,
    ttl: float,
    holder: str | None = None,
    steal_if_expired: bool = True,
) -> Lease | None:
    """Async twin of :func:`acquire_sync`."""
    return await asyncio.to_thread(
        lambda: acquire_sync(
            storage, key, ttl=ttl, holder=holder, steal_if_expired=steal_if_expired
        )
    )


# ----------------------------------------------------------------------
# Document — typed mutable document with CAS retry
# ----------------------------------------------------------------------


class Document:
    """A mutable, etag-guarded document with a read-modify-write loop.

    ``model`` may be a pydantic BaseModel subclass, in which case values
    are validated on read and serialized on write; without it, values are
    plain JSON-serializable objects.
    """

    def __init__(self, storage: BlobStorage, key: str, model: type | None = None):
        check_key(key)
        self.storage = storage
        self.key = key
        self.model = model

    def _to_bytes(self, value: Any) -> bytes:
        if self.model is not None:
            return value.model_dump_json().encode()
        return _encode(value)

    def _from_bytes(self, data: bytes) -> Any:
        if self.model is not None:
            return self.model.model_validate_json(data)
        return _decode(data)

    def get_sync(self) -> tuple[Any, str] | None:
        """Read the document. Returns (value, etag), or None if absent."""
        obj = self.storage.get_object_sync(self.key)
        if obj is None:
            return None
        return self._from_bytes(obj.data), obj.etag

    def update_sync(
        self,
        fn: Callable[[Any], Any],
        *,
        create: Any = None,
        max_attempts: int = _UPDATE_ATTEMPTS,
    ) -> Any:
        """
        Apply `fn` to the current value and CAS-write the result, retrying
        on conflict.

        `fn` must be pure — it may run several times. When the document is
        absent, `fn` is applied to `create`; with no `create`, an absent
        document is an error.

        Returns:
            The value that was written.

        Raises:
            CairnDBError: If the document is absent and no `create` was
                given, or the write kept conflicting for `max_attempts`.
        """
        for _ in range(max_attempts):
            obj = self.storage.get_object_sync(self.key)
            if obj is None:
                if create is None:
                    raise CairnDBError(
                        f"document {self.key!r} does not exist and no create value was given"
                    )
                new = fn(create)
                if self.storage.put_object_sync(self.key, self._to_bytes(new), if_absent=True):
                    return new
            else:
                new = fn(self._from_bytes(obj.data))
                if self.storage.put_object_sync(
                    self.key, self._to_bytes(new), if_match=obj.etag
                ):
                    return new

        raise CairnDBError(
            f"update of {self.key!r} kept conflicting after {max_attempts} attempts"
        )

    def delete_sync(self) -> None:
        """Delete the document; deleting a missing document is a no-op."""
        self.storage.delete_object_sync(self.key)

    async def get(self) -> tuple[Any, str] | None:
        """Async twin of :meth:`get_sync`."""
        return await asyncio.to_thread(self.get_sync)

    async def update(
        self,
        fn: Callable[[Any], Any],
        *,
        create: Any = None,
        max_attempts: int = _UPDATE_ATTEMPTS,
    ) -> Any:
        """Async twin of :meth:`update_sync`."""
        return await asyncio.to_thread(
            lambda: self.update_sync(fn, create=create, max_attempts=max_attempts)
        )

    async def delete(self) -> None:
        """Async twin of :meth:`delete_sync`."""
        await asyncio.to_thread(self.delete_sync)
