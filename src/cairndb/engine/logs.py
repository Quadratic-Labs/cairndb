"""Layer 2 of the engine: named commit logs.

A named log is a full CairnDB commit log — same dense numbering, same
put-if-absent arbitration, same Committer — living under ``logs/{name}/``
instead of the root ``log/`` prefix. It is implemented by routing the
commit/snapshot storage API through the generic conditional-object API
(NamespacedStorage), so backends need no changes.

One dense sequence per log: ordering is total *within* a log, and sharding
across logs is the throughput story. There is no sequencer process; the
bucket's put-if-absent arbitration sequences appends.

Log names may contain [a-z0-9._-]; the root log is ``db.log()`` (name None)
and keeps its original layout for backward compatibility.
"""

import asyncio
import re
from collections.abc import AsyncIterator

from cairndb.committer import Committer, CommitterConfig, RevalidateHook
from cairndb.core.log import Commit, Event, SequencedEvent
from cairndb.core.types import SequenceNumber
from cairndb.storage.base import (
    BlobStorage,
    StoredObject,
    commit_key,
    parse_commit_key,
    parse_snapshot_key,
    snapshot_key,
    snapshot_prefix,
)

_LOG_NAME_RE = re.compile(r"^[a-z0-9._-]+$")


def check_log_name(name: str) -> None:
    """Validate a log name (it becomes a key prefix segment)."""
    if not _LOG_NAME_RE.match(name):
        raise ValueError(f"invalid log name {name!r}: must match {_LOG_NAME_RE.pattern}")


class NamespacedStorage(BlobStorage):
    """A BlobStorage view whose commit log and snapshots live under a prefix.

    Commit and snapshot operations are rerouted through the base store's
    generic conditional-object API under ``{namespace}/``; the generic
    object API itself is delegated unprefixed, so coordination documents
    stay global.
    """

    def __init__(self, base: BlobStorage, namespace: str):
        self.base = base
        self.namespace = namespace.rstrip("/")

    def _key(self, relative: str) -> str:
        return f"{self.namespace}/{relative}"

    # -- commit log ------------------------------------------------------

    async def put_commit(self, number: int, data: bytes) -> bool:
        etag = await self.base.put_object(self._key(commit_key(number)), data, if_absent=True)
        return etag is not None

    async def get_commit(self, number: int) -> bytes | None:
        obj = await self.base.get_object(self._key(commit_key(number)))
        return obj.data if obj else None

    async def list_commits(self, after: int = 0) -> list[int]:
        keys = await self.base.list_objects(self._key("log/"))
        numbers = [n for k in keys if (n := parse_commit_key(k)) is not None and n > after]
        return sorted(numbers)

    async def delete_commits_before(self, number: int) -> int:
        deleted = 0
        for k in await self.base.list_objects(self._key("log/")):
            n = parse_commit_key(k)
            if n is not None and n < number:
                await self.base.delete_object(k)
                deleted += 1
        return deleted

    # -- snapshots ---------------------------------------------------------

    async def put_snapshot(self, schema: str, number: int, data: bytes) -> bool:
        etag = await self.base.put_object(
            self._key(snapshot_key(schema, number)), data, if_absent=True
        )
        return etag is not None

    async def list_snapshots(self, schema: str) -> list[int]:
        keys = await self.base.list_objects(self._key(snapshot_prefix(schema)))
        numbers = [n for k in keys if (n := parse_snapshot_key(k)) is not None]
        return sorted(numbers)

    async def get_snapshot(self, schema: str, number: int) -> bytes:
        obj = await self.base.get_object(self._key(snapshot_key(schema, number)))
        if obj is None:
            from cairndb.core.exceptions import StorageError

            raise StorageError(f"Snapshot v{schema}/{number} not found under {self.namespace}/")
        return obj.data

    async def delete_snapshots_before(self, schema: str, number: int) -> int:
        deleted = 0
        for k in await self.base.list_objects(self._key(snapshot_prefix(schema))):
            n = parse_snapshot_key(k)
            if n is not None and n < number:
                await self.base.delete_object(k)
                deleted += 1
        return deleted

    # -- generic object API: delegated unprefixed --------------------------

    def get_object_sync(self, key: str) -> StoredObject | None:
        return self.base.get_object_sync(key)

    def put_object_sync(
        self,
        key: str,
        data: bytes,
        *,
        if_match: str | None = None,
        if_absent: bool = False,
    ) -> str | None:
        return self.base.put_object_sync(key, data, if_match=if_match, if_absent=if_absent)

    def delete_object_sync(self, key: str) -> None:
        self.base.delete_object_sync(key)

    def list_objects_sync(self, prefix: str = "") -> list[str]:
        return self.base.list_objects_sync(prefix)


def log_storage(base: BlobStorage, name: str | None) -> BlobStorage:
    """The storage view holding log `name`'s commits and snapshots.

    The root log (name None) is the base storage itself; a named log is the
    base namespaced under ``logs/{name}/``. Commit/snapshot consumers — the
    Committer, projections, the snapshot and GC jobs — operate on the log
    this view selects.
    """
    if name is None:
        return base
    check_log_name(name)
    return NamespacedStorage(base, f"logs/{name}")


class Log:
    """A named (or the root) commit log: append, read, tail.

    The write path is the standard Committer (group commit, durable ack);
    it is created lazily on first append and closed by :meth:`close`.
    """

    def __init__(
        self,
        storage: BlobStorage,
        name: str | None = None,
        *,
        committer_config: CommitterConfig | None = None,
        revalidate: RevalidateHook | None = None,
    ):
        self.name = name
        self.storage = log_storage(storage, name)
        self._committer_config = committer_config
        self._revalidate = revalidate
        self._committer: Committer | None = None

    @property
    def committer(self) -> Committer:
        """The log's committer, created on first use."""
        if self._committer is None:
            self._committer = Committer(
                self.storage, self._committer_config, revalidate=self._revalidate
            )
        return self._committer

    # -- write -------------------------------------------------------------

    async def append(self, event: Event) -> SequenceNumber:
        """Append one event; durable once returned."""
        return await self.committer.append(event)

    async def append_many(self, events: list[Event]) -> list[SequenceNumber]:
        """Append several events, preserving relative order."""
        return await self.committer.append_many(events)

    # -- read ----------------------------------------------------------------

    async def read_commits(
        self, after: int = 0, end_at: int | None = None
    ) -> AsyncIterator[Commit]:
        """Yield commits > `after` in order until a gap (the tail)."""
        number = after + 1
        while end_at is None or number <= end_at:
            data = await self.storage.get_commit(number)
            if data is None:
                return
            yield Commit.from_msgpack(data)
            number += 1

    async def read(
        self, after: int = 0, end_at: int | None = None
    ) -> AsyncIterator[SequencedEvent]:
        """Yield sequenced events from commits > `after` until the tail."""
        async for commit in self.read_commits(after, end_at):
            for sequenced in commit.sequenced_events():
                yield sequenced

    async def tail(
        self, after: int = 0, *, poll_interval: float = 1.0
    ) -> AsyncIterator[SequencedEvent]:
        """
        Follow the log forever: yield events as commits land.

        Steady state costs one GET per interval (404 = idle). The caller
        stops by breaking out of the iteration.
        """
        number = after
        while True:
            got_any = False  # pragma: no mutate (only read for truthiness)
            async for commit in self.read_commits(number):
                number = commit.number
                got_any = True
                for sequenced in commit.sequenced_events():
                    yield sequenced
            if not got_any:
                await asyncio.sleep(poll_interval)

    async def current_tail(self) -> int:
        """Highest existing commit number (0 = empty log). One LIST."""
        commits = await self.storage.list_commits(after=0)
        return commits[-1] if commits else 0

    async def close(self) -> None:
        """Drain and close the committer, if one was created."""
        if self._committer is not None:
            await self._committer.close()
            self._committer = None
