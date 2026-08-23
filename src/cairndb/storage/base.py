"""Abstract blob storage interface.

The storage layer exposes the commit log and snapshot store as immutable,
conditionally-written objects:

    log/{number:012d}.msgpack            one object per commit
    snapshots/v{schema}/{number:012d}.sqlite

put_commit is a put-if-absent: the bucket itself arbitrates which writer
wins a commit number. This is the only concurrency primitive in the log.

The generic object API below exposes the same conditional-write machinery
for key-addressed objects, so consumers (e.g. an orchestrator's control
plane) can keep etag-guarded mutable documents in the same bucket.
"""

import asyncio
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass

COMMIT_WIDTH = 12

_COMMIT_KEY_RE = re.compile(r"(\d{" + str(COMMIT_WIDTH) + r"})\.msgpack$")
_SNAPSHOT_KEY_RE = re.compile(r"(\d{" + str(COMMIT_WIDTH) + r"})\.sqlite$")


def commit_key(number: int) -> str:
    """Relative key of a commit object."""
    return f"log/{number:0{COMMIT_WIDTH}d}.msgpack"


def snapshot_key(schema: str, number: int) -> str:
    """Relative key of a snapshot object."""
    return f"{snapshot_prefix(schema)}{number:0{COMMIT_WIDTH}d}.sqlite"


def snapshot_prefix(schema: str) -> str:
    """Relative key prefix of all snapshots for a projection schema version."""
    return f"snapshots/v{schema}/"


def parse_commit_key(key: str) -> int | None:
    """Extract the commit number from a commit object key, or None."""
    match = _COMMIT_KEY_RE.search(key)
    return int(match.group(1)) if match else None


def parse_snapshot_key(key: str) -> int | None:
    """Extract the commit number from a snapshot object key, or None."""
    match = _SNAPSHOT_KEY_RE.search(key)
    return int(match.group(1)) if match else None


@dataclass(frozen=True, slots=True)
class StoredObject:
    """An object's content together with the etag observed at read time.

    The etag is an opaque, backend-specific token; it is only meaningful
    when passed back to the same store as the ``if_match`` precondition
    of :meth:`BlobStorage.put_object`.
    """

    data: bytes
    etag: str


class BlobStorage(ABC):
    """
    Abstract interface for blob storage backends.

    All backends (filesystem, S3, GCS, Azure) implement this interface.
    In the log/snapshot API every write is either a put-if-absent of an
    immutable object or a delete (garbage collection); nothing is ever
    overwritten. The generic object API additionally supports mutable,
    etag-guarded documents (compare-and-swap replacement).
    """

    @abstractmethod
    async def put_commit(self, number: int, data: bytes) -> bool:
        """
        Write commit object `number` if and only if it does not exist.

        Returns:
            True if this call created the object (the writer won the race);
            False if the object already exists (lost the race).

        Raises:
            StorageError: On any failure other than losing the race.
        """

    @abstractmethod
    async def get_commit(self, number: int) -> bytes | None:
        """
        Read commit object `number`.

        Returns:
            The serialized commit, or None if it does not exist.

        Raises:
            StorageError: On any failure other than absence.
        """

    @abstractmethod
    async def list_commits(self, after: int = 0) -> list[int]:
        """
        List commit numbers greater than `after`, ascending.

        Used for bootstrap/resync; steady-state polling should use
        get_commit(last + 1) instead.
        """

    @abstractmethod
    async def put_snapshot(self, schema: str, number: int, data: bytes) -> bool:
        """
        Write the snapshot for `schema` at commit `number` if absent.

        Returns:
            True if created; False if it already exists (an identical
            snapshot — replay is deterministic — so False is success too).
        """

    @abstractmethod
    async def list_snapshots(self, schema: str) -> list[int]:
        """List snapshot commit numbers for `schema`, ascending."""

    async def find_latest_snapshot(self, schema: str) -> int | None:
        """Return the highest snapshot commit number for `schema`, or None."""
        numbers = await self.list_snapshots(schema)
        return numbers[-1] if numbers else None

    @abstractmethod
    async def get_snapshot(self, schema: str, number: int) -> bytes:
        """
        Read the snapshot for `schema` at commit `number`.

        Raises:
            StorageError: If the snapshot does not exist or the read fails.
        """

    @abstractmethod
    async def delete_commits_before(self, number: int) -> int:
        """Delete commit objects with number < `number`. Returns count deleted."""

    @abstractmethod
    async def delete_snapshots_before(self, schema: str, number: int) -> int:
        """Delete snapshots for `schema` with number < `number`. Returns count deleted."""

    # ------------------------------------------------------------------
    # Generic conditional object API
    #
    # Key-addressed objects with etag-based optimistic concurrency, for
    # consumers that need mutable, conditionally-replaced documents next
    # to the immutable log (e.g. a control plane's lease/state files).
    #
    # Keys are relative to the store root. The `log/` and `snapshots/`
    # prefixes are reserved for CairnDB's own commit log and snapshot
    # store; generic objects must live under other prefixes.
    #
    # The *_sync methods are the primitive operations (the cloud SDK
    # calls are synchronous underneath); the async methods wrap them in
    # a worker thread, mirroring how the log/snapshot API is built.
    # Synchronous consumers may call the *_sync methods directly.
    # ------------------------------------------------------------------

    @abstractmethod
    def get_object_sync(self, key: str) -> StoredObject | None:
        """
        Read object `key` together with its current etag.

        Returns:
            A StoredObject, or None if the object does not exist.

        Raises:
            StorageError: On any failure other than absence.
        """

    @abstractmethod
    def put_object_sync(
        self,
        key: str,
        data: bytes,
        *,
        if_match: str | None = None,
        if_absent: bool = False,
    ) -> str | None:
        """
        Write object `key`, optionally guarded by a precondition.

        With no precondition the write is unconditional. With
        ``if_absent=True`` it succeeds only if the object does not exist
        (put-if-absent). With ``if_match=<etag>`` it succeeds only if the
        object still carries that etag (compare-and-swap); an object that
        has been deleted in the meantime also fails the precondition.

        Returns:
            The new etag on success; None if the precondition failed.

        Raises:
            ValueError: If both preconditions are given.
            StorageError: On any failure other than a failed precondition.
        """

    _APPEND_ATTEMPTS = 8

    def append_object_sync(self, key: str, data: bytes) -> bool:
        """
        Append `data` to object `key`, creating the object if absent.

        Atomic per call with respect to concurrent appends: each call's
        bytes land contiguously and none are lost, though ordering across
        concurrent appenders is unspecified. Intended for append-only
        streams (e.g. .jsonl observability logs); do not mix concurrent
        appends with unconditional ``put_object_sync`` rewrites of the
        same key — a full rewrite may discard a concurrent append.

        This default implementation is a compare-and-swap read-modify-write
        loop (O(object size) per append). Backends with a native append
        primitive override it with a true O(len(data)) append.

        Returns:
            True when the append landed; False when the CAS fallback
            exhausted its retries under contention.

        Raises:
            StorageError: On any failure other than contention.
        """
        for _ in range(self._APPEND_ATTEMPTS):
            obj = self.get_object_sync(key)
            if obj is None:
                if self.put_object_sync(key, data, if_absent=True) is not None:
                    return True
            elif (
                self.put_object_sync(key, obj.data + data, if_match=obj.etag)
                is not None
            ):
                return True
        return False

    @abstractmethod
    def delete_object_sync(self, key: str) -> None:
        """Delete object `key`. Deleting a missing object is a no-op."""

    @abstractmethod
    def list_objects_sync(self, prefix: str = "") -> list[str]:
        """List keys starting with `prefix`, ascending, relative to the store root."""

    async def get_object(self, key: str) -> StoredObject | None:
        """Async wrapper around :meth:`get_object_sync`."""
        return await asyncio.to_thread(self.get_object_sync, key)

    async def put_object(
        self,
        key: str,
        data: bytes,
        *,
        if_match: str | None = None,
        if_absent: bool = False,
    ) -> str | None:
        """Async wrapper around :meth:`put_object_sync`."""
        return await asyncio.to_thread(
            lambda: self.put_object_sync(key, data, if_match=if_match, if_absent=if_absent)
        )

    async def append_object(self, key: str, data: bytes) -> bool:
        """Async wrapper around :meth:`append_object_sync`."""
        return await asyncio.to_thread(self.append_object_sync, key, data)

    async def delete_object(self, key: str) -> None:
        """Async wrapper around :meth:`delete_object_sync`."""
        await asyncio.to_thread(self.delete_object_sync, key)

    async def list_objects(self, prefix: str = "") -> list[str]:
        """Async wrapper around :meth:`list_objects_sync`."""
        return await asyncio.to_thread(self.list_objects_sync, prefix)

    @staticmethod
    def _check_object_preconditions(if_match: str | None, if_absent: bool) -> None:
        """Reject contradictory put_object preconditions."""
        if if_match is not None and if_absent:
            raise ValueError("if_match and if_absent are mutually exclusive")
