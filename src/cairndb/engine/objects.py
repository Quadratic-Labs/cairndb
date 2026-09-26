"""Layer 0 of the engine: the conditional key-value store.

A thin, namespaced facade over the BlobStorage generic object API, plus the
watch helper. Keys under the engine's reserved prefixes (the commit logs,
snapshots, and transaction markers) are rejected so application objects can
never collide with the engine's own state.
"""

import asyncio
import time

from cairndb.storage.base import BlobStorage, StoredObject

#: Prefixes owned by the engine; application objects must live elsewhere.
RESERVED_PREFIXES = ("log/", "snapshots/", "logs/", "txapplied/")


def check_key(key: str) -> None:
    """Reject empty keys and keys under an engine-reserved prefix."""
    if not key:
        raise ValueError("object key must be non-empty")
    for prefix in RESERVED_PREFIXES:
        if key.startswith(prefix):
            raise ValueError(f"key {key!r} is under the reserved prefix {prefix!r}")


class Objects:
    """Conditional key-addressed objects: get / put / delete / list / wait_for.

    Semantics are exactly those of the underlying store (see
    docs/concepts/storage-model.md): etags are opaque and backend-native;
    ``put`` with ``if_match`` is a compare-and-swap, with ``if_absent`` a
    put-if-absent, and returns None when the precondition failed.
    """

    def __init__(self, storage: BlobStorage):
        self.storage = storage

    # ------------------------------------------------------------------
    # Primitive (sync) form
    # ------------------------------------------------------------------

    def get_sync(self, key: str) -> StoredObject | None:
        """Read `key` with its current etag, or None if absent."""
        check_key(key)
        return self.storage.get_object_sync(key)

    def put_sync(
        self,
        key: str,
        data: bytes,
        *,
        if_match: str | None = None,
        if_absent: bool = False,
    ) -> str | None:
        """Write `key`; returns the new etag, or None if the precondition failed."""
        check_key(key)
        return self.storage.put_object_sync(key, data, if_match=if_match, if_absent=if_absent)

    def delete_sync(self, key: str) -> None:
        """Delete `key`; deleting a missing object is a no-op."""
        check_key(key)
        self.storage.delete_object_sync(key)

    def list_sync(self, prefix: str = "") -> list[str]:
        """List keys under `prefix`, ascending."""
        return self.storage.list_objects_sync(prefix)

    # ------------------------------------------------------------------
    # Async form
    # ------------------------------------------------------------------

    async def get(self, key: str) -> StoredObject | None:
        """Async twin of :meth:`get_sync`."""
        check_key(key)
        return await self.storage.get_object(key)

    async def put(
        self,
        key: str,
        data: bytes,
        *,
        if_match: str | None = None,
        if_absent: bool = False,
    ) -> str | None:
        """Async twin of :meth:`put_sync`."""
        check_key(key)
        return await self.storage.put_object(key, data, if_match=if_match, if_absent=if_absent)

    async def delete(self, key: str) -> None:
        """Async twin of :meth:`delete_sync`."""
        check_key(key)
        await self.storage.delete_object(key)

    async def list(self, prefix: str = "") -> list[str]:
        """Async twin of :meth:`list_sync`."""
        return await self.storage.list_objects(prefix)

    # ------------------------------------------------------------------
    # Watch
    # ------------------------------------------------------------------

    async def wait_for(
        self,
        key: str,
        *,
        timeout: float = 60.0,
        poll_interval: float = 1.0,
        changed_from: str | None = None,
    ) -> StoredObject:
        """
        Poll until `key` exists (default) or no longer carries `changed_from`.

        One GET per interval; polling is the correctness mechanism, there is
        no push channel to miss.

        Args:
            key: Object key to watch
            timeout: Maximum seconds to wait
            poll_interval: Seconds between polls
            changed_from: If given, wait until the object's etag differs
                from this value (an object that has been deleted also counts
                as changed and returns on its next reappearance)

        Returns:
            The observed object.

        Raises:
            TimeoutError: If the condition is not met within `timeout`.
        """
        check_key(key)
        deadline = time.monotonic() + timeout
        while True:
            obj = await self.storage.get_object(key)
            if obj is not None and (changed_from is None or obj.etag != changed_from):
                return obj
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Object {key!r} did not "
                    + ("appear" if changed_from is None else "change")
                    + f" within {timeout}s"
                )
            await asyncio.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
