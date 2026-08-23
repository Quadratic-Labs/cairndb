"""Filesystem-based blob storage implementation."""

import fcntl
import hashlib
import os
import threading
from pathlib import Path

import structlog

from cairndb.core.exceptions import StorageError
from cairndb.storage.base import (
    BlobStorage,
    StoredObject,
    commit_key,
    parse_commit_key,
    parse_snapshot_key,
    snapshot_key,
    snapshot_prefix,
)

logger = structlog.get_logger(__name__)


class FilesystemStorage(BlobStorage):
    """
    Local filesystem storage backend.

    Put-if-absent is emulated with write-to-temp + os.link(): the link is
    atomic and fails with EEXIST if the target exists, and readers can never
    observe a partially written object. This makes the backend a faithful
    race simulator for tests, in addition to being the dev backend.

    Storage layout mirrors the blob layout:
        {root_path}/log/{number:012d}.msgpack
        {root_path}/snapshots/v{schema}/{number:012d}.sqlite
    """

    def __init__(self, root_path: str | Path):
        """
        Initialize filesystem storage.

        Args:
            root_path: Root directory for all storage
        """
        self.root_path = Path(root_path).absolute()
        (self.root_path / "log").mkdir(parents=True, exist_ok=True)
        (self.root_path / "snapshots").mkdir(parents=True, exist_ok=True)  # pragma: no mutate

        logger.info("filesystem_storage_initialized", root_path=str(self.root_path))

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _put_if_absent(self, relative_key: str, data: bytes) -> bool:
        """Atomically create the file if absent; True if this call created it."""
        target = self.root_path / relative_key
        target.parent.mkdir(parents=True, exist_ok=True)  # pragma: no mutate

        # Unique temp name per process/thread so concurrent writers never collide
        tmp = target.with_name(
            f".{target.name}.tmp-{os.getpid()}-{threading.get_ident()}"
        )

        try:
            with open(tmp, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())

            try:
                os.link(tmp, target)
                return True
            except FileExistsError:
                return False

        except OSError as e:
            raise StorageError(f"Failed to write {relative_key}: {e}") from e
        finally:
            tmp.unlink(missing_ok=True)

    def _read(self, relative_key: str) -> bytes | None:
        path = self.root_path / relative_key
        try:
            return path.read_bytes()
        except FileNotFoundError:
            return None
        except OSError as e:
            raise StorageError(f"Failed to read {relative_key}: {e}") from e

    # ------------------------------------------------------------------
    # BlobStorage interface
    # ------------------------------------------------------------------

    async def put_commit(self, number: int, data: bytes) -> bool:
        created = self._put_if_absent(commit_key(number), data)
        if created:
            logger.info("commit_written", number=number, size_bytes=len(data))
        else:
            logger.debug("commit_lost_race", number=number)
        return created

    async def get_commit(self, number: int) -> bytes | None:
        return self._read(commit_key(number))

    async def list_commits(self, after: int = 0) -> list[int]:
        log_dir = self.root_path / "log"
        numbers = sorted(
            n
            for entry in log_dir.iterdir()
            if (n := parse_commit_key(entry.name)) is not None and n > after
        )
        logger.debug("commits_listed", after=after, count=len(numbers))
        return numbers

    async def put_snapshot(self, schema: str, number: int, data: bytes) -> bool:
        created = self._put_if_absent(snapshot_key(schema, number), data)
        logger.info(
            "snapshot_written" if created else "snapshot_already_exists",
            schema=schema,
            number=number,
            size_bytes=len(data),
        )
        return created

    async def list_snapshots(self, schema: str) -> list[int]:
        snap_dir = self.root_path / snapshot_prefix(schema)
        if not snap_dir.is_dir():
            return []
        return sorted(
            n
            for entry in snap_dir.iterdir()
            if (n := parse_snapshot_key(entry.name)) is not None
        )

    async def get_snapshot(self, schema: str, number: int) -> bytes:
        data = self._read(snapshot_key(schema, number))
        if data is None:
            raise StorageError(f"Snapshot not found: schema v{schema}, commit {number}")
        return data

    async def delete_commits_before(self, number: int) -> int:
        log_dir = self.root_path / "log"
        deleted = 0
        for entry in log_dir.iterdir():
            n = parse_commit_key(entry.name)
            if n is not None and n < number:
                entry.unlink(missing_ok=True)
                deleted += 1
        logger.info("commits_deleted", before=number, count=deleted)
        return deleted

    async def delete_snapshots_before(self, schema: str, number: int) -> int:
        snap_dir = self.root_path / snapshot_prefix(schema)
        if not snap_dir.is_dir():
            return 0
        deleted = 0
        for entry in snap_dir.iterdir():
            n = parse_snapshot_key(entry.name)
            if n is not None and n < number:
                entry.unlink(missing_ok=True)
                deleted += 1
        logger.info("snapshots_deleted", schema=schema, before=number, count=deleted)
        return deleted

    # ------------------------------------------------------------------
    # Generic conditional object API
    #
    # Etags are the MD5 of the content — the same semantics as S3 etags
    # for simple puts. Conditional writes and deletes are serialised by
    # an exclusive flock on a persistent sibling `.{name}.lock` file, so
    # the compare and the replace are atomic with respect to each other.
    # The kernel releases a flock when its holder dies, so a crashed
    # writer never leaves a stale lock. Lock files are never unlinked:
    # removing one while a writer waits on its descriptor would let two
    # writers hold "the" lock at once. They are content-less dot-files,
    # invisible to list_objects_sync. POSIX only (flock).
    # ------------------------------------------------------------------

    @staticmethod
    def _etag(data: bytes) -> str:
        return hashlib.md5(data).hexdigest()

    def _object_path(self, key: str) -> Path:
        target = (self.root_path / key).resolve()
        if not target.is_relative_to(self.root_path):
            raise StorageError(f"Object key escapes storage root: {key}")
        return target

    def _lock_path(self, target: Path) -> Path:
        return target.with_name(f".{target.name}.lock")

    def get_object_sync(self, key: str) -> StoredObject | None:
        target = self._object_path(key)
        try:
            data = target.read_bytes()
        except FileNotFoundError:
            return None
        except OSError as e:
            raise StorageError(f"Failed to read object {key}: {e}") from e
        return StoredObject(data=data, etag=self._etag(data))

    def put_object_sync(
        self,
        key: str,
        data: bytes,
        *,
        if_match: str | None = None,
        if_absent: bool = False,
    ) -> str | None:
        self._check_object_preconditions(if_match, if_absent)
        target = self._object_path(key)

        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self._lock_path(target), os.O_CREAT | os.O_RDWR)
        except OSError as e:
            raise StorageError(f"Failed to lock object {key}: {e}") from e

        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                try:
                    current: bytes | None = target.read_bytes()
                except FileNotFoundError:
                    current = None

                if if_absent and current is not None:
                    return None
                if if_match is not None and (
                    current is None or self._etag(current) != if_match
                ):
                    return None

                tmp = target.with_name(
                    f".{target.name}.tmp-{os.getpid()}-{threading.get_ident()}"
                )
                with open(tmp, "wb") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, target)
                return self._etag(data)
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError as e:
            raise StorageError(f"Failed to write object {key}: {e}") from e
        finally:
            os.close(fd)

    def append_object_sync(self, key: str, data: bytes) -> bool:
        """True append: O(len(data)) under the same lock CAS writers hold."""
        target = self._object_path(key)

        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self._lock_path(target), os.O_CREAT | os.O_RDWR)
        except OSError as e:
            raise StorageError(f"Failed to lock object {key}: {e}") from e

        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                with open(target, "ab") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                return True
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError as e:
            raise StorageError(f"Failed to append to object {key}: {e}") from e
        finally:
            os.close(fd)

    def delete_object_sync(self, key: str) -> None:
        target = self._object_path(key)
        if not target.parent.is_dir():
            return
        try:
            fd = os.open(self._lock_path(target), os.O_CREAT | os.O_RDWR)
        except OSError as e:
            raise StorageError(f"Failed to lock object {key}: {e}") from e
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                target.unlink(missing_ok=True)
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError as e:
            raise StorageError(f"Failed to delete object {key}: {e}") from e
        finally:
            os.close(fd)

    def list_objects_sync(self, prefix: str = "") -> list[str]:
        if prefix:
            # Narrow the walk to the deepest directory the prefix implies.
            dir_part = prefix.rsplit("/", 1)[0] if "/" in prefix else ""
            base = self._object_path(dir_part) if dir_part else self.root_path
        else:
            base = self.root_path
        if not base.is_dir():
            return []

        keys = []
        for entry in base.rglob("*"):
            if not entry.is_file():
                continue
            rel = entry.relative_to(self.root_path).as_posix()
            # Lock and temp files are dot-files; keys never contain them.
            if any(part.startswith(".") for part in rel.split("/")):
                continue
            if rel.startswith(prefix):
                keys.append(rel)
        return sorted(keys)
