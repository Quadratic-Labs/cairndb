"""Tests for filesystem storage backend."""

import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from cairndb.core.exceptions import StorageError
from cairndb.storage.filesystem import FilesystemStorage


@pytest.fixture
def temp_storage_dir():
    """Create temporary directory for storage tests."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def storage(temp_storage_dir):
    """Create filesystem storage instance."""
    return FilesystemStorage(temp_storage_dir)


class TestLayoutAndErrors:
    def test_init_creates_documented_layout(self, temp_storage_dir):
        FilesystemStorage(temp_storage_dir / "ledger")
        assert (temp_storage_dir / "ledger" / "log").is_dir()
        assert (temp_storage_dir / "ledger" / "snapshots").is_dir()

    @pytest.mark.asyncio
    async def test_write_failure_raises_storage_error(self, storage):
        """An OSError during the write surfaces as StorageError — the temp
        cleanup must not mask it (the temp file may not exist yet)."""
        (storage.root_path / "log").chmod(0o500)
        try:
            with pytest.raises(StorageError):
                await storage.put_commit(1, b"x")
        finally:
            (storage.root_path / "log").chmod(0o755)

    @pytest.mark.asyncio
    async def test_gc_tolerates_concurrent_deletion(self, storage, monkeypatch):
        """GC is documented as safe to run concurrently: a file vanishing
        between the directory listing and the unlink is not an error."""
        await storage.put_commit(1, b"a")
        await storage.put_commit(2, b"b")
        await storage.put_snapshot("1", 1, b"s")
        await storage.put_snapshot("1", 2, b"s")

        import os

        real_unlink = Path.unlink

        def racing_unlink(self, missing_ok=False):
            os.remove(self)  # a concurrent GC got there first
            return real_unlink(self, missing_ok=missing_ok)

        monkeypatch.setattr(Path, "unlink", racing_unlink)
        assert await storage.delete_commits_before(3) == 2
        assert await storage.delete_snapshots_before("1", 3) == 2

    @pytest.mark.asyncio
    async def test_delete_snapshots_of_unknown_schema_returns_zero(self, storage):
        assert await storage.delete_snapshots_before("9", 10) == 0

    def test_delete_of_never_written_key_is_noop(self, storage):
        storage.put_object_sync("a/x", b"v")
        storage.delete_object_sync("a/y")  # parent exists, key never locked
        assert storage.get_object_sync("a/x") is not None


class TestObjectListing:
    def test_partial_prefix_within_directory(self, storage):
        storage.put_object_sync("state/x1", b"1")
        storage.put_object_sync("state/y1", b"2")
        assert storage.list_objects_sync("state/x") == ["state/x1"]

    def test_prefix_without_slash_spans_directories(self, storage):
        storage.put_object_sync("state/x1", b"1")
        storage.put_object_sync("statexyz/f", b"2")
        assert storage.list_objects_sync("state") == ["state/x1", "statexyz/f"]

    def test_prefix_narrows_walk_to_deepest_directory(self, storage, monkeypatch):
        storage.put_object_sync("a/b/x", b"1")
        walked = []
        real_rglob = Path.rglob

        def spy_rglob(self, pattern):
            walked.append(self)
            return real_rglob(self, pattern)

        monkeypatch.setattr(Path, "rglob", spy_rglob)
        assert storage.list_objects_sync("a/b/x") == ["a/b/x"]
        assert walked == [storage.root_path / "a" / "b"]


class TestCommitLog:
    @pytest.mark.asyncio
    async def test_put_and_get_commit(self, storage):
        assert await storage.put_commit(1, b"commit-1") is True
        assert await storage.get_commit(1) == b"commit-1"

    @pytest.mark.asyncio
    async def test_get_missing_commit_returns_none(self, storage):
        assert await storage.get_commit(999) is None

    @pytest.mark.asyncio
    async def test_put_if_absent_loses_when_exists(self, storage):
        assert await storage.put_commit(1, b"winner") is True
        assert await storage.put_commit(1, b"loser") is False
        # The winner's content is untouched
        assert await storage.get_commit(1) == b"winner"

    @pytest.mark.asyncio
    async def test_list_commits_orders_and_filters(self, storage):
        for n in [3, 1, 2, 10]:
            await storage.put_commit(n, f"c{n}".encode())

        assert await storage.list_commits() == [1, 2, 3, 10]
        assert await storage.list_commits(after=2) == [3, 10]
        assert await storage.list_commits(after=10) == []

    @pytest.mark.asyncio
    async def test_delete_commits_before(self, storage):
        for n in range(1, 6):
            await storage.put_commit(n, b"x")

        deleted = await storage.delete_commits_before(4)

        assert deleted == 3
        assert await storage.list_commits() == [4, 5]

    def test_concurrent_writers_exactly_one_wins(self, storage):
        """The put-if-absent primitive under a real thread race."""
        n_writers = 16

        with ThreadPoolExecutor(max_workers=n_writers) as pool:
            results = list(
                pool.map(
                    lambda i: storage._put_if_absent("log/000000000001.msgpack", f"w{i}".encode()),
                    range(n_writers),
                )
            )

        assert results.count(True) == 1
        # Winner's content is intact and complete
        winner = results.index(True)
        content = (storage.root_path / "log/000000000001.msgpack").read_bytes()
        assert content == f"w{winner}".encode()


class TestSnapshots:
    @pytest.mark.asyncio
    async def test_put_find_get_snapshot(self, storage):
        assert await storage.put_snapshot("1.0.0", 40, b"snap-40") is True
        assert await storage.find_latest_snapshot("1.0.0") == 40
        assert await storage.get_snapshot("1.0.0", 40) == b"snap-40"

    @pytest.mark.asyncio
    async def test_find_latest_picks_highest(self, storage):
        for n in [10, 40, 25]:
            await storage.put_snapshot("1.0.0", n, b"x")

        assert await storage.find_latest_snapshot("1.0.0") == 40

    @pytest.mark.asyncio
    async def test_no_snapshots_returns_none(self, storage):
        assert await storage.find_latest_snapshot("1.0.0") is None

    @pytest.mark.asyncio
    async def test_schema_versions_are_isolated(self, storage):
        await storage.put_snapshot("1.0.0", 40, b"v1")
        await storage.put_snapshot("2.0.0", 10, b"v2")

        assert await storage.find_latest_snapshot("1.0.0") == 40
        assert await storage.find_latest_snapshot("2.0.0") == 10
        assert await storage.get_snapshot("2.0.0", 10) == b"v2"

    @pytest.mark.asyncio
    async def test_duplicate_snapshot_is_harmless(self, storage):
        assert await storage.put_snapshot("1.0.0", 40, b"snap") is True
        assert await storage.put_snapshot("1.0.0", 40, b"snap") is False
        assert await storage.get_snapshot("1.0.0", 40) == b"snap"

    @pytest.mark.asyncio
    async def test_get_missing_snapshot_raises(self, storage):
        with pytest.raises(StorageError):
            await storage.get_snapshot("1.0.0", 999)

    @pytest.mark.asyncio
    async def test_delete_snapshots_before(self, storage):
        for n in [10, 20, 30]:
            await storage.put_snapshot("1.0.0", n, b"x")

        deleted = await storage.delete_snapshots_before("1.0.0", 30)

        assert deleted == 2
        assert await storage.find_latest_snapshot("1.0.0") == 30
        with pytest.raises(StorageError):
            await storage.get_snapshot("1.0.0", 10)
