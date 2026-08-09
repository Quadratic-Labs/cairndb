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
