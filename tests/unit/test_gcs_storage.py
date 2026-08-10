"""Unit tests for GCS storage backend."""

from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import NotFound as GCSNotFound
from google.api_core.exceptions import PreconditionFailed as GCSPreconditionFailed

from cairndb.core.exceptions import StorageError


@pytest.fixture
def mock_bucket():
    with patch("cairndb.storage.gcs.gcs.Client") as factory:
        client = MagicMock()
        factory.return_value = client
        bucket = MagicMock()
        client.bucket.return_value = bucket
        yield bucket


@pytest.fixture
def storage(mock_bucket):
    from cairndb.storage.gcs import GCSStorage

    return GCSStorage(bucket="test-bucket", prefix="my/prefix")


class TestPutCommit:
    @pytest.mark.asyncio
    async def test_put_commit_uses_generation_precondition(self, storage, mock_bucket):
        blob = MagicMock()
        mock_bucket.blob.return_value = blob

        assert await storage.put_commit(42, b"data") is True

        mock_bucket.blob.assert_called_once_with("my/prefix/log/000000000042.msgpack")
        blob.upload_from_string.assert_called_once_with(b"data", if_generation_match=0)

    @pytest.mark.asyncio
    async def test_precondition_failed_means_lost_race(self, storage, mock_bucket):
        blob = MagicMock()
        blob.upload_from_string.side_effect = GCSPreconditionFailed("exists")
        mock_bucket.blob.return_value = blob

        assert await storage.put_commit(42, b"data") is False

    @pytest.mark.asyncio
    async def test_other_errors_raise_storage_error(self, storage, mock_bucket):
        blob = MagicMock()
        blob.upload_from_string.side_effect = RuntimeError("boom")
        mock_bucket.blob.return_value = blob

        with pytest.raises(StorageError):
            await storage.put_commit(42, b"data")


class TestGetCommit:
    @pytest.mark.asyncio
    async def test_get_commit_returns_bytes(self, storage, mock_bucket):
        blob = MagicMock()
        blob.download_as_bytes.return_value = b"data"
        mock_bucket.blob.return_value = blob

        assert await storage.get_commit(42) == b"data"

    @pytest.mark.asyncio
    async def test_missing_commit_returns_none(self, storage, mock_bucket):
        blob = MagicMock()
        blob.download_as_bytes.side_effect = GCSNotFound("missing")
        mock_bucket.blob.return_value = blob

        assert await storage.get_commit(42) is None


class TestListCommits:
    @pytest.mark.asyncio
    async def test_list_parses_numbers_in_order(self, storage, mock_bucket):
        blobs = [MagicMock(), MagicMock(), MagicMock()]
        blobs[0].name = "my/prefix/log/000000000002.msgpack"
        blobs[1].name = "my/prefix/log/000000000001.msgpack"
        blobs[2].name = "my/prefix/log/junk.txt"
        mock_bucket.list_blobs.return_value = blobs

        assert await storage.list_commits() == [1, 2]

    @pytest.mark.asyncio
    async def test_list_after_uses_start_offset(self, storage, mock_bucket):
        mock_bucket.list_blobs.return_value = []

        await storage.list_commits(after=5)

        kwargs = mock_bucket.list_blobs.call_args.kwargs
        # start_offset is inclusive, so the implementation starts just past `after`
        assert kwargs["start_offset"] == "my/prefix/log/000000000005.msgpack\x00"


class TestSnapshots:
    @pytest.mark.asyncio
    async def test_put_snapshot_is_conditional(self, storage, mock_bucket):
        blob = MagicMock()
        mock_bucket.blob.return_value = blob

        assert await storage.put_snapshot("1.0.0", 40, b"snap") is True

        mock_bucket.blob.assert_called_once_with(
            "my/prefix/snapshots/v1.0.0/000000000040.sqlite"
        )
        blob.upload_from_string.assert_called_once_with(b"snap", if_generation_match=0)

    @pytest.mark.asyncio
    async def test_duplicate_snapshot_returns_false(self, storage, mock_bucket):
        blob = MagicMock()
        blob.upload_from_string.side_effect = GCSPreconditionFailed("exists")
        mock_bucket.blob.return_value = blob

        assert await storage.put_snapshot("1.0.0", 40, b"snap") is False

    @pytest.mark.asyncio
    async def test_find_latest_snapshot(self, storage, mock_bucket):
        blobs = [MagicMock(), MagicMock()]
        blobs[0].name = "my/prefix/snapshots/v1.0.0/000000000010.sqlite"
        blobs[1].name = "my/prefix/snapshots/v1.0.0/000000000040.sqlite"
        mock_bucket.list_blobs.return_value = blobs

        assert await storage.find_latest_snapshot("1.0.0") == 40

    @pytest.mark.asyncio
    async def test_get_missing_snapshot_raises(self, storage, mock_bucket):
        blob = MagicMock()
        blob.download_as_bytes.side_effect = GCSNotFound("missing")
        mock_bucket.blob.return_value = blob

        with pytest.raises(StorageError):
            await storage.get_snapshot("1.0.0", 40)


class TestDeletion:
    @pytest.mark.asyncio
    async def test_delete_commits_before(self, storage, mock_bucket):
        blobs = []
        for n in range(1, 6):
            b = MagicMock()
            b.name = f"my/prefix/log/{n:012d}.msgpack"
            blobs.append(b)
        mock_bucket.list_blobs.return_value = blobs

        deleted = await storage.delete_commits_before(4)

        assert deleted == 3
        deleted_keys = [c.args[0] for c in mock_bucket.blob.call_args_list]
        assert deleted_keys == [
            "my/prefix/log/000000000001.msgpack",
            "my/prefix/log/000000000002.msgpack",
            "my/prefix/log/000000000003.msgpack",
        ]
