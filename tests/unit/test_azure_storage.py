"""Unit tests for Azure Blob Storage backend."""

import pytest
from unittest.mock import patch, MagicMock

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from cairndb.core.exceptions import StorageError


@pytest.fixture
def mock_container():
    with patch("cairndb.storage.azure.BlobServiceClient") as service_cls:
        service = MagicMock()
        service_cls.from_connection_string.return_value = service
        container = MagicMock()
        service.get_container_client.return_value = container
        yield container


@pytest.fixture
def storage(mock_container):
    from cairndb.storage.azure import AzureBlobStorage

    return AzureBlobStorage(
        container="test-container",
        prefix="my/prefix",
        connection_string="UseDevelopmentStorage=true",
    )


class TestInit:
    def test_requires_credentials(self):
        from cairndb.storage.azure import AzureBlobStorage

        with pytest.raises(StorageError):
            AzureBlobStorage(container="c")


class TestPutCommit:
    @pytest.mark.asyncio
    async def test_put_commit_does_not_overwrite(self, storage, mock_container):
        blob = MagicMock()
        mock_container.get_blob_client.return_value = blob

        assert await storage.put_commit(42, b"data") is True

        mock_container.get_blob_client.assert_called_once_with(
            "my/prefix/log/000000000042.msgpack"
        )
        blob.upload_blob.assert_called_once_with(b"data", overwrite=False)

    @pytest.mark.asyncio
    async def test_exists_means_lost_race(self, storage, mock_container):
        blob = MagicMock()
        blob.upload_blob.side_effect = ResourceExistsError("exists")
        mock_container.get_blob_client.return_value = blob

        assert await storage.put_commit(42, b"data") is False

    @pytest.mark.asyncio
    async def test_other_errors_raise_storage_error(self, storage, mock_container):
        blob = MagicMock()
        blob.upload_blob.side_effect = RuntimeError("boom")
        mock_container.get_blob_client.return_value = blob

        with pytest.raises(StorageError):
            await storage.put_commit(42, b"data")


class TestGetCommit:
    @pytest.mark.asyncio
    async def test_get_commit_returns_bytes(self, storage, mock_container):
        blob = MagicMock()
        blob.download_blob.return_value.readall.return_value = b"data"
        mock_container.get_blob_client.return_value = blob

        assert await storage.get_commit(42) == b"data"

    @pytest.mark.asyncio
    async def test_missing_commit_returns_none(self, storage, mock_container):
        blob = MagicMock()
        blob.download_blob.side_effect = ResourceNotFoundError("missing")
        mock_container.get_blob_client.return_value = blob

        assert await storage.get_commit(42) is None


class TestListCommits:
    @pytest.mark.asyncio
    async def test_list_parses_numbers_in_order(self, storage, mock_container):
        blobs = [MagicMock(), MagicMock(), MagicMock()]
        blobs[0].name = "my/prefix/log/000000000002.msgpack"
        blobs[1].name = "my/prefix/log/000000000001.msgpack"
        blobs[2].name = "my/prefix/log/junk.txt"
        mock_container.list_blobs.return_value = blobs

        assert await storage.list_commits() == [1, 2]
        mock_container.list_blobs.assert_called_once_with(
            name_starts_with="my/prefix/log/"
        )

    @pytest.mark.asyncio
    async def test_list_after_filters_client_side(self, storage, mock_container):
        blobs = []
        for n in range(1, 6):
            b = MagicMock()
            b.name = f"my/prefix/log/{n:012d}.msgpack"
            blobs.append(b)
        mock_container.list_blobs.return_value = blobs

        assert await storage.list_commits(after=3) == [4, 5]


class TestSnapshots:
    @pytest.mark.asyncio
    async def test_put_snapshot_is_conditional(self, storage, mock_container):
        blob = MagicMock()
        mock_container.get_blob_client.return_value = blob

        assert await storage.put_snapshot("1.0.0", 40, b"snap") is True

        mock_container.get_blob_client.assert_called_once_with(
            "my/prefix/snapshots/v1.0.0/000000000040.sqlite"
        )
        blob.upload_blob.assert_called_once_with(b"snap", overwrite=False)

    @pytest.mark.asyncio
    async def test_duplicate_snapshot_returns_false(self, storage, mock_container):
        blob = MagicMock()
        blob.upload_blob.side_effect = ResourceExistsError("exists")
        mock_container.get_blob_client.return_value = blob

        assert await storage.put_snapshot("1.0.0", 40, b"snap") is False

    @pytest.mark.asyncio
    async def test_find_latest_snapshot(self, storage, mock_container):
        blobs = [MagicMock(), MagicMock()]
        blobs[0].name = "my/prefix/snapshots/v1.0.0/000000000010.sqlite"
        blobs[1].name = "my/prefix/snapshots/v1.0.0/000000000040.sqlite"
        mock_container.list_blobs.return_value = blobs

        assert await storage.find_latest_snapshot("1.0.0") == 40

    @pytest.mark.asyncio
    async def test_get_missing_snapshot_raises(self, storage, mock_container):
        blob = MagicMock()
        blob.download_blob.side_effect = ResourceNotFoundError("missing")
        mock_container.get_blob_client.return_value = blob

        with pytest.raises(StorageError):
            await storage.get_snapshot("1.0.0", 40)


class TestDeletion:
    @pytest.mark.asyncio
    async def test_delete_commits_before(self, storage, mock_container):
        blobs = []
        for n in range(1, 6):
            b = MagicMock()
            b.name = f"my/prefix/log/{n:012d}.msgpack"
            blobs.append(b)
        mock_container.list_blobs.return_value = blobs

        deleted = await storage.delete_commits_before(4)

        assert deleted == 3
        deleted_keys = [c.args[0] for c in mock_container.delete_blob.call_args_list]
        assert deleted_keys == [
            "my/prefix/log/000000000001.msgpack",
            "my/prefix/log/000000000002.msgpack",
            "my/prefix/log/000000000003.msgpack",
        ]
