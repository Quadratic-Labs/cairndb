"""Unit tests for Azure Blob Storage backend."""

from unittest.mock import MagicMock, patch

import pytest
from azure.core import MatchConditions
from azure.core.exceptions import (
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)

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

    def test_prefix_strips_trailing_slash_only(self):
        with patch("cairndb.storage.azure.BlobServiceClient"):
            from cairndb.storage.azure import AzureBlobStorage

            s = AzureBlobStorage(container="c", prefix="pX/", connection_string="cs")
            assert s._prefix == "pX"

    def test_connection_string_and_container_are_wired(self):
        with patch("cairndb.storage.azure.BlobServiceClient") as service_cls:
            from cairndb.storage.azure import AzureBlobStorage

            AzureBlobStorage(container="tank", connection_string="cs")
            service_cls.from_connection_string.assert_called_once_with("cs")
            service = service_cls.from_connection_string.return_value
            service.get_container_client.assert_called_once_with("tank")

    def test_account_url_uses_default_azure_credential(self):
        with (
            patch("cairndb.storage.azure.BlobServiceClient") as service_cls,
            patch("azure.identity.DefaultAzureCredential") as cred_cls,
        ):
            from cairndb.storage.azure import AzureBlobStorage

            AzureBlobStorage(container="c", account_url="https://acct.blob.core.windows.net")
            service_cls.assert_called_once_with(
                account_url="https://acct.blob.core.windows.net",
                credential=cred_cls.return_value,
            )

    @pytest.mark.asyncio
    async def test_default_prefix_is_empty(self, mock_container):
        from cairndb.storage.azure import AzureBlobStorage

        bare = AzureBlobStorage(container="c", connection_string="cs")
        blob = MagicMock()
        mock_container.get_blob_client.return_value = blob
        await bare.put_commit(1, b"d")
        mock_container.get_blob_client.assert_called_once_with("log/000000000001.msgpack")


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
        mock_container.get_blob_client.assert_called_once_with(
            "my/prefix/log/000000000042.msgpack"
        )

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
        mock_container.list_blobs.assert_called_once_with(
            name_starts_with="my/prefix/snapshots/v1.0.0/"
        )

    @pytest.mark.asyncio
    async def test_get_snapshot_returns_bytes(self, storage, mock_container):
        blob = MagicMock()
        blob.download_blob.return_value.readall.return_value = b"snap"
        mock_container.get_blob_client.return_value = blob

        assert await storage.get_snapshot("1.0.0", 40) == b"snap"
        mock_container.get_blob_client.assert_called_once_with(
            "my/prefix/snapshots/v1.0.0/000000000040.sqlite"
        )

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
        mock_container.list_blobs.assert_called_once_with(name_starts_with="my/prefix/log/")
        deleted_keys = [c.args[0] for c in mock_container.delete_blob.call_args_list]
        assert deleted_keys == [
            "my/prefix/log/000000000001.msgpack",
            "my/prefix/log/000000000002.msgpack",
            "my/prefix/log/000000000003.msgpack",
        ]

    @pytest.mark.asyncio
    async def test_delete_snapshots_before(self, storage, mock_container):
        blobs = []
        for n in (1, 2, 3, 4):
            b = MagicMock()
            b.name = f"my/prefix/snapshots/v1.0.0/{n:012d}.sqlite"
            blobs.append(b)
        junk = MagicMock()
        junk.name = "my/prefix/snapshots/v1.0.0/notes.txt"
        blobs.append(junk)
        mock_container.list_blobs.return_value = blobs

        deleted = await storage.delete_snapshots_before("1.0.0", 4)

        assert deleted == 3
        mock_container.list_blobs.assert_called_once_with(
            name_starts_with="my/prefix/snapshots/v1.0.0/"
        )
        deleted_keys = [c.args[0] for c in mock_container.delete_blob.call_args_list]
        assert deleted_keys == [
            f"my/prefix/snapshots/v1.0.0/{n:012d}.sqlite" for n in (1, 2, 3)
        ]


class TestConditionalObjects:
    def test_get_object_returns_data_and_etag(self, storage, mock_container):
        blob = MagicMock()
        downloader = blob.download_blob.return_value
        downloader.readall.return_value = b"v"
        downloader.properties.etag = '"abc"'
        mock_container.get_blob_client.return_value = blob

        obj = storage.get_object_sync("state/x")
        assert obj.data == b"v"
        assert obj.etag == '"abc"'
        mock_container.get_blob_client.assert_called_once_with("my/prefix/state/x")

    def test_get_missing_object_returns_none(self, storage, mock_container):
        blob = MagicMock()
        blob.download_blob.side_effect = ResourceNotFoundError("missing")
        mock_container.get_blob_client.return_value = blob
        assert storage.get_object_sync("state/x") is None

    def test_put_unconditional(self, storage, mock_container):
        blob = MagicMock()
        blob.upload_blob.return_value = {"etag": '"e1"'}
        mock_container.get_blob_client.return_value = blob

        assert storage.put_object_sync("state/x", b"v") == '"e1"'
        mock_container.get_blob_client.assert_called_once_with("my/prefix/state/x")
        blob.upload_blob.assert_called_once_with(b"v", overwrite=True)

    def test_put_if_match_maps_to_if_not_modified(self, storage, mock_container):
        blob = MagicMock()
        blob.upload_blob.return_value = {"etag": '"e2"'}
        mock_container.get_blob_client.return_value = blob

        assert storage.put_object_sync("state/x", b"v", if_match='"e1"') == '"e2"'
        blob.upload_blob.assert_called_once_with(
            b"v",
            overwrite=True,
            etag='"e1"',
            match_condition=MatchConditions.IfNotModified,
        )

    def test_put_if_absent_maps_to_no_overwrite(self, storage, mock_container):
        blob = MagicMock()
        blob.upload_blob.return_value = {"etag": '"e1"'}
        mock_container.get_blob_client.return_value = blob

        assert storage.put_object_sync("state/x", b"v", if_absent=True) == '"e1"'
        blob.upload_blob.assert_called_once_with(b"v", overwrite=False)

    def test_put_precondition_failures_return_none(self, storage, mock_container):
        blob = MagicMock()
        mock_container.get_blob_client.return_value = blob

        blob.upload_blob.side_effect = ResourceExistsError("exists")
        assert storage.put_object_sync("s/x", b"v", if_absent=True) is None

        blob.upload_blob.side_effect = ResourceModifiedError("modified")
        assert storage.put_object_sync("s/x", b"v", if_match='"e"') is None

        # if_match on a blob deleted in the meantime: precondition failed.
        blob.upload_blob.side_effect = ResourceNotFoundError("gone")
        assert storage.put_object_sync("s/x", b"v", if_match='"e"') is None

    def test_put_both_preconditions_rejected(self, storage):
        with pytest.raises(ValueError):
            storage.put_object_sync("s/x", b"v", if_match='"e"', if_absent=True)

    def test_delete_object_and_missing_noop(self, storage, mock_container):
        blob = MagicMock()
        mock_container.get_blob_client.return_value = blob
        storage.delete_object_sync("state/x")
        mock_container.get_blob_client.assert_called_once_with("my/prefix/state/x")
        blob.delete_blob.assert_called_once_with()

        blob.delete_blob.side_effect = ResourceNotFoundError("missing")
        storage.delete_object_sync("state/x")  # no-op

    def test_list_objects_strips_prefix_and_directory_stubs(self, storage, mock_container):
        real = MagicMock()
        real.name = "my/prefix/state/x"
        real.metadata = None
        other = MagicMock()
        other.name = "my/prefix/config/a"
        other.metadata = {}
        # ADLS Gen2 (hierarchical namespace) directory stub: not a key.
        stub = MagicMock()
        stub.name = "my/prefix/state"
        stub.metadata = {"hdi_isfolder": "true"}
        mock_container.list_blobs.return_value = [real, stub, other]

        assert storage.list_objects_sync() == ["config/a", "state/x"]
        mock_container.list_blobs.assert_called_once_with(
            name_starts_with="my/prefix/", include=["metadata"]
        )
