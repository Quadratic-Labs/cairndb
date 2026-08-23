"""Unit tests for the generic conditional object API on all backends."""

import threading
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest
from azure.core import MatchConditions
from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)
from google.api_core.exceptions import NotFound as GCSNotFound
from google.api_core.exceptions import PreconditionFailed as GCSPreconditionFailed

from cairndb.core.exceptions import StorageError
from cairndb.storage import StoredObject
from cairndb.storage.base import BlobStorage
from cairndb.storage.filesystem import FilesystemStorage

# ---------------------------------------------------------------------------
# Filesystem — the real-semantics backend
# ---------------------------------------------------------------------------


@pytest.fixture
def fs(tmp_path):
    return FilesystemStorage(tmp_path)


class TestFilesystemObjects:
    def test_get_missing_returns_none(self, fs):
        assert fs.get_object_sync("state/flow/run.json") is None

    def test_put_get_roundtrip(self, fs):
        etag = fs.put_object_sync("state/flow/run.json", b"v1")
        obj = fs.get_object_sync("state/flow/run.json")
        assert obj == StoredObject(data=b"v1", etag=etag)

    def test_unconditional_put_overwrites(self, fs):
        fs.put_object_sync("k", b"v1")
        etag2 = fs.put_object_sync("k", b"v2")
        assert fs.get_object_sync("k") == StoredObject(data=b"v2", etag=etag2)

    def test_if_absent_wins_only_once(self, fs):
        assert fs.put_object_sync("k", b"v1", if_absent=True) is not None
        assert fs.put_object_sync("k", b"v2", if_absent=True) is None
        assert fs.get_object_sync("k").data == b"v1"

    def test_if_match_success_and_stale(self, fs):
        etag1 = fs.put_object_sync("k", b"v1")
        etag2 = fs.put_object_sync("k", b"v2", if_match=etag1)
        assert etag2 is not None
        # The old etag is now stale
        assert fs.put_object_sync("k", b"v3", if_match=etag1) is None
        assert fs.get_object_sync("k").data == b"v2"

    def test_if_match_on_missing_object_fails(self, fs):
        assert fs.put_object_sync("k", b"v", if_match="deadbeef") is None

    def test_append_creates_when_absent(self, fs):
        assert fs.append_object_sync("s.jsonl", b"a\n") is True
        assert fs.get_object_sync("s.jsonl").data == b"a\n"

    def test_append_extends_existing(self, fs):
        fs.put_object_sync("s.jsonl", b"a\n")
        assert fs.append_object_sync("s.jsonl", b"b\n") is True
        obj = fs.get_object_sync("s.jsonl")
        assert obj.data == b"a\nb\n"
        # etag reflects the appended content (computed from data on read)
        assert obj.etag == fs._etag(b"a\nb\n")

    def test_concurrent_appends_lose_nothing(self, fs):
        def appender(line: bytes):
            for _ in range(20):
                assert fs.append_object_sync("s.jsonl", line)

        threads = [
            threading.Thread(target=appender, args=(f"{i}\n".encode(),))
            for i in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        lines = fs.get_object_sync("s.jsonl").data.decode().splitlines()
        assert len(lines) == 80
        for i in range(4):
            assert lines.count(str(i)) == 20


# ---------------------------------------------------------------------------
# Base append fallback — the compare-and-swap rewrite loop
# ---------------------------------------------------------------------------


class _MinimalStorage(FilesystemStorage):
    """A backend with no native append: exercises the base CAS fallback."""

    append_object_sync = BlobStorage.append_object_sync


class TestAppendFallback:
    @pytest.fixture
    def minimal(self, tmp_path):
        return _MinimalStorage(tmp_path)

    def test_fallback_creates_and_extends(self, minimal):
        assert minimal.append_object_sync("s.jsonl", b"a\n") is True
        assert minimal.append_object_sync("s.jsonl", b"b\n") is True
        assert minimal.get_object_sync("s.jsonl").data == b"a\nb\n"

    def test_fallback_retries_on_contention_then_gives_up(self, minimal):
        minimal.put_object_sync("s.jsonl", b"a\n")
        real_put = minimal.put_object_sync
        calls = {"n": 0}

        def contended_put(key, data, **kwargs):
            if kwargs.get("if_match") is not None:
                calls["n"] += 1
                real_put(key, b"someone else\n")  # invalidate the etag
                return None
            return real_put(key, data, **kwargs)

        minimal.put_object_sync = contended_put
        assert minimal.append_object_sync("s.jsonl", b"b\n") is False
        assert calls["n"] == minimal._APPEND_ATTEMPTS

    def test_contradictory_preconditions_raise(self, fs):
        with pytest.raises(ValueError):
            fs.put_object_sync("k", b"v", if_match="e", if_absent=True)

    def test_delete_is_idempotent(self, fs):
        fs.put_object_sync("k", b"v")
        fs.delete_object_sync("k")
        fs.delete_object_sync("k")
        assert fs.get_object_sync("k") is None

    def test_if_match_after_delete_fails(self, fs):
        etag = fs.put_object_sync("k", b"v")
        fs.delete_object_sync("k")
        assert fs.put_object_sync("k", b"v2", if_match=etag) is None

    def test_list_objects_filters_locks_and_temps(self, fs):
        fs.put_object_sync("state/flow/a.json", b"1")
        fs.put_object_sync("state/flow/b.json", b"2")
        fs.put_object_sync("other/c.json", b"3")
        assert fs.list_objects_sync("state/") == [
            "state/flow/a.json",
            "state/flow/b.json",
        ]
        assert fs.list_objects_sync() == [
            "other/c.json",
            "state/flow/a.json",
            "state/flow/b.json",
        ]

    def test_list_objects_missing_prefix_dir(self, fs):
        assert fs.list_objects_sync("nope/") == []

    def test_key_escaping_root_raises(self, fs):
        with pytest.raises(StorageError):
            fs.put_object_sync("../escape", b"v")

    def test_concurrent_cas_exactly_one_winner(self, fs):
        """N threads race a compare-and-swap from the same etag; one wins."""
        base_etag = fs.put_object_sync("k", b"base")
        results: list[str | None] = []
        barrier = threading.Barrier(8)

        def contender(i: int) -> None:
            barrier.wait()
            results.append(
                fs.put_object_sync("k", f"winner-{i}".encode(), if_match=base_etag)
            )

        threads = [threading.Thread(target=contender, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        winners = [r for r in results if r is not None]
        assert len(winners) == 1
        assert fs.get_object_sync("k").etag == winners[0]

    @pytest.mark.asyncio
    async def test_async_wrappers(self, fs):
        etag = await fs.put_object(
            "k", b"v1", if_absent=True
        )
        obj = await fs.get_object("k")
        assert obj.data == b"v1"
        assert await fs.put_object("k", b"v2", if_match=etag) is not None
        assert await fs.list_objects() == ["k"]
        await fs.delete_object("k")
        assert await fs.get_object("k") is None


# ---------------------------------------------------------------------------
# Azure — mocked SDK
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_container():
    with patch("cairndb.storage.azure.BlobServiceClient") as service_cls:
        service = MagicMock()
        service_cls.from_connection_string.return_value = service
        container = MagicMock()
        service.get_container_client.return_value = container
        yield container


@pytest.fixture
def azure_storage(mock_container):
    from cairndb.storage.azure import AzureBlobStorage

    return AzureBlobStorage(
        container="test-container",
        prefix="my/prefix",
        connection_string="UseDevelopmentStorage=true",
    )


class TestAzureObjects:
    def test_get_returns_data_and_etag(self, azure_storage, mock_container):
        blob = MagicMock()
        downloader = blob.download_blob.return_value
        downloader.readall.return_value = b"data"
        downloader.properties.etag = '"0xETAG"'
        mock_container.get_blob_client.return_value = blob

        obj = azure_storage.get_object_sync("state/run.json")

        assert obj == StoredObject(data=b"data", etag='"0xETAG"')
        mock_container.get_blob_client.assert_called_once_with(
            "my/prefix/state/run.json"
        )

    def test_get_missing_returns_none(self, azure_storage, mock_container):
        blob = MagicMock()
        blob.download_blob.side_effect = ResourceNotFoundError("missing")
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.get_object_sync("k") is None

    def test_put_if_match_sends_condition(self, azure_storage, mock_container):
        blob = MagicMock()
        blob.upload_blob.return_value = {"etag": '"0xNEW"'}
        mock_container.get_blob_client.return_value = blob

        etag = azure_storage.put_object_sync("k", b"v", if_match='"0xOLD"')

        assert etag == '"0xNEW"'
        blob.upload_blob.assert_called_once_with(
            b"v",
            overwrite=True,
            etag='"0xOLD"',
            match_condition=MatchConditions.IfNotModified,
        )

    def test_put_if_match_mismatch_returns_none(self, azure_storage, mock_container):
        blob = MagicMock()
        blob.upload_blob.side_effect = ResourceModifiedError("412")
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.put_object_sync("k", b"v", if_match='"0xOLD"') is None

    def test_put_if_match_on_deleted_blob_returns_none(
        self, azure_storage, mock_container
    ):
        blob = MagicMock()
        blob.upload_blob.side_effect = ResourceNotFoundError("404")
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.put_object_sync("k", b"v", if_match='"0xOLD"') is None

    def test_put_if_absent_maps_to_no_overwrite(self, azure_storage, mock_container):
        blob = MagicMock()
        blob.upload_blob.return_value = {"etag": '"0xNEW"'}
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.put_object_sync("k", b"v", if_absent=True) == '"0xNEW"'
        blob.upload_blob.assert_called_once_with(b"v", overwrite=False)

    def test_put_if_absent_conflict_returns_none(self, azure_storage, mock_container):
        blob = MagicMock()
        blob.upload_blob.side_effect = ResourceExistsError("exists")
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.put_object_sync("k", b"v", if_absent=True) is None

    def test_unconditional_put_overwrites(self, azure_storage, mock_container):
        blob = MagicMock()
        blob.upload_blob.return_value = {"etag": '"0xNEW"'}
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.put_object_sync("k", b"v") == '"0xNEW"'
        blob.upload_blob.assert_called_once_with(b"v", overwrite=True)

    def test_other_errors_raise_storage_error(self, azure_storage, mock_container):
        blob = MagicMock()
        blob.upload_blob.side_effect = RuntimeError("boom")
        mock_container.get_blob_client.return_value = blob

        with pytest.raises(StorageError):
            azure_storage.put_object_sync("k", b"v")

    def test_delete_ignores_missing(self, azure_storage, mock_container):
        blob = MagicMock()
        blob.delete_blob.side_effect = ResourceNotFoundError("missing")
        mock_container.get_blob_client.return_value = blob

        azure_storage.delete_object_sync("k")

    def test_list_strips_store_prefix(self, azure_storage, mock_container):
        blobs = [MagicMock(), MagicMock()]
        blobs[0].name = "my/prefix/state/b.json"
        blobs[0].metadata = None
        blobs[1].name = "my/prefix/state/a.json"
        blobs[1].metadata = None
        mock_container.list_blobs.return_value = blobs

        assert azure_storage.list_objects_sync("state/") == [
            "state/a.json",
            "state/b.json",
        ]
        mock_container.list_blobs.assert_called_once_with(
            name_starts_with="my/prefix/state/", include=["metadata"]
        )

    def test_put_over_append_blob_replaces_via_delete(
        self, azure_storage, mock_container
    ):
        error = ResourceExistsError("409")
        error.error_code = "InvalidBlobType"
        blob = MagicMock()
        blob.upload_blob.side_effect = [error, {"etag": '"0xNEW"'}]
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.put_object_sync("k", b"v") == '"0xNEW"'
        blob.delete_blob.assert_called_once_with()
        assert blob.upload_blob.call_args_list[-1].kwargs == {"overwrite": True}

    def test_cas_put_over_append_blob_deletes_conditionally(
        self, azure_storage, mock_container
    ):
        error = ResourceExistsError("409")
        error.error_code = "InvalidBlobType"
        blob = MagicMock()
        blob.upload_blob.side_effect = [error, {"etag": '"0xNEW"'}]
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.put_object_sync("k", b"v", if_match='"0xOLD"') == '"0xNEW"'
        blob.delete_blob.assert_called_once_with(
            etag='"0xOLD"', match_condition=MatchConditions.IfNotModified
        )
        assert blob.upload_blob.call_args_list[-1].kwargs == {"overwrite": False}

    def test_cas_put_over_append_blob_stale_etag_loses(
        self, azure_storage, mock_container
    ):
        error = ResourceExistsError("409")
        error.error_code = "InvalidBlobType"
        blob = MagicMock()
        blob.upload_blob.side_effect = error
        blob.delete_blob.side_effect = ResourceModifiedError("412")
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.put_object_sync("k", b"v", if_match='"0xOLD"') is None

    def test_if_absent_conflict_is_still_a_plain_loss(
        self, azure_storage, mock_container
    ):
        error = ResourceExistsError("409")
        error.error_code = "InvalidBlobType"
        blob = MagicMock()
        blob.upload_blob.side_effect = error
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.put_object_sync("k", b"v", if_absent=True) is None
        blob.delete_blob.assert_not_called()

    def test_append_uses_append_block(self, azure_storage, mock_container):
        blob = MagicMock()
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.append_object_sync("s.jsonl", b"a\n") is True
        blob.append_block.assert_called_once_with(b"a\n")
        blob.create_append_blob.assert_not_called()

    def test_append_creates_append_blob_when_missing(
        self, azure_storage, mock_container
    ):
        blob = MagicMock()
        blob.append_block.side_effect = [ResourceNotFoundError("404"), MagicMock()]
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.append_object_sync("s.jsonl", b"a\n") is True
        blob.create_append_blob.assert_called_once_with(
            match_condition=MatchConditions.IfMissing
        )
        assert blob.append_block.call_count == 2

    def test_append_create_race_is_harmless(self, azure_storage, mock_container):
        blob = MagicMock()
        blob.append_block.side_effect = [ResourceNotFoundError("404"), MagicMock()]
        blob.create_append_blob.side_effect = ResourceExistsError("exists")
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.append_object_sync("s.jsonl", b"a\n") is True
        assert blob.append_block.call_count == 2

    def test_append_on_block_blob_falls_back_to_cas(
        self, azure_storage, mock_container
    ):
        error = HttpResponseError("409")
        error.error_code = "InvalidBlobType"
        blob = MagicMock()
        blob.append_block.side_effect = error
        downloader = blob.download_blob.return_value
        downloader.readall.return_value = b"a\n"
        downloader.properties.etag = '"0xOLD"'
        blob.upload_blob.return_value = {"etag": '"0xNEW"'}
        mock_container.get_blob_client.return_value = blob

        assert azure_storage.append_object_sync("s.jsonl", b"b\n") is True
        blob.upload_blob.assert_called_once_with(
            b"a\nb\n",
            overwrite=True,
            etag='"0xOLD"',
            match_condition=MatchConditions.IfNotModified,
        )

    def test_append_other_errors_raise_storage_error(
        self, azure_storage, mock_container
    ):
        blob = MagicMock()
        blob.append_block.side_effect = RuntimeError("boom")
        mock_container.get_blob_client.return_value = blob

        with pytest.raises(StorageError):
            azure_storage.append_object_sync("s.jsonl", b"a\n")


# ---------------------------------------------------------------------------
# S3 — mocked SDK
# ---------------------------------------------------------------------------


def _client_error(code: str, operation: str = "PutObject") -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": "test"}}, operation
    )


@pytest.fixture
def mock_s3_client():
    with patch("cairndb.storage.s3.boto3.client") as factory:
        client = MagicMock()
        factory.return_value = client
        yield client


@pytest.fixture
def s3_storage(mock_s3_client):
    from cairndb.storage.s3 import S3Storage

    return S3Storage(bucket="test-bucket", prefix="my/prefix")


class TestS3Objects:
    def test_get_returns_data_and_etag(self, s3_storage, mock_s3_client):
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=b"data")),
            "ETag": '"abc"',
        }

        obj = s3_storage.get_object_sync("state/run.json")

        assert obj == StoredObject(data=b"data", etag='"abc"')
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="my/prefix/state/run.json"
        )

    def test_get_missing_returns_none(self, s3_storage, mock_s3_client):
        mock_s3_client.get_object.side_effect = _client_error("NoSuchKey", "GetObject")

        assert s3_storage.get_object_sync("k") is None

    def test_put_if_match_sends_header(self, s3_storage, mock_s3_client):
        mock_s3_client.put_object.return_value = {"ETag": '"new"'}

        assert s3_storage.put_object_sync("k", b"v", if_match='"old"') == '"new"'
        mock_s3_client.put_object.assert_called_once_with(
            Bucket="test-bucket", Key="my/prefix/k", Body=b"v", IfMatch='"old"'
        )

    def test_put_if_absent_sends_wildcard(self, s3_storage, mock_s3_client):
        mock_s3_client.put_object.return_value = {"ETag": '"new"'}

        assert s3_storage.put_object_sync("k", b"v", if_absent=True) == '"new"'
        mock_s3_client.put_object.assert_called_once_with(
            Bucket="test-bucket", Key="my/prefix/k", Body=b"v", IfNoneMatch="*"
        )

    @pytest.mark.parametrize(
        "code", ["PreconditionFailed", "ConditionalRequestConflict", "NoSuchKey"]
    )
    def test_conditional_failures_return_none(self, s3_storage, mock_s3_client, code):
        mock_s3_client.put_object.side_effect = _client_error(code)

        assert s3_storage.put_object_sync("k", b"v", if_match='"old"') is None

    def test_unconditional_error_raises(self, s3_storage, mock_s3_client):
        mock_s3_client.put_object.side_effect = _client_error("AccessDenied")

        with pytest.raises(StorageError):
            s3_storage.put_object_sync("k", b"v")

    def test_list_strips_store_prefix(self, s3_storage, mock_s3_client):
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "my/prefix/state/b.json"},
                {"Key": "my/prefix/state/a.json"},
            ],
            "IsTruncated": False,
        }

        assert s3_storage.list_objects_sync("state/") == [
            "state/a.json",
            "state/b.json",
        ]


# ---------------------------------------------------------------------------
# GCS — mocked SDK
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_bucket():
    with patch("cairndb.storage.gcs.gcs.Client") as factory:
        client = MagicMock()
        factory.return_value = client
        bucket = MagicMock()
        client.bucket.return_value = bucket
        yield bucket


@pytest.fixture
def gcs_storage(mock_bucket):
    from cairndb.storage.gcs import GCSStorage

    return GCSStorage(bucket="test-bucket", prefix="my/prefix")


class TestGCSObjects:
    def test_get_returns_generation_etag(self, gcs_storage, mock_bucket):
        blob = MagicMock()
        blob.download_as_bytes.return_value = b"data"
        blob.generation = 1234
        mock_bucket.get_blob.return_value = blob

        obj = gcs_storage.get_object_sync("state/run.json")

        assert obj == StoredObject(data=b"data", etag="1234")
        mock_bucket.get_blob.assert_called_once_with("my/prefix/state/run.json")

    def test_get_missing_returns_none(self, gcs_storage, mock_bucket):
        mock_bucket.get_blob.return_value = None

        assert gcs_storage.get_object_sync("k") is None

    def test_put_if_match_maps_to_generation(self, gcs_storage, mock_bucket):
        blob = MagicMock()
        blob.generation = 1235
        mock_bucket.blob.return_value = blob

        assert gcs_storage.put_object_sync("k", b"v", if_match="1234") == "1235"
        blob.upload_from_string.assert_called_once_with(b"v", if_generation_match=1234)

    def test_put_if_absent_maps_to_generation_zero(self, gcs_storage, mock_bucket):
        blob = MagicMock()
        blob.generation = 1
        mock_bucket.blob.return_value = blob

        assert gcs_storage.put_object_sync("k", b"v", if_absent=True) == "1"
        blob.upload_from_string.assert_called_once_with(b"v", if_generation_match=0)

    def test_precondition_failed_returns_none(self, gcs_storage, mock_bucket):
        blob = MagicMock()
        blob.upload_from_string.side_effect = GCSPreconditionFailed("412")
        mock_bucket.blob.return_value = blob

        assert gcs_storage.put_object_sync("k", b"v", if_match="1234") is None

    def test_invalid_etag_raises(self, gcs_storage, mock_bucket):
        with pytest.raises(StorageError):
            gcs_storage.put_object_sync("k", b"v", if_match="not-a-generation")

    def test_delete_ignores_missing(self, gcs_storage, mock_bucket):
        blob = MagicMock()
        blob.delete.side_effect = GCSNotFound("missing")
        mock_bucket.blob.return_value = blob

        gcs_storage.delete_object_sync("k")
