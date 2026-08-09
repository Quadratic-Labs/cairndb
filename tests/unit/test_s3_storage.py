"""Unit tests for S3 storage backend."""

import pytest
from unittest.mock import patch, MagicMock

import botocore.exceptions

from cairndb.core.exceptions import StorageError


def _client_error(code: str, operation: str = "PutObject") -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": "test"}}, operation
    )


@pytest.fixture
def mock_client():
    with patch("cairndb.storage.s3.boto3.client") as factory:
        client = MagicMock()
        factory.return_value = client
        yield client


@pytest.fixture
def storage(mock_client):
    from cairndb.storage.s3 import S3Storage

    return S3Storage(bucket="test-bucket", prefix="my/prefix")


class TestPutCommit:
    @pytest.mark.asyncio
    async def test_put_commit_uses_conditional_write(self, storage, mock_client):
        result = await storage.put_commit(42, b"data")

        assert result is True
        mock_client.put_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="my/prefix/log/000000000042.msgpack",
            Body=b"data",
            IfNoneMatch="*",
        )

    @pytest.mark.asyncio
    async def test_precondition_failed_means_lost_race(self, storage, mock_client):
        mock_client.put_object.side_effect = _client_error("PreconditionFailed")

        assert await storage.put_commit(42, b"data") is False

    @pytest.mark.asyncio
    async def test_conditional_request_conflict_means_lost_race(self, storage, mock_client):
        # 409: concurrent conditional writers collided mid-flight
        mock_client.put_object.side_effect = _client_error("ConditionalRequestConflict")

        assert await storage.put_commit(42, b"data") is False

    @pytest.mark.asyncio
    async def test_other_errors_raise_storage_error(self, storage, mock_client):
        mock_client.put_object.side_effect = _client_error("AccessDenied")

        with pytest.raises(StorageError):
            await storage.put_commit(42, b"data")


class TestGetCommit:
    @pytest.mark.asyncio
    async def test_get_commit_returns_body(self, storage, mock_client):
        body = MagicMock()
        body.read.return_value = b"data"
        mock_client.get_object.return_value = {"Body": body}

        assert await storage.get_commit(42) == b"data"
        mock_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="my/prefix/log/000000000042.msgpack"
        )

    @pytest.mark.asyncio
    async def test_missing_commit_returns_none(self, storage, mock_client):
        mock_client.get_object.side_effect = _client_error("NoSuchKey", "GetObject")

        assert await storage.get_commit(42) is None

    @pytest.mark.asyncio
    async def test_other_errors_raise_storage_error(self, storage, mock_client):
        mock_client.get_object.side_effect = _client_error("AccessDenied", "GetObject")

        with pytest.raises(StorageError):
            await storage.get_commit(42)


class TestListCommits:
    @pytest.mark.asyncio
    async def test_list_parses_numbers_in_order(self, storage, mock_client):
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "my/prefix/log/000000000002.msgpack"},
                {"Key": "my/prefix/log/000000000001.msgpack"},
                {"Key": "my/prefix/log/junk.txt"},
            ],
            "IsTruncated": False,
        }

        assert await storage.list_commits() == [1, 2]

    @pytest.mark.asyncio
    async def test_list_after_uses_start_after(self, storage, mock_client):
        mock_client.list_objects_v2.return_value = {"Contents": [], "IsTruncated": False}

        await storage.list_commits(after=5)

        kwargs = mock_client.list_objects_v2.call_args.kwargs
        assert kwargs["StartAfter"] == "my/prefix/log/000000000005.msgpack"

    @pytest.mark.asyncio
    async def test_list_paginates(self, storage, mock_client):
        mock_client.list_objects_v2.side_effect = [
            {
                "Contents": [{"Key": "my/prefix/log/000000000001.msgpack"}],
                "IsTruncated": True,
                "NextContinuationToken": "token",
            },
            {
                "Contents": [{"Key": "my/prefix/log/000000000002.msgpack"}],
                "IsTruncated": False,
            },
        ]

        assert await storage.list_commits() == [1, 2]


class TestSnapshots:
    @pytest.mark.asyncio
    async def test_put_snapshot_is_conditional(self, storage, mock_client):
        assert await storage.put_snapshot("1.0.0", 40, b"snap") is True

        mock_client.put_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="my/prefix/snapshots/v1.0.0/000000000040.sqlite",
            Body=b"snap",
            IfNoneMatch="*",
        )

    @pytest.mark.asyncio
    async def test_duplicate_snapshot_returns_false(self, storage, mock_client):
        mock_client.put_object.side_effect = _client_error("PreconditionFailed")

        assert await storage.put_snapshot("1.0.0", 40, b"snap") is False

    @pytest.mark.asyncio
    async def test_find_latest_snapshot(self, storage, mock_client):
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "my/prefix/snapshots/v1.0.0/000000000010.sqlite"},
                {"Key": "my/prefix/snapshots/v1.0.0/000000000040.sqlite"},
            ],
            "IsTruncated": False,
        }

        assert await storage.find_latest_snapshot("1.0.0") == 40

    @pytest.mark.asyncio
    async def test_find_latest_snapshot_none(self, storage, mock_client):
        mock_client.list_objects_v2.return_value = {"Contents": [], "IsTruncated": False}

        assert await storage.find_latest_snapshot("1.0.0") is None

    @pytest.mark.asyncio
    async def test_get_missing_snapshot_raises(self, storage, mock_client):
        mock_client.get_object.side_effect = _client_error("NoSuchKey", "GetObject")

        with pytest.raises(StorageError):
            await storage.get_snapshot("1.0.0", 40)


class TestDeletion:
    @pytest.mark.asyncio
    async def test_delete_commits_before(self, storage, mock_client):
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"my/prefix/log/{n:012d}.msgpack"} for n in range(1, 6)
            ],
            "IsTruncated": False,
        }

        deleted = await storage.delete_commits_before(4)

        assert deleted == 3
        delete_call = mock_client.delete_objects.call_args.kwargs
        deleted_keys = [o["Key"] for o in delete_call["Delete"]["Objects"]]
        assert deleted_keys == [
            "my/prefix/log/000000000001.msgpack",
            "my/prefix/log/000000000002.msgpack",
            "my/prefix/log/000000000003.msgpack",
        ]


class TestKeyConstruction:
    def test_key_without_prefix(self, mock_client):
        from cairndb.storage.s3 import S3Storage

        s = S3Storage(bucket="b", prefix="")
        assert s._key("log/x.msgpack") == "log/x.msgpack"

    def test_key_with_prefix(self, storage):
        assert storage._key("log/x.msgpack") == "my/prefix/log/x.msgpack"
