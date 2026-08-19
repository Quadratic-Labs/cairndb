"""Unit tests for S3 storage backend."""

from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest

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
        # after=0: no StartAfter at all, and the exact kwarg names matter.
        mock_client.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket", Prefix="my/prefix/log/"
        )

    @pytest.mark.asyncio
    async def test_list_after_uses_start_after(self, storage, mock_client):
        mock_client.list_objects_v2.return_value = {"Contents": [], "IsTruncated": False}

        await storage.list_commits(after=1)

        kwargs = mock_client.list_objects_v2.call_args.kwargs
        assert kwargs["StartAfter"] == "my/prefix/log/000000000001.msgpack"

    @pytest.mark.asyncio
    async def test_list_after_also_filters_client_side(self, storage, mock_client):
        # StartAfter is best-effort; the boundary key must still be excluded.
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "my/prefix/log/000000000005.msgpack"},
                {"Key": "my/prefix/log/000000000006.msgpack"},
            ],
            "IsTruncated": False,
        }

        assert await storage.list_commits(after=5) == [6]

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
        second_call = mock_client.list_objects_v2.call_args_list[1].kwargs
        assert second_call["ContinuationToken"] == "token"

    @pytest.mark.asyncio
    async def test_list_handles_pages_without_contents_or_truncation(
        self, storage, mock_client
    ):
        # A final page may omit both Contents and IsTruncated entirely.
        mock_client.list_objects_v2.return_value = {}

        assert await storage.list_commits() == []
        mock_client.list_objects_v2.assert_called_once()


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
    async def test_get_snapshot_returns_bytes(self, storage, mock_client):
        body = MagicMock()
        body.read.return_value = b"snap"
        mock_client.get_object.return_value = {"Body": body}

        assert await storage.get_snapshot("1.0.0", 40) == b"snap"
        mock_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="my/prefix/snapshots/v1.0.0/000000000040.sqlite"
        )

    @pytest.mark.asyncio
    async def test_list_snapshots_queries_schema_prefix(self, storage, mock_client):
        mock_client.list_objects_v2.return_value = {"Contents": [], "IsTruncated": False}

        assert await storage.list_snapshots("1.0.0") == []
        mock_client.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket", Prefix="my/prefix/snapshots/v1.0.0/"
        )

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
        mock_client.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket", Prefix="my/prefix/log/"
        )
        mock_client.delete_objects.assert_called_once_with(
            Bucket="test-bucket",
            Delete={
                "Objects": [
                    {"Key": f"my/prefix/log/{n:012d}.msgpack"} for n in (1, 2, 3)
                ],
                "Quiet": True,
            },
        )

    @pytest.mark.asyncio
    async def test_delete_snapshots_before(self, storage, mock_client):
        contents = [
            {"Key": f"my/prefix/snapshots/v1.0.0/{n:012d}.sqlite"} for n in (1, 2, 3, 4)
        ]
        contents.append({"Key": "my/prefix/snapshots/v1.0.0/notes.txt"})
        mock_client.list_objects_v2.return_value = {
            "Contents": contents,
            "IsTruncated": False,
        }

        deleted = await storage.delete_snapshots_before("1.0.0", 4)

        assert deleted == 3
        mock_client.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket", Prefix="my/prefix/snapshots/v1.0.0/"
        )
        delete_call = mock_client.delete_objects.call_args.kwargs
        deleted_keys = [o["Key"] for o in delete_call["Delete"]["Objects"]]
        assert deleted_keys == [
            f"my/prefix/snapshots/v1.0.0/{n:012d}.sqlite" for n in (1, 2, 3)
        ]

    @pytest.mark.asyncio
    async def test_delete_batches_at_the_s3_limit_of_1000(self, storage, mock_client):
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"my/prefix/log/{n:012d}.msgpack"} for n in range(1, 1002)
            ],
            "IsTruncated": False,
        }

        assert await storage.delete_commits_before(2000) == 1001
        batch_sizes = [
            len(c.kwargs["Delete"]["Objects"])
            for c in mock_client.delete_objects.call_args_list
        ]
        assert batch_sizes == [1000, 1]


class TestKeyConstruction:
    def test_key_without_prefix(self, mock_client):
        from cairndb.storage.s3 import S3Storage

        s = S3Storage(bucket="b", prefix="")
        assert s._key("log/x.msgpack") == "log/x.msgpack"

    def test_key_with_prefix(self, storage):
        assert storage._key("log/x.msgpack") == "my/prefix/log/x.msgpack"

    def test_prefix_strips_trailing_slash_only(self, mock_client):
        from cairndb.storage.s3 import S3Storage

        s = S3Storage(bucket="b", prefix="pX/")
        assert s._prefix == "pX"

    def test_default_prefix_is_empty(self, mock_client):
        from cairndb.storage.s3 import S3Storage

        s = S3Storage(bucket="b")
        assert s._key("log/x.msgpack") == "log/x.msgpack"


class TestConditionalObjects:
    def test_put_both_preconditions_rejected(self, storage):
        with pytest.raises(ValueError):
            storage.put_object_sync("s/x", b"v", if_match='"e"', if_absent=True)

    def test_unconditional_put_never_swallows_precondition_errors(
        self, storage, mock_client
    ):
        # Lost-race codes only mean "precondition failed" under a
        # precondition; on a plain put they are real errors.
        mock_client.put_object.side_effect = _client_error("PreconditionFailed")
        with pytest.raises(StorageError):
            storage.put_object_sync("s/x", b"v")

    def test_delete_object(self, storage, mock_client):
        storage.delete_object_sync("state/x")
        mock_client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="my/prefix/state/x"
        )

    def test_delete_missing_object_is_noop(self, storage, mock_client):
        mock_client.delete_object.side_effect = _client_error("NoSuchKey", "DeleteObject")
        storage.delete_object_sync("state/x")  # no-op

    def test_delete_other_errors_raise(self, storage, mock_client):
        mock_client.delete_object.side_effect = _client_error(
            "AccessDenied", "DeleteObject"
        )
        with pytest.raises(StorageError):
            storage.delete_object_sync("state/x")

    def test_list_objects_strips_prefix(self, storage, mock_client):
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "my/prefix/state/x"},
                {"Key": "my/prefix/config/a"},
            ],
            "IsTruncated": False,
        }

        assert storage.list_objects_sync() == ["config/a", "state/x"]
        mock_client.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket", Prefix="my/prefix/"
        )
