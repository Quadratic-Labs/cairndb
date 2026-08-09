"""Amazon S3 storage backend."""

import asyncio

import structlog

try:
    import boto3
    import botocore.exceptions
except ImportError as e:
    raise ImportError(
        "boto3 is required for S3 storage. Install with: pip install boto3"
    ) from e

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

# 412 = object already exists (IfNoneMatch failed); 409 = concurrent
# conditional writers collided mid-flight. Both mean "this call did not
# create the object" — the committer re-reads the tail and retries.
_LOST_RACE_CODES = ("PreconditionFailed", "ConditionalRequestConflict", "412", "409")
_NOT_FOUND_CODES = ("NoSuchKey", "404")


class S3Storage(BlobStorage):
    """
    Amazon S3 storage backend.

    Put-if-absent uses `IfNoneMatch="*"` (native S3 conditional writes;
    also supported by MinIO and most S3-compatible stores since late 2024).
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        region: str | None = None,
        endpoint_url: str | None = None,
    ):
        """
        Initialize S3 storage.

        Args:
            bucket: S3 bucket name
            prefix: Key prefix for all objects
            region: AWS region (e.g. "us-east-1")
            endpoint_url: Custom endpoint for S3-compatible stores (localstack, MinIO)
        """
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")

        client_kwargs: dict = {}
        if region:
            client_kwargs["region_name"] = region
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url

        self._client = boto3.client("s3", **client_kwargs)

        logger.info(
            "s3_storage_initialized",
            bucket=bucket,
            prefix=self._prefix,
            region=region,
            endpoint_url=endpoint_url,
        )

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    def _key(self, path: str) -> str:
        """Prepend prefix to produce the full S3 key."""
        if self._prefix:
            return f"{self._prefix}/{path}"
        return path

    # ------------------------------------------------------------------
    # Synchronous helpers (executed via asyncio.to_thread)
    # ------------------------------------------------------------------

    def _put_if_absent(self, key: str, data: bytes) -> bool:
        try:
            self._client.put_object(
                Bucket=self._bucket, Key=key, Body=data, IfNoneMatch="*"
            )
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in _LOST_RACE_CODES:
                return False
            raise

    def _get_object(self, key: str) -> bytes | None:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            return response["Body"].read()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in _NOT_FOUND_CODES:
                return None
            raise

    def _list_keys(self, prefix: str, start_after: str | None = None) -> list[str]:
        """Paginate list_objects_v2 and return all matching keys."""
        keys: list[str] = []
        kwargs: dict = {"Bucket": self._bucket, "Prefix": prefix}
        if start_after:
            kwargs["StartAfter"] = start_after

        while True:
            response = self._client.list_objects_v2(**kwargs)
            for obj in response.get("Contents", []):
                keys.append(obj["Key"])
            if not response.get("IsTruncated", False):
                break
            kwargs["ContinuationToken"] = response["NextContinuationToken"]

        return keys

    def _delete_keys(self, keys: list[str]) -> int:
        deleted = 0
        for batch_start in range(0, len(keys), 1000):
            batch = keys[batch_start:batch_start + 1000]
            self._client.delete_objects(
                Bucket=self._bucket,
                Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
            )
            deleted += len(batch)
        return deleted

    # ------------------------------------------------------------------
    # BlobStorage interface
    # ------------------------------------------------------------------

    async def put_commit(self, number: int, data: bytes) -> bool:
        key = self._key(commit_key(number))
        try:
            created = await asyncio.to_thread(self._put_if_absent, key, data)
        except Exception as e:
            logger.error("commit_write_failed", number=number, error=str(e))
            raise StorageError(f"Failed to write commit {number} to S3: {e}") from e

        if created:
            logger.info("commit_written", number=number, size_bytes=len(data))
        else:
            logger.debug("commit_lost_race", number=number)
        return created

    async def get_commit(self, number: int) -> bytes | None:
        key = self._key(commit_key(number))
        try:
            return await asyncio.to_thread(self._get_object, key)
        except Exception as e:
            logger.error("commit_read_failed", number=number, error=str(e))
            raise StorageError(f"Failed to read commit {number} from S3: {e}") from e

    async def list_commits(self, after: int = 0) -> list[int]:
        prefix = self._key("log/")
        start_after = self._key(commit_key(after)) if after > 0 else None
        try:
            keys = await asyncio.to_thread(self._list_keys, prefix, start_after)
        except Exception as e:
            logger.error("commit_list_failed", error=str(e))
            raise StorageError(f"Failed to list commits from S3: {e}") from e

        numbers = sorted(
            n for k in keys if (n := parse_commit_key(k)) is not None and n > after
        )
        logger.debug("commits_listed", after=after, count=len(numbers))
        return numbers

    async def put_snapshot(self, schema: str, number: int, data: bytes) -> bool:
        key = self._key(snapshot_key(schema, number))
        try:
            created = await asyncio.to_thread(self._put_if_absent, key, data)
        except Exception as e:
            logger.error("snapshot_write_failed", number=number, error=str(e))
            raise StorageError(f"Failed to write snapshot to S3: {e}") from e

        logger.info(
            "snapshot_written" if created else "snapshot_already_exists",
            schema=schema,
            number=number,
            size_bytes=len(data),
        )
        return created

    async def list_snapshots(self, schema: str) -> list[int]:
        prefix = self._key(snapshot_prefix(schema))
        try:
            keys = await asyncio.to_thread(self._list_keys, prefix)
        except Exception as e:
            logger.error("snapshot_list_failed", error=str(e))
            raise StorageError(f"Failed to list snapshots from S3: {e}") from e

        return sorted(n for k in keys if (n := parse_snapshot_key(k)) is not None)

    async def get_snapshot(self, schema: str, number: int) -> bytes:
        key = self._key(snapshot_key(schema, number))
        try:
            data = await asyncio.to_thread(self._get_object, key)
        except Exception as e:
            logger.error("snapshot_read_failed", number=number, error=str(e))
            raise StorageError(f"Failed to read snapshot from S3: {e}") from e

        if data is None:
            raise StorageError(f"Snapshot not found: schema v{schema}, commit {number}")
        return data

    async def delete_commits_before(self, number: int) -> int:
        prefix = self._key("log/")
        try:
            keys = await asyncio.to_thread(self._list_keys, prefix)
            targets = [
                k for k in keys if (n := parse_commit_key(k)) is not None and n < number
            ]
            deleted = await asyncio.to_thread(self._delete_keys, targets)
        except Exception as e:
            raise StorageError(f"Failed to delete commits from S3: {e}") from e

        logger.info("commits_deleted", before=number, count=deleted)
        return deleted

    async def delete_snapshots_before(self, schema: str, number: int) -> int:
        prefix = self._key(snapshot_prefix(schema))
        try:
            keys = await asyncio.to_thread(self._list_keys, prefix)
            targets = [
                k for k in keys if (n := parse_snapshot_key(k)) is not None and n < number
            ]
            deleted = await asyncio.to_thread(self._delete_keys, targets)
        except Exception as e:
            raise StorageError(f"Failed to delete snapshots from S3: {e}") from e

        logger.info("snapshots_deleted", schema=schema, before=number, count=deleted)
        return deleted

    # ------------------------------------------------------------------
    # Generic conditional object API
    #
    # Etags are S3 etags. if_match maps to `IfMatch`, if_absent to
    # `IfNoneMatch: "*"` (native conditional writes, late-2024+; also
    # supported by MinIO and most S3-compatible stores).
    # ------------------------------------------------------------------

    def get_object_sync(self, key: str) -> StoredObject | None:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=self._key(key))
            return StoredObject(data=response["Body"].read(), etag=response["ETag"])
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in _NOT_FOUND_CODES:
                return None
            raise StorageError(f"Failed to read object {key} from S3: {e}") from e
        except Exception as e:
            raise StorageError(f"Failed to read object {key} from S3: {e}") from e

    def put_object_sync(
        self,
        key: str,
        data: bytes,
        *,
        if_match: str | None = None,
        if_absent: bool = False,
    ) -> str | None:
        self._check_object_preconditions(if_match, if_absent)
        kwargs: dict[str, str | bytes] = {
            "Bucket": self._bucket,
            "Key": self._key(key),
            "Body": data,
        }
        if if_match is not None:
            kwargs["IfMatch"] = if_match
        elif if_absent:
            kwargs["IfNoneMatch"] = "*"
        try:
            response = self._client.put_object(**kwargs)
            return str(response["ETag"])
        except botocore.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]
            conditional = if_match is not None or if_absent
            # A 404 under IfMatch means the object was deleted meanwhile:
            # the precondition can no longer hold.
            if conditional and code in _LOST_RACE_CODES + _NOT_FOUND_CODES:
                return None
            raise StorageError(f"Failed to write object {key} to S3: {e}") from e
        except Exception as e:
            raise StorageError(f"Failed to write object {key} to S3: {e}") from e

    def delete_object_sync(self, key: str) -> None:
        try:
            self._client.delete_object(Bucket=self._bucket, Key=self._key(key))
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in _NOT_FOUND_CODES:
                return
            raise StorageError(f"Failed to delete object {key} from S3: {e}") from e
        except Exception as e:
            raise StorageError(f"Failed to delete object {key} from S3: {e}") from e

    def list_objects_sync(self, prefix: str = "") -> list[str]:
        try:
            names = self._list_keys(self._key(prefix))
        except Exception as e:
            raise StorageError(f"Failed to list objects from S3: {e}") from e
        if self._prefix:
            cut = len(self._prefix) + 1
            names = [n[cut:] for n in names]
        return sorted(names)
