"""Google Cloud Storage backend."""

import asyncio

import structlog

try:
    from google.cloud import storage as gcs
    from google.api_core.exceptions import NotFound as GCSNotFound
    from google.api_core.exceptions import PreconditionFailed as GCSPreconditionFailed
except ImportError as e:
    raise ImportError(
        "google-cloud-storage is required for GCS storage. "
        "Install with: pip install google-cloud-storage"
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


class GCSStorage(BlobStorage):
    """
    Google Cloud Storage backend.

    Put-if-absent uses `if_generation_match=0`, which succeeds only when
    the object does not exist (generation 0).
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        project: str | None = None,
        credentials_path: str | None = None,
    ):
        """
        Initialize GCS storage.

        Args:
            bucket: GCS bucket name
            prefix: Object prefix for all blobs
            project: GCP project ID
            credentials_path: Path to a service-account JSON key file
        """
        self._prefix = prefix.rstrip("/")

        client_kwargs: dict = {}
        if project:
            client_kwargs["project"] = project
        if credentials_path:
            from google.oauth2 import service_account

            client_kwargs["credentials"] = (
                service_account.Credentials.from_service_account_file(credentials_path)
            )

        self._client = gcs.Client(**client_kwargs)
        self._bucket = self._client.bucket(bucket)

        logger.info(
            "gcs_storage_initialized",
            bucket=bucket,
            prefix=self._prefix,
            project=project,
        )

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    def _key(self, path: str) -> str:
        if self._prefix:
            return f"{self._prefix}/{path}"
        return path

    # ------------------------------------------------------------------
    # Synchronous helpers (executed via asyncio.to_thread)
    # ------------------------------------------------------------------

    def _put_if_absent(self, key: str, data: bytes) -> bool:
        blob = self._bucket.blob(key)
        try:
            blob.upload_from_string(data, if_generation_match=0)
            return True
        except GCSPreconditionFailed:
            return False

    def _get_object(self, key: str) -> bytes | None:
        blob = self._bucket.blob(key)
        try:
            return blob.download_as_bytes()
        except GCSNotFound:
            return None

    def _list_keys(self, prefix: str, start_offset: str | None = None) -> list[str]:
        blobs = self._bucket.list_blobs(prefix=prefix, start_offset=start_offset)
        return [blob.name for blob in blobs]

    def _delete_keys(self, keys: list[str]) -> int:
        deleted = 0
        for key in keys:
            try:
                self._bucket.blob(key).delete()
                deleted += 1
            except GCSNotFound:
                pass
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
            raise StorageError(f"Failed to write commit {number} to GCS: {e}") from e

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
            raise StorageError(f"Failed to read commit {number} from GCS: {e}") from e

    async def list_commits(self, after: int = 0) -> list[int]:
        prefix = self._key("log/")
        # start_offset is inclusive, so start just past the `after` key
        start = self._key(commit_key(after)) + "\x00" if after > 0 else None
        try:
            keys = await asyncio.to_thread(self._list_keys, prefix, start)
        except Exception as e:
            logger.error("commit_list_failed", error=str(e))
            raise StorageError(f"Failed to list commits from GCS: {e}") from e

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
            raise StorageError(f"Failed to write snapshot to GCS: {e}") from e

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
            raise StorageError(f"Failed to list snapshots from GCS: {e}") from e

        return sorted(n for k in keys if (n := parse_snapshot_key(k)) is not None)

    async def get_snapshot(self, schema: str, number: int) -> bytes:
        key = self._key(snapshot_key(schema, number))
        try:
            data = await asyncio.to_thread(self._get_object, key)
        except Exception as e:
            logger.error("snapshot_read_failed", number=number, error=str(e))
            raise StorageError(f"Failed to read snapshot from GCS: {e}") from e

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
            raise StorageError(f"Failed to delete commits from GCS: {e}") from e

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
            raise StorageError(f"Failed to delete snapshots from GCS: {e}") from e

        logger.info("snapshots_deleted", schema=schema, before=number, count=deleted)
        return deleted

    # ------------------------------------------------------------------
    # Generic conditional object API
    #
    # Etags are GCS generation numbers (stringified). if_match maps to
    # `if_generation_match=<generation>`, if_absent to
    # `if_generation_match=0`. Both are enforced atomically by the service.
    # ------------------------------------------------------------------

    def get_object_sync(self, key: str) -> StoredObject | None:
        try:
            blob = self._bucket.get_blob(self._key(key))
            if blob is None:
                return None
            # get_blob pinned the generation, so this download cannot
            # observe a newer write than the etag we return.
            data = blob.download_as_bytes()
        except GCSNotFound:
            return None
        except Exception as e:
            raise StorageError(f"Failed to read object {key} from GCS: {e}") from e
        return StoredObject(data=data, etag=str(blob.generation))

    def put_object_sync(
        self,
        key: str,
        data: bytes,
        *,
        if_match: str | None = None,
        if_absent: bool = False,
    ) -> str | None:
        self._check_object_preconditions(if_match, if_absent)
        blob = self._bucket.blob(self._key(key))
        if if_match is not None:
            try:
                generation = int(if_match)
            except ValueError as e:
                raise StorageError(
                    f"Invalid GCS etag {if_match!r}: expected a generation number"
                ) from e
        try:
            if if_match is not None:
                blob.upload_from_string(data, if_generation_match=generation)
            elif if_absent:
                blob.upload_from_string(data, if_generation_match=0)
            else:
                blob.upload_from_string(data)
        except (GCSPreconditionFailed, GCSNotFound):
            if if_match is not None or if_absent:
                return None
            raise StorageError(f"Failed to write object {key} to GCS: precondition")
        except Exception as e:
            raise StorageError(f"Failed to write object {key} to GCS: {e}") from e
        return str(blob.generation)

    def delete_object_sync(self, key: str) -> None:
        try:
            self._bucket.blob(self._key(key)).delete()
        except GCSNotFound:
            pass
        except Exception as e:
            raise StorageError(f"Failed to delete object {key} from GCS: {e}") from e

    def list_objects_sync(self, prefix: str = "") -> list[str]:
        try:
            names = self._list_keys(self._key(prefix))
        except Exception as e:
            raise StorageError(f"Failed to list objects from GCS: {e}") from e
        if self._prefix:
            cut = len(self._prefix) + 1
            names = [n[cut:] for n in names]
        return sorted(names)
