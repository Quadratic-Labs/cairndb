"""Azure Blob Storage backend."""

import asyncio

import structlog

try:
    from azure.storage.blob import BlobServiceClient
    from azure.core import MatchConditions
    from azure.core.exceptions import (
        ResourceExistsError,
        ResourceModifiedError,
        ResourceNotFoundError,
    )
except ImportError as e:
    raise ImportError(
        "azure-storage-blob is required for Azure storage. "
        "Install with: pip install azure-storage-blob"
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


class AzureBlobStorage(BlobStorage):
    """
    Azure Blob Storage backend.

    Put-if-absent uses `upload_blob(..., overwrite=False)`, which sets the
    `If-None-Match: *` condition and raises ResourceExistsError on conflict.

    Provide either *connection_string* (simplest) or *account_url*.  When only
    account_url is given, ``DefaultAzureCredential`` is used for authentication.
    """

    def __init__(
        self,
        container: str,
        prefix: str = "",
        connection_string: str | None = None,
        account_url: str | None = None,
    ):
        """
        Initialize Azure Blob Storage.

        Args:
            container: Azure container name
            prefix: Blob name prefix
            connection_string: Full Azure Storage connection string
            account_url: Storage account URL (uses DefaultAzureCredential)
        """
        if not connection_string and not account_url:
            raise StorageError(
                "Azure storage requires either 'connection_string' or 'account_url'"
            )

        self._prefix = prefix.rstrip("/")

        if connection_string:
            self._service_client = BlobServiceClient.from_connection_string(connection_string)
        else:
            from azure.identity import DefaultAzureCredential

            self._service_client = BlobServiceClient(
                account_url=account_url, credential=DefaultAzureCredential()
            )

        self._container_client = self._service_client.get_container_client(container)

        logger.info(
            "azure_storage_initialized",
            container=container,
            prefix=self._prefix,
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
        blob_client = self._container_client.get_blob_client(key)
        try:
            blob_client.upload_blob(data, overwrite=False)
            return True
        except ResourceExistsError:
            return False

    def _get_object(self, key: str) -> bytes | None:
        blob_client = self._container_client.get_blob_client(key)
        try:
            return blob_client.download_blob().readall()
        except ResourceNotFoundError:
            return None

    def _list_keys(self, prefix: str) -> list[str]:
        return [b.name for b in self._container_client.list_blobs(name_starts_with=prefix)]

    def _delete_keys(self, keys: list[str]) -> int:
        deleted = 0
        for key in keys:
            try:
                self._container_client.delete_blob(key)
                deleted += 1
            except ResourceNotFoundError:
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
            raise StorageError(f"Failed to write commit {number} to Azure: {e}") from e

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
            raise StorageError(f"Failed to read commit {number} from Azure: {e}") from e

    async def list_commits(self, after: int = 0) -> list[int]:
        prefix = self._key("log/")
        try:
            keys = await asyncio.to_thread(self._list_keys, prefix)
        except Exception as e:
            logger.error("commit_list_failed", error=str(e))
            raise StorageError(f"Failed to list commits from Azure: {e}") from e

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
            raise StorageError(f"Failed to write snapshot to Azure: {e}") from e

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
            raise StorageError(f"Failed to list snapshots from Azure: {e}") from e

        return sorted(n for k in keys if (n := parse_snapshot_key(k)) is not None)

    async def get_snapshot(self, schema: str, number: int) -> bytes:
        key = self._key(snapshot_key(schema, number))
        try:
            data = await asyncio.to_thread(self._get_object, key)
        except Exception as e:
            logger.error("snapshot_read_failed", number=number, error=str(e))
            raise StorageError(f"Failed to read snapshot from Azure: {e}") from e

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
            raise StorageError(f"Failed to delete commits from Azure: {e}") from e

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
            raise StorageError(f"Failed to delete snapshots from Azure: {e}") from e

        logger.info("snapshots_deleted", schema=schema, before=number, count=deleted)
        return deleted

    # ------------------------------------------------------------------
    # Generic conditional object API
    #
    # Etags are Azure blob etags. if_match maps to an `If-Match` header
    # (MatchConditions.IfNotModified); if_absent maps to `If-None-Match: *`
    # (overwrite=False). Both are enforced atomically by the service.
    # ------------------------------------------------------------------

    def get_object_sync(self, key: str) -> StoredObject | None:
        blob_client = self._container_client.get_blob_client(self._key(key))
        try:
            downloader = blob_client.download_blob()
            data = downloader.readall()
            etag = downloader.properties.etag
        except ResourceNotFoundError:
            return None
        except Exception as e:
            raise StorageError(f"Failed to read object {key} from Azure: {e}") from e
        return StoredObject(data=data, etag=etag)

    def put_object_sync(
        self,
        key: str,
        data: bytes,
        *,
        if_match: str | None = None,
        if_absent: bool = False,
    ) -> str | None:
        self._check_object_preconditions(if_match, if_absent)
        blob_client = self._container_client.get_blob_client(self._key(key))
        try:
            if if_match is not None:
                props = blob_client.upload_blob(
                    data,
                    overwrite=True,
                    etag=if_match,
                    match_condition=MatchConditions.IfNotModified,
                )
            elif if_absent:
                props = blob_client.upload_blob(data, overwrite=False)
            else:
                props = blob_client.upload_blob(data, overwrite=True)
        except (ResourceExistsError, ResourceModifiedError):
            return None
        except ResourceNotFoundError:
            # if_match on a blob deleted in the meantime: precondition failed.
            if if_match is not None:
                return None
            raise StorageError(f"Failed to write object {key} to Azure: not found")
        except Exception as e:
            raise StorageError(f"Failed to write object {key} to Azure: {e}") from e
        return str(props["etag"])

    def delete_object_sync(self, key: str) -> None:
        blob_client = self._container_client.get_blob_client(self._key(key))
        try:
            blob_client.delete_blob()
        except ResourceNotFoundError:
            pass
        except Exception as e:
            raise StorageError(f"Failed to delete object {key} from Azure: {e}") from e

    def list_objects_sync(self, prefix: str = "") -> list[str]:
        try:
            names = self._list_keys(self._key(prefix))
        except Exception as e:
            raise StorageError(f"Failed to list objects from Azure: {e}") from e
        if self._prefix:
            cut = len(self._prefix) + 1
            names = [n[cut:] for n in names]
        return sorted(names)
