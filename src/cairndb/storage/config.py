"""Storage backend configuration, shared by committer, client, and jobs.

Each backend has its own config dataclass, discriminated by the ``type``
class attribute:

- :class:`FilesystemStorageConfig` — local filesystem (dev/testing)
- :class:`S3StorageConfig` — Amazon S3 (and S3-compatible stores with
  conditional-write support)
- :class:`AzureStorageConfig` — Azure Blob Storage
- :class:`GCSStorageConfig` — Google Cloud Storage

Validation happens at construction: an instance that exists is usable.
Build one directly, or from a ``type``-discriminated dict with
:meth:`StorageConfig.from_dict`, or from ``CAIRNDB_*`` environment
variables with :meth:`StorageConfig.from_env`.
"""

import os
from dataclasses import dataclass
from typing import Any, ClassVar

from cairndb.core.exceptions import ConfigurationError
from cairndb.storage.base import BlobStorage


@dataclass
class StorageConfig:
    """Base class for storage backend configuration.

    Instantiate a backend subclass directly, or dispatch on the ``type``
    discriminator with :meth:`from_dict` / :meth:`from_env`.
    """

    type: ClassVar[str]

    def create_storage(self) -> BlobStorage:
        """Create a storage backend instance from this config."""
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StorageConfig:
        """Build the config subclass selected by ``data["type"]``.

        ``type`` defaults to ``"filesystem"``; the remaining keys are the
        selected subclass's fields.

        Raises:
            ConfigurationError: If ``type`` names no known backend.
            ValueError: If the fields are invalid for the selected backend.
        """
        data = dict(data)
        storage_type = data.pop("type", "filesystem")
        try:
            config_cls = _BACKENDS[storage_type]
        except KeyError:
            raise ConfigurationError(f"Unknown storage type: '{storage_type}'") from None
        try:
            return config_cls(**data)
        except TypeError as exc:
            raise ValueError(f"Invalid {storage_type} storage config: {exc}") from None

    @classmethod
    def from_env(cls) -> StorageConfig:
        """Build configuration from ``CAIRNDB_*`` environment variables.

        Supported variables:
            CAIRNDB_STORAGE_TYPE            - filesystem | s3 | azure | gcs
            CAIRNDB_STORAGE_PATH            - filesystem root path
            CAIRNDB_STORAGE_PREFIX          - Blob key prefix
            CAIRNDB_S3_BUCKET               - S3 bucket
            CAIRNDB_GCS_BUCKET              - GCS bucket (else CAIRNDB_S3_BUCKET)
            CAIRNDB_S3_REGION               - AWS region
            CAIRNDB_S3_ENDPOINT_URL         - Custom S3 endpoint
            CAIRNDB_AZURE_CONTAINER         - Azure container name
            CAIRNDB_AZURE_CONNECTION_STRING - Azure connection string
            CAIRNDB_AZURE_ACCOUNT_URL       - Azure account URL
            CAIRNDB_GCS_PROJECT             - GCP project ID
            CAIRNDB_GCS_CREDENTIALS_PATH    - GCP key file
        """
        env = os.environ
        storage_type = env.get("CAIRNDB_STORAGE_TYPE", "filesystem")
        prefix = env.get("CAIRNDB_STORAGE_PREFIX", "")

        if storage_type == "filesystem":
            return FilesystemStorageConfig(path=env.get("CAIRNDB_STORAGE_PATH", ""))
        if storage_type == "s3":
            return S3StorageConfig(
                bucket=env.get("CAIRNDB_S3_BUCKET", ""),
                prefix=prefix,
                region=env.get("CAIRNDB_S3_REGION"),
                endpoint_url=env.get("CAIRNDB_S3_ENDPOINT_URL"),
            )
        if storage_type == "azure":
            return AzureStorageConfig(
                container=env.get("CAIRNDB_AZURE_CONTAINER", ""),
                prefix=prefix,
                connection_string=env.get("CAIRNDB_AZURE_CONNECTION_STRING"),
                account_url=env.get("CAIRNDB_AZURE_ACCOUNT_URL"),
            )
        if storage_type == "gcs":
            return GCSStorageConfig(
                # CAIRNDB_S3_BUCKET was the only bucket variable before
                # CAIRNDB_GCS_BUCKET existed; still honoured for existing jobs.
                bucket=env.get("CAIRNDB_GCS_BUCKET") or env.get("CAIRNDB_S3_BUCKET", ""),
                prefix=prefix,
                project=env.get("CAIRNDB_GCS_PROJECT"),
                credentials_path=env.get("CAIRNDB_GCS_CREDENTIALS_PATH"),
            )
        raise ConfigurationError(f"Unknown storage type: '{storage_type}'")


@dataclass
class FilesystemStorageConfig(StorageConfig):
    """Local filesystem storage (dev/testing)."""

    type: ClassVar[str] = "filesystem"

    path: str  # root directory for all storage

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("filesystem storage requires 'path' to be set")

    def create_storage(self) -> BlobStorage:
        from cairndb.storage.filesystem import FilesystemStorage

        return FilesystemStorage(self.path)


@dataclass
class S3StorageConfig(StorageConfig):
    """Amazon S3, and S3-compatible stores with conditional-write support."""

    type: ClassVar[str] = "s3"

    bucket: str
    prefix: str = ""
    region: str | None = None  # e.g. "us-east-1"
    endpoint_url: str | None = None  # S3-compatible endpoint (localstack, MinIO)

    def __post_init__(self) -> None:
        if not self.bucket:
            raise ValueError("s3 storage requires 'bucket' to be set")

    def create_storage(self) -> BlobStorage:
        from cairndb.storage.s3 import S3Storage

        return S3Storage(
            bucket=self.bucket,
            prefix=self.prefix,
            region=self.region,
            endpoint_url=self.endpoint_url,
        )


@dataclass
class AzureStorageConfig(StorageConfig):
    """Azure Blob Storage."""

    type: ClassVar[str] = "azure"

    container: str
    prefix: str = ""
    connection_string: str | None = None
    account_url: str | None = None  # uses DefaultAzureCredential

    def __post_init__(self) -> None:
        if not self.container:
            raise ValueError("azure storage requires 'container' to be set")
        if not self.connection_string and not self.account_url:
            raise ValueError("azure storage requires 'connection_string' or 'account_url'")

    def create_storage(self) -> BlobStorage:
        from cairndb.storage.azure import AzureBlobStorage

        return AzureBlobStorage(
            container=self.container,
            prefix=self.prefix,
            connection_string=self.connection_string,
            account_url=self.account_url,
        )


@dataclass
class GCSStorageConfig(StorageConfig):
    """Google Cloud Storage."""

    type: ClassVar[str] = "gcs"

    bucket: str
    prefix: str = ""
    project: str | None = None  # GCP project ID
    credentials_path: str | None = None  # service-account JSON key file

    def __post_init__(self) -> None:
        if not self.bucket:
            raise ValueError("gcs storage requires 'bucket' to be set")

    def create_storage(self) -> BlobStorage:
        from cairndb.storage.gcs import GCSStorage

        return GCSStorage(
            bucket=self.bucket,
            prefix=self.prefix,
            project=self.project,
            credentials_path=self.credentials_path,
        )


_BACKENDS: dict[str, type[StorageConfig]] = {
    FilesystemStorageConfig.type: FilesystemStorageConfig,
    S3StorageConfig.type: S3StorageConfig,
    AzureStorageConfig.type: AzureStorageConfig,
    GCSStorageConfig.type: GCSStorageConfig,
}
