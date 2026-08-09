"""Storage backend configuration, shared by committer, client, and jobs."""

import os
from typing import Literal

from pydantic import BaseModel, Field

from cairndb.storage.base import BlobStorage


class StorageConfig(BaseModel):
    """
    Storage backend configuration.

    Supports multiple backend types:
    - filesystem: Local filesystem (dev/testing)
    - s3: Amazon S3 (and S3-compatible stores with conditional-write support)
    - azure: Azure Blob Storage
    - gcs: Google Cloud Storage
    """

    type: Literal["filesystem", "s3", "azure", "gcs"] = Field(
        default="filesystem",
        description="Storage backend type",
    )

    # Filesystem options
    path: str | None = Field(
        default=None,
        description="Root path for filesystem storage",
    )

    # S3 options
    bucket: str | None = Field(
        default=None,
        description="S3 / GCS bucket name",
    )
    prefix: str | None = Field(
        default="",
        description="S3 / GCS / Azure key prefix",
    )
    region: str | None = Field(
        default=None,
        description="AWS region (e.g. 'us-east-1')",
    )
    endpoint_url: str | None = Field(
        default=None,
        description="Custom S3-compatible endpoint (localstack, MinIO)",
    )

    # Azure options
    container: str | None = Field(
        default=None,
        description="Azure container name",
    )
    azure_connection_string: str | None = Field(
        default=None,
        description="Azure Storage connection string",
    )
    azure_account_url: str | None = Field(
        default=None,
        description="Azure Storage account URL (uses DefaultAzureCredential)",
    )

    # GCS options
    project: str | None = Field(
        default=None,
        description="GCP project ID",
    )
    credentials_path: str | None = Field(
        default=None,
        description="Path to GCP service-account JSON key file",
    )

    def validate_config(self) -> None:
        """Validate that required fields are present for the selected type."""
        if self.type == "filesystem" and not self.path:
            raise ValueError("filesystem storage requires 'path' to be set")
        elif self.type == "s3" and not self.bucket:
            raise ValueError("s3 storage requires 'bucket' to be set")
        elif self.type == "azure" and not self.container:
            raise ValueError("azure storage requires 'container' to be set")
        elif self.type == "azure" and not self.azure_connection_string and not self.azure_account_url:
            raise ValueError(
                "azure storage requires 'azure_connection_string' or 'azure_account_url'"
            )
        elif self.type == "gcs" and not self.bucket:
            raise ValueError("gcs storage requires 'bucket' to be set")

    def create_storage(self) -> BlobStorage:
        """Create a storage backend instance from this config."""
        from cairndb.storage import create_storage as _factory

        self.validate_config()

        kwargs: dict[str, str] = {}
        if self.type == "filesystem":
            kwargs["path"] = self.path  # type: ignore[assignment]
        elif self.type == "s3":
            if self.bucket:
                kwargs["bucket"] = self.bucket
            if self.prefix:
                kwargs["prefix"] = self.prefix
            if self.region:
                kwargs["region"] = self.region
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
        elif self.type == "azure":
            if self.container:
                kwargs["container"] = self.container
            if self.azure_connection_string:
                kwargs["connection_string"] = self.azure_connection_string
            if self.azure_account_url:
                kwargs["account_url"] = self.azure_account_url
            if self.prefix:
                kwargs["prefix"] = self.prefix
        elif self.type == "gcs":
            if self.bucket:
                kwargs["bucket"] = self.bucket
            if self.prefix:
                kwargs["prefix"] = self.prefix
            if self.project:
                kwargs["project"] = self.project
            if self.credentials_path:
                kwargs["credentials_path"] = self.credentials_path

        return _factory(self.type, **kwargs)

    @classmethod
    def from_env(cls) -> "StorageConfig":
        """Build configuration from ``CAIRNDB_*`` environment variables.

        Supported variables:
            CAIRNDB_STORAGE_TYPE            - filesystem | s3 | azure | gcs
            CAIRNDB_STORAGE_PATH            - filesystem root path
            CAIRNDB_STORAGE_PREFIX          - Blob key prefix
            CAIRNDB_S3_BUCKET               - S3/GCS bucket
            CAIRNDB_S3_REGION               - AWS region
            CAIRNDB_S3_ENDPOINT_URL         - Custom S3 endpoint
            CAIRNDB_AZURE_CONTAINER         - Azure container name
            CAIRNDB_AZURE_CONNECTION_STRING - Azure connection string
            CAIRNDB_AZURE_ACCOUNT_URL       - Azure account URL
            CAIRNDB_GCS_PROJECT             - GCP project ID
            CAIRNDB_GCS_CREDENTIALS_PATH    - GCP key file
        """
        return cls(
            type=os.environ.get("CAIRNDB_STORAGE_TYPE", "filesystem"),  # type: ignore[arg-type]
            path=os.environ.get("CAIRNDB_STORAGE_PATH"),
            bucket=os.environ.get("CAIRNDB_S3_BUCKET"),
            prefix=os.environ.get("CAIRNDB_STORAGE_PREFIX", ""),
            region=os.environ.get("CAIRNDB_S3_REGION"),
            endpoint_url=os.environ.get("CAIRNDB_S3_ENDPOINT_URL"),
            container=os.environ.get("CAIRNDB_AZURE_CONTAINER"),
            azure_connection_string=os.environ.get("CAIRNDB_AZURE_CONNECTION_STRING"),
            azure_account_url=os.environ.get("CAIRNDB_AZURE_ACCOUNT_URL"),
            project=os.environ.get("CAIRNDB_GCS_PROJECT"),
            credentials_path=os.environ.get("CAIRNDB_GCS_CREDENTIALS_PATH"),
        )
