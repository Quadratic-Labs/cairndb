"""Client configuration."""

import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class ClientConfig(BaseModel):
    """
    Client configuration for CairnDB.

    Configures:
    - Storage backend (where the commit log and snapshots live)
    - Projection database path
    - Updater behavior (polling interval)
    - Copy-on-write settings
    """

    # Storage configuration (same fields as StorageConfig, flattened)
    storage_type: Literal["filesystem", "s3", "azure", "gcs"] = Field(
        default="filesystem",
        description="Storage backend type",
    )

    storage_path: str | None = Field(
        default=None,
        description="Root path for filesystem storage",
    )

    storage_bucket: str | None = Field(
        default=None,
        description="S3/GCS bucket name",
    )

    storage_prefix: str | None = Field(
        default="",
        description="S3/GCS/Azure key prefix",
    )

    storage_region: str | None = Field(
        default=None,
        description="AWS region (e.g. 'us-east-1')",
    )

    storage_endpoint_url: str | None = Field(
        default=None,
        description="Custom S3-compatible endpoint (localstack, MinIO)",
    )

    # Azure-specific
    storage_container: str | None = Field(
        default=None,
        description="Azure container name",
    )

    storage_azure_connection_string: str | None = Field(
        default=None,
        description="Azure Storage connection string",
    )

    storage_azure_account_url: str | None = Field(
        default=None,
        description="Azure Storage account URL (uses DefaultAzureCredential)",
    )

    # GCS-specific
    storage_credentials_path: str | None = Field(
        default=None,
        description="Path to GCP service-account JSON key file",
    )

    # Projection configuration
    db_path: str = Field(
        default="./projection.db",
        description="Path to SQLite projection database",
    )

    # Updater configuration
    poll_interval_seconds: float = Field(
        default=5.0,
        gt=0.0,
        le=3600.0,
        description="Polling interval for checking new commits (one GET when idle)",
    )

    # Copy-on-write configuration
    use_reflink: bool = Field(
        default=True,
        description="Use copy-on-write (reflink) when available",
    )

    # Schema tracking
    schema_version: str = Field(
        default="1.0.0",
        description=(
            "Projection schema version; selects the snapshots/v{version}/ "
            "prefix used for bootstrap"
        ),
    )

    @property
    def db_path_obj(self) -> Path:
        """Get db_path as Path object."""
        return Path(self.db_path)

    @property
    def new_db_path(self) -> str:
        """Get new database path for atomic swap."""
        return f"{self.db_path}.new"

    @classmethod
    def from_dict(cls, data: dict) -> ClientConfig:
        """Create config from dictionary."""
        return cls(**data)

    @classmethod
    def from_env(cls) -> ClientConfig:
        """Build configuration from ``CAIRNDB_*`` environment variables.

        Supported variables:
            CAIRNDB_STORAGE_TYPE              - filesystem | s3 | azure | gcs
            CAIRNDB_STORAGE_PATH              - filesystem root path
            CAIRNDB_AZURE_CONTAINER           - Azure container name
            CAIRNDB_AZURE_CONNECTION_STRING   - Azure connection string
            CAIRNDB_AZURE_ACCOUNT_URL         - Azure account URL
            CAIRNDB_S3_BUCKET                 - S3/GCS bucket
            CAIRNDB_S3_REGION                 - AWS region
            CAIRNDB_S3_ENDPOINT_URL           - Custom S3 endpoint
            CAIRNDB_STORAGE_PREFIX            - Blob key prefix
            CAIRNDB_GCS_CREDENTIALS_PATH      - GCP key file
            CAIRNDB_DB_PATH                   - SQLite projection path
            CAIRNDB_POLL_INTERVAL             - Poll interval seconds
            CAIRNDB_SCHEMA_VERSION            - Projection schema version
        """
        return cls(
            storage_type=os.environ.get("CAIRNDB_STORAGE_TYPE", "filesystem"),  # type: ignore[arg-type]
            storage_path=os.environ.get("CAIRNDB_STORAGE_PATH"),
            storage_bucket=os.environ.get("CAIRNDB_S3_BUCKET"),
            storage_prefix=os.environ.get("CAIRNDB_STORAGE_PREFIX", ""),
            storage_region=os.environ.get("CAIRNDB_S3_REGION"),
            storage_endpoint_url=os.environ.get("CAIRNDB_S3_ENDPOINT_URL"),
            storage_container=os.environ.get("CAIRNDB_AZURE_CONTAINER"),
            storage_azure_connection_string=os.environ.get("CAIRNDB_AZURE_CONNECTION_STRING"),
            storage_azure_account_url=os.environ.get("CAIRNDB_AZURE_ACCOUNT_URL"),
            storage_credentials_path=os.environ.get("CAIRNDB_GCS_CREDENTIALS_PATH"),
            db_path=os.environ.get("CAIRNDB_DB_PATH", "./projection.db"),
            poll_interval_seconds=float(os.environ.get("CAIRNDB_POLL_INTERVAL", "5.0")),
            schema_version=os.environ.get("CAIRNDB_SCHEMA_VERSION", "1.0.0"),
        )

    def validate_storage(self) -> None:
        """Validate storage configuration."""
        if self.storage_type == "filesystem" and not self.storage_path:
            raise ValueError("filesystem storage requires 'storage_path'")
        elif self.storage_type in ["s3", "gcs"] and not self.storage_bucket:
            raise ValueError(f"{self.storage_type} storage requires 'storage_bucket'")
        elif self.storage_type == "azure" and not self.storage_container:
            raise ValueError("azure storage requires 'storage_container'")
        elif self.storage_type == "azure" and not (
            self.storage_azure_connection_string or self.storage_azure_account_url
        ):
            raise ValueError(
                "azure storage requires 'storage_azure_connection_string' or 'storage_azure_account_url'"
            )
