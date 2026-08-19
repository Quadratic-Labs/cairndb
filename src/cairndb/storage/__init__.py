"""Blob storage abstraction for CairnDB."""

from typing import Any

from cairndb.storage.base import BlobStorage, StoredObject
from cairndb.storage.config import (
    AzureStorageConfig,
    FilesystemStorageConfig,
    GCSStorageConfig,
    S3StorageConfig,
    StorageConfig,
)


def create_storage(storage_type: str, **kwargs: Any) -> BlobStorage:
    """
    Factory — create a storage backend from its type name.

    Args:
        storage_type: One of "filesystem", "s3", "azure", "gcs"
        **kwargs: Fields of the matching StorageConfig subclass.

    Returns:
        A configured BlobStorage instance

    Raises:
        ConfigurationError: If storage_type is unknown
        ValueError: If the fields are invalid for the selected backend
    """
    return StorageConfig.from_dict({"type": storage_type, **kwargs}).create_storage()


__all__ = [
    "AzureStorageConfig",
    "BlobStorage",
    "FilesystemStorageConfig",
    "GCSStorageConfig",
    "S3StorageConfig",
    "StorageConfig",
    "StoredObject",
    "create_storage",
]
