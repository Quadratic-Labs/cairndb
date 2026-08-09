"""Blob storage abstraction for CairnDB."""

from cairndb.storage.base import BlobStorage, StoredObject
from cairndb.core.exceptions import ConfigurationError




def create_storage(storage_type: str, **kwargs) -> BlobStorage:
    """
    Factory — create a storage backend from its type name.

    Args:
        storage_type: One of "filesystem", "s3", "azure", "gcs"
        **kwargs: Backend-specific options passed directly to the constructor.
                  See each backend class for the full parameter list.

    Returns:
        A configured BlobStorage instance

    Raises:
        ConfigurationError: If storage_type is unknown
    """
    if storage_type == "filesystem":
        from cairndb.storage.filesystem import FilesystemStorage

        return FilesystemStorage(kwargs["path"])

    if storage_type == "s3":
        from cairndb.storage.s3 import S3Storage

        return S3Storage(**kwargs)

    if storage_type == "azure":
        from cairndb.storage.azure import AzureBlobStorage

        return AzureBlobStorage(**kwargs)

    if storage_type == "gcs":
        from cairndb.storage.gcs import GCSStorage

        return GCSStorage(**kwargs)

    raise ConfigurationError(f"Unknown storage type: '{storage_type}'")


from cairndb.storage.config import StorageConfig  # noqa: E402  (avoids import cycle)

__all__ = ["BlobStorage", "StorageConfig", "StoredObject", "create_storage"]
