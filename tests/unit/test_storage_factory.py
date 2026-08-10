"""Unit tests for the storage factory."""

from unittest.mock import MagicMock, patch

import pytest

from cairndb.core.exceptions import ConfigurationError
from cairndb.storage import create_storage

# ------------------------------------------------------------------
# Filesystem — no external mocks needed
# ------------------------------------------------------------------


def test_create_filesystem_storage(tmp_path):
    storage = create_storage("filesystem", path=str(tmp_path))

    from cairndb.storage.filesystem import FilesystemStorage

    assert isinstance(storage, FilesystemStorage)


# ------------------------------------------------------------------
# S3
# ------------------------------------------------------------------


def test_create_s3_storage():
    with patch("cairndb.storage.s3.boto3.client"):
        storage = create_storage("s3", bucket="my-bucket", prefix="pfx")

    from cairndb.storage.s3 import S3Storage

    assert isinstance(storage, S3Storage)


def test_create_s3_storage_with_region_and_endpoint():
    with patch("cairndb.storage.s3.boto3.client") as mock_factory:
        create_storage(
            "s3",
            bucket="b",
            region="eu-west-1",
            endpoint_url="http://localhost:4566",
        )

    mock_factory.assert_called_once()
    kwargs = mock_factory.call_args[1]
    assert kwargs["region_name"] == "eu-west-1"
    assert kwargs["endpoint_url"] == "http://localhost:4566"


# ------------------------------------------------------------------
# Azure
# ------------------------------------------------------------------


def test_create_azure_storage():
    with patch("cairndb.storage.azure.BlobServiceClient") as mock_svc:
        mock_svc.from_connection_string.return_value = MagicMock()
        storage = create_storage(
            "azure",
            container="my-container",
            connection_string="fake-conn-str",
        )

    from cairndb.storage.azure import AzureBlobStorage

    assert isinstance(storage, AzureBlobStorage)


# ------------------------------------------------------------------
# GCS
# ------------------------------------------------------------------


def test_create_gcs_storage():
    with patch("cairndb.storage.gcs.gcs.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.bucket.return_value = MagicMock()

        storage = create_storage("gcs", bucket="my-bucket", project="my-project")

    from cairndb.storage.gcs import GCSStorage

    assert isinstance(storage, GCSStorage)


# ------------------------------------------------------------------
# Unknown type
# ------------------------------------------------------------------


def test_create_storage_unknown_type_raises():
    with pytest.raises(ConfigurationError, match="Unknown storage type"):
        create_storage("unknown_backend")
