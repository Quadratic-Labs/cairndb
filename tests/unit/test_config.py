"""Tests for storage and client configuration."""

import pytest

from cairndb.client.config import ClientConfig
from cairndb.storage.config import StorageConfig


class TestStorageConfig:
    def test_default_is_filesystem(self):
        config = StorageConfig()
        assert config.type == "filesystem"

    def test_filesystem_requires_path(self):
        config = StorageConfig(type="filesystem")
        with pytest.raises(ValueError, match="path"):
            config.validate_config()

    def test_s3_requires_bucket(self):
        config = StorageConfig(type="s3")
        with pytest.raises(ValueError, match="bucket"):
            config.validate_config()

    def test_azure_requires_container_and_credentials(self):
        with pytest.raises(ValueError, match="container"):
            StorageConfig(type="azure").validate_config()

        with pytest.raises(ValueError, match="connection_string"):
            StorageConfig(type="azure", container="c").validate_config()

    def test_gcs_requires_bucket(self):
        config = StorageConfig(type="gcs")
        with pytest.raises(ValueError, match="bucket"):
            config.validate_config()

    def test_create_storage_filesystem(self, temp_dir):
        from cairndb.storage.filesystem import FilesystemStorage

        config = StorageConfig(type="filesystem", path=str(temp_dir))
        storage = config.create_storage()

        assert isinstance(storage, FilesystemStorage)

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("CAIRNDB_STORAGE_TYPE", "s3")
        monkeypatch.setenv("CAIRNDB_S3_BUCKET", "my-bucket")
        monkeypatch.setenv("CAIRNDB_STORAGE_PREFIX", "app1")

        config = StorageConfig.from_env()

        assert config.type == "s3"
        assert config.bucket == "my-bucket"
        assert config.prefix == "app1"


class TestClientConfig:
    def test_defaults(self):
        config = ClientConfig()

        assert config.storage_type == "filesystem"
        assert config.db_path == "./projection.db"
        assert config.poll_interval_seconds == 5.0
        assert config.schema_version == "1.0.0"

    def test_new_db_path(self):
        config = ClientConfig(db_path="/data/proj.db")
        assert config.new_db_path == "/data/proj.db.new"

    def test_validate_storage_filesystem_requires_path(self):
        config = ClientConfig(storage_type="filesystem")
        with pytest.raises(ValueError, match="storage_path"):
            config.validate_storage()

    def test_validate_storage_s3_requires_bucket(self):
        config = ClientConfig(storage_type="s3")
        with pytest.raises(ValueError, match="storage_bucket"):
            config.validate_storage()

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("CAIRNDB_STORAGE_TYPE", "filesystem")
        monkeypatch.setenv("CAIRNDB_STORAGE_PATH", "/data/ledger")
        monkeypatch.setenv("CAIRNDB_DB_PATH", "/data/proj.db")
        monkeypatch.setenv("CAIRNDB_POLL_INTERVAL", "2.5")
        monkeypatch.setenv("CAIRNDB_SCHEMA_VERSION", "2.0.0")

        config = ClientConfig.from_env()

        assert config.storage_path == "/data/ledger"
        assert config.db_path == "/data/proj.db"
        assert config.poll_interval_seconds == 2.5
        assert config.schema_version == "2.0.0"

    def test_from_dict(self):
        config = ClientConfig.from_dict(
            {"storage_type": "filesystem", "storage_path": "/x", "db_path": "/y.db"}
        )
        assert config.storage_path == "/x"
