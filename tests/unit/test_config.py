"""Tests for storage and client configuration."""

import inspect

import pytest
import typer.main

from cairndb import CairnDB
from cairndb.cli import app
from cairndb.client.config import ClientConfig
from cairndb.core.exceptions import ConfigurationError
from cairndb.engine.projection import Projection
from cairndb.jobs.snapshot import SnapshotBuilder
from cairndb.storage.base import DEFAULT_SCHEMA_VERSION
from cairndb.storage.config import (
    AzureStorageConfig,
    FilesystemStorageConfig,
    GCSStorageConfig,
    S3StorageConfig,
    StorageConfig,
)


class TestStorageConfig:
    def test_from_dict_defaults_to_filesystem(self, temp_dir):
        config = StorageConfig.from_dict({"path": str(temp_dir)})
        assert isinstance(config, FilesystemStorageConfig)

    def test_from_dict_unknown_type(self):
        with pytest.raises(ConfigurationError, match="Unknown storage type"):
            StorageConfig.from_dict({"type": "ftp"})

    def test_from_dict_rejects_foreign_fields(self):
        with pytest.raises(ValueError, match="s3"):
            StorageConfig.from_dict({"type": "s3", "bucket": "b", "container": "c"})

    def test_filesystem_requires_path(self):
        with pytest.raises(ValueError, match="path"):
            FilesystemStorageConfig(path="")

    def test_s3_requires_bucket(self):
        with pytest.raises(ValueError, match="bucket"):
            S3StorageConfig(bucket="")

    def test_azure_requires_container_and_credentials(self):
        with pytest.raises(ValueError, match="container"):
            AzureStorageConfig(container="", connection_string="cs")

        with pytest.raises(ValueError, match="connection_string"):
            AzureStorageConfig(container="c")

    def test_gcs_requires_bucket(self):
        with pytest.raises(ValueError, match="bucket"):
            GCSStorageConfig(bucket="")

    def test_create_storage_filesystem(self, temp_dir):
        from cairndb.storage.filesystem import FilesystemStorage

        config = FilesystemStorageConfig(path=str(temp_dir))
        storage = config.create_storage()

        assert isinstance(storage, FilesystemStorage)

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("CAIRNDB_STORAGE_TYPE", "s3")
        monkeypatch.setenv("CAIRNDB_S3_BUCKET", "my-bucket")
        monkeypatch.setenv("CAIRNDB_STORAGE_PREFIX", "app1")

        config = StorageConfig.from_env()

        assert isinstance(config, S3StorageConfig)
        assert config.bucket == "my-bucket"
        assert config.prefix == "app1"

    def test_from_env_gcs_bucket(self, monkeypatch):
        monkeypatch.setenv("CAIRNDB_STORAGE_TYPE", "gcs")
        monkeypatch.setenv("CAIRNDB_GCS_BUCKET", "gcs-bucket")
        monkeypatch.setenv("CAIRNDB_S3_BUCKET", "s3-bucket")

        config = StorageConfig.from_env()

        assert isinstance(config, GCSStorageConfig)
        assert config.bucket == "gcs-bucket"  # the GCS variable wins

    def test_from_env_gcs_bucket_falls_back_to_s3_variable(self, monkeypatch):
        # Deployments predating CAIRNDB_GCS_BUCKET keep working.
        monkeypatch.setenv("CAIRNDB_STORAGE_TYPE", "gcs")
        monkeypatch.delenv("CAIRNDB_GCS_BUCKET", raising=False)
        monkeypatch.setenv("CAIRNDB_S3_BUCKET", "legacy-bucket")

        assert StorageConfig.from_env().bucket == "legacy-bucket"

    def test_from_env_missing_required_field(self, monkeypatch):
        monkeypatch.setenv("CAIRNDB_STORAGE_TYPE", "filesystem")
        monkeypatch.delenv("CAIRNDB_STORAGE_PATH", raising=False)

        with pytest.raises(ValueError, match="path"):
            StorageConfig.from_env()


class TestClientConfig:
    def test_defaults(self):
        config = ClientConfig()

        assert config.storage is None
        assert config.db_path == "./projection.db"
        assert config.poll_interval_seconds == 5.0
        assert config.schema_version == DEFAULT_SCHEMA_VERSION == "1"

    def test_new_db_path(self):
        config = ClientConfig(db_path="/data/proj.db")
        assert config.new_db_path == "/data/proj.db.new"

    def test_create_storage_requires_storage(self):
        config = ClientConfig()
        with pytest.raises(ValueError, match="storage"):
            config.create_storage()

    def test_poll_interval_bounds(self):
        with pytest.raises(ValueError, match="poll_interval_seconds"):
            ClientConfig(poll_interval_seconds=0.0)

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("CAIRNDB_STORAGE_TYPE", "filesystem")
        monkeypatch.setenv("CAIRNDB_STORAGE_PATH", "/data/ledger")
        monkeypatch.setenv("CAIRNDB_DB_PATH", "/data/proj.db")
        monkeypatch.setenv("CAIRNDB_POLL_INTERVAL", "2.5")
        monkeypatch.setenv("CAIRNDB_SCHEMA_VERSION", "2.0.0")

        config = ClientConfig.from_env()

        assert isinstance(config.storage, FilesystemStorageConfig)
        assert config.storage.path == "/data/ledger"
        assert config.db_path == "/data/proj.db"
        assert config.poll_interval_seconds == 2.5
        assert config.schema_version == "2.0.0"

    def test_from_dict(self):
        config = ClientConfig.from_dict(
            {"storage": {"type": "filesystem", "path": "/x"}, "db_path": "/y.db"}
        )
        assert isinstance(config.storage, FilesystemStorageConfig)
        assert config.storage.path == "/x"
        assert config.db_path == "/y.db"


def test_every_component_defaults_to_the_same_schema_version():
    """Snapshots written with defaults must be found by clients with defaults.

    Regression: Projection defaulted to "1" (snapshots/v1/) while the CLI,
    ClientConfig and SnapshotBuilder defaulted to "1.0.0" (snapshots/v1.0.0/),
    so default snapshots were silently never used.
    """

    def default(fn, name):
        return inspect.signature(fn).parameters[name].default

    cli = typer.main.get_command(app)
    cli_defaults = {
        name: next(p.default for p in cli.commands[name].params if p.name == "schema_version")
        for name in ("snapshot", "gc")
    }

    assert {
        "ClientConfig": ClientConfig().schema_version,
        "Projection": default(Projection.__init__, "version"),
        "CairnDB.projection": default(CairnDB.projection, "version"),
        "SnapshotBuilder": default(SnapshotBuilder.__init__, "schema_version"),
        "cli snapshot": cli_defaults["snapshot"],
        "cli gc": cli_defaults["gc"],
    } == dict.fromkeys(
        [
            "ClientConfig",
            "Projection",
            "CairnDB.projection",
            "SnapshotBuilder",
            "cli snapshot",
            "cli gc",
        ],
        DEFAULT_SCHEMA_VERSION,
    )
