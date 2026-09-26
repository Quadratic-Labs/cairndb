"""Client configuration."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cairndb.storage.base import DEFAULT_SCHEMA_VERSION, BlobStorage
from cairndb.storage.config import StorageConfig


@dataclass
class ClientConfig:
    """
    Client configuration for CairnDB.

    Configures:
    - Storage backend (where the commit log and snapshots live)
    - Projection database path
    - Updater behavior (polling interval)
    - Copy-on-write settings

    ``storage`` may be left None when the storage backend is created
    separately and passed alongside the config (as BackgroundUpdater
    and Projector accept).

    Attributes:
        storage: Storage backend config (see cairndb.storage.config).
        db_path: Path to the SQLite projection database.
        poll_interval_seconds: Polling interval for checking new commits
            (one GET when idle; 0 exclusive to 3600).
        use_reflink: Use copy-on-write (reflink) when available.
        schema_version: Projection schema version; selects the
            snapshots/v{version}/ prefix used for bootstrap.
    """

    storage: StorageConfig | None = None
    db_path: str = "./projection.db"
    poll_interval_seconds: float = 5.0
    use_reflink: bool = True
    schema_version: str = DEFAULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not 0.0 < self.poll_interval_seconds <= 3600.0:
            raise ValueError("poll_interval_seconds must be in (0, 3600]")

    def create_storage(self) -> BlobStorage:
        """Create the storage backend from the ``storage`` config."""
        if self.storage is None:
            raise ValueError("no storage configured: set ClientConfig.storage")
        return self.storage.create_storage()

    @property
    def db_path_obj(self) -> Path:
        """Get db_path as Path object."""
        return Path(self.db_path)

    @property
    def new_db_path(self) -> str:
        """Get new database path for atomic swap."""
        return f"{self.db_path}.new"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ClientConfig:
        """Create config from a dictionary.

        ``data["storage"]`` may be a StorageConfig or a dict with a
        ``type`` discriminator (see :meth:`StorageConfig.from_dict`).
        """
        data = dict(data)
        storage = data.pop("storage", None)
        if storage is not None and not isinstance(storage, StorageConfig):
            storage = StorageConfig.from_dict(storage)
        return cls(storage=storage, **data)

    @classmethod
    def from_env(cls) -> ClientConfig:
        """Build configuration from ``CAIRNDB_*`` environment variables.

        Storage variables are handled by :meth:`StorageConfig.from_env`;
        client variables::

            CAIRNDB_DB_PATH        - SQLite projection path
            CAIRNDB_POLL_INTERVAL  - Poll interval seconds
            CAIRNDB_SCHEMA_VERSION - Projection schema version
        """
        return cls(
            storage=StorageConfig.from_env(),
            db_path=os.environ.get("CAIRNDB_DB_PATH", "./projection.db"),
            poll_interval_seconds=float(os.environ.get("CAIRNDB_POLL_INTERVAL", "5.0")),
            schema_version=os.environ.get("CAIRNDB_SCHEMA_VERSION", DEFAULT_SCHEMA_VERSION),
        )
