"""Snapshot builder - creates SQLite snapshots from log replay.

Designed to run as a scheduled serverless job. Building is idempotent and
race-safe: replay is deterministic and the upload is put-if-absent, so two
jobs racing produce one object with identical content either way.
"""

import asyncio
import os
import shutil
import tempfile
from pathlib import Path

import structlog

from cairndb.client.projector import SchemaInitializer
from cairndb.client.registry import HandlerRegistry
from cairndb.client.replay import ReplayEngine
from cairndb.core.exceptions import ReplayError
from cairndb.storage.base import BlobStorage

logger = structlog.get_logger(__name__)


class SnapshotBuilder:
    """
    Builds SQLite snapshots by replaying the commit log from scratch.

    Always replays from the beginning of the log to guarantee determinism —
    two builds over the same commits produce equivalent databases.

    Workflow:
    1. Create a temporary SQLite database (metadata table + app schema)
    2. Replay all commits (up to end_at if specified)
    3. Upload the database with put-if-absent under its commit number
    4. Clean up temporary files
    """

    def __init__(
        self,
        storage: BlobStorage,
        registry: HandlerRegistry,
        init_schema: SchemaInitializer | None = None,
        schema_version: str = "1.0.0",
    ):
        """
        Initialize the snapshot builder.

        Args:
            storage: Blob storage backend (read commits, write snapshots)
            registry: Event handler registry with all handlers registered
            init_schema: Optional async callback creating app tables at a
                given db path before replay
            schema_version: Projection schema version — selects the
                snapshots/v{version}/ prefix
        """
        self.storage = storage
        self.registry = registry
        self.init_schema = init_schema
        self.schema_version = schema_version
        self.replay_engine = ReplayEngine(storage, registry)

        logger.info(
            "snapshot_builder_initialized",
            schema_version=schema_version,
            has_schema_initializer=init_schema is not None,
        )

    async def build(self, end_at: int | None = None) -> int:
        """
        Build a snapshot by replaying the log from the beginning.

        Args:
            end_at: Optional last commit number to include (point-in-time
                snapshot). None means everything currently in the log.

        Returns:
            The commit number the snapshot represents.

        Raises:
            ReplayError: If the log is empty or replay fails
            StorageError: If reads or the upload fail
        """
        logger.info(
            "snapshot_build_started",
            schema_version=self.schema_version,
            end_at=end_at,
        )

        tmp_dir = tempfile.mkdtemp(prefix="cairndb-snapshot-")
        db_path = os.path.join(tmp_dir, "snapshot.db")

        try:
            await self.replay_engine.initialize_metadata_table(db_path)
            if self.init_schema:
                await self.init_schema(db_path)

            last_sequence = await self.replay_engine.replay(
                db_path, after=0, end_at=end_at
            )

            if last_sequence is None:
                raise ReplayError("No commits available to build a snapshot from")

            data = await asyncio.to_thread(Path(db_path).read_bytes)

            created = await self.storage.put_snapshot(
                self.schema_version, last_sequence.commit, data
            )

            logger.info(
                "snapshot_build_completed",
                commit=last_sequence.commit,
                size_bytes=len(data),
                created=created,  # False = identical snapshot already existed
            )
            return last_sequence.commit

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
