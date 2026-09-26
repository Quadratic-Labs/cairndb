"""SQLite projection updater with atomic swap."""

import asyncio
import os
from collections.abc import Awaitable, Callable
from pathlib import Path

import structlog

from cairndb.client.config import ClientConfig
from cairndb.client.discovery import DiscoveryService
from cairndb.client.registry import HandlerRegistry
from cairndb.client.replay import ReplayEngine
from cairndb.core.exceptions import ReplayError
from cairndb.core.types import SequenceNumber
from cairndb.storage.base import BlobStorage
from cairndb.utils.filesystem import atomic_swap, copy_database

logger = structlog.get_logger(__name__)

# Async callback creating the application's projection tables at db_path.
# Called whenever a brand-new empty base is created (no snapshot available).
SchemaInitializer = Callable[[str], Awaitable[None]]


class Projector:
    """
    Manages SQLite projection updates with atomic swaps.

    Update workflow:

    1. Build the new database:

       - fresh start with a snapshot available: download it as the base
         (the snapshot carries schema and data)
       - otherwise: copy projection.db -> projection.db.new (COW if available),
         or create an empty base (metadata + init_schema) if nothing exists

    2. Replay new commits onto the new database (one transaction each)
    3. Atomically rename: projection.db.new -> projection.db

    This ensures readers never see partial updates. Updates are serialized
    with an internal lock, so a background poll and a manual trigger can
    never double-apply commits.
    """

    def __init__(
        self,
        config: ClientConfig,
        storage: BlobStorage,
        registry: HandlerRegistry,
        init_schema: SchemaInitializer | None = None,
    ):
        """
        Initialize the projector.

        Args:
            config: Client configuration
            storage: Blob storage backend
            registry: Event handler registry
            init_schema: Optional callback creating application tables on a
                fresh, empty projection (not needed when snapshots exist or
                handlers create their own tables)
        """
        self.config = config
        self.storage = storage
        self.registry = registry
        self.init_schema = init_schema
        self.discovery = DiscoveryService(storage)
        self.replay_engine = ReplayEngine(storage, registry)
        self._update_lock = asyncio.Lock()

        logger.info(
            "projector_initialized",
            db_path=config.db_path,
            use_reflink=config.use_reflink,
        )

    async def initialize_projection(self) -> None:
        """
        Initialize a new, empty projection database.

        Creates the database file, the metadata table, and — when an
        init_schema callback was provided — the application tables.

        Raises:
            FileExistsError: If projection already exists
        """
        if os.path.exists(self.config.db_path):
            raise FileExistsError(f"Projection already exists: {self.config.db_path}")

        await self.replay_engine.initialize_metadata_table(self.config.db_path)
        if self.init_schema:
            await self.init_schema(self.config.db_path)

        logger.info("projection_initialized", db_path=self.config.db_path)

    async def apply_updates(self) -> tuple[bool, SequenceNumber | None]:
        """
        Bring the projection up to the log tail, atomically.

        Steady state costs a single GET (404 = already caught up).

        Returns:
            Tuple of (updates_applied, current_sequence)

        Raises:
            ReplayError: If update fails
        """
        async with self._update_lock:
            return await self._apply_updates_locked()

    async def _apply_updates_locked(self) -> tuple[bool, SequenceNumber | None]:
        current = await self.get_current_sequence()

        snapshot_number: int | None = None
        if current is None:
            # Bootstrap: the snapshot decides where the tail starts. Do NOT
            # rely on GET log/1 — GC may have pruned the head of the log.
            snapshot_number = await self.discovery.find_latest_snapshot(
                self.config.schema_version
            )

        if snapshot_number is None and not await self.discovery.has_new_commits(current):
            # Cheap steady-state poll: one GET on the next commit number
            logger.debug("no_updates_available")
            return False, current

        new_db_path = self.config.new_db_path

        try:
            after, bootstrapped = await self._prepare_new_db(
                current, snapshot_number, new_db_path
            )

            last_sequence = await self.replay_engine.replay(new_db_path, after=after)

            if last_sequence is None:
                if bootstrapped:
                    # Snapshot already covers the whole log; its metadata
                    # carries the sequence it represents.
                    last_sequence = await self.replay_engine.get_last_applied_sequence(
                        new_db_path
                    )
                if last_sequence is None:
                    # The commit seen by the poll vanished (GC race); no-op.
                    os.remove(new_db_path)
                    return False, current

            atomic_swap(new_db_path, self.config.db_path)

            logger.info(
                "projection_updated",
                previous_sequence=str(current) if current else None,
                new_sequence=str(last_sequence),
            )
            return True, last_sequence

        except Exception as e:
            if os.path.exists(new_db_path):
                os.remove(new_db_path)

            logger.error("projection_update_failed", error=str(e))
            raise ReplayError(f"Failed to update projection: {e}") from e

    async def _prepare_new_db(
        self,
        current: SequenceNumber | None,
        snapshot_number: int | None,
        new_db_path: str,
    ) -> tuple[int, bool]:
        """
        Create the base for the new projection.

        Returns:
            (after, bootstrapped): the commit number replay should continue
            after, and whether the base came from a snapshot.
        """
        if current is None and snapshot_number is not None:
            data = await self.storage.get_snapshot(
                self.config.schema_version, snapshot_number
            )
            await asyncio.to_thread(Path(new_db_path).write_bytes, data)

            logger.info(
                "bootstrapped_from_snapshot",
                schema=self.config.schema_version,
                snapshot_number=snapshot_number,
                size_bytes=len(data),
            )
            return snapshot_number, True

        # Incremental update (or fresh start without snapshot): copy the
        # current projection and replay on top of it.
        if os.path.exists(self.config.db_path):
            copy_database(
                self.config.db_path,
                new_db_path,
                use_reflink=self.config.use_reflink,
            )
        else:
            # Nothing local and no snapshot: start from an empty projection.
            await self.replay_engine.initialize_metadata_table(new_db_path)
            if self.init_schema:
                await self.init_schema(new_db_path)
        return (current.commit if current else 0), False

    async def rebuild_from_scratch(self) -> SequenceNumber | None:
        """
        Rebuild the projection from the ledger, discarding local state.

        Uses the latest snapshot as the base when available, then replays
        the log tail. Useful for corruption recovery, schema migrations,
        and testing.

        Returns:
            Final sequence number

        Raises:
            ReplayError: If rebuild fails
        """
        async with self._update_lock:
            logger.info("rebuilding_projection_from_scratch")

            if os.path.exists(self.config.db_path):
                os.remove(self.config.db_path)

            updated, last_sequence = await self._apply_updates_locked()

            # pragma-note: updated=True always leaves the file, updated=False
            # never creates it — the two conditions cannot disagree.
            if not updated and not os.path.exists(self.config.db_path):  # pragma: no mutate
                # Empty ledger: leave a valid empty projection behind
                await self.replay_engine.initialize_metadata_table(self.config.db_path)
                if self.init_schema:
                    await self.init_schema(self.config.db_path)

            logger.info(
                "projection_rebuilt",
                last_sequence=str(last_sequence) if last_sequence else None,
            )
            return last_sequence

    async def get_current_sequence(self) -> SequenceNumber | None:
        """
        Get the current projection sequence.

        Returns:
            Current sequence number, or None if projection is empty
        """
        if not os.path.exists(self.config.db_path):
            return None

        return await self.replay_engine.get_last_applied_sequence(self.config.db_path)

    async def has_updates_available(self) -> bool:
        """
        Check if new commits are available (single GET).

        Returns:
            True if new commits can be applied
        """
        current = await self.get_current_sequence()
        return await self.discovery.has_new_commits(current)
