"""Commit replay engine."""

from collections.abc import AsyncIterator

import aiosqlite
import structlog

from cairndb.client.registry import HandlerRegistry
from cairndb.core.exceptions import ReplayError
from cairndb.core.log import Commit
from cairndb.core.types import SequenceNumber
from cairndb.storage.base import BlobStorage

logger = structlog.get_logger(__name__)


class ReplayEngine:
    """
    Engine for replaying commits to a SQLite database.

    Responsibilities:
    - Stream commits from storage (sequential GETs; the log is dense)
    - Apply events in order via registered handlers
    - One SQLite transaction per commit (the natural atomicity boundary)
    - Track last applied sequence in the _cairndb_metadata table
    """

    def __init__(self, storage: BlobStorage, registry: HandlerRegistry):
        """
        Initialize the replay engine.

        Args:
            storage: Blob storage backend
            registry: Event handler registry
        """
        self.storage = storage
        self.registry = registry
        logger.info("replay_engine_initialized")

    async def iter_commits(
        self, after: int, end_at: int | None = None
    ) -> AsyncIterator[Commit]:
        """
        Yield commits with numbers > `after`, in order, until a gap (404).

        Because the log is dense, a missing number means the tail is reached.

        Args:
            after: Yield commits strictly after this number
            end_at: Stop after this commit number (inclusive), if given
        """
        number = after + 1
        while end_at is None or number <= end_at:
            data = await self.storage.get_commit(number)
            if data is None:
                return
            yield Commit.from_msgpack(data)
            number += 1

    async def replay(
        self,
        db_path: str,
        after: int,
        end_at: int | None = None,
    ) -> SequenceNumber | None:
        """
        Replay commits (after, end_at] onto a database.

        Each commit is applied in its own transaction, so readers of a
        crashed replay never see a partially applied commit.

        Args:
            db_path: Path to SQLite database (opened in write mode)
            after: Replay commits strictly after this number
            end_at: Stop after this commit number (inclusive), if given

        Returns:
            Last applied sequence number, or None if nothing was applied

        Raises:
            ReplayError: If a handler or the database fails
        """
        last_sequence: SequenceNumber | None = None
        commit_count = 0
        event_count = 0

        async with aiosqlite.connect(db_path) as db:
            # Self-sufficient: app-created bases may lack the metadata table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS _cairndb_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            await db.commit()

            async for commit in self.iter_commits(after, end_at):
                try:
                    event_count += await self._apply_commit(db, commit)
                except ReplayError:
                    raise
                except Exception as e:
                    raise ReplayError(
                        f"Failed to replay commit {commit.number}: {e}"
                    ) from e

                last_sequence = commit.last_sequence
                commit_count += 1

        if commit_count:
            logger.info(
                "replay_completed",
                commits_applied=commit_count,
                events_applied=event_count,
                last_sequence=str(last_sequence),
            )
        return last_sequence

    async def _apply_commit(self, db: aiosqlite.Connection, commit: Commit) -> int:
        """Apply one commit inside one transaction. Returns events applied."""
        applied = 0

        await db.execute("BEGIN")  # pragma: no mutate (SQL keywords are case-insensitive)
        try:
            for sequenced in commit.sequenced_events():
                if not self.registry.has_handler(sequenced.event_type):
                    # Skip events without handlers (allows schema evolution)
                    logger.warning(
                        "no_handler_for_event",
                        event_type=sequenced.event_type,
                        sequence=str(sequenced.sequence),
                    )
                    continue

                handler = self.registry.get_handler(sequenced.event_type)
                try:
                    await handler(db, sequenced)
                except Exception as e:
                    raise ReplayError(
                        f"Handler failed for {sequenced.event_type} "
                        f"at {sequenced.sequence}: {e}"
                    ) from e
                applied += 1

            # Record progress inside the same transaction as the data
            await self._set_last_applied(db, commit.last_sequence)
            await db.commit()

        except Exception:
            await db.rollback()
            raise

        return applied

    # ------------------------------------------------------------------
    # Metadata tracking
    # ------------------------------------------------------------------

    async def initialize_metadata_table(self, db_path: str) -> None:
        """
        Initialize the metadata table for tracking replay state.

        Creates a _cairndb_metadata table if it doesn't exist.

        Args:
            db_path: Path to SQLite database
        """
        async with aiosqlite.connect(db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS _cairndb_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)

            await db.commit()

            logger.info("metadata_table_initialized", db_path=db_path)

    async def get_last_applied_sequence(self, db_path: str) -> SequenceNumber | None:
        """
        Get the last applied sequence number from the database.

        Args:
            db_path: Path to SQLite database

        Returns:
            Last applied sequence, or None if no commits have been applied
        """
        async with aiosqlite.connect(db_path) as db:
            try:
                cursor = await db.execute("""
                    SELECT value FROM _cairndb_metadata
                    WHERE key = 'last_applied_sequence'
                """)
            except aiosqlite.OperationalError:
                # No metadata table: nothing has ever been applied
                return None

            row = await cursor.fetchone()

            if row:
                return SequenceNumber.from_string(row[0])

            return None

    async def update_last_applied_sequence(
        self, db_path: str, sequence: SequenceNumber
    ) -> None:
        """
        Update the last applied sequence in the database.

        Args:
            db_path: Path to SQLite database
            sequence: Last applied sequence number
        """
        async with aiosqlite.connect(db_path) as db:
            await self._set_last_applied(db, sequence)
            await db.commit()

            logger.debug("last_applied_sequence_updated", sequence=str(sequence))

    @staticmethod
    async def _set_last_applied(
        db: aiosqlite.Connection, sequence: SequenceNumber
    ) -> None:
        await db.execute("""
            INSERT OR REPLACE INTO _cairndb_metadata (key, value)
            VALUES ('last_applied_sequence', ?)
        """, (str(sequence),))
