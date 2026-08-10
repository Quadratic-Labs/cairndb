"""Layer 3 of the engine: declarative projections.

A Projection binds a handler registry, a local SQLite file, and one log
(the root log or a named one) together behind a small object:

    proj = db.projection("orders_view", version="2", log="orders")

    @proj.on("order.placed")
    async def apply_placed(conn, entry): ...

    await proj.refresh()

It is a facade over the existing replay machinery (HandlerRegistry,
Projector, BackgroundUpdater); snapshots for a named log's projection live
under that log's namespace and bound bootstrap time exactly as for the
root log.
"""

import sqlite3
from collections.abc import Awaitable, Callable
from pathlib import Path

import structlog

from cairndb.client.config import ClientConfig
from cairndb.client.registry import EventHandler, HandlerRegistry
from cairndb.client.replay import ReplayEngine
from cairndb.client.updater import BackgroundUpdater
from cairndb.core.types import SequenceNumber
from cairndb.storage.base import BlobStorage

logger = structlog.get_logger(__name__)

SchemaInitializer = Callable[[str], Awaitable[None]]


class Projection:
    """A derived, read-only SQLite view of one log."""

    def __init__(
        self,
        storage: BlobStorage,
        name: str,
        *,
        version: str = "1",
        db_path: str | None = None,
        poll_interval: float = 5.0,
        init_schema: SchemaInitializer | None = None,
    ):
        """
        Args:
            storage: The log's storage view (already namespaced for a
                named log — pass ``Log.storage``)
            name: Projection name; also names the default db file
            version: Projection schema version — selects the
                ``snapshots/v{version}/`` prefix used for bootstrap
            db_path: Local SQLite path (default ``./{name}.v{version}.sqlite``)
            poll_interval: Background poll interval in seconds
            init_schema: Optional async callback creating application
                tables on a fresh, empty projection
        """
        self.storage = storage
        self.name = name
        self.registry = HandlerRegistry()
        self.config = ClientConfig(
            db_path=db_path or f"./{name}.v{version}.sqlite",
            schema_version=version,
            poll_interval_seconds=poll_interval,
        )
        self._init_schema = init_schema
        self._updater = BackgroundUpdater(
            self.config, storage, self.registry, init_schema=init_schema
        )

    # -- handler registration ------------------------------------------------

    def on(self, event_type: str) -> Callable[[EventHandler], EventHandler]:
        """Decorator registering the handler for `event_type`.

        Handlers are ``async def handler(conn, entry)`` receiving an
        aiosqlite connection and a SequencedEvent; one SQLite transaction
        wraps each commit.
        """
        return self.registry.handler(event_type)

    # -- refresh / background updates ----------------------------------------

    async def refresh(self) -> SequenceNumber | None:
        """Catch the projection up to the log tail now (one GET when idle).

        Returns:
            The projection's current sequence, or None while empty.
        """
        await self._updater.trigger_update()
        return await self._updater.projector.get_current_sequence()

    async def start(self) -> None:
        """Start the background polling updater."""
        await self._updater.start()

    async def stop(self) -> None:
        """Stop the background updater."""
        await self._updater.stop()

    async def wait_for(self, sequence: SequenceNumber | str, timeout: float = 30.0) -> bool:
        """Block until the projection has applied `sequence` (read-your-writes)."""
        return await self._updater.wait_for_sequence(str(sequence), timeout)

    # -- reading ---------------------------------------------------------------

    @property
    def path(self) -> str:
        """Path of the local SQLite projection file."""
        return self.config.db_path

    def connect(self) -> sqlite3.Connection:
        """Open a read-only connection to the projection.

        The projection is derived state: application code must never write
        to it, and mode=ro enforces that.
        """
        return sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)

    # -- time travel -------------------------------------------------------------

    async def as_of(self, commit: int, dest_path: str | None = None) -> str:
        """
        Build a throwaway projection of the log *through* commit `commit`.

        Bootstraps from the latest snapshot at or before the target when
        one exists (subject to GC retention: if the log head before the
        target was pruned and no usable snapshot remains, the build fails),
        then replays up to and including `commit`.

        Returns:
            Path of the newly built SQLite file.
        """
        dest = dest_path or f"{self.path}.asof-{commit:012d}"
        Path(dest).unlink(missing_ok=True)

        replay_engine = ReplayEngine(self.storage, self.registry)

        snapshots = await self.storage.list_snapshots(self.config.schema_version)
        base = max((n for n in snapshots if n <= commit), default=None)

        if base is not None:
            data = await self.storage.get_snapshot(self.config.schema_version, base)
            Path(dest).write_bytes(data)
        else:
            await replay_engine.initialize_metadata_table(dest)
            if self._init_schema:
                await self._init_schema(dest)

        await replay_engine.replay(dest, after=base or 0, end_at=commit)
        logger.info("as_of_built", name=self.name, commit=commit, dest=dest, base=base)
        return dest
