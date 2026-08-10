"""SQLAlchemy connection provider with lazy reconnection.

CairnDBClient is the main entry point for applications.  It owns a
read-only SQLAlchemy engine that is transparently rebuilt whenever the
background updater atomically swaps the projection file (detected via
mtime change on the next session request).
"""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from cairndb.client.config import ClientConfig
from cairndb.client.registry import HandlerRegistry
from cairndb.client.updater import BackgroundUpdater
from cairndb.utils.filesystem import get_file_mtime

logger = structlog.get_logger(__name__)


def _create_storage(config: ClientConfig):
    """Create a storage backend from client config.

    Uses the ``create_storage`` factory from ``cairndb.storage`` to
    support all backends (filesystem, s3, azure, gcs).

    Raises:
        ValueError: If required storage fields are missing.
        ConfigurationError: If the storage type is unknown.
    """
    from cairndb.storage import create_storage

    config.validate_storage()

    kwargs: dict[str, str] = {}
    if config.storage_type == "filesystem":
        kwargs["path"] = config.storage_path  # type: ignore[assignment]
    elif config.storage_type == "s3":
        if config.storage_bucket:
            kwargs["bucket"] = config.storage_bucket
        if config.storage_prefix:
            kwargs["prefix"] = config.storage_prefix
        if config.storage_region:
            kwargs["region"] = config.storage_region
        if config.storage_endpoint_url:
            kwargs["endpoint_url"] = config.storage_endpoint_url
    elif config.storage_type == "azure":
        if config.storage_container:
            kwargs["container"] = config.storage_container
        if config.storage_azure_connection_string:
            kwargs["connection_string"] = config.storage_azure_connection_string
        if config.storage_azure_account_url:
            kwargs["account_url"] = config.storage_azure_account_url
        if config.storage_prefix:
            kwargs["prefix"] = config.storage_prefix
    elif config.storage_type == "gcs":
        if config.storage_bucket:
            kwargs["bucket"] = config.storage_bucket
        if config.storage_prefix:
            kwargs["prefix"] = config.storage_prefix
        if config.storage_credentials_path:
            kwargs["credentials_path"] = config.storage_credentials_path

    return create_storage(config.storage_type, **kwargs)


class CairnDBClient:
    """Main entry point for CairnDB client applications.

    Responsibilities:
    - Starts/stops the background polling updater.
    - Provides read-only SQLAlchemy sessions via ``get_session()``.
    - Transparently reconnects when the projection file is swapped.
    - Exposes ``wait_for_sequence()`` for read-your-writes consistency.

    Typical usage::

        registry = HandlerRegistry()

        @registry.handler("user.created")
        async def handle_user_created(db, entry):
            await db.execute("INSERT INTO users ...")

        config = ClientConfig(
            storage_type="filesystem",
            storage_path="./data",
            db_path="./projection.db",
        )

        client = CairnDBClient(config, registry)
        await client.start()

        async with client.get_session() as session:
            result = await session.execute(select(User))

        await client.stop()
    """

    def __init__(
        self, config: ClientConfig, registry: HandlerRegistry, init_schema=None
    ) -> None:
        self._config = config
        self._registry = registry
        self._storage = _create_storage(config)
        self._updater = BackgroundUpdater(
            config, self._storage, registry, init_schema=init_schema
        )
        self._engine: AsyncEngine | None = None
        self._db_mtime: float | None = None

        logger.info(
            "cairndb_client_initialized",
            db_path=config.db_path,
            storage_type=config.storage_type,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the background projection updater."""
        await self._updater.start()
        logger.info("cairndb_client_started")

    async def stop(self) -> None:
        """Stop the updater and dispose the SQLAlchemy engine."""
        await self._updater.stop()

        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._db_mtime = None

        logger.info("cairndb_client_stopped")

    # ------------------------------------------------------------------
    # Session access
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def get_session(self) -> AsyncIterator[AsyncSession]:
        """Yield a read-only ``AsyncSession`` against the current projection.

        If the projection file has been atomically swapped since the last
        session (detected by comparing ``st_mtime``), the engine is silently
        disposed and recreated before the session is opened.

        Raises:
            FileNotFoundError: When the projection database does not exist.
        """
        await self._ensure_engine()

        session = AsyncSession(self._engine)
        try:
            yield session
        finally:
            await session.close()

    # ------------------------------------------------------------------
    # Consistency helpers
    # ------------------------------------------------------------------

    async def wait_for_sequence(self, sequence: str, timeout: float = 30.0) -> bool:
        """Block until *sequence* is visible in the projection.

        Internally triggers polling updates while waiting.  Useful
        immediately after writing an event when you need to query the
        result right away.

        Args:
            sequence: The sequence number string returned by the write API.
            timeout: Maximum seconds to wait.

        Returns:
            ``True`` once visible; ``False`` if *timeout* elapsed first.
        """
        return await self._updater.wait_for_sequence(sequence, timeout)

    async def trigger_update(self) -> tuple[bool, str | None]:
        """Force an immediate projection update outside the normal poll cycle.

        Returns:
            ``(applied, new_sequence)`` – whether new data was applied, and
            the resulting sequence string (or ``None``).
        """
        return await self._updater.trigger_update()

    @property
    def is_running(self) -> bool:
        """Whether the background updater is currently active."""
        return self._updater.is_running

    # ------------------------------------------------------------------
    # Internal – engine management
    # ------------------------------------------------------------------

    async def _ensure_engine(self) -> None:
        """Reconnect if the projection is missing or its mtime changed."""
        if not os.path.exists(self._config.db_path):
            raise FileNotFoundError(
                f"Projection database not found: {self._config.db_path}"
            )

        current_mtime = get_file_mtime(self._config.db_path)

        if self._engine is None or current_mtime != self._db_mtime:
            await self._reconnect()

    async def _reconnect(self) -> None:
        """Dispose the current engine and open a fresh read-only one.

        The SQLite URI ``?mode=ro`` flag ensures the database is opened
        read-only at the driver level – any write attempt raises
        ``OperationalError``.  The ``uri=true`` parameter tells SQLAlchemy
        to pass the filename as a SQLite URI.
        """
        if self._engine:
            await self._engine.dispose()
            logger.debug("engine_disposed")

        db_path = self._config.db_path
        url = f"sqlite+aiosqlite:///file:{db_path}?uri=true&mode=ro"

        self._engine = create_async_engine(url)
        self._db_mtime = get_file_mtime(db_path)

        logger.info(
            "engine_reconnected",
            db_path=db_path,
            mtime=self._db_mtime,
        )
