"""Standalone demo: a committer and a reader sharing one commit log.

Usage::

    CAIRNDB_STORAGE_TYPE=filesystem CAIRNDB_STORAGE_PATH=./data \\
        python -m cairndb.client.run

The demo:

1. Loads ``ClientConfig.from_env()``
2. Registers the demo event handlers from ``demo_handlers``
3. Starts a ``BackgroundUpdater`` (reader) polling the log
4. Appends a ``demo.ping`` event every few seconds (writer)
5. Prints the projection row count after each poll, until SIGTERM/SIGINT

There is no server anywhere: the writer and reader only talk to storage.
"""

import asyncio
import signal

import aiosqlite
import structlog

from cairndb.client.config import ClientConfig
from cairndb.client.demo_handlers import init_schema, registry
from cairndb.client.updater import BackgroundUpdater
from cairndb.committer import Committer
from cairndb.core.log import Event
from cairndb.core.types import EventType, SchemaVersion, Timestamp

logger = structlog.get_logger(__name__)


async def _count_events(db_path: str) -> int:
    try:
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM demo_events")
            row = await cursor.fetchone()
            return row[0] if row else 0
    except aiosqlite.Error:
        return 0


async def run_demo() -> None:
    config = ClientConfig.from_env()
    storage = config.create_storage()

    await init_schema(config.db_path)

    updater = BackgroundUpdater(config, storage, registry)
    await updater.start()

    committer = Committer(storage)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    logger.info("demo_started", db_path=config.db_path)

    n = 0
    while not stop.is_set():
        event = Event(
            event_type=EventType("demo.ping"),
            timestamp=Timestamp.now(),
            payload={"n": n},
            schema_version=SchemaVersion("1.0.0"),
        )
        sequence = await committer.append(event)  # durable once returned
        count = await _count_events(config.db_path)
        logger.info("demo_tick", sequence=str(sequence), projected_rows=count)
        n += 1

        try:
            await asyncio.wait_for(stop.wait(), timeout=3.0)
        except TimeoutError:
            pass

    await committer.close()
    await updater.stop()
    logger.info("demo_stopped")


def main() -> None:
    """Entry point for ``python -m cairndb.client.run``."""
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ]
    )
    asyncio.run(run_demo())


if __name__ == "__main__":
    main()
