"""Demo event handlers and schema for the demo entrypoint."""

import json

import aiosqlite
import structlog

from cairndb.client.registry import HandlerRegistry
from cairndb.core.log import SequencedEvent

logger = structlog.get_logger(__name__)

registry = HandlerRegistry()


async def init_schema(db_path: str) -> None:
    """Create the demo_events projection table if it does not exist."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS demo_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sequence    TEXT    NOT NULL,
                event_type  TEXT    NOT NULL,
                payload     TEXT    NOT NULL,
                effective_at TEXT   NOT NULL
            )
            """
        )
        await db.commit()
    logger.info("demo_schema_initialized")


@registry.handler("demo.ping")
async def handle_demo_ping(db: aiosqlite.Connection, entry: SequencedEvent) -> None:
    """Insert a row into demo_events for every demo.ping event."""
    await db.execute(
        "INSERT INTO demo_events (sequence, event_type, payload, effective_at) "
        "VALUES (:seq, :et, :payload, :ts)",
        {
            "seq": str(entry.sequence),
            "et": str(entry.event_type),
            "payload": json.dumps(entry.payload),
            "ts": entry.timestamp.to_iso(),
        },
    )
    logger.debug("demo_ping_handled", sequence=str(entry.sequence))
