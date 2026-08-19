"""Shared fixtures and helpers for CairnDB tests."""

import shutil
import tempfile
from pathlib import Path

import aiosqlite
import pytest

from cairndb.client.config import ClientConfig
from cairndb.client.registry import HandlerRegistry
from cairndb.committer import Committer
from cairndb.core.log import Event
from cairndb.core.types import EventType, SchemaVersion, SequenceNumber, Timestamp
from cairndb.storage.config import FilesystemStorageConfig
from cairndb.storage.filesystem import FilesystemStorage


def make_event(
    event_type: str = "user.created",
    payload: dict | None = None,
) -> Event:
    """Create a test event."""
    return Event(
        event_type=EventType(event_type),
        timestamp=Timestamp.now(),
        payload=payload if payload is not None else {},
        schema_version=SchemaVersion("1.0.0"),
    )


def make_user_registry() -> HandlerRegistry:
    """Registry with handlers for a simple users projection."""
    registry = HandlerRegistry()

    @registry.handler("user.created")
    async def handle_user_created(db, entry):
        await db.execute(
            "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
            (
                entry.payload["id"],
                entry.payload["name"],
                entry.payload.get("email", ""),
            ),
        )

    @registry.handler("user.updated")
    async def handle_user_updated(db, entry):
        await db.execute(
            "UPDATE users SET name = ? WHERE id = ?",
            (entry.payload["name"], entry.payload["id"]),
        )

    return registry


USERS_DDL = """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL DEFAULT ''
    )
"""


async def init_users_projection(db_path: str) -> None:
    """Create the users projection database (metadata + app schema)."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS _cairndb_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        await db.execute(USERS_DDL)
        await db.commit()


def user_created(user_id: int, name: str, email: str = "") -> Event:
    """Shorthand for a user.created event."""
    return make_event("user.created", {"id": user_id, "name": name, "email": email})


async def commit_events(
    storage: FilesystemStorage, *events: Event, one_per_commit: bool = False
) -> list[SequenceNumber]:
    """Append events to the log via a throwaway committer."""
    async with Committer(storage) as committer:
        if one_per_commit:
            return [await committer.append(e) for e in events]
        return await committer.append_many(list(events))


async def query_users(db_path: str) -> list[tuple]:
    """Read all users from a projection database."""
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT id, name, email FROM users ORDER BY id")
        return list(await cursor.fetchall())


@pytest.fixture
def temp_dir():
    """A temporary directory, removed after the test."""
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def storage(temp_dir):
    """Filesystem storage rooted in the temp dir."""
    return FilesystemStorage(temp_dir / "ledger")


@pytest.fixture
def client_config(temp_dir):
    """Client config pointing at the temp ledger and projection."""
    return ClientConfig(
        storage=FilesystemStorageConfig(path=str(temp_dir / "ledger")),
        db_path=str(temp_dir / "projection.db"),
        poll_interval_seconds=0.1,
    )


@pytest.fixture
def user_registry():
    """Registry with users-table handlers."""
    return make_user_registry()
