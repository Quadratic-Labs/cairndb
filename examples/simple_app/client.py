"""CairnDB client for the simple_app example.

Usage:
    # Terminal 1 — write some events:
    python -m examples.simple_app.writer

    # Terminal 2 — run this client to query the projection:
    python -m examples.simple_app.client

The client starts the background updater, waits for the projection to catch
up, and then prints all users.
"""

import asyncio
import os

import aiosqlite
from sqlalchemy import select

from cairndb.client.config import ClientConfig
from cairndb.client.connection import CairnDBClient
from cairndb.storage.config import FilesystemStorageConfig
from examples.simple_app.handlers import registry
from examples.simple_app.models import Base, User

# ---------------------------------------------------------------------------
# Configuration — must match the writer's STORAGE_PATH
# ---------------------------------------------------------------------------

STORAGE_PATH = "./data"
DB_PATH = "./projection.db"


async def main() -> None:
    config = ClientConfig(
        storage=FilesystemStorageConfig(path=STORAGE_PATH),
        db_path=DB_PATH,
        poll_interval_seconds=2.0,
    )

    # Bootstrap the projection schema on first run
    if not os.path.exists(DB_PATH):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.executescript("""
                CREATE TABLE users (
                    id   TEXT NOT NULL PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL
                );
                CREATE TABLE posts (
                    id        TEXT NOT NULL PRIMARY KEY,
                    author_id TEXT NOT NULL,
                    title     TEXT NOT NULL,
                    body      TEXT NOT NULL
                );
                CREATE TABLE _cairndb_metadata (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            """)
            await db.commit()

    client = CairnDBClient(config, registry)

    # Start background polling
    await client.start()

    # Trigger one immediate update so we don't have to wait for the poll interval
    await client.trigger_update()

    # Query
    async with client.get_session() as session:
        result = await session.execute(select(User).order_by(User.id))
        users = result.scalars().all()

        if users:
            print("Users in projection:")
            for user in users:
                print(f"  {user.id}: {user.name} <{user.email}>")
        else:
            print("No users yet. Write some events with examples.simple_app.writer first.")

    await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
