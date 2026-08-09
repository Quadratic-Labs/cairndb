"""Write events for the simple_app example.

Usage:
    # Terminal 1 — append some events (durable once each append returns):
    python -m examples.simple_app.writer

    # Terminal 2 — run the client to query the projection:
    python -m examples.simple_app.client

There is no server: the committer talks directly to storage. Run as many
writer processes as you like — the bucket serializes them via put-if-absent.
"""

import asyncio
import uuid

from cairndb import Committer, Event, EventType, SchemaVersion, Timestamp
from cairndb.storage.filesystem import FilesystemStorage

# ---------------------------------------------------------------------------
# Configuration — must match the client's STORAGE_PATH
# ---------------------------------------------------------------------------

STORAGE_PATH = "./data"


def _event(event_type: str, payload: dict) -> Event:
    return Event(
        event_type=EventType(event_type),
        timestamp=Timestamp.now(),
        payload=payload,
        schema_version=SchemaVersion("1.0.0"),
    )


async def main() -> None:
    storage = FilesystemStorage(STORAGE_PATH)
    alice_id = str(uuid.uuid4())
    bob_id = str(uuid.uuid4())

    async with Committer(storage) as committer:
        sequences = await committer.append_many(
            [
                _event(
                    "user.created",
                    {"id": alice_id, "name": "Alice", "email": "alice@example.com"},
                ),
                _event(
                    "user.created",
                    {"id": bob_id, "name": "Bob", "email": "bob@example.com"},
                ),
                _event(
                    "post.created",
                    {
                        "id": str(uuid.uuid4()),
                        "author_id": alice_id,
                        "title": "Hello",
                        "body": "First post!",
                    },
                ),
            ]
        )

    print(f"Committed {len(sequences)} events, last sequence: {sequences[-1]}")


if __name__ == "__main__":
    asyncio.run(main())
