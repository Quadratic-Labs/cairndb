"""Application-defined event handlers.

Each handler maps one event type to the SQL that mutates the projection.
Handlers receive a raw ``aiosqlite`` connection (not an ORM session) so they
can issue precise, low-level statements during log replay.
"""

from cairndb.client.registry import HandlerRegistry

registry = HandlerRegistry()


@registry.handler("user.created")
async def handle_user_created(db, entry):
    await db.execute(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
        (entry.payload["id"], entry.payload["name"], entry.payload["email"]),
    )


@registry.handler("user.updated")
async def handle_user_updated(db, entry):
    await db.execute(
        "UPDATE users SET name = ?, email = ? WHERE id = ?",
        (entry.payload["name"], entry.payload["email"], entry.payload["id"]),
    )


@registry.handler("post.created")
async def handle_post_created(db, entry):
    await db.execute(
        "INSERT INTO posts (id, author_id, title, body) VALUES (?, ?, ?, ?)",
        (
            entry.payload["id"],
            entry.payload["author_id"],
            entry.payload["title"],
            entry.payload["body"],
        ),
    )
