"""Unit tests for the projection facade (root and named logs)."""

import aiosqlite
import pytest

from cairndb import CairnDB
from tests.conftest import USERS_DDL, make_event


async def init_users(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute(USERS_DDL)
        await conn.commit()


@pytest.fixture
def db(storage):
    return CairnDB(storage)


def users_projection(db, temp_dir, log=None):
    proj = db.projection(
        "users_view",
        db_path=str(temp_dir / "users_view.sqlite"),
        log=log,
        poll_interval=0.1,
        init_schema=init_users,
    )

    @proj.on("user.created")
    async def handle_created(conn, entry):
        await conn.execute(
            "INSERT INTO users (id, name) VALUES (?, ?)",
            (entry.payload["id"], entry.payload["name"]),
        )

    return proj


async def test_projection_refresh_and_read(db, temp_dir):
    proj = users_projection(db, temp_dir)
    await db.log().append(make_event("user.created", {"id": 1, "name": "ada"}))
    await proj.refresh()

    with proj.connect() as conn:
        rows = conn.execute("SELECT id, name FROM users").fetchall()
    assert rows == [(1, "ada")]
    await db.close()


async def test_projection_is_read_only(db, temp_dir):
    proj = users_projection(db, temp_dir)
    await db.log().append(make_event("user.created", {"id": 1, "name": "ada"}))
    await proj.refresh()

    import sqlite3

    with proj.connect() as conn, pytest.raises(sqlite3.OperationalError):
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'x')")
    await db.close()


async def test_projection_of_named_log(db, temp_dir):
    proj = users_projection(db, temp_dir, log="people")
    await db.log("people").append(make_event("user.created", {"id": 7, "name": "bob"}))
    # An event on the root log must NOT reach this projection.
    await db.log().append(make_event("user.created", {"id": 8, "name": "eve"}))
    await proj.refresh()

    with proj.connect() as conn:
        rows = conn.execute("SELECT id, name FROM users").fetchall()
    assert rows == [(7, "bob")]
    await db.close()


async def test_projection_wait_for_read_your_writes(db, temp_dir):
    proj = users_projection(db, temp_dir)
    seq = await db.log().append(make_event("user.created", {"id": 1, "name": "ada"}))
    assert await proj.wait_for(seq, timeout=10)

    with proj.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 1
    await db.close()


async def test_projection_as_of_time_travel(db, temp_dir):
    proj = users_projection(db, temp_dir)
    log = db.log()
    seq1 = await log.append(make_event("user.created", {"id": 1, "name": "ada"}))
    await log.append(make_event("user.created", {"id": 2, "name": "bob"}))

    old_path = await proj.as_of(commit=seq1.commit)

    import sqlite3

    with sqlite3.connect(old_path) as conn:
        rows = conn.execute("SELECT id, name FROM users").fetchall()
    assert rows == [(1, "ada")]
    await db.close()
