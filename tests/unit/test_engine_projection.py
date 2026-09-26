"""Unit tests for the projection facade (root and named logs)."""

import sqlite3
from pathlib import Path

import aiosqlite
import pytest

from cairndb import CairnDB
from cairndb.engine.projection import Projection
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

    with sqlite3.connect(old_path) as conn:
        rows = conn.execute("SELECT id, name FROM users").fetchall()
    assert rows == [(1, "ada")]
    await db.close()


async def test_projection_constructor_defaults_and_wiring(storage, temp_dir):
    proj = Projection(storage, "pview")
    assert proj.name == "pview"
    assert proj.config.schema_version == "1"
    assert proj.config.poll_interval_seconds == 5.0
    assert proj.config.db_path == "./pview.v1.sqlite"

    custom = Projection(
        storage,
        "pview",
        version="2",
        db_path=str(temp_dir / "x.sqlite"),
        poll_interval=0.25,
    )
    assert custom.config.schema_version == "2"
    assert custom.config.poll_interval_seconds == 0.25


async def test_projection_wait_for_forwards_default_timeout(db, temp_dir, monkeypatch):
    proj = users_projection(db, temp_dir)
    captured = []

    async def spy(sequence, timeout):
        captured.append((sequence, timeout))
        return True

    monkeypatch.setattr(proj._updater, "wait_for_sequence", spy)
    assert await proj.wait_for("000123") is True
    assert captured == [("000123", 30.0)]
    await db.close()


async def test_as_of_bootstraps_from_snapshot(db, temp_dir, storage):
    proj = users_projection(db, temp_dir)
    log = db.log()
    seq1 = await log.append(make_event("user.created", {"id": 1, "name": "ada"}))
    seq2 = await log.append(make_event("user.created", {"id": 2, "name": "bob"}))

    # Publish the state at commit 1 as a snapshot, then GC the log head:
    # from here on, only a snapshot bootstrap can reach either target.
    at_one = await proj.as_of(commit=seq1.commit, dest_path=str(temp_dir / "snap-src.sqlite"))
    await storage.put_snapshot(proj.config.schema_version, seq1.commit, Path(at_one).read_bytes())
    await storage.delete_commits_before(seq2.commit)

    dest = await proj.as_of(commit=seq2.commit, dest_path=str(temp_dir / "asof2.sqlite"))
    with sqlite3.connect(dest) as conn:
        rows = conn.execute("SELECT id, name FROM users ORDER BY id").fetchall()
    assert rows == [(1, "ada"), (2, "bob")]

    # Target exactly at the snapshot commit: the snapshot itself qualifies (<=).
    dest1 = await proj.as_of(commit=seq1.commit, dest_path=str(temp_dir / "asof1.sqlite"))
    with sqlite3.connect(dest1) as conn:
        rows = conn.execute("SELECT id, name FROM users").fetchall()
    assert rows == [(1, "ada")]
    await db.close()


async def test_projection_replays_with_a_given_registry(db, temp_dir):
    """The app and the snapshot job can share one module-level registry."""
    from tests.conftest import make_user_registry

    shared = make_user_registry()
    proj = db.projection(
        "shared",
        db_path=str(temp_dir / "shared.sqlite"),
        init_schema=init_users,
        registry=shared,
    )
    assert proj.registry is shared

    await db.log().append(make_event("user.created", {"id": 1, "name": "ada"}))
    await proj.refresh()
    with proj.connect() as conn:
        assert conn.execute("SELECT id, name FROM users").fetchall() == [(1, "ada")]
    await db.close()


async def test_projection_registry_cannot_be_replaced(storage):
    # Replay is wired to the registry at construction: a silently ignored
    # reassignment would be worse than an error.
    proj = Projection(storage, "p")
    with pytest.raises(AttributeError):
        proj.registry = None
