"""Unit tests for CairnDBClient connection provider."""

import os
import time
import tempfile

import pytest
import aiosqlite
from pathlib import Path
from sqlalchemy import text

from cairndb.client.config import ClientConfig
from cairndb.client.registry import HandlerRegistry
from cairndb.client.connection import CairnDBClient, _create_storage


# ---------------------------------------------------------------------------
# _create_storage factory
# ---------------------------------------------------------------------------


def test_create_storage_filesystem():
    """Filesystem storage is created when type is 'filesystem'."""
    config = ClientConfig(
        storage_type="filesystem",
        storage_path="/tmp/test_storage",
    )
    storage = _create_storage(config)

    from cairndb.storage.filesystem import FilesystemStorage

    assert isinstance(storage, FilesystemStorage)


def test_create_storage_missing_path_raises():
    """Filesystem storage without path raises ValueError."""
    config = ClientConfig(
        storage_type="filesystem",
        storage_path=None,
    )
    with pytest.raises(ValueError, match="filesystem storage requires"):
        _create_storage(config)


def test_create_storage_s3():
    """S3 storage is created when type is 's3'."""
    config = ClientConfig(
        storage_type="s3",
        storage_bucket="my-bucket",
    )
    storage = _create_storage(config)

    from cairndb.storage.s3 import S3Storage

    assert isinstance(storage, S3Storage)


# ---------------------------------------------------------------------------
# CairnDBClient initialization
# ---------------------------------------------------------------------------


def test_client_init():
    """Client can be instantiated with valid config."""
    config = ClientConfig(
        storage_type="filesystem",
        storage_path="/tmp/test_storage",
        db_path="/tmp/test.db",
    )
    registry = HandlerRegistry()
    client = CairnDBClient(config, registry)

    assert client.is_running is False
    assert client._engine is None
    assert client._db_mtime is None


# ---------------------------------------------------------------------------
# Engine lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_engine_creates_engine():
    """_ensure_engine creates engine on first call when db exists."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "test.db")

        async with aiosqlite.connect(db_path) as db:
            await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            await db.commit()

        config = ClientConfig(
            storage_type="filesystem",
            storage_path=temp_dir,
            db_path=db_path,
        )
        registry = HandlerRegistry()
        client = CairnDBClient(config, registry)

        assert client._engine is None

        await client._ensure_engine()

        assert client._engine is not None
        assert client._db_mtime is not None

        await client._engine.dispose()


@pytest.mark.asyncio
async def test_ensure_engine_raises_when_no_db():
    """_ensure_engine raises FileNotFoundError when projection is missing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config = ClientConfig(
            storage_type="filesystem",
            storage_path=temp_dir,
            db_path=str(Path(temp_dir) / "nonexistent.db"),
        )
        registry = HandlerRegistry()
        client = CairnDBClient(config, registry)

        with pytest.raises(FileNotFoundError, match="Projection database not found"):
            await client._ensure_engine()


@pytest.mark.asyncio
async def test_reconnect_on_mtime_change():
    """Engine is rebuilt when the file mtime changes."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "proj.db")

        async with aiosqlite.connect(db_path) as db:
            await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            await db.commit()

        config = ClientConfig(
            storage_type="filesystem",
            storage_path=temp_dir,
            db_path=db_path,
        )
        registry = HandlerRegistry()
        client = CairnDBClient(config, registry)

        # First call – creates engine
        await client._ensure_engine()
        first_engine = client._engine
        first_mtime = client._db_mtime

        # Second call with same mtime – reuses engine
        await client._ensure_engine()
        assert client._engine is first_engine

        # Touch file to change mtime
        time.sleep(0.05)
        os.utime(db_path)

        # Third call – detects mtime change, reconnects
        await client._ensure_engine()
        assert client._engine is not first_engine
        assert client._db_mtime != first_mtime

        await client._engine.dispose()


@pytest.mark.asyncio
async def test_stop_disposes_engine():
    """stop() disposes the engine and clears internal state."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "proj.db")

        async with aiosqlite.connect(db_path) as db:
            await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            await db.commit()

        config = ClientConfig(
            storage_type="filesystem",
            storage_path=temp_dir,
            db_path=db_path,
        )
        registry = HandlerRegistry()
        client = CairnDBClient(config, registry)

        await client._ensure_engine()
        assert client._engine is not None

        await client.stop()

        assert client._engine is None
        assert client._db_mtime is None


# ---------------------------------------------------------------------------
# get_session context manager
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_session_yields_session():
    """get_session yields a usable AsyncSession that can read data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "proj.db")

        async with aiosqlite.connect(db_path) as db:
            await db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            await db.execute("INSERT INTO items VALUES (1, 'hello')")
            await db.commit()

        config = ClientConfig(
            storage_type="filesystem",
            storage_path=temp_dir,
            db_path=db_path,
        )
        registry = HandlerRegistry()
        client = CairnDBClient(config, registry)

        async with client.get_session() as session:
            result = await session.execute(text("SELECT name FROM items WHERE id = 1"))
            row = result.fetchone()
            assert row[0] == "hello"

        await client.stop()


@pytest.mark.asyncio
async def test_get_session_raises_when_no_db():
    """get_session raises FileNotFoundError when the projection is missing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config = ClientConfig(
            storage_type="filesystem",
            storage_path=temp_dir,
            db_path=str(Path(temp_dir) / "missing.db"),
        )
        registry = HandlerRegistry()
        client = CairnDBClient(config, registry)

        with pytest.raises(FileNotFoundError):
            async with client.get_session() as session:
                pass  # pragma: no cover


# ---------------------------------------------------------------------------
# is_running property
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_is_running_reflects_updater_state():
    """is_running tracks start/stop state correctly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "proj.db")

        async with aiosqlite.connect(db_path) as db:
            await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            await db.execute("""
                CREATE TABLE _cairndb_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            await db.commit()

        config = ClientConfig(
            storage_type="filesystem",
            storage_path=temp_dir,
            db_path=db_path,
        )
        registry = HandlerRegistry()
        client = CairnDBClient(config, registry)

        assert client.is_running is False

        await client.start()
        assert client.is_running is True

        await client.stop()
        assert client.is_running is False
