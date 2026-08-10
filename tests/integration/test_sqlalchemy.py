"""SQLAlchemy integration: read-only sessions, reconnect, read-your-writes."""

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from cairndb.client.connection import CairnDBClient
from cairndb.client.updater import BackgroundUpdater
from cairndb.committer import Committer
from tests.conftest import (
    commit_events,
    init_users_projection,
    user_created,
)


async def test_write_then_query_via_sqlalchemy(storage, user_registry, client_config):
    """Full path: committer -> log -> updater -> projection -> SQLAlchemy."""
    await init_users_projection(client_config.db_path)

    client = CairnDBClient(client_config, user_registry)
    await client.start()

    try:
        async with Committer(storage) as committer:
            seq = await committer.append(user_created(1, "Alice", "alice@example.com"))

        assert await client.wait_for_sequence(str(seq), timeout=5.0)

        async with client.get_session() as session:
            result = await session.execute(
                text("SELECT name, email FROM users WHERE id = 1")
            )
            row = result.fetchone()
            assert row == ("Alice", "alice@example.com")
    finally:
        await client.stop()


async def test_read_only_enforcement(storage, user_registry, client_config):
    """Application sessions cannot write to the projection."""
    await init_users_projection(client_config.db_path)

    client = CairnDBClient(client_config, user_registry)
    try:
        async with client.get_session() as session:
            with pytest.raises(OperationalError):
                await session.execute(
                    text("INSERT INTO users (id, name) VALUES (99, 'intruder')")
                )
                await session.commit()
    finally:
        await client.stop()


async def test_wait_for_sequence_timeout(storage, user_registry, client_config):
    """wait_for_sequence returns False when the sequence never arrives."""
    await init_users_projection(client_config.db_path)

    client = CairnDBClient(client_config, user_registry)
    try:
        reached = await client.wait_for_sequence("000000000099:000000", timeout=0.3)
        assert reached is False
    finally:
        await client.stop()


async def test_updater_applies_in_background(storage, user_registry, client_config):
    """The background updater picks up commits without manual triggers."""
    await init_users_projection(client_config.db_path)

    updater = BackgroundUpdater(client_config, storage, user_registry)
    await updater.start()

    try:
        await commit_events(storage, user_created(1, "Alice"))

        for _ in range(50):  # up to 5s at 0.1s poll interval
            if updater.update_count > 0:
                break
            await asyncio.sleep(0.1)

        assert updater.update_count > 0
    finally:
        await updater.stop()


async def test_trigger_update_reports_nothing_new(storage, user_registry, client_config):
    await init_users_projection(client_config.db_path)

    updater = BackgroundUpdater(client_config, storage, user_registry)
    updated, seq = await updater.trigger_update()

    assert updated is False
    assert seq is None
