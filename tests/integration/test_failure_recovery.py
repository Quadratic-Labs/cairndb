"""Failure handling: handler errors, unknown events, stale files, recovery."""

import os

import pytest

from cairndb.client.projector import Projector
from cairndb.client.registry import HandlerRegistry
from cairndb.core.exceptions import ReplayError

from tests.conftest import (
    commit_events,
    init_users_projection,
    make_event,
    query_users,
    user_created,
)


async def test_handler_failure_rolls_back_the_commit(storage, client_config, temp_dir):
    """A failing handler aborts its whole commit; the projection is untouched."""
    registry = HandlerRegistry()

    @registry.handler("user.created")
    async def ok_handler(db, entry):
        await db.execute(
            "INSERT INTO users (id, name) VALUES (?, ?)",
            (entry.payload["id"], entry.payload["name"]),
        )

    @registry.handler("bad.event")
    async def bad_handler(db, entry):
        raise RuntimeError("handler exploded")

    # One commit containing a good event AND a bad event
    await commit_events(storage, user_created(1, "Alice"), make_event("bad.event"))

    await init_users_projection(client_config.db_path)
    projector = Projector(client_config, storage, registry)

    with pytest.raises(ReplayError):
        await projector.apply_updates()

    # Nothing from the failed commit is visible; projection still at start
    assert await query_users(client_config.db_path) == []
    assert await projector.get_current_sequence() is None


async def test_unknown_event_type_is_skipped(storage, user_registry, client_config):
    """Events without handlers are skipped (schema evolution), not fatal."""
    await commit_events(
        storage,
        user_created(1, "Alice"),
        make_event("future.event", {"whatever": True}),
        user_created(2, "Bob"),
    )

    await init_users_projection(client_config.db_path)
    projector = Projector(client_config, storage, user_registry)

    updated, _ = await projector.apply_updates()

    assert updated is True
    assert await query_users(client_config.db_path) == [(1, "Alice", ""), (2, "Bob", "")]


async def test_stale_new_file_does_not_block_update(
    storage, user_registry, client_config
):
    """A leftover projection.db.new from a crashed update is overwritten."""
    await init_users_projection(client_config.db_path)

    # Simulate a crash that left a stale .new file behind
    with open(client_config.new_db_path, "wb") as f:
        f.write(b"garbage from a crashed update")

    await commit_events(storage, user_created(1, "Alice"))

    projector = Projector(client_config, storage, user_registry)
    updated, _ = await projector.apply_updates()

    assert updated is True
    assert await query_users(client_config.db_path) == [(1, "Alice", "")]
    assert not os.path.exists(client_config.new_db_path)


async def test_failed_update_cleans_up_new_file(storage, client_config):
    """The .new file never survives a failed update."""
    registry = HandlerRegistry()

    @registry.handler("bad.event")
    async def bad_handler(db, entry):
        raise RuntimeError("boom")

    await commit_events(storage, make_event("bad.event"))
    await init_users_projection(client_config.db_path)

    projector = Projector(client_config, storage, registry)
    with pytest.raises(ReplayError):
        await projector.apply_updates()

    assert not os.path.exists(client_config.new_db_path)


async def test_rebuild_from_scratch_recovers_corruption(
    storage, user_registry, client_config
):
    """Deleting/corrupting the local projection is always recoverable."""
    await commit_events(storage, user_created(1, "Alice"))
    await commit_events(storage, user_created(2, "Bob"))

    await init_users_projection(client_config.db_path)
    projector = Projector(
        client_config, storage, user_registry, init_schema=init_users_projection
    )
    await projector.apply_updates()

    # Corrupt the local projection
    with open(client_config.db_path, "wb") as f:
        f.write(b"not a sqlite file")

    last = await projector.rebuild_from_scratch()

    assert last is not None
    assert await query_users(client_config.db_path) == [(1, "Alice", ""), (2, "Bob", "")]
