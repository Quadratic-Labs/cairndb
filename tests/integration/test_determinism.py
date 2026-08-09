"""Determinism and idempotency of replay.

Replaying the same log must always produce the same SQLite state, and
re-running apply_updates against an up-to-date projection must be a no-op.
"""

import pytest

from cairndb.client.projector import Projector
from cairndb.client.replay import ReplayEngine

from tests.conftest import (
    commit_events,
    init_users_projection,
    make_event,
    query_users,
    user_created,
)


async def test_replay_determinism(storage, user_registry, temp_dir):
    """Two independent replays of the same log yield identical projections."""
    await commit_events(
        storage,
        user_created(1, "Alice", "alice@example.com"),
        user_created(2, "Bob"),
    )
    await commit_events(storage, make_event("user.updated", {"id": 1, "name": "Alicia"}))
    await commit_events(storage, user_created(3, "Carol"))

    results = []
    engines = []
    for name in ("a.db", "b.db"):
        db_path = str(temp_dir / name)
        await init_users_projection(db_path)
        engine = ReplayEngine(storage, user_registry)
        last = await engine.replay(db_path, after=0)
        engines.append((engine, db_path, last))
        results.append(await query_users(db_path))

    assert results[0] == results[1]
    assert engines[0][2] == engines[1][2]  # same final sequence
    # Metadata tracking is identical too
    assert await engines[0][0].get_last_applied_sequence(
        engines[0][1]
    ) == await engines[1][0].get_last_applied_sequence(engines[1][1])


async def test_apply_updates_is_idempotent(storage, user_registry, client_config):
    """Re-polling an up-to-date projection changes nothing."""
    await init_users_projection(client_config.db_path)
    projector = Projector(client_config, storage, user_registry)

    await commit_events(storage, user_created(1, "Alice"))
    updated, seq1 = await projector.apply_updates()
    assert updated is True
    state1 = await query_users(client_config.db_path)

    for _ in range(3):
        updated, seq = await projector.apply_updates()
        assert updated is False
        assert seq == seq1

    assert await query_users(client_config.db_path) == state1


async def test_rebuild_matches_incremental(storage, user_registry, client_config):
    """A from-scratch rebuild equals the incrementally maintained projection."""
    await init_users_projection(client_config.db_path)
    projector = Projector(
        client_config, storage, user_registry, init_schema=init_users_projection
    )

    for i in range(1, 6):
        await commit_events(storage, user_created(i, f"user-{i}"))
        await projector.apply_updates()  # incremental, one commit at a time

    incremental_state = await query_users(client_config.db_path)

    last = await projector.rebuild_from_scratch()

    assert await query_users(client_config.db_path) == incremental_state
    assert last == await projector.get_current_sequence()
