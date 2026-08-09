"""Integration tests for the replay engine and projector."""

import pytest

from cairndb.client.projector import Projector
from cairndb.client.replay import ReplayEngine
from cairndb.core.types import SequenceNumber

from tests.conftest import (
    commit_events,
    init_users_projection,
    make_event,
    query_users,
    user_created,
)


class TestReplayEngine:
    async def test_replays_committed_events_in_order(
        self, storage, user_registry, temp_dir
    ):
        await commit_events(
            storage,
            user_created(1, "Alice"),
            user_created(2, "Bob"),
        )
        await commit_events(storage, user_created(3, "Carol"))  # commit 2

        db_path = str(temp_dir / "replay.db")
        await init_users_projection(db_path)

        engine = ReplayEngine(storage, user_registry)
        last = await engine.replay(db_path, after=0)

        assert last == SequenceNumber(2, 0)
        assert await query_users(db_path) == [
            (1, "Alice", ""),
            (2, "Bob", ""),
            (3, "Carol", ""),
        ]

    async def test_replay_after_skips_applied_commits(
        self, storage, user_registry, temp_dir
    ):
        await commit_events(storage, user_created(1, "Alice"))  # commit 1
        await commit_events(storage, user_created(2, "Bob"))  # commit 2

        db_path = str(temp_dir / "replay.db")
        await init_users_projection(db_path)

        engine = ReplayEngine(storage, user_registry)
        last = await engine.replay(db_path, after=1)

        assert last == SequenceNumber(2, 0)
        assert await query_users(db_path) == [(2, "Bob", "")]

    async def test_replay_end_at_bounds_replay(self, storage, user_registry, temp_dir):
        for i in range(1, 5):
            await commit_events(storage, user_created(i, f"user-{i}"))

        db_path = str(temp_dir / "replay.db")
        await init_users_projection(db_path)

        engine = ReplayEngine(storage, user_registry)
        last = await engine.replay(db_path, after=0, end_at=2)

        assert last == SequenceNumber(2, 0)
        assert len(await query_users(db_path)) == 2

    async def test_last_applied_tracked_per_commit(
        self, storage, user_registry, temp_dir
    ):
        await commit_events(storage, user_created(1, "Alice"), user_created(2, "Bob"))

        db_path = str(temp_dir / "replay.db")
        await init_users_projection(db_path)

        engine = ReplayEngine(storage, user_registry)
        await engine.replay(db_path, after=0)

        assert await engine.get_last_applied_sequence(db_path) == SequenceNumber(1, 1)


class TestProjector:
    async def test_apply_updates_full_cycle(self, storage, user_registry, client_config):
        await init_users_projection(client_config.db_path)
        projector = Projector(client_config, storage, user_registry)

        # Nothing to apply on an empty log
        updated, seq = await projector.apply_updates()
        assert updated is False and seq is None

        await commit_events(storage, user_created(1, "Alice"))

        updated, seq = await projector.apply_updates()
        assert updated is True
        assert seq == SequenceNumber(1, 0)
        assert await query_users(client_config.db_path) == [(1, "Alice", "")]

    async def test_incremental_updates(self, storage, user_registry, client_config):
        await init_users_projection(client_config.db_path)
        projector = Projector(client_config, storage, user_registry)

        await commit_events(storage, user_created(1, "Alice"))
        await projector.apply_updates()

        await commit_events(storage, user_created(2, "Bob"))
        await commit_events(
            storage, make_event("user.updated", {"id": 1, "name": "Alicia"})
        )

        updated, seq = await projector.apply_updates()

        assert updated is True
        assert seq == SequenceNumber(3, 0)
        assert await query_users(client_config.db_path) == [
            (1, "Alicia", ""),
            (2, "Bob", ""),
        ]

    async def test_has_updates_available(self, storage, user_registry, client_config):
        await init_users_projection(client_config.db_path)
        projector = Projector(client_config, storage, user_registry)

        assert await projector.has_updates_available() is False

        await commit_events(storage, user_created(1, "Alice"))
        assert await projector.has_updates_available() is True

        await projector.apply_updates()
        assert await projector.has_updates_available() is False
