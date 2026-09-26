"""Regression tests for snapshot bootstrap.

The v1 projector discarded the snapshot returned by discovery and replayed
only post-snapshot WALs onto an empty database, silently losing all
pre-snapshot history. These tests pin the corrected behavior: a fresh
client bootstraps from the snapshot *as its base*, then replays the tail.
"""


from cairndb.client.projector import Projector
from cairndb.client.replay import ReplayEngine
from cairndb.core.types import SequenceNumber
from cairndb.storage.base import DEFAULT_SCHEMA_VERSION
from tests.conftest import (
    commit_events,
    init_users_projection,
    query_users,
    user_created,
)


async def build_snapshot(storage, registry, temp_dir, schema: str = DEFAULT_SCHEMA_VERSION) -> int:
    """Replay the whole log into a snapshot and upload it. Returns its number."""
    snap_path = str(temp_dir / "snapshot-build.db")
    await init_users_projection(snap_path)

    engine = ReplayEngine(storage, registry)
    last = await engine.replay(snap_path, after=0)
    assert last is not None

    with open(snap_path, "rb") as f:
        await storage.put_snapshot(schema, last.commit, f.read())
    return last.commit


class TestSnapshotBootstrap:
    async def test_fresh_client_sees_pre_snapshot_events(
        self, storage, user_registry, client_config, temp_dir
    ):
        """THE regression: events before the snapshot must not be lost."""
        # History: 2 commits, then a snapshot, then 1 more commit
        await commit_events(storage, user_created(1, "Alice"))
        await commit_events(storage, user_created(2, "Bob"))
        snapshot_number = await build_snapshot(storage, user_registry, temp_dir)
        assert snapshot_number == 2
        await commit_events(storage, user_created(3, "Carol"))

        # A fresh client (no local projection at all) bootstraps
        projector = Projector(client_config, storage, user_registry)
        updated, seq = await projector.apply_updates()

        assert updated is True
        assert seq == SequenceNumber(3, 0)
        # All three users visible — including those from before the snapshot
        assert await query_users(client_config.db_path) == [
            (1, "Alice", ""),
            (2, "Bob", ""),
            (3, "Carol", ""),
        ]

    async def test_bootstrap_with_snapshot_at_tail(
        self, storage, user_registry, client_config, temp_dir
    ):
        """Snapshot covering the whole log: no tail replay needed."""
        await commit_events(storage, user_created(1, "Alice"))
        await build_snapshot(storage, user_registry, temp_dir)

        projector = Projector(client_config, storage, user_registry)
        updated, seq = await projector.apply_updates()

        assert updated is True
        assert seq == SequenceNumber(1, 0)
        assert await query_users(client_config.db_path) == [(1, "Alice", "")]

    async def test_snapshot_for_other_schema_is_ignored(
        self, storage, user_registry, client_config, temp_dir
    ):
        """Snapshots of a different projection schema version don't apply."""
        await commit_events(storage, user_created(1, "Alice"))
        await build_snapshot(storage, user_registry, temp_dir, schema="9.9.9")
        await init_users_projection(client_config.db_path)  # needed w/o snapshot

        projector = Projector(client_config, storage, user_registry)
        updated, _ = await projector.apply_updates()

        # Replayed from the log, not the foreign snapshot
        assert updated is True
        assert await query_users(client_config.db_path) == [(1, "Alice", "")]

    async def test_rebuild_from_scratch_uses_snapshot(
        self, storage, user_registry, client_config, temp_dir
    ):
        await commit_events(storage, user_created(1, "Alice"))
        await commit_events(storage, user_created(2, "Bob"))
        await build_snapshot(storage, user_registry, temp_dir)
        await commit_events(storage, user_created(3, "Carol"))

        # Client had already applied everything, then rebuilds
        await init_users_projection(client_config.db_path)
        projector = Projector(client_config, storage, user_registry)
        await projector.apply_updates()

        last = await projector.rebuild_from_scratch()

        assert last == SequenceNumber(3, 0)
        assert len(await query_users(client_config.db_path)) == 3
