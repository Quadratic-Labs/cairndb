"""Tests for the snapshot builder and garbage collection jobs."""

import pytest

from cairndb.client.projector import Projector
from cairndb.core.exceptions import ReplayError
from cairndb.jobs.gc import collect_garbage
from cairndb.jobs.snapshot import SnapshotBuilder
from tests.conftest import (
    commit_events,
    init_users_projection,
    query_users,
    user_created,
)


class TestSnapshotBuilder:
    async def test_build_uploads_replayed_state(self, storage, user_registry):
        await commit_events(storage, user_created(1, "Alice"))
        await commit_events(storage, user_created(2, "Bob"))

        builder = SnapshotBuilder(
            storage, user_registry, init_schema=init_users_projection
        )
        commit = await builder.build()

        assert commit == 2
        assert await storage.find_latest_snapshot("1.0.0") == 2

    async def test_snapshot_bootstraps_a_fresh_client(
        self, storage, user_registry, client_config
    ):
        """A snapshot produced by the job is a valid client base."""
        await commit_events(storage, user_created(1, "Alice"))
        builder = SnapshotBuilder(
            storage, user_registry, init_schema=init_users_projection
        )
        await builder.build()
        await commit_events(storage, user_created(2, "Bob"))  # tail after snapshot

        projector = Projector(client_config, storage, user_registry)
        updated, _ = await projector.apply_updates()

        assert updated is True
        assert await query_users(client_config.db_path) == [
            (1, "Alice", ""),
            (2, "Bob", ""),
        ]

    async def test_point_in_time_snapshot(self, storage, user_registry):
        for i in range(1, 4):
            await commit_events(storage, user_created(i, f"user-{i}"))

        builder = SnapshotBuilder(
            storage, user_registry, init_schema=init_users_projection
        )
        commit = await builder.build(end_at=2)

        assert commit == 2

    async def test_duplicate_build_is_harmless(self, storage, user_registry):
        await commit_events(storage, user_created(1, "Alice"))

        builder = SnapshotBuilder(
            storage, user_registry, init_schema=init_users_projection
        )
        assert await builder.build() == 1
        assert await builder.build() == 1  # put-if-absent: second is a no-op

    async def test_empty_log_raises(self, storage, user_registry):
        builder = SnapshotBuilder(storage, user_registry)

        with pytest.raises(ReplayError):
            await builder.build()


class TestGarbageCollection:
    async def _snapshot_at(self, storage, number: int) -> None:
        await storage.put_snapshot("1.0.0", number, f"snap-{number}".encode())

    async def test_keeps_newest_snapshots(self, storage):
        for n in [10, 20, 30, 40]:
            await self._snapshot_at(storage, n)

        result = await collect_garbage(storage, "1.0.0", keep_snapshots=2)

        assert result.snapshots_deleted == 2
        assert result.oldest_kept_snapshot == 30
        assert await storage.list_snapshots("1.0.0") == [30, 40]

    async def test_no_deletion_when_under_limit(self, storage):
        await self._snapshot_at(storage, 10)

        result = await collect_garbage(storage, "1.0.0", keep_snapshots=3)

        assert result.snapshots_deleted == 0
        assert await storage.list_snapshots("1.0.0") == [10]

    async def test_log_untouched_without_prune_flag(self, storage, user_registry):
        for i in range(1, 4):
            await commit_events(storage, user_created(i, f"u{i}"))
        await self._snapshot_at(storage, 3)

        result = await collect_garbage(storage, "1.0.0", keep_snapshots=1)

        assert result.commits_deleted == 0
        assert await storage.list_commits() == [1, 2, 3]

    async def test_prune_log_removes_covered_commits(self, storage, user_registry):
        for i in range(1, 5):
            await commit_events(storage, user_created(i, f"u{i}"))
        await self._snapshot_at(storage, 2)
        await self._snapshot_at(storage, 3)

        result = await collect_garbage(
            storage, "1.0.0", keep_snapshots=1, prune_log=True
        )

        # Snapshot 2 deleted; snapshot 3 kept; commits 1..3 covered by it
        assert result.oldest_kept_snapshot == 3
        assert result.commits_deleted == 3
        assert await storage.list_commits() == [4]

    async def test_pruned_log_still_bootstraps_clients(
        self, storage, user_registry, client_config
    ):
        """After GC with pruning, a fresh client still sees full state."""
        for i in range(1, 4):
            await commit_events(storage, user_created(i, f"user-{i}"))

        builder = SnapshotBuilder(
            storage, user_registry, init_schema=init_users_projection
        )
        await builder.build()  # snapshot at 3
        await commit_events(storage, user_created(4, "Dave"))  # commit 4

        await collect_garbage(storage, "1.0.0", keep_snapshots=1, prune_log=True)
        assert await storage.list_commits() == [4]

        projector = Projector(client_config, storage, user_registry)
        updated, _ = await projector.apply_updates()

        assert updated is True
        assert len(await query_users(client_config.db_path)) == 4
