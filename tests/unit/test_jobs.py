"""Unit tests for the snapshot and GC jobs' retention and cleanup contracts."""

import os
import shutil
import tempfile
from pathlib import Path

import pytest

from cairndb.jobs.gc import collect_garbage
from cairndb.jobs.snapshot import SnapshotBuilder
from tests.conftest import commit_events, init_users_projection, make_user_registry, user_created


class TestGarbageCollection:
    async def test_default_retention_keeps_three(self, storage):
        for n in range(1, 6):
            await storage.put_snapshot("1.0.0", n, b"s")

        result = await collect_garbage(storage, "1.0.0")

        assert result.snapshots_deleted == 2
        assert result.oldest_kept_snapshot == 3
        assert await storage.list_snapshots("1.0.0") == [3, 4, 5]

    async def test_at_the_retention_boundary_deletes_nothing(self, storage, monkeypatch):
        for n in (1, 2, 3):
            await storage.put_snapshot("1.0.0", n, b"s")

        calls = []
        real_delete = storage.delete_snapshots_before

        async def spy_delete(schema, number):
            calls.append(number)
            return await real_delete(schema, number)

        monkeypatch.setattr(storage, "delete_snapshots_before", spy_delete)

        result = await collect_garbage(storage, "1.0.0", keep_snapshots=3)

        assert result.snapshots_deleted == 0
        assert result.oldest_kept_snapshot == 1
        assert calls == []  # conservative: no delete round-trip when full


class TestSnapshotBuilder:
    async def test_build_uses_identifiable_tmp_and_wires_registry(
        self, storage, monkeypatch
    ):
        registry = make_user_registry()
        builder = SnapshotBuilder(storage, registry, init_schema=init_users_projection)
        assert builder.registry is registry

        await commit_events(storage, user_created(1, "ada"))

        seen = {}
        real_mkdtemp = tempfile.mkdtemp

        def spy_mkdtemp(prefix=None):
            seen["prefix"] = prefix
            return real_mkdtemp(prefix=prefix)

        monkeypatch.setattr("cairndb.jobs.snapshot.tempfile.mkdtemp", spy_mkdtemp)

        commit = await builder.build()
        assert commit == 1
        assert seen == {"prefix": "cairndb-snapshot-"}
        assert await storage.get_snapshot("1.0.0", 1)

    async def test_cleanup_failure_never_masks_the_real_error(self, storage):
        left = {}

        async def sabotage(db_path):
            left["dir"] = Path(db_path).parent
            os.chmod(left["dir"], 0o500)  # cleanup will fail: dir is read-only
            raise RuntimeError("schema exploded")

        builder = SnapshotBuilder(storage, make_user_registry(), init_schema=sabotage)
        try:
            with pytest.raises(RuntimeError, match="schema exploded"):
                await builder.build()
        finally:
            os.chmod(left["dir"], 0o755)
            shutil.rmtree(left["dir"], ignore_errors=True)
