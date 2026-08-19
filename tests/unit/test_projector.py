"""Unit tests for the projector's bootstrap, steady state, and rebuild paths."""

import os
import sqlite3
from dataclasses import replace

import pytest

from cairndb.client.projector import Projector
from tests.conftest import (
    commit_events,
    init_users_projection,
    make_user_registry,
    user_created,
)


@pytest.fixture
def projector(client_config, storage):
    return Projector(
        client_config, storage, make_user_registry(), init_schema=init_users_projection
    )


def _tables(db_path: str) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return {r[0] for r in rows}


async def test_initialize_projection(client_config, storage):
    registry = make_user_registry()
    projector = Projector(
        client_config, storage, registry, init_schema=init_users_projection
    )
    assert projector.registry is registry

    await projector.initialize_projection()
    assert os.path.exists(client_config.db_path)
    assert {"_cairndb_metadata", "users"} <= _tables(client_config.db_path)

    with pytest.raises(FileExistsError):
        await projector.initialize_projection()


async def test_steady_state_poll_is_read_only(projector, storage, monkeypatch):
    await commit_events(storage, user_created(1, "a"))
    updated, seq = await projector.apply_updates()
    assert updated is True

    calls = []
    real_prepare = projector._prepare_new_db

    async def spy_prepare(*args, **kwargs):
        calls.append(1)
        return await real_prepare(*args, **kwargs)

    monkeypatch.setattr(projector, "_prepare_new_db", spy_prepare)

    updated, seq2 = await projector.apply_updates()
    assert updated is False
    assert seq2 == seq
    assert calls == []  # caught up: one GET, no copy, no replay


async def test_vanished_commit_poll_is_a_noop(projector, storage, monkeypatch):
    await commit_events(storage, user_created(1, "a"))
    _, seq = await projector.apply_updates()

    # The poll claims a new commit but replay finds none (GC race).
    async def always_new(current):
        return True

    monkeypatch.setattr(projector.discovery, "has_new_commits", always_new)

    updated, seq2 = await projector.apply_updates()
    assert updated is False
    assert seq2 == seq
    assert not os.path.exists(projector.config.new_db_path)


async def test_copy_forwards_reflink_setting(client_config, storage, monkeypatch):
    config = replace(client_config, use_reflink=False)
    projector = Projector(
        config, storage, make_user_registry(), init_schema=init_users_projection
    )
    await commit_events(storage, user_created(1, "a"))
    await projector.apply_updates()  # fresh start: empty base, no copy yet
    await commit_events(storage, user_created(2, "b"))

    seen = {}
    from cairndb.utils.filesystem import copy_database as real_copy

    def spy_copy(src, dst, use_reflink=True):
        seen["use_reflink"] = use_reflink
        return real_copy(src, dst, use_reflink=use_reflink)

    monkeypatch.setattr("cairndb.client.projector.copy_database", spy_copy)

    updated, _ = await projector.apply_updates()  # incremental: copies the base
    assert updated is True
    assert seen == {"use_reflink": False}


async def test_rebuild_on_empty_ledger_leaves_valid_projection(
    projector, client_config
):
    await projector.initialize_projection()
    assert await projector.rebuild_from_scratch() is None
    assert os.path.exists(client_config.db_path)
    assert {"_cairndb_metadata", "users"} <= _tables(client_config.db_path)
