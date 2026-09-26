"""Unit tests for the CLI: the reference loader and the commands."""

import asyncio
import sqlite3

import pytest
import typer
from typer.testing import CliRunner

from cairndb.cli import _load_ref, app
from cairndb.core.types import SequenceNumber
from cairndb.engine.logs import Log
from cairndb.storage.base import DEFAULT_SCHEMA_VERSION
from cairndb.storage.filesystem import FilesystemStorage
from tests.conftest import make_user_registry, user_created


def test_load_ref_returns_the_attribute():
    assert _load_ref("cairndb.core.types:SequenceNumber") is SequenceNumber


def test_load_ref_rejects_missing_colon():
    with pytest.raises(typer.BadParameter, match="module:attribute"):
        _load_ref("just.a.module.path")


def test_load_ref_splits_on_the_first_colon():
    # The right-hand side is the attribute verbatim — a stray colon there
    # must surface as a missing attribute, not re-split the module path.
    with pytest.raises(typer.BadParameter, match="has no attribute"):
        _load_ref("cairndb.core.types:SequenceNumber:extra")


def test_load_ref_rejects_missing_attribute():
    with pytest.raises(typer.BadParameter, match="has no attribute"):
        _load_ref("cairndb.core.types:Nope")


# ----------------------------------------------------------------------
# Commands, end to end on a filesystem bucket. Tests are sync: the CLI
# runs its own event loop (asyncio.run) per command.
# ----------------------------------------------------------------------


#: Loaded by the CLI through --handlers tests.unit.test_cli:registry
registry = make_user_registry()

HANDLERS = ["--handlers", "tests.unit.test_cli:registry"]
INIT = ["--init-schema", "tests.conftest:init_users_projection"]
runner = CliRunner()


def _append(root, log_name, *events):
    async def go():
        log = Log(FilesystemStorage(root), log_name)
        for event in events:
            await log.append(event)
        await log.close()

    asyncio.run(go())


@pytest.fixture
def bucket(temp_dir, monkeypatch):
    root = temp_dir / "ledger"
    monkeypatch.setenv("CAIRNDB_STORAGE_TYPE", "filesystem")
    monkeypatch.setenv("CAIRNDB_STORAGE_PATH", str(root))
    monkeypatch.delenv("CAIRNDB_LOG", raising=False)
    return root


def _snapshots(root, prefix=""):
    base = root / prefix / "snapshots" / f"v{DEFAULT_SCHEMA_VERSION}"
    return sorted(p.name for p in base.glob("*.sqlite")) if base.exists() else []


def test_snapshot_defaults_to_the_root_log(bucket):
    _append(bucket, None, user_created(1, "ada"))

    result = runner.invoke(app, ["snapshot", *HANDLERS, *INIT])

    assert result.exit_code == 0, result.output
    assert "root log" in result.output
    assert _snapshots(bucket) == ["000000000001.sqlite"]


def test_snapshot_of_a_named_log(bucket):
    _append(bucket, "users", user_created(1, "ada"), user_created(2, "bob"))

    result = runner.invoke(app, ["snapshot", "--log", "users", *HANDLERS, *INIT])

    assert result.exit_code == 0, result.output
    assert "log 'users'" in result.output
    assert _snapshots(bucket, "logs/users") == ["000000000002.sqlite"]
    assert _snapshots(bucket) == []  # the root log is untouched


def test_log_can_come_from_the_environment(bucket, monkeypatch):
    _append(bucket, "users", user_created(1, "ada"))
    monkeypatch.setenv("CAIRNDB_LOG", "users")

    result = runner.invoke(app, ["snapshot", *HANDLERS, *INIT])

    assert result.exit_code == 0, result.output
    assert _snapshots(bucket, "logs/users") == ["000000000001.sqlite"]


def test_gc_of_a_named_log_keeps_other_logs(bucket):
    for i in range(3):
        _append(bucket, "users", user_created(i, f"u{i}"))
        assert runner.invoke(app, ["snapshot", "--log", "users", *HANDLERS, *INIT]).exit_code == 0
    _append(bucket, None, user_created(9, "root"))
    assert runner.invoke(app, ["snapshot", *HANDLERS, *INIT]).exit_code == 0

    result = runner.invoke(app, ["gc", "--log", "users", "--keep-snapshots", "1"])

    assert result.exit_code == 0, result.output
    assert "2 snapshots" in result.output
    assert _snapshots(bucket, "logs/users") == ["000000000003.sqlite"]
    assert _snapshots(bucket) == ["000000000001.sqlite"]


def test_rebuild_of_a_named_log(bucket, temp_dir, monkeypatch):
    _append(bucket, "users", user_created(1, "ada"))
    db_path = temp_dir / "users.sqlite"
    monkeypatch.setenv("CAIRNDB_DB_PATH", str(db_path))

    result = runner.invoke(app, ["rebuild", "--log", "users", *HANDLERS, *INIT])

    assert result.exit_code == 0, result.output
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT id, name FROM users").fetchall() == [(1, "ada")]


def test_invalid_log_name_is_a_usage_error(bucket):
    result = runner.invoke(app, ["gc", "--log", "Not/Valid"])

    assert result.exit_code == 2
    assert "invalid log name" in result.output
