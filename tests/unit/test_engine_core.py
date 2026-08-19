"""Unit tests for the CairnDB engine facade itself: configure, wiring, lifecycle."""

import pytest

from cairndb import CairnDB
from cairndb.storage.config import FilesystemStorageConfig
from cairndb.storage.filesystem import FilesystemStorage


@pytest.fixture
def db(storage):
    return CairnDB(storage)


async def test_configure_from_storage_config(temp_dir):
    cfg = FilesystemStorageConfig(path=str(temp_dir / "ledger"))
    db = CairnDB.configure(cfg)
    assert isinstance(db.storage, FilesystemStorage)
    assert db.storage.root_path == (temp_dir / "ledger").absolute()


async def test_configure_from_nested_and_flat_dicts(temp_dir):
    nested = CairnDB.configure(
        {"storage": {"type": "filesystem", "path": str(temp_dir / "a")}}
    )
    assert isinstance(nested.storage, FilesystemStorage)
    assert nested.storage.root_path == (temp_dir / "a").absolute()

    # A dict without the "storage" key is taken as the config itself.
    flat = CairnDB.configure({"type": "filesystem", "path": str(temp_dir / "b")})
    assert isinstance(flat.storage, FilesystemStorage)
    assert flat.storage.root_path == (temp_dir / "b").absolute()


async def test_projection_facade_wiring(db, temp_dir):
    proj = db.projection("pv")
    assert proj.name == "pv"
    assert proj.config.schema_version == "1"
    assert proj.config.poll_interval_seconds == 5.0

    custom = db.projection(
        "pv2", version="2", db_path=str(temp_dir / "x.sqlite"), poll_interval=0.25
    )
    assert custom.config.schema_version == "2"
    assert custom.config.poll_interval_seconds == 0.25
    await db.close()


async def test_wait_for_sequence_forwards_default_timeout(db, monkeypatch):
    proj = db.projection("pv")
    captured = []

    async def spy(sequence, timeout):
        captured.append((sequence, timeout))
        return True

    monkeypatch.setattr(proj, "wait_for", spy)
    assert await db.wait_for_sequence(proj, "000042") is True
    assert captured == [("000042", 30.0)]
    await db.close()


async def test_close_resets_transaction_manager(db):
    async with db.transact() as tx:
        tx.put("k", b"1")
    await db.close()

    # The engine is reusable after close: a fresh manager is created lazily.
    async with db.transact() as tx2:
        tx2.put("k", b"2")
    assert (await db.objects.get("k")).data == b"2"
    await db.close()
