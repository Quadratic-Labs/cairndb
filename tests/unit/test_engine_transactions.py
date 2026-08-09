"""Unit tests for multi-key transactions."""

import pytest

from cairndb import CairnDB, TransactionConflict
from cairndb.engine.transactions import APPLIED_MARKER_PREFIX, TransactionManager


@pytest.fixture
def db(storage):
    return CairnDB(storage)


async def test_transaction_applies_all_writes(db):
    async with db.transact() as tx:
        tx.put("accounts/alice", b"90")
        tx.put("accounts/bob", b"110")

    assert (await db.objects.get("accounts/alice")).data == b"90"
    assert (await db.objects.get("accounts/bob")).data == b"110"
    await db.close()


async def test_transaction_read_your_writes(db):
    await db.objects.put("accounts/alice", b"100")
    async with db.transact() as tx:
        assert await tx.get("accounts/alice") == b"100"
        tx.put("accounts/alice", b"90")
        assert await tx.get("accounts/alice") == b"90"
    await db.close()


async def test_transaction_delete(db):
    await db.objects.put("k", b"v")
    async with db.transact() as tx:
        tx.delete("k")
    assert await db.objects.get("k") is None
    await db.close()


async def test_empty_transaction_is_noop(db):
    async with db.transact() as tx:
        pass
    assert tx.sequence is None
    await db.close()


async def test_exception_discards_transaction(db):
    with pytest.raises(RuntimeError):
        async with db.transact() as tx:
            tx.put("k", b"v")
            raise RuntimeError("boom")
    assert await db.objects.get("k") is None
    await db.close()


async def test_stale_read_set_conflicts(db):
    await db.objects.put("k", b"v1")

    tx = db.transact()
    await tx.__aenter__()
    await tx.get("k")
    # A direct writer changes k between our read and our commit.
    await db.objects.put("k", b"v2")
    tx.put("k", b"v3")
    with pytest.raises(TransactionConflict):
        await db._transactions.commit(tx)

    # The failed transaction wrote nothing.
    assert (await db.objects.get("k")).data == b"v2"
    await db.close()


async def test_committed_since_begin_conflicts(db, storage):
    """A transaction conflicts with one that committed after it began."""
    await db.objects.put("k", b"v0")

    tx = db.transact()
    await tx.__aenter__()
    await tx.get("k")
    tx.put("other", b"x")

    # A second engine commits a write to k while tx is open. Read the key
    # BEFORE tx's read? No: after tx began and read — this lands in the
    # begin-tail scan because it goes through the tx log.
    db2 = CairnDB(storage)
    async with db2.transact() as tx2:
        tx2.put("k", b"v1")
    await db2.close()

    with pytest.raises(TransactionConflict):
        await db._transactions.commit(tx)
    await db.close()


async def test_non_overlapping_transactions_both_commit(db, storage):
    tx = db.transact()
    await tx.__aenter__()
    await tx.get("a")
    tx.put("a", b"1")

    db2 = CairnDB(storage)
    async with db2.transact() as tx2:
        tx2.put("b", b"2")
    await db2.close()

    seq = await db._transactions.commit(tx)
    assert seq is not None
    assert (await db.objects.get("a")).data == b"1"
    assert (await db.objects.get("b")).data == b"2"
    await db.close()


async def test_notes_are_recorded_in_tx_log(db):
    async with db.transact() as tx:
        tx.put("k", b"v")
        tx.note("transfer.completed", {"amount": 10})

    entries = [e async for e in db._transactions.log.read()]
    assert len(entries) == 1
    assert entries[0].payload["notes"] == [
        {"event_type": "transfer.completed", "payload": {"amount": 10}}
    ]
    await db.close()


async def test_recovery_applies_committed_but_unapplied_tx(db, storage):
    """Crash between commit point and apply: recovery finishes the job."""
    async with db.transact() as tx:
        tx.put("a", b"1")

    # Simulate a tx that reached its commit point but never applied:
    # append a record through a manager whose apply we bypass.
    manager = TransactionManager(storage)
    tx2 = manager.transaction()
    tx2._begin_tail = await manager.log.current_tail()
    tx2.put("b", b"2")
    await manager.log.append(tx2.to_event())  # commit point only — no apply
    await manager.close()

    assert await db.objects.get("b") is None
    markers = await storage.list_objects(APPLIED_MARKER_PREFIX)
    assert len(markers) == 1  # only the first tx was applied

    recovered = await db.recover_transactions()
    assert recovered == 1
    assert (await db.objects.get("b")).data == b"2"

    # Recovery is idempotent.
    assert await db.recover_transactions() == 0
    await db.close()
