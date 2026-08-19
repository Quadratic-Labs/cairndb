"""Unit tests for multi-key transactions."""

import pytest

from cairndb import CairnDB, TransactionConflict
from cairndb.core.log import Commit, Event
from cairndb.core.types import Timestamp
from cairndb.engine.transactions import (
    APPLIED_MARKER_PREFIX,
    TX_EVENT_TYPE,
    TX_SCHEMA_VERSION,
    TransactionManager,
)
from cairndb.storage.base import commit_key
from tests.conftest import make_event


@pytest.fixture
def db(storage):
    return CairnDB(storage)


def _tx_event(reads: dict | None = None, ops: list | None = None) -> Event:
    """A transaction record; omit `ops` for a minimal record without them."""
    payload: dict = {"reads": reads if reads is not None else {}, "notes": []}
    if ops is not None:
        payload["ops"] = ops
    return Event(
        event_type=TX_EVENT_TYPE,
        timestamp=Timestamp.now(),
        payload=payload,
        schema_version=TX_SCHEMA_VERSION,
    )


async def _commit_without_apply(storage, key: str, data: bytes) -> None:
    """Append a record that reaches its commit point but is never applied."""
    manager = TransactionManager(storage)
    tx = manager.transaction()
    tx._begin_tail = await manager.log.current_tail()
    tx.put(key, data)
    await manager.log.append(tx.to_event())
    await manager.close()


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

    # Simulate a tx that reached its commit point but never applied.
    await _commit_without_apply(storage, "b", b"2")

    assert await db.objects.get("b") is None
    markers = await storage.list_objects(APPLIED_MARKER_PREFIX)
    assert len(markers) == 1  # only the first tx was applied

    recovered = await db.recover_transactions()
    assert recovered == 1
    assert (await db.objects.get("b")).data == b"2"

    # Recovery is idempotent.
    assert await db.recover_transactions() == 0
    await db.close()


async def test_tx_record_shape_and_sequence(db):
    await db.objects.put("k", b"v")
    async with db.transact() as tx:
        assert await tx.get("k") == b"v"
        tx.put("out", b"x")

    assert tx.sequence is not None
    assert tx.sequence.commit == 1

    (entry,) = [e async for e in db._transactions.log.read()]
    assert entry.schema_version == TX_SCHEMA_VERSION
    assert set(entry.payload["reads"]) == {"k"}
    assert entry.payload["ops"] == [{"op": "put", "key": "out", "data": b"x"}]
    await db.close()


async def test_commit_of_unentered_transaction_raises(db):
    tx = db.transact()
    tx.put("k", b"v")
    with pytest.raises(RuntimeError):
        await db._transactions.commit(tx)
    await db.close()


async def test_read_your_writes_when_put_precedes_first_get(db):
    await db.objects.put("k", b"old")
    async with db.transact() as tx:
        tx.put("k", b"new")
        assert await tx.get("k") == b"new"
    await db.close()


async def test_repeated_reads_of_unstaged_key(db):
    await db.objects.put("k", b"v")
    async with db.transact() as tx:
        assert await tx.get("k") == b"v"
        assert await tx.get("k") == b"v"
    await db.close()


async def test_staged_delete_reads_as_absent(db):
    await db.objects.put("k", b"v")
    async with db.transact() as tx:
        tx.delete("k")
        assert await tx.get("k") is None
    await db.close()


async def test_tx_manager_committer_wiring(db):
    manager = db._transactions
    committer = manager.log.committer
    # One event per commit is the layer-2/3 conflict-detection invariant.
    assert committer.config.max_events_per_commit == 1
    assert committer._revalidate == manager._revalidate
    await db.close()


async def test_tx_log_is_namespaced(db, storage):
    async with db.transact() as tx:
        tx.put("k", b"v")
    assert storage.get_object_sync(f"logs/_tx/{commit_key(1)}") is not None
    assert await storage.get_commit(1) is None  # root log untouched
    await db.close()


async def test_committed_but_unapplied_overlap_conflicts(db, storage):
    """Layer 2: a conflicting record past its commit point but never
    applied leaves the read key's etag unchanged — only the begin-tail
    log scan can catch the overlap."""
    await db.objects.put("k", b"v0")
    tx = db.transact()
    await tx.__aenter__()
    await tx.get("k")
    tx.put("other", b"x")

    await _commit_without_apply(storage, "k", b"v1")

    with pytest.raises(TransactionConflict):
        await db._transactions.commit(tx)
    await db.close()


async def test_scan_tolerates_minimal_tx_record(db, storage):
    tx = db.transact()
    await tx.__aenter__()
    await tx.get("k")
    tx.put("k2", b"x")

    # A record without an "ops" field must scan as writing nothing.
    manager = TransactionManager(storage)
    await manager.log.append(_tx_event())
    await manager.close()

    assert await db._transactions.commit(tx) is not None
    await db.close()


async def test_append_registers_read_set_for_revalidation(db, monkeypatch):
    manager = db._transactions
    await db.objects.put("r", b"x")
    tx = db.transact()
    await tx.__aenter__()
    await tx.get("r")
    tx.put("w", b"1")

    seen = {}
    real_append = manager.log.append

    async def spy_append(event):
        seen["pending"] = manager._pending_reads.get(id(event))
        return await real_append(event)

    monkeypatch.setattr(manager.log, "append", spy_append)

    assert await manager.commit(tx) is not None
    assert seen["pending"] == {"r"}  # in-flight record is revalidatable
    assert manager._pending_reads == {}  # and the entry does not leak
    await db.close()


async def test_revalidate_rejects_only_conflicting_records(db):
    manager = db._transactions
    now = Timestamp.now()
    interleaved = [
        Commit(
            number=1, created_at=now,
            events=(_tx_event(ops=[{"op": "put", "key": "k", "data": b"1"}]),),
        ),
        Commit(
            number=2, created_at=now,
            events=(_tx_event(ops=[{"op": "delete", "key": "m"}]),),
        ),
    ]

    conflicted = _tx_event(reads={"k": None}, ops=[{"op": "put", "key": "x", "data": b"2"}])
    unrelated = _tx_event(reads={"z": None}, ops=[{"op": "put", "key": "y", "data": b"3"}])
    # The record's serialized reads are empty; the true read set is only
    # known in-process via _pending_reads, which must take priority.
    masked = _tx_event(reads={}, ops=[])
    manager._pending_reads[id(masked)] = {"m"}
    # A record with no "reads" field at all must scan as reading nothing.
    bare = Event(
        event_type=TX_EVENT_TYPE, timestamp=now,
        payload={"ops": []}, schema_version=TX_SCHEMA_VERSION,
    )

    try:
        decisions = await manager._revalidate(
            [conflicted, unrelated, masked, bare], interleaved
        )
    finally:
        manager._pending_reads.clear()
    assert decisions == [None, unrelated, None, bare]
    await db.close()


async def test_recover_applies_commit_one_and_skips_non_tx_events(db):
    manager = db._transactions
    commit = Commit(
        number=1,
        created_at=Timestamp.now(),
        events=(
            make_event(),  # non-tx event: skipped, not fatal
            _tx_event(),  # minimal record without ops: applies nothing
            _tx_event(ops=[{"op": "put", "key": "k", "data": b"v"}]),
        ),
    )
    await manager.log.storage.put_commit(1, commit.to_msgpack())

    assert await manager.recover() == 1
    assert (await db.objects.get("k")).data == b"v"
    await db.close()


async def test_recover_survives_gc_of_applied_prefix(db, storage):
    async with db.transact() as tx1:
        tx1.put("a1", b"x")
    async with db.transact() as tx2:
        tx2.put("a2", b"x")
    await _commit_without_apply(storage, "b", b"2")

    # GC the applied head of the tx log; markers 1 and 2 remain, so the
    # scan must start after the contiguous applied prefix.
    assert await db._transactions.log.storage.delete_commits_before(3) == 2

    assert await db.recover_transactions() == 1
    assert (await db.objects.get("b")).data == b"2"
    await db.close()


async def test_recover_continues_past_applied_gap(db, storage):
    for key in ("a", "b", "c"):
        await _commit_without_apply(storage, key, key.encode())
    # Commit 2 is marked applied (say, by a competing recoverer): the scan
    # must skip it and still recover commit 3.
    storage.put_object_sync(f"{APPLIED_MARKER_PREFIX}000000000002", b"")

    assert await db.recover_transactions() == 2
    assert (await db.objects.get("a")).data == b"a"
    assert await db.objects.get("b") is None
    assert (await db.objects.get("c")).data == b"c"
    await db.close()


async def test_apply_marker_is_empty_and_put_if_absent(db, storage):
    async with db.transact() as tx:
        tx.put("k", b"v1")
    marker = f"{APPLIED_MARKER_PREFIX}000000000001"
    assert storage.get_object_sync(marker).data == b""

    # A re-apply (recovery racing a live commit) must not clobber the
    # existing marker.
    storage.put_object_sync(marker, b"sentinel")
    await db._transactions._apply_commit(1)
    assert (await db.objects.get("k")).data == b"v1"
    assert storage.get_object_sync(marker).data == b"sentinel"
    await db.close()
