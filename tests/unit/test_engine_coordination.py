"""Unit tests for the engine's coordination layer: claim, Lease, Document."""

import json
from datetime import timedelta

import pytest

from cairndb import CairnDB, LeaseLost
from cairndb.core.exceptions import CairnDBError
from cairndb.core.types import Timestamp


@pytest.fixture
def db(storage):
    return CairnDB(storage)


# ----------------------------------------------------------------------
# claim
# ----------------------------------------------------------------------


async def test_claim_first_caller_wins(db):
    result = await db.claim("dispatch/etl:2026-08-09", {"run_id": "r1"})
    assert result.won
    assert result.value == {"run_id": "r1"}


async def test_claim_losers_converge_on_winner_value(db):
    await db.claim("dispatch/k", {"run_id": "winner"})
    result = await db.claim("dispatch/k", {"run_id": "loser"})
    assert not result.won
    assert result.value == {"run_id": "winner"}


async def test_claim_sync_twin(db):
    result = db.claim_sync("dispatch/sync", {"v": 1})
    assert result.won
    assert not db.claim_sync("dispatch/sync", {"v": 2}).won


async def test_claim_rejects_reserved_prefix(db):
    with pytest.raises(ValueError):
        await db.claim("log/000000000001.msgpack", {})


# ----------------------------------------------------------------------
# Lease
# ----------------------------------------------------------------------


async def test_lease_acquire_fresh(db):
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    assert lease is not None
    assert lease.epoch == 1
    assert lease.holder == "w1"


async def test_lease_held_returns_none(db):
    assert await db.lease("state/run-1", ttl=60, holder="w1") is not None
    assert await db.lease("state/run-1", ttl=60, holder="w2") is None


async def test_lease_renew_extends_deadline(db):
    lease = await db.lease("state/run-1", ttl=60)
    old_deadline = lease.deadline_at
    await lease.renew()
    assert lease.deadline_at >= old_deadline


async def test_lease_release_then_reacquire_bumps_epoch(db):
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    await lease.release(state={"status": "completed"})

    again = await db.lease("state/run-1", ttl=60, holder="w2")
    assert again is not None
    assert again.epoch == 2
    assert again.state == {"status": "completed"}


async def test_expired_lease_is_stolen_and_fences_old_holder(db, storage):
    lease = await db.lease("state/run-1", ttl=60, holder="w1")

    # Force expiry by rewriting the deadline into the past (simulates a
    # crashed holder whose ttl elapsed).
    past = Timestamp.now() - timedelta(seconds=1)
    obj = storage.get_object_sync("state/run-1")
    doc = lease._parse(obj.data)
    storage.put_object_sync(
        "state/run-1",
        lease._doc_bytes(doc["epoch"], doc["holder"], past, doc["state"]),
        if_match=obj.etag,
    )

    thief = await db.lease("state/run-1", ttl=60, holder="w2")
    assert thief is not None
    assert thief.epoch == 2

    # The fenced original holder can no longer write anything.
    with pytest.raises(LeaseLost):
        await lease.renew()
    with pytest.raises(LeaseLost):
        await lease.release()


async def test_expired_lease_not_stolen_when_disabled(db, storage):
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    past = Timestamp.now() - timedelta(seconds=1)
    obj = storage.get_object_sync("state/run-1")
    doc = lease._parse(obj.data)
    storage.put_object_sync(
        "state/run-1",
        lease._doc_bytes(doc["epoch"], doc["holder"], past, doc["state"]),
        if_match=obj.etag,
    )
    assert await db.lease("state/run-1", ttl=60, steal_if_expired=False) is None


async def test_signal_observed_on_renew_without_fencing(db, storage):
    # A signal moves the document's etag, so the holder's next guarded
    # write necessarily loses its CAS; it must absorb the signal and
    # retry, not raise LeaseLost or clobber the signal unread.
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    assert await db.signal("state/run-1", lambda s: {"cancel_requested": True}) == {
        "cancel_requested": True
    }

    await lease.renew()
    assert lease.state == {"cancel_requested": True}
    assert lease.epoch == 1  # signaled, not fenced

    # The signal also survived in storage, and the lease still works.
    doc = json.loads(storage.get_object_sync("state/run-1").data)
    assert doc["state"] == {"cancel_requested": True}
    await lease.release()


async def test_signal_on_missing_lease_returns_none(db):
    assert await db.signal("state/nope", lambda s: {"x": 1}) is None


async def test_update_state_merges_concurrent_signal(db):
    lease = await db.lease(
        "state/run-1", ttl=60, holder="w1", state_fn=lambda s: {"progress": 0}
    )
    await db.signal("state/run-1", lambda s: {**s, "cancel_requested": True})

    # The holder's stale self.state has no flag; update_state must apply
    # fn to the absorbed fresh state so both writes survive.
    written = await lease.update_state(lambda s: {**s, "progress": 1})
    assert written == {"progress": 1, "cancel_requested": True}
    assert lease.state == written


async def test_write_deliberately_discards_signal(db):
    # Plain write replaces state unconditionally — documented behavior;
    # cooperating holders use renew/update_state instead.
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    await db.signal("state/run-1", lambda s: {"cancel_requested": True})
    await lease.write({"progress": 0.5})
    assert lease.state == {"progress": 0.5}


async def test_release_preserves_signaled_state(db):
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    await db.signal("state/run-1", lambda s: {"cancel_requested": True})
    await lease.release()  # no explicit state: freshest state kept

    again = await db.lease("state/run-1", ttl=60, holder="w2")
    assert again.state == {"cancel_requested": True}


async def test_lease_write_updates_state(db):
    lease = await db.lease("state/run-1", ttl=60)
    await lease.write({"progress": 0.5})
    assert lease.state == {"progress": 0.5}


async def test_lease_state_fn_seeds_fresh_state(db):
    lease = await db.lease(
        "state/run-1",
        ttl=60,
        holder="w1",
        state_fn=lambda s: {"attempt": 1} if s is None else s,
    )
    assert lease is not None
    assert lease.state == {"attempt": 1}


async def test_lease_state_fn_transitions_on_reacquire(db):
    first = await db.lease(
        "state/run-1",
        ttl=60,
        holder="w1",
        state_fn=lambda s: {"attempt": 1},
    )
    await first.release()

    second = await db.lease(
        "state/run-1",
        ttl=60,
        holder="w2",
        state_fn=lambda s: {"attempt": s["attempt"] + 1},
    )
    assert second is not None
    assert second.epoch == 2
    assert second.state == {"attempt": 2}


async def test_lease_state_fn_exception_aborts_without_writing(db, storage):
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    await lease.release(state={"status": "closed"})
    before = storage.get_object_sync("state/run-1")

    class AlreadyClosed(Exception):
        pass

    def refuse(state):
        raise AlreadyClosed(state)

    with pytest.raises(AlreadyClosed):
        await db.lease("state/run-1", ttl=60, holder="w2", state_fn=refuse)

    after = storage.get_object_sync("state/run-1")
    assert after.etag == before.etag  # nothing was written


async def test_lease_state_fn_not_applied_when_held(db):
    assert await db.lease("state/run-1", ttl=60, holder="w1") is not None
    calls = []

    def spy(state):
        calls.append(state)
        return state

    assert await db.lease("state/run-1", ttl=60, holder="w2", state_fn=spy) is None
    assert calls == []


# ----------------------------------------------------------------------
# Document
# ----------------------------------------------------------------------


class QueueState:
    """Pydantic-model-shaped stub: Document only needs the model_*_json pair."""

    def __init__(self, inflight: int = 0):
        self.inflight = inflight

    def model_dump_json(self) -> str:
        return json.dumps({"inflight": self.inflight})

    @classmethod
    def model_validate_json(cls, data: bytes) -> QueueState:
        return cls(**json.loads(data))


async def test_document_update_creates_from_initial(db):
    doc = db.doc("queues/default", model=QueueState)
    state = await doc.update(
        lambda q: QueueState(inflight=q.inflight + 1),
        create=QueueState(),
    )
    assert state.inflight == 1

    value, etag = await doc.get()
    assert value.inflight == 1
    assert etag


async def test_document_update_retries_on_conflict(db, storage):
    doc = db.doc("counters/c")
    await doc.update(lambda v: v, create={"n": 0})

    calls = 0
    original_put = storage.put_object_sync

    def contended_put(key, data, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            # Interleave a competing write before the first CAS attempt.
            original_put(key, b'{"n": 100}')
        return original_put(key, data, **kwargs)

    storage.put_object_sync = contended_put
    try:
        result = await doc.update(lambda v: {"n": v["n"] + 1})
    finally:
        storage.put_object_sync = original_put

    assert result == {"n": 101}


async def test_document_update_absent_without_create_raises(db):
    with pytest.raises(CairnDBError):
        await db.doc("missing/doc").update(lambda v: v)


async def test_document_delete_is_idempotent(db):
    doc = db.doc("d/x")
    await doc.update(lambda v: v, create={"a": 1})
    await doc.delete()
    await doc.delete()
    assert await doc.get() is None
