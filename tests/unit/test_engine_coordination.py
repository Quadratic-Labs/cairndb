"""Unit tests for the engine's coordination layer: claim, Lease, Document."""

import json
from contextlib import contextmanager
from datetime import timedelta

import pytest

from cairndb import CairnDB, LeaseLost
from cairndb.core.exceptions import CairnDBError
from cairndb.core.types import Timestamp
from cairndb.engine.coordination import (
    Lease,
    _encode,
    acquire,
    acquire_sync,
    cooperative_write_sync,
)


@pytest.fixture
def db(storage):
    return CairnDB(storage)


def _expire(storage, key: str) -> None:
    """Rewrite the lease deadline into the past (simulates a crashed
    holder whose ttl elapsed)."""
    obj = storage.get_object_sync(key)
    doc = Lease._parse(obj.data)
    storage.put_object_sync(
        key,
        Lease._doc_bytes(
            doc["epoch"], doc["holder"], Timestamp.now() - timedelta(seconds=1), doc["state"]
        ),
        if_match=obj.etag,
    )


@contextmanager
def _interleave_before_next_put(storage, write):
    """Run `write()` just before the next put goes through, so that put's
    precondition observes a concurrent writer and must lose."""
    original_put = storage.put_object_sync
    calls = 0

    def racing_put(key, data, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            write()
        return original_put(key, data, **kwargs)

    storage.put_object_sync = racing_put
    try:
        yield
    finally:
        storage.put_object_sync = original_put


# ----------------------------------------------------------------------
# document encoding
# ----------------------------------------------------------------------


def test_encode_is_canonical():
    # The encoded document is a cross-process protocol: compact separators
    # and sorted keys keep the bytes (and content-derived etags) deterministic.
    assert _encode({"b": 1, "a": [1, 2]}) == b'{"a":[1,2],"b":1}'


# ----------------------------------------------------------------------
# claim
# ----------------------------------------------------------------------


async def test_claim_first_caller_wins(db, storage):
    result = await db.claim("dispatch/etl:2026-08-09", {"run_id": "r1"})
    assert result.won is True
    assert result.key == "dispatch/etl:2026-08-09"
    assert result.value == {"run_id": "r1"}
    assert result.etag == storage.get_object_sync("dispatch/etl:2026-08-09").etag


async def test_claim_losers_converge_on_winner_value(db, storage):
    await db.claim("dispatch/k", {"run_id": "winner"})
    result = await db.claim("dispatch/k", {"run_id": "loser"})
    assert result.won is False
    assert result.key == "dispatch/k"
    assert result.value == {"run_id": "winner"}
    assert result.etag == storage.get_object_sync("dispatch/k").etag


async def test_claim_sync_twin(db):
    result = db.claim_sync("dispatch/sync", {"v": 1})
    assert result.won
    assert result.key == "dispatch/sync"
    assert result.value == {"v": 1}
    assert not db.claim_sync("dispatch/sync", {"v": 2}).won


async def test_lease_sync_facade(db, storage):
    lease = db.lease_sync("state/run-1", ttl=60, holder="w1", state_fn=lambda s: {"n": 0})
    assert lease is not None
    assert lease.holder == "w1"
    assert lease.state == {"n": 0}
    assert db.lease_sync("state/run-1", ttl=60, holder="w2") is None  # actively held

    _expire(storage, "state/run-1")
    assert db.lease_sync("state/run-1", ttl=60, holder="w2", steal_if_expired=False) is None
    thief = db.lease_sync("state/run-1", ttl=60, holder="w2")  # steals by default
    assert thief is not None
    assert thief.epoch == 2


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
    _expire(storage, "state/run-1")

    thief = await db.lease("state/run-1", ttl=60, holder="w2")
    assert thief is not None
    assert thief.epoch == 2

    # The fenced original holder can no longer write anything.
    with pytest.raises(LeaseLost):
        await lease.renew()
    with pytest.raises(LeaseLost):
        await lease.release()


async def test_attach_returns_the_holders_own_handle(db):
    lease = await db.lease("state/run-1", ttl=60, holder="w1", state_fn=lambda s: {"n": 1})

    same = await db.attach_lease("state/run-1", holder="w1", ttl=60)
    assert same is not None
    assert (same.epoch, same.holder, same.state) == (lease.epoch, "w1", {"n": 1})
    assert same.deadline_at == lease.deadline_at


async def test_attach_writes_nothing(db, storage):
    await db.lease("state/run-1", ttl=60, holder="w1")
    before = storage.get_object_sync("state/run-1")

    assert await db.attach_lease("state/run-1", holder="w1", ttl=60) is not None

    after = storage.get_object_sync("state/run-1")
    assert after.etag == before.etag
    assert after.data == before.data


async def test_two_handles_on_one_period_do_not_fence_each_other(db):
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    attached = await db.attach_lease("state/run-1", holder="w1", ttl=60)

    # Each write moves the etag the other one cached; identity is
    # (key, epoch, holder), so both absorb and continue.
    await attached.update_state(lambda s: {"from": "attached"})
    await lease.update_state(lambda s: {**(s or {}), "from": "acquirer"})
    await attached.renew()

    assert lease.epoch == attached.epoch
    assert lease.state == {"from": "acquirer"}


async def test_attach_refuses_a_lease_that_is_not_yours(db):
    await db.lease("state/run-1", ttl=60, holder="w1")
    assert await db.attach_lease("state/run-1", holder="w2", ttl=60) is None


async def test_attach_refuses_absent_and_released_leases(db):
    assert await db.attach_lease("state/run-1", holder="w1", ttl=60) is None

    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    await lease.release()
    assert await db.attach_lease("state/run-1", holder="w1", ttl=60) is None


async def test_attach_refuses_an_expired_lease(db, storage):
    await db.lease("state/run-1", ttl=60, holder="w1")
    _expire(storage, "state/run-1")

    # Refused on purpose: a sweeper may already have concluded w1 dead, so
    # ownership is resumed with an acquisition that takes a fresh epoch.
    assert await db.attach_lease("state/run-1", holder="w1", ttl=60) is None
    resumed = await db.lease("state/run-1", ttl=60, holder="w1")
    assert resumed.epoch == 2


async def test_attach_after_a_steal_gives_the_thief_only(db, storage):
    await db.lease("state/run-1", ttl=60, holder="w1")
    _expire(storage, "state/run-1")
    thief = await db.lease("state/run-1", ttl=60, holder="w2")

    assert await db.attach_lease("state/run-1", holder="w1", ttl=60) is None
    stolen = await db.attach_lease("state/run-1", holder="w2", ttl=60)
    assert stolen is not None
    assert stolen.epoch == thief.epoch == 2


async def test_an_attached_handle_is_fenced_by_a_steal(db, storage):
    await db.lease("state/run-1", ttl=60, holder="w1")
    attached = await db.attach_lease("state/run-1", holder="w1", ttl=60)

    _expire(storage, "state/run-1")
    assert await db.lease("state/run-1", ttl=60, holder="w2") is not None

    with pytest.raises(LeaseLost):
        await attached.renew()


def test_attach_sync_facade_and_argument_checks(db):
    db.lease_sync("state/run-1", ttl=60, holder="w1")
    assert db.attach_lease_sync("state/run-1", holder="w1", ttl=60) is not None

    with pytest.raises(ValueError):
        db.attach_lease_sync("state/run-1", holder="w1", ttl=0)
    with pytest.raises(ValueError):
        db.attach_lease_sync("state/run-1", holder="", ttl=60)


async def test_expired_lease_not_stolen_when_disabled(db, storage):
    assert await db.lease("state/run-1", ttl=60, holder="w1") is not None
    _expire(storage, "state/run-1")
    assert await db.lease("state/run-1", ttl=60, steal_if_expired=False) is None


def test_lease_expiry_boundary_is_inclusive():
    now = Timestamp.now()
    assert Lease._is_expired({"holder": "w1", "deadline_at": now}, now) is True


async def test_lease_ttl_must_be_positive_boundary(db):
    with pytest.raises(ValueError):
        await db.lease("state/run-1", ttl=0)
    assert await db.lease("state/run-1", ttl=0.5) is not None


async def test_acquire_steals_expired_lease_by_default(storage):
    # steal_if_expired=True is the coordination-layer default; the engine
    # facade always passes it explicitly, so exercise the module functions
    # directly with the argument omitted.
    assert acquire_sync(storage, "state/run-1", ttl=60, holder="w1") is not None
    _expire(storage, "state/run-1")
    stolen = await acquire(storage, "state/run-1", ttl=60, holder="w2")
    assert stolen is not None and stolen.epoch == 2
    _expire(storage, "state/run-1")
    stolen = acquire_sync(storage, "state/run-1", ttl=60, holder="w3")
    assert stolen is not None and stolen.epoch == 3


async def test_guarded_writes_preserve_ownership(db, storage):
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    await lease.renew()
    assert lease.holder == "w1"
    await lease.write({"p": 1})
    assert lease.holder == "w1"
    await lease.update_state(lambda s: s)
    assert lease.holder == "w1"

    doc = json.loads(storage.get_object_sync("state/run-1").data)
    assert doc["holder"] == "w1"
    assert await db.lease("state/run-1", ttl=60, holder="w2", steal_if_expired=False) is None


async def test_cooperative_write_absorbed_after_prior_guarded_write(db):
    # The etag observed at a successful write must guard the next one: a
    # cooperative write landing between two guarded writes is absorbed,
    # not clobbered.
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    await lease.write({"p": 0})
    await db.cooperative_write("state/run-1", lambda s: {**s, "cancel_requested": True})
    await lease.renew()
    assert lease.state == {"p": 0, "cancel_requested": True}


async def test_deleted_lease_document_fences_holder(db, storage):
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    storage.delete_object_sync("state/run-1")
    with pytest.raises(LeaseLost):
        await lease.renew()


async def test_same_holder_reacquiring_fences_its_old_lease(db, storage):
    # A crashed-and-restarted worker may reuse its holder name; the epoch
    # alone must fence the stale Lease object.
    old = await db.lease("state/run-1", ttl=60, holder="w1")
    _expire(storage, "state/run-1")
    new = await db.lease("state/run-1", ttl=60, holder="w1")
    assert new.epoch == 2
    with pytest.raises(LeaseLost):
        await old.renew()


async def test_repeated_cooperative_writes_exhaust_guarded_write_patience(db):
    # Every retry must stay CAS-guarded: when a fresh cooperative write
    # lands before each write attempt, the attempt bound runs out and the
    # write fails rather than clobbering a write it never observed.
    lease = await db.lease("state/run-1", ttl=60, holder="w1")

    def written_to_every_attempt(state):
        # Deliberately impure: simulates an outsider writing in the
        # window between the holder's read and its write, every time.
        db.cooperative_write_sync("state/run-1", lambda s: {"seq": ((s or {}).get("seq") or 0) + 1})
        return {"mine": True}

    with pytest.raises(LeaseLost):
        await lease.update_state(written_to_every_attempt)


async def test_writes_after_release_raise_lease_lost(db):
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    await lease.release()
    with pytest.raises(LeaseLost):
        await lease.renew()
    with pytest.raises(LeaseLost):
        await lease.write({"p": 1})


async def test_fresh_acquire_race_converges(db, storage):
    # Another acquirer creates the document between our read and our
    # put-if-absent; we must lose the put and converge on their lease.
    def rival_creates():
        storage.put_object_sync(
            "state/run-1",
            Lease._doc_bytes(1, "rival", Timestamp.now() + timedelta(seconds=60), None),
            if_absent=True,
        )

    with _interleave_before_next_put(storage, rival_creates):
        assert await db.lease("state/run-1", ttl=60, holder="w1") is None


async def test_steal_race_converges(db, storage):
    # Another thief steals the expired lease between our read and our CAS;
    # we must lose the CAS and observe their now-active lease.
    assert await db.lease("state/run-1", ttl=60, holder="w1") is not None
    _expire(storage, "state/run-1")

    def rival_steals():
        obj = storage.get_object_sync("state/run-1")
        doc = Lease._parse(obj.data)
        storage.put_object_sync(
            "state/run-1",
            Lease._doc_bytes(
                doc["epoch"] + 1, "rival", Timestamp.now() + timedelta(seconds=60), doc["state"]
            ),
            if_match=obj.etag,
        )

    with _interleave_before_next_put(storage, rival_steals):
        assert await db.lease("state/run-1", ttl=60, holder="w2") is None


async def test_steal_writes_faithful_document(db, storage):
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    await lease.write({"x": 1})
    _expire(storage, "state/run-1")

    thief = await db.lease("state/run-1", ttl=60, holder="w2")
    assert thief is not None
    doc = json.loads(storage.get_object_sync("state/run-1").data)
    assert doc["epoch"] == 2
    assert doc["holder"] == "w2"
    assert doc["state"] == {"x": 1}


async def test_stolen_lease_is_fully_functional(db, storage):
    assert await db.lease("state/run-1", ttl=60, holder="w1") is not None
    _expire(storage, "state/run-1")

    thief = await db.lease("state/run-1", ttl=60, holder="w2")
    assert thief.key == "state/run-1"
    assert thief.ttl == 60
    assert thief.holder == "w2"
    assert thief.deadline_at > Timestamp.now()

    # It renews against live storage and absorbs cooperative writes like
    # any lease.
    await db.cooperative_write("state/run-1", lambda s: {"cancel_requested": True})
    await thief.renew()
    assert thief.state == {"cancel_requested": True}


async def test_cooperative_write_race_merges_rather_than_clobbers(db, storage):
    # A competing write lands between our write's read and its CAS; we
    # must lose the CAS, re-read, and write both merged.
    await db.lease("state/run-1", ttl=60, holder="w1", state_fn=lambda s: {"n": 0})

    def rival_write():
        cooperative_write_sync(storage, "state/run-1", lambda s: {**s, "other": 1})

    with _interleave_before_next_put(storage, rival_write):
        written = await db.cooperative_write("state/run-1", lambda s: {**s, "cancel_requested": True})

    assert written == {"n": 0, "other": 1, "cancel_requested": True}


async def test_cooperative_write_observed_on_renew_without_fencing(db, storage):
    # A cooperative write moves the document's etag, so the holder's next
    # guarded write necessarily loses its CAS; it must absorb the write
    # and retry, not raise LeaseLost or clobber it unread.
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    assert await db.cooperative_write("state/run-1", lambda s: {"cancel_requested": True}) == {
        "cancel_requested": True
    }

    await lease.renew()
    assert lease.state == {"cancel_requested": True}
    assert lease.epoch == 1  # written to, not fenced

    # The write also survived in storage, and the lease still works.
    doc = json.loads(storage.get_object_sync("state/run-1").data)
    assert doc["state"] == {"cancel_requested": True}
    await lease.release()


async def test_cooperative_write_on_missing_lease_returns_none(db):
    assert await db.cooperative_write("state/nope", lambda s: {"x": 1}) is None


async def test_update_state_merges_concurrent_cooperative_write(db):
    lease = await db.lease(
        "state/run-1", ttl=60, holder="w1", state_fn=lambda s: {"progress": 0}
    )
    await db.cooperative_write("state/run-1", lambda s: {**s, "cancel_requested": True})

    # The holder's stale self.state has no flag; update_state must apply
    # fn to the absorbed fresh state so both writes survive.
    written = await lease.update_state(lambda s: {**s, "progress": 1})
    assert written == {"progress": 1, "cancel_requested": True}
    assert lease.state == written


async def test_write_deliberately_discards_cooperative_write(db):
    # Plain write replaces state unconditionally — documented behavior;
    # cooperating holders use renew/update_state instead.
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    await db.cooperative_write("state/run-1", lambda s: {"cancel_requested": True})
    await lease.write({"progress": 0.5})
    assert lease.state == {"progress": 0.5}


async def test_release_preserves_cooperatively_written_state(db):
    lease = await db.lease("state/run-1", ttl=60, holder="w1")
    await db.cooperative_write("state/run-1", lambda s: {"cancel_requested": True})
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


async def test_document_create_race_converges_on_existing(db, storage):
    # A competitor creates the document between our absent-read and our
    # put-if-absent; we must lose the put and update their value instead.
    doc = db.doc("counters/c")

    def rival_creates():
        storage.put_object_sync("counters/c", b'{"n": 100}', if_absent=True)

    with _interleave_before_next_put(storage, rival_creates):
        result = await doc.update(lambda v: {"n": v["n"] + 1}, create={"n": 0})

    assert result == {"n": 101}


async def test_document_update_persists_what_it_returns(db):
    doc = db.doc("counters/c")
    await doc.update(lambda v: v, create={"n": 0})
    result = await doc.update(lambda v: {"n": v["n"] + 1})
    value, _ = await doc.get()
    assert value == result == {"n": 1}


async def test_document_update_honors_max_attempts(db, storage):
    doc = db.doc("counters/c")
    await doc.update(lambda v: v, create={"n": 0})

    original_put = storage.put_object_sync
    contended = 0

    def always_contended_put(key, data, **kwargs):
        nonlocal contended
        contended += 1
        # A fresh competing write before every CAS attempt (distinct
        # content each time — etags are content-derived).
        original_put(key, json.dumps({"rival": contended}).encode())
        return original_put(key, data, **kwargs)

    attempts = 0

    def bump(v):
        nonlocal attempts
        attempts += 1
        return {"n": attempts}

    storage.put_object_sync = always_contended_put
    try:
        with pytest.raises(CairnDBError):
            await doc.update(bump, max_attempts=3)
    finally:
        storage.put_object_sync = original_put

    assert attempts == 3
