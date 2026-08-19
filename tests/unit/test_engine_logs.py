"""Unit tests for named logs and the objects facade."""

import asyncio

import pytest

from cairndb import CairnDB
from cairndb.committer import CommitterConfig
from cairndb.core.exceptions import StorageError
from cairndb.engine.logs import Log, NamespacedStorage
from cairndb.storage.base import commit_key, snapshot_key
from tests.conftest import make_event


@pytest.fixture
def db(storage):
    return CairnDB(storage)


# ----------------------------------------------------------------------
# Objects facade
# ----------------------------------------------------------------------


async def test_objects_roundtrip(db):
    etag = await db.objects.put("config/app.json", b"{}")
    obj = await db.objects.get("config/app.json")
    assert obj.data == b"{}"
    assert obj.etag == etag


async def test_objects_cas_and_put_if_absent(db):
    etag = await db.objects.put("k", b"v1")
    assert await db.objects.put("k", b"v2", if_absent=True) is None
    assert await db.objects.put("k", b"v2", if_match="wrong") is None
    assert await db.objects.put("k", b"v2", if_match=etag) is not None


async def test_objects_rejects_reserved_prefixes(db):
    for key in ("log/x", "snapshots/v1/x", "logs/orders/log/x", "txapplied/1"):
        with pytest.raises(ValueError):
            await db.objects.put(key, b"")


async def test_objects_wait_for_appearance(db):
    async def create_later():
        await asyncio.sleep(0.05)
        await db.objects.put("signals/go", b"ok")

    task = asyncio.create_task(create_later())
    obj = await db.objects.wait_for("signals/go", timeout=5, poll_interval=0.01)
    assert obj.data == b"ok"
    await task


async def test_objects_wait_for_change(db):
    etag = await db.objects.put("signals/x", b"v1")

    async def change_later():
        await asyncio.sleep(0.05)
        await db.objects.put("signals/x", b"v2")

    task = asyncio.create_task(change_later())
    obj = await db.objects.wait_for(
        "signals/x", timeout=5, poll_interval=0.01, changed_from=etag
    )
    assert obj.data == b"v2"
    await task


async def test_objects_wait_for_timeout(db):
    with pytest.raises(TimeoutError):
        await db.objects.wait_for("never/appears", timeout=0.05, poll_interval=0.01)


# ----------------------------------------------------------------------
# NamespacedStorage
# ----------------------------------------------------------------------


async def test_namespace_strips_trailing_slash_only(storage):
    ns = NamespacedStorage(storage, "tenantX/")
    assert ns.namespace == "tenantX"
    assert await ns.put_commit(1, b"c1") is True
    assert storage.get_object_sync(f"tenantX/{commit_key(1)}") is not None


async def test_namespaced_put_commit_is_put_if_absent(storage):
    ns = NamespacedStorage(storage, "logs/orders")
    assert await ns.put_commit(1, b"first") is True
    assert await ns.put_commit(1, b"second") is False
    assert await ns.get_commit(1) == b"first"


async def test_namespaced_list_commits_default_and_after(storage):
    ns = NamespacedStorage(storage, "logs/orders")
    for n in (1, 2, 3):
        await ns.put_commit(n, b"c")
    assert await ns.list_commits() == [1, 2, 3]
    assert await ns.list_commits(after=2) == [3]


async def test_delete_commits_before_prunes_only_this_log(storage):
    ns = NamespacedStorage(storage, "logs/orders")
    for n in (1, 2, 3):
        await ns.put_commit(n, b"c")
    # Decoys: the root log and a sibling log must be untouched.
    await storage.put_commit(1, b"root")
    other = NamespacedStorage(storage, "logs/other")
    await other.put_commit(1, b"other")

    assert await ns.delete_commits_before(3) == 2
    assert await ns.list_commits() == [3]
    assert await storage.get_commit(1) is not None
    assert await other.get_commit(1) is not None


async def test_namespaced_snapshot_roundtrip(storage):
    ns = NamespacedStorage(storage, "logs/orders")
    assert await ns.put_snapshot("1", 2, b"snap2") is True
    assert await ns.put_snapshot("1", 5, b"snap5") is True
    assert await ns.put_snapshot("1", 5, b"other") is False  # snapshots are immutable

    assert await ns.get_snapshot("1", 5) == b"snap5"
    assert storage.get_object_sync(f"logs/orders/{snapshot_key('1', 5)}") is not None
    assert await ns.list_snapshots("1") == [2, 5]
    with pytest.raises(StorageError):
        await ns.get_snapshot("1", 99)


async def test_delete_snapshots_before_prunes_only_this_log(storage):
    ns = NamespacedStorage(storage, "logs/orders")
    for n in (1, 2, 3):
        await ns.put_snapshot("1", n, b"s")
    # Decoys: the root store's and a sibling log's snapshots must survive.
    await storage.put_snapshot("1", 1, b"root")
    other = NamespacedStorage(storage, "logs/other")
    await other.put_snapshot("1", 1, b"other")

    assert await ns.delete_snapshots_before("1", 3) == 2
    assert await ns.list_snapshots("1") == [3]
    assert await storage.get_snapshot("1", 1) == b"root"
    assert await other.get_snapshot("1", 1) == b"other"


async def test_generic_object_api_is_unprefixed(storage):
    ns = NamespacedStorage(storage, "logs/orders")
    storage.put_object_sync("other/y", b"decoy")

    etag = ns.put_object_sync("state/x", b"v1")
    assert etag is not None
    assert storage.get_object_sync("state/x").data == b"v1"  # no logs/orders/ prefix
    assert ns.get_object_sync("state/x").data == b"v1"

    assert ns.put_object_sync("state/x", b"v2", if_absent=True) is None
    assert ns.put_object_sync("state/x", b"v2", if_match="wrong") is None
    assert ns.put_object_sync("state/x", b"v2", if_match=etag) is not None
    assert ns.put_object_sync("state/x", b"v3") is not None  # unconditional by default

    assert ns.list_objects_sync("state/") == ["state/x"]
    assert ns.list_objects_sync() == storage.list_objects_sync()

    ns.delete_object_sync("state/x")
    assert storage.get_object_sync("state/x") is None


# ----------------------------------------------------------------------
# Named logs
# ----------------------------------------------------------------------


async def test_log_constructor_wiring(storage):
    cfg = CommitterConfig(max_events_per_commit=7)

    async def hook(events, commits):
        return events

    log = Log(storage, "orders", committer_config=cfg, revalidate=hook)
    assert log.name == "orders"
    assert log.committer.config is cfg
    assert log.committer._revalidate is hook
    await log.close()

    root = Log(storage)
    assert root.name is None
    assert root.storage is storage


async def test_read_commits_yields_dense_range(db):
    log = db.log("orders")
    for i in range(3):
        await log.append(make_event(payload={"i": i}))
    assert [c.number async for c in log.read_commits()] == [1, 2, 3]
    assert [c.number async for c in log.read_commits(end_at=2)] == [1, 2]
    await db.close()


async def test_read_honors_end_at(db):
    log = db.log("orders")
    for i in range(3):
        await log.append(make_event(payload={"i": i}))
    events = [e async for e in log.read(end_at=2)]
    assert [e.payload["i"] for e in events] == [0, 1]
    await db.close()


class _Interrupt(Exception):
    """Raised from a stubbed sleep to break out of tail()."""


async def test_tail_sleeps_default_interval_when_idle(db, monkeypatch):
    log = db.log("orders")
    intervals = []

    async def spy_sleep(delay):
        intervals.append(delay)
        raise _Interrupt

    monkeypatch.setattr("cairndb.engine.logs.asyncio.sleep", spy_sleep)
    with pytest.raises(_Interrupt):
        async with asyncio.timeout(5):  # a tail that never sleeps busy-loops
            async for _ in log.tail():
                pass
    assert intervals == [1.0]


async def test_tail_probes_again_after_activity_before_sleeping(db, monkeypatch):
    # Steady-state contract: after yielding a batch, tail re-reads from the
    # new tail and only sleeps once a poll comes back empty.
    log = db.log("orders")
    await log.append(make_event(payload={"i": 0}))

    gets = []
    real_get = log.storage.get_commit

    async def counting_get(number):
        gets.append(number)
        return await real_get(number)

    monkeypatch.setattr(log.storage, "get_commit", counting_get)

    async def spy_sleep(delay):
        raise _Interrupt

    monkeypatch.setattr("cairndb.engine.logs.asyncio.sleep", spy_sleep)

    received = []
    with pytest.raises(_Interrupt):
        async with asyncio.timeout(5):
            async for entry in log.tail():
                received.append(entry.payload["i"])
    assert received == [0]
    assert gets == [1, 2, 2]  # hit, miss ends the round, fresh probe before sleeping
    await db.close()


async def test_current_tail_tracks_highest_commit(db):
    log = db.log("orders")
    assert await log.current_tail() == 0
    await log.append(make_event())
    assert await log.current_tail() == 1
    await log.append(make_event())
    assert await log.current_tail() == 2
    await db.close()


async def test_named_log_append_and_read(db):
    orders = db.log("orders")
    seq = await orders.append(make_event("order.placed", {"id": 1}))
    assert seq.commit == 1

    entries = [entry async for entry in orders.read()]
    assert len(entries) == 1
    assert entries[0].event_type == "order.placed"
    assert entries[0].payload == {"id": 1}
    await db.close()


async def test_named_logs_are_independent(db):
    seq_a = await db.log("a").append(make_event("e", {"log": "a"}))
    seq_b = await db.log("b").append(make_event("e", {"log": "b"}))

    # Each log has its own dense sequence starting at 1.
    assert seq_a.commit == 1
    assert seq_b.commit == 1

    a_entries = [e async for e in db.log("a").read()]
    b_entries = [e async for e in db.log("b").read()]
    assert [e.payload for e in a_entries] == [{"log": "a"}]
    assert [e.payload for e in b_entries] == [{"log": "b"}]
    await db.close()


async def test_named_log_layout_and_root_log_compat(db, storage):
    await db.log("orders").append(make_event())
    await db.log().append(make_event())

    # Named log under logs/{name}/, root log under the original log/ prefix.
    assert storage.get_object_sync(f"logs/orders/{commit_key(1)}") is not None
    assert await storage.get_commit(1) is not None
    await db.close()


async def test_named_log_dense_numbering_across_committers(db, storage):
    log = db.log("orders")
    await log.append_many([make_event(payload={"i": i}) for i in range(3)])
    await log.close()

    # A second engine over the same bucket continues the same sequence.
    db2 = CairnDB(storage)
    seq = await db2.log("orders").append(make_event(payload={"i": 99}))
    assert seq.commit == 2  # first three were group-committed into commit 1
    await db2.close()
    await db.close()


async def test_log_tail_follows_new_commits(db):
    log = db.log("orders")
    await log.append(make_event(payload={"i": 0}))

    received = []

    async def follow():
        async for entry in log.tail(poll_interval=0.01):
            received.append(entry.payload["i"])
            if len(received) == 2:
                return

    follower = asyncio.create_task(follow())
    await asyncio.sleep(0.05)
    await log.append(make_event(payload={"i": 1}))
    await asyncio.wait_for(follower, timeout=5)
    assert received == [0, 1]
    await db.close()


async def test_invalid_log_name_rejected(db):
    for bad in ("Orders", "a/b", "", "a b"):
        with pytest.raises(ValueError):
            db.log(bad)
