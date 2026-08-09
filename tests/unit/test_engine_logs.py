"""Unit tests for named logs and the objects facade."""

import asyncio

import pytest

from cairndb import CairnDB
from cairndb.storage.base import commit_key

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
# Named logs
# ----------------------------------------------------------------------


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
