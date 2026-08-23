"""Live integration tests against a real Azure Blob Storage container.

The unit tests for the Azure backend assert exact SDK calls; these tests
validate the assumptions those mocks encode, against the actual service:

- put-if-absent: ``upload_blob(overwrite=False)`` really is atomic
  If-None-Match: * arbitration (including under concurrency)
- etag CAS: ``etag=`` + ``MatchConditions.IfNotModified`` accepts the
  current etag and rejects stale ones, from both put- and get-observed etags
- 404 under If-Match (blob deleted in the meantime) is a precondition
  failure, not an error
- delete/list/GC semantics on real blobs
- and one end-to-end pass of the coordination kernel (claim, lease,
  transaction) on Azure

Opt-in only — excluded from the normal suite and from mutation testing:

    export CAIRNDB_AZURE_CONTAINER=<existing container>
    export CAIRNDB_AZURE_CONNECTION_STRING='...'   # or CAIRNDB_AZURE_ACCOUNT_URL
    CAIRNDB_AZURE_INTEGRATION=1 pytest tests/integration/test_azure_live.py -v

The standard SDK names (AZURE_STORAGE_CONTAINER, AZURE_STORAGE_CONNECTION_STRING,
AZURE_STORAGE_ACCOUNT_URL) are accepted as fallbacks. The variables must be
`export`ed — plain shell assignments are invisible to make/pytest.

or ``make azure-integration``. Every test works under a unique
``cairndb-it-*`` prefix and deletes everything it created, so a shared
container stays clean even across failed runs of different prefixes.
Do NOT export CAIRNDB_AZURE_INTEGRATION while running mutmut.
"""

import asyncio
import os
import uuid

import pytest

from cairndb.core.exceptions import StorageError

pytestmark = [
    pytest.mark.azure_live,
    pytest.mark.skipif(
        os.environ.get("CAIRNDB_AZURE_INTEGRATION") != "1",
        reason="live Azure suite: set CAIRNDB_AZURE_INTEGRATION=1 and CAIRNDB_AZURE_* creds",
    ),
]

pytest.importorskip("azure.storage.blob")


def _env(*names: str) -> str | None:
    """First non-empty value among the given environment variables."""
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


@pytest.fixture
def live_storage():
    from cairndb.storage.azure import AzureBlobStorage

    container = _env("CAIRNDB_AZURE_CONTAINER", "AZURE_STORAGE_CONTAINER")
    connection_string = _env(
        "CAIRNDB_AZURE_CONNECTION_STRING", "AZURE_STORAGE_CONNECTION_STRING"
    )
    account_url = _env("CAIRNDB_AZURE_ACCOUNT_URL", "AZURE_STORAGE_ACCOUNT_URL")
    if not container or not (connection_string or account_url):
        status = {
            name: ("set" if os.environ.get(name) else "MISSING")
            for name in (
                "CAIRNDB_AZURE_CONTAINER",
                "CAIRNDB_AZURE_CONNECTION_STRING",
                "CAIRNDB_AZURE_ACCOUNT_URL",
                "AZURE_STORAGE_CONTAINER",
                "AZURE_STORAGE_CONNECTION_STRING",
                "AZURE_STORAGE_ACCOUNT_URL",
            )
        }
        pytest.fail(
            "CAIRNDB_AZURE_INTEGRATION=1 is set, but the container and credentials "
            "did not reach this process (variables set without `export` are not "
            f"visible to make/pytest). Environment seen: {status}"
        )

    prefix = f"cairndb-it-{uuid.uuid4().hex[:12]}"
    storage = AzureBlobStorage(
        container=container,
        prefix=prefix,
        connection_string=connection_string,
        account_url=account_url,
    )
    try:
        yield storage
    finally:
        # Thorough cleanup via the raw client: on hierarchical-namespace
        # accounts, directories are real objects — delete deepest-first so
        # they empty out, then remove the run-prefix directory itself.
        container_client = storage._container_client
        leftovers = sorted(
            (b.name for b in container_client.list_blobs(name_starts_with=f"{prefix}/")),
            key=lambda name: name.count("/"),
            reverse=True,
        )
        for name in [*leftovers, prefix]:
            try:
                container_client.delete_blob(name)
            except Exception as e:  # noqa: BLE001 — best-effort cleanup
                print(f"cleanup: could not delete {name!r}: {e}")


async def test_commit_put_if_absent_is_atomic(live_storage):
    assert await live_storage.put_commit(1, b"first") is True
    assert await live_storage.put_commit(1, b"second") is False
    assert await live_storage.get_commit(1) == b"first"

    # Concurrent writers racing for one number: the service must arbitrate
    # exactly one winner, and the object must hold that winner's payload.
    payloads = [f"writer-{i}".encode() for i in range(6)]
    outcomes = await asyncio.gather(
        *(live_storage.put_commit(2, p) for p in payloads)
    )
    assert sum(outcomes) == 1
    winner = payloads[outcomes.index(True)]
    assert await live_storage.get_commit(2) == winner


async def test_commit_listing_and_gc(live_storage):
    for n in (1, 2, 3, 4):
        assert await live_storage.put_commit(n, b"c") is True

    assert await live_storage.get_commit(99) is None
    assert await live_storage.list_commits() == [1, 2, 3, 4]
    assert await live_storage.list_commits(after=2) == [3, 4]

    assert await live_storage.delete_commits_before(3) == 2
    assert await live_storage.list_commits() == [3, 4]
    assert await live_storage.get_commit(1) is None


async def test_snapshot_lifecycle(live_storage):
    assert await live_storage.put_snapshot("1.0.0", 5, b"snap5") is True
    assert await live_storage.put_snapshot("1.0.0", 5, b"other") is False  # immutable
    assert await live_storage.get_snapshot("1.0.0", 5) == b"snap5"

    assert await live_storage.put_snapshot("1.0.0", 2, b"snap2") is True
    assert await live_storage.list_snapshots("1.0.0") == [2, 5]
    assert await live_storage.find_latest_snapshot("1.0.0") == 5
    assert await live_storage.list_snapshots("2.0.0") == []

    with pytest.raises(StorageError):
        await live_storage.get_snapshot("1.0.0", 99)

    assert await live_storage.delete_snapshots_before("1.0.0", 5) == 1
    assert await live_storage.list_snapshots("1.0.0") == [5]


async def test_object_cas_ladder(live_storage):
    s = live_storage

    # Unconditional put; the returned etag must be usable as if_match.
    put_etag = s.put_object_sync("state/x", b"v1")
    assert put_etag is not None

    obj = s.get_object_sync("state/x")
    assert obj is not None
    assert obj.data == b"v1"

    # if_absent on an existing blob loses.
    assert s.put_object_sync("state/x", b"v2", if_absent=True) is None
    assert s.get_object_sync("state/x").data == b"v1"

    # CAS with the put-observed etag wins; the get-observed etag of the
    # new version wins the next round (both sources must be usable).
    etag2 = s.put_object_sync("state/x", b"v2", if_match=put_etag)
    assert etag2 is not None
    get_etag = s.get_object_sync("state/x").etag
    etag3 = s.put_object_sync("state/x", b"v3", if_match=get_etag)
    assert etag3 is not None

    # A stale etag loses without modifying anything.
    assert s.put_object_sync("state/x", b"clobber", if_match=put_etag) is None
    assert s.get_object_sync("state/x").data == b"v3"

    # Mutually exclusive preconditions are rejected client-side.
    with pytest.raises(ValueError):
        s.put_object_sync("state/x", b"v", if_match=etag3, if_absent=True)

    # if_match on a blob deleted in the meantime is a precondition
    # failure (404 under If-Match), not an error.
    s.delete_object_sync("state/x")
    assert s.get_object_sync("state/x") is None
    assert s.put_object_sync("state/x", b"v4", if_match=etag3) is None

    # Deleting a missing blob is a no-op; if_absent then wins again.
    s.delete_object_sync("state/x")
    assert s.put_object_sync("state/x", b"v4", if_absent=True) is not None

    s.put_object_sync("config/a", b"1")
    assert s.list_objects_sync() == ["config/a", "state/x"]
    assert s.list_objects_sync("state/") == ["state/x"]


async def test_object_append_ladder(live_storage):
    s = live_storage

    # First append creates the append blob; later appends extend it.
    assert s.append_object_sync("logs/s.jsonl", b"a\n") is True
    assert s.append_object_sync("logs/s.jsonl", b"b\n") is True
    assert s.get_object_sync("logs/s.jsonl").data == b"a\nb\n"

    # The appended blob still supports the conditional object API: an
    # unconditional put replaces it (Put Blob resets the blob type), and
    # a fresh append on the replaced key falls back to the CAS rewrite
    # because the key now holds a block blob.
    etag = s.put_object_sync("logs/s.jsonl", b"rewritten\n")
    assert etag is not None
    assert s.append_object_sync("logs/s.jsonl", b"c\n") is True
    assert s.get_object_sync("logs/s.jsonl").data == b"rewritten\nc\n"


async def test_engine_kernel_smoke(live_storage):
    """One pass of the coordination kernel over real Azure CAS."""
    from cairndb import CairnDB

    db = CairnDB(live_storage)
    try:
        # claim: unique constraint, losers converge.
        won = await db.claim("dispatch/job:1", {"run": "a"})
        assert won.won is True
        lost = await db.claim("dispatch/job:1", {"run": "b"})
        assert lost.won is False
        assert lost.value == {"run": "a"}

        # lease: ownership, held-exclusion, signal absorbed without fencing.
        lease = await db.lease("state/run-1", ttl=60, holder="w1")
        assert lease is not None
        assert await db.lease("state/run-1", ttl=60, holder="w2") is None
        await db.signal("state/run-1", lambda s: {"cancel_requested": True})
        await lease.renew()
        assert lease.state == {"cancel_requested": True}
        assert lease.epoch == 1
        await lease.release()

        # transaction: multi-key atomicity over the _tx log.
        async with db.transact() as tx:
            tx.put("accounts/alice", b"90")
            tx.put("accounts/bob", b"110")
        assert (await db.objects.get("accounts/alice")).data == b"90"
        assert (await db.objects.get("accounts/bob")).data == b"110"
    finally:
        await db.close()
