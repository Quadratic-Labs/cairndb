# Quickstart

This walkthrough builds a small application against a local directory. It
touches every layer: an event log, a SQL projection, coordination
primitives, and a multi-key transaction. Nothing here needs a cloud
account. Switching to a real bucket is a one-line configuration change.

All snippets run inside one `async def main()`. The complete script is at
the [end of the page](#the-complete-script).

## 1. Open an engine

One engine is one bucket, or one prefix of one bucket. Everything hangs
off the `CairnDB` object:

```python
from cairndb import CairnDB

db = CairnDB.configure({"storage": {"type": "filesystem", "path": "./data"}})
# In production, for example:
#   {"storage": {"type": "s3", "bucket": "myapp"}}
#   {"storage": {"type": "azure", "container": "myapp", "account_url": "https://…"}}
...
await db.close()
```

`CairnDB` is also an async context manager (`async with
CairnDB.configure(...) as db:`). Closing drains pending log writes and
stops background projection updaters.

An engine instance holds no authoritative state. Any number of processes
can open the same bucket at the same time. That is the point.

## 2. Append events to a log

A log is a named, append-only, totally ordered sequence of immutable
events:

```python
from cairndb import Event, EventType, SchemaVersion, Timestamp

users = db.log("users")
seq = await users.append(
    Event(
        event_type=EventType("user.created"),
        timestamp=Timestamp.now(),
        payload={"id": 1, "name": "Alice"},
        schema_version=SchemaVersion("1"),
    )
)
print(seq)  # 000000000001:000000  →  (commit 1, index 0)
```

**When `append` returns, the event is durable in the bucket.** The
returned {term}`sequence number` is the event's position in the log's
total order, which every writer of that log shares. Run as many writer
processes as you like: the bucket serializes them.

## 3. Project the log into SQLite

Reads go through a *projection*: a local SQLite file that handlers build
by replaying the log. Declare the schema and one handler per event type:

```python
import aiosqlite

async def init_schema(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
        )
        await conn.commit()

proj = db.projection("users_view", log="users", init_schema=init_schema)

@proj.on("user.created")
async def on_user_created(conn, entry):
    # conn: aiosqlite connection; entry: SequencedEvent
    await conn.execute(
        "INSERT INTO users (id, name) VALUES (?, ?)",
        (entry.payload["id"], entry.payload["name"]),
    )
```

Bring the projection up to date, then query it with plain `sqlite3`:

```python
await proj.refresh()            # catch up now; or proj.start() to poll in the background
await proj.wait_for(seq)        # read-your-writes: block until `seq` is applied

with proj.connect() as conn:    # read-only connection (mode=ro)
    print(conn.execute("SELECT id, name FROM users").fetchall())   # [(1, 'Alice')]
```

:::{important}
Handlers must be **deterministic**: the same events in must produce the
same rows out. Replay is how everything works: fresh clients, snapshots,
recovery, and time travel. Never read the clock, generate random IDs, or
call external services in a handler. Take such values from the event
payload instead.
:::

The projection file (`./users_view.v1.sqlite` by default) is a
disposable cache. Delete it, and the next refresh rebuilds it from the
log.

## 4. Coordinate workers

Coordination primitives work directly on storage. They need no log and
no projection.

```python
from cairndb import LeaseLost

# Unique constraint: exactly one caller wins; every loser reads back the
# winner's value, so duplicate callers converge.
result = await db.claim("dispatch/etl:2026-09-25", {"run_id": "r1"})
if result.won:
    ...

# Fenced ownership with expiry. None if someone else actively holds it.
lease = await db.lease("state/run-r1", ttl=60, holder="worker-a")
if lease is not None:
    try:
        ...                                  # do some work
        await lease.renew()                  # heartbeat between steps
        await lease.release(state={"status": "done"})
    except LeaseLost:
        pass                                 # fenced: someone else owns it now; discard our outcome

# A JSON document with a retrying read-modify-write loop.
counters = db.doc("metrics/daily")
await counters.update(lambda c: {**c, "runs": c["runs"] + 1}, create={"runs": 0})
```

## 5. Change several keys atomically

Plain conditional writes change one key at a time. A transaction changes
many keys, or none of them:

```python
import json
from cairndb import TransactionConflict

try:
    async with db.transact() as tx:
        alice = json.loads(await tx.get("accounts/alice"))   # reads join the read set
        bob = json.loads(await tx.get("accounts/bob"))
        tx.put("accounts/alice", json.dumps({"balance": alice["balance"] - 10}).encode())
        tx.put("accounts/bob", json.dumps({"balance": bob["balance"] + 10}).encode())
        tx.note("transfer.completed", {"amount": 10})         # audit record
except TransactionConflict:
    ...  # someone changed what we read: retry or report
```

Leaving the `async with` block commits. If a concurrent transaction wrote
anything this one read, the commit raises `TransactionConflict`.

## The complete script

```python
import asyncio
import json

import aiosqlite

from cairndb import CairnDB, Event, EventType, LeaseLost, SchemaVersion, Timestamp


async def init_schema(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
        )
        await conn.commit()


async def main() -> None:
    async with CairnDB.configure({"storage": {"type": "filesystem", "path": "./data"}}) as db:
        # Log + projection
        seq = await db.log("users").append(
            Event(
                event_type=EventType("user.created"),
                timestamp=Timestamp.now(),
                payload={"id": 1, "name": "Alice"},
                schema_version=SchemaVersion("1"),
            )
        )
        proj = db.projection("users_view", log="users", init_schema=init_schema)

        @proj.on("user.created")
        async def on_user_created(conn, entry):
            await conn.execute(
                "INSERT INTO users (id, name) VALUES (?, ?)",
                (entry.payload["id"], entry.payload["name"]),
            )

        await proj.refresh()
        await proj.wait_for(seq)
        with proj.connect() as conn:
            print(conn.execute("SELECT id, name FROM users").fetchall())

        # Coordination
        result = await db.claim("dispatch/etl:2026-09-25", {"run_id": "r1"})
        print("claim won:", result.won, result.value)

        lease = await db.lease("state/run-r1", ttl=60, holder="worker-a")
        if lease is not None:
            try:
                await lease.renew()
                await lease.release(state={"status": "done"})
            except LeaseLost:
                pass

        counters = db.doc("metrics/daily")
        print(await counters.update(lambda c: {**c, "runs": c["runs"] + 1}, create={"runs": 0}))

        # Transaction
        await db.objects.put("accounts/alice", json.dumps({"balance": 100}).encode())
        await db.objects.put("accounts/bob", json.dumps({"balance": 0}).encode())
        async with db.transact() as tx:
            alice = json.loads(await tx.get("accounts/alice"))
            bob = json.loads(await tx.get("accounts/bob"))
            tx.put("accounts/alice", json.dumps({"balance": alice["balance"] - 10}).encode())
            tx.put("accounts/bob", json.dumps({"balance": bob["balance"] + 10}).encode())
            tx.note("transfer.completed", {"amount": 10})
        print((await db.objects.get("accounts/alice")).data)   # b'{"balance": 90}'


asyncio.run(main())
```

Look inside `./data` afterwards. Every piece of state is a plain file:
commits under `logs/users/log/`, the transaction record under
`logs/_tx/log/`, and documents under the keys you chose. A bucket looks
exactly the same.

## Next steps

- [Concepts](../concepts/index.md): what each layer guarantees, and why.
- [Operations](../guides/operations.md): schedule snapshots and garbage
  collection before your log gets long.
- [Deployment](../deployment/index.md): move from `./data` to a real bucket.
- `examples/simple_app` in the repository: a users-and-posts app on the
  lower-level `Committer` / `CairnDBClient` API with SQLAlchemy models
  (see [Lower-level API](../guides/lower-level-api.md)).
