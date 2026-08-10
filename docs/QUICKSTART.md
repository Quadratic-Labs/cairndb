# CairnDB Quick Start

Get a working serverless database engine — logs, coordination, and SQL
projections with zero servers — in 5 minutes.

## Prerequisites

- Python 3.14 or later

## Installation

```bash
pip install -e ".[dev,cli]"     # from a checkout
# or: pip install cairndb[cli,s3]   # from PyPI, with the S3 backend
```

## The 5-minute demo

A committer and a reader sharing a filesystem "bucket", in one process:

```bash
CAIRNDB_STORAGE_TYPE=filesystem \
CAIRNDB_STORAGE_PATH=./demo-data \
CAIRNDB_DB_PATH=./demo-projection.db \
CAIRNDB_POLL_INTERVAL=1 \
python -m cairndb.client.run
```

You'll see a `demo.ping` event appended every ~3 seconds (each one durable
in `./demo-data/log/` before it is acknowledged) and the reader's
projection row count catching up on each poll. Ctrl-C to stop. Delete
`demo-projection.db` at any time — it is a disposable cache, fully
rebuilt from the log.

## Building your own application

### 1. Configure the engine

One engine = one bucket. Everything hangs off the `CairnDB` facade
(see [ENGINE_API.md](ENGINE_API.md) for the full API and semantics):

```python
from cairndb import CairnDB

db = CairnDB.configure({"storage": {"type": "s3", "bucket": "myapp"}})
# filesystem for dev: {"storage": {"type": "filesystem", "path": "./data"}}
...
await db.close()
```

### 2. Write events to a log

Events are immutable facts on a named, totally ordered commit log:

```python
from cairndb import Event, EventType, SchemaVersion, Timestamp

seq = await db.log("users").append(Event(
    event_type=EventType("user.created"),
    timestamp=Timestamp.now(),
    payload={"id": 1, "name": "Alice"},
    schema_version=SchemaVersion("1.0.0"),
))
# `seq` = (commit, index): a total order shared by all of this log's
# writers. append() returning IS the durability guarantee.
```

Run as many writer processes as you like — the bucket serializes them per
log. Use separate named logs for independent domains; each log has its
own sequence and its own throughput budget.

### 3. Project and query

Handlers replay events into a local, read-only SQLite file. They must be
deterministic: same events in, same rows out — replay is how everything
(clients, snapshots, recovery, time travel) works.

```python
import aiosqlite

async def init_schema(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        await conn.commit()

proj = db.projection("users_view", log="users", init_schema=init_schema)

@proj.on("user.created")
async def handle_user_created(conn, entry):
    # entry is a SequencedEvent: .sequence, .payload, .timestamp, .metadata
    await conn.execute(
        "INSERT INTO users (id, name) VALUES (?, ?)",
        (entry.payload["id"], entry.payload["name"]),
    )

await proj.start()                    # background polling (or: await proj.refresh())
await proj.wait_for(seq)              # read-your-writes for a specific flow

with proj.connect() as conn:          # sqlite3, mode=ro — writes raise
    rows = conn.execute("SELECT name FROM users").fetchall()

old = await proj.as_of(commit=42)     # time travel: projection through commit 42
```

### 4. Coordinate workers — no log required

Coordination is storage-level: put-if-absent and compare-and-swap on
documents.

```python
# Unique constraint: exactly one caller wins; losers converge on the
# winner's value (idempotent dispatch, cron-tick dedup, step checkpoints).
result = await db.claim("dispatch/daily-etl:2026-08-10", {"run_id": rid})

# Fenced ownership: expired leases can be stolen; a fenced holder's
# writes raise LeaseLost and must be discarded.
lease = await db.lease(f"state/{rid}", ttl=120, holder=worker_id)
if lease is not None:
    try:
        ...                                   # do the work
        await lease.renew()                   # heartbeat between steps
        await lease.release(state={"status": "done"})
    except LeaseLost:
        return                                # someone else owns it now

# Typed document with a retrying read-modify-write loop:
counters = db.doc("metrics/daily")
await counters.update(lambda c: {**c, "runs": c["runs"] + 1}, create={"runs": 0})
```

### 5. Change several keys atomically

```python
from cairndb import TransactionConflict

try:
    async with db.transact() as tx:
        alice = await tx.get("accounts/alice")     # reads join the read set
        tx.put("accounts/alice", debit(alice, 10))
        tx.put("accounts/bob", credit(bob, 10))
        tx.note("transfer.completed", {"amount": 10})
except TransactionConflict:
    ...  # read set was written concurrently — retry or surface
```

The commit point is a record on the engine's transaction log; run
`await db.recover_transactions()` at startup (or on a cron) to finish any
transaction that crashed between commit and apply.

### 6. Schedule snapshots and retention

Snapshots bound client startup time (bootstrap = download snapshot +
replay tail). Run as a cron/scheduled container job:

```bash
cairndb snapshot --handlers myapp.projections:registry \
                  --init-schema myapp.projections:init_schema \
                  --schema-version 1.0.0

cairndb gc --keep-snapshots 3               # snapshots only
cairndb gc --keep-snapshots 3 --prune-log   # also drop covered history
```

`infra/` contains Azure Container Apps Jobs templates wiring both to a
schedule. Building a snapshot is idempotent and race-safe — overlapping
runs are harmless.

## Under the hood: the lower-level API

The facade wraps components you can use directly when you need more
control — the engine adds no storage semantics of its own:

```python
from cairndb import Committer, Event
from cairndb.client import CairnDBClient, ClientConfig, HandlerRegistry
from cairndb.storage import StorageConfig

storage = StorageConfig.from_env().create_storage()

# Write path: the commit protocol, group commit, durable ack.
async with Committer(storage) as committer:
    seq = await committer.append(event)

# Read path: SQLAlchemy sessions over the polling projection client.
registry = HandlerRegistry()
client = CairnDBClient(ClientConfig.from_env(), registry, init_schema=init_schema)
await client.start()
async with client.get_session() as session:   # read-only
    ...
await client.wait_for_sequence(str(seq), timeout=10)
```

If your events carry invariants that other writers could invalidate
(uniqueness, balance checks), pass a `revalidate` hook to `Committer`; it
runs whenever another writer's commits interleave with yours. This is the
same mechanism the transaction layer uses for conflict detection.

## Configuration reference (env vars)

| Variable | Purpose |
|---|---|
| `CAIRNDB_STORAGE_TYPE` | `filesystem` \| `s3` \| `gcs` \| `azure` |
| `CAIRNDB_STORAGE_PATH` | filesystem root |
| `CAIRNDB_S3_BUCKET` / `CAIRNDB_S3_REGION` / `CAIRNDB_S3_ENDPOINT_URL` | S3 settings |
| `CAIRNDB_AZURE_CONTAINER` / `CAIRNDB_AZURE_CONNECTION_STRING` / `CAIRNDB_AZURE_ACCOUNT_URL` | Azure settings |
| `CAIRNDB_GCS_PROJECT` / `CAIRNDB_GCS_CREDENTIALS_PATH` | GCS settings |
| `CAIRNDB_STORAGE_PREFIX` | key prefix inside the bucket |
| `CAIRNDB_DB_PATH` | local projection path (client) |
| `CAIRNDB_POLL_INTERVAL` | seconds between polls (client) |
| `CAIRNDB_SCHEMA_VERSION` | projection schema version (client) |

S3-compatible stores (MinIO, R2, …) must support conditional writes
(`If-None-Match`); AWS S3, GCS, and Azure support them natively.
