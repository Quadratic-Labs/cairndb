# CairnDB

**A serverless database engine on blob storage — the bucket is the only server.**

A cairn is a stack of stones raised one by one, by many independent hands,
with no custodian — and it stands for centuries. CairnDB works the same
way: all state lives in an object-storage bucket, every writer is just a
library call, and the bucket itself arbitrates concurrency through
conditional writes. No database server, no coordinator, no idle compute.

On that substrate CairnDB exposes the layers a database engine is made of:

| Layer | You get | Engine analogy |
|---|---|---|
| Objects | etag-guarded key-value documents (CAS, put-if-absent) | atomic page writes |
| Coordination | `claim` (unique constraint), `lease` (fenced ownership), `doc` (retrying read-modify-write) | locks & constraints |
| Logs | named, append-only commit logs — dense, totally ordered, durable on ack | WAL |
| Transactions | optimistic multi-key atomicity, coordinated by a system log | transaction manager |
| Projections | deterministic replay into local read-only SQLite, snapshots, time travel | indexes & materialized views |

```python
from cairndb import CairnDB

db = CairnDB.configure({"storage": {"type": "s3", "bucket": "myapp"}})

await db.objects.put("config/app.json", data)                  # conditional KV
result = await db.claim("dispatch/etl:2026-08-10", {"run": 1})  # exactly-one winner
lease  = await db.lease("state/run-1", ttl=120)                 # fenced ownership
seq    = await db.log("orders").append(event)                   # durable, ordered
async with db.transact() as tx:                                 # multi-key atomicity
    tx.put("accounts/alice", alice)
    tx.put("accounts/bob", bob)
proj   = db.projection("orders_view", log="orders")             # SQL over the log
```

The full design and semantics live in [docs/ENGINE_API.md](docs/ENGINE_API.md);
the storage invariants in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Use Cases

- Small to medium datasets (up to ~10 GB per projection) with complex read
  queries and low/medium write volume
- Strong auditability and determinism requirements: event sourcing,
  time-travel reads, rebuild-anywhere recovery
- Coordination state for serverless/scale-to-zero systems: workflow
  ownership, exactly-once dispatch, checkpoints (this is how
  [Flowlet](https://github.com/Quadratic-Labs/flowlet) runs its control
  plane)
- "I want a database but refuse to run or rent a database server"

## How It Works

```
writers ──PUT log/N+1 (if-absent)──▶  ┌────────────────────────┐
                                      │  Blob storage bucket   │
readers ◀──GET log/N+1 (poll)──────── │  log/000000000042.msgpack
                                      │  logs/{name}/...        │
                                      │  snapshots/v1/...sqlite │
snapshot job (cron) ◀──replay──────▶  └────────────────────────┘
```

1. Every write batch becomes an immutable, numbered commit object. The
   bucket accepts exactly one writer per number (`If-None-Match`), so each
   log is dense, gap-free, and totally ordered — no clocks, no sequencer
   service. Coordination documents use the same conditional-write
   machinery with etags (compare-and-swap).
2. Clients replay commits through registered event handlers into a local
   SQLite file, swapped atomically so readers never see partial state.
3. A scheduled job replays the log into snapshot files that bound client
   startup time; garbage collection prunes what snapshots cover. Nothing
   outside the bucket needs to survive.

Multiple concurrent writers are safe by construction: a writer that loses
the race for commit N+1 fetches the winner's commits, optionally
revalidates its events against them, and retries at N+2. A fenced lease
holder's writes are rejected by the storage itself.

## Installation

```bash
# Requires Python 3.14+
pip install cairndb            # filesystem backend only
pip install cairndb[s3]        # + Amazon S3 / S3-compatible
pip install cairndb[gcs]       # + Google Cloud Storage
pip install cairndb[azure]     # + Azure Blob Storage
pip install cairndb[cli]       # + `cairndb` CLI (snapshot/gc/rebuild jobs)
```

## Quick Start

```python
from cairndb import CairnDB, Event, EventType, SchemaVersion, Timestamp

db = CairnDB.configure({"storage": {"type": "filesystem", "path": "./data"}})

# Write: durable and totally ordered as soon as append() returns
seq = await db.log("users").append(
    Event(
        event_type=EventType("user.created"),
        timestamp=Timestamp.now(),
        payload={"id": 1, "name": "Alice"},
        schema_version=SchemaVersion("1.0.0"),
    )
)

# Project: replay events into local SQLite, declaratively
proj = db.projection("users_view", log="users", init_schema=init_users)

@proj.on("user.created")
async def handle_user_created(conn, entry):
    await conn.execute(
        "INSERT INTO users (id, name) VALUES (?, ?)",
        (entry.payload["id"], entry.payload["name"]),
    )

await proj.wait_for(seq)                     # read-your-writes
with proj.connect() as conn:                 # read-only SQLite
    rows = conn.execute("SELECT name FROM users").fetchall()

await db.close()
```

Coordination needs no handlers or projections — it is storage-level:

```python
result = await db.claim(f"dispatch/{key}", {"run_id": run_id})
if result.won:
    lease = await db.lease(f"state/{run_id}", ttl=120, holder=worker_id)
    ...work, calling await lease.renew() as a heartbeat...
    await lease.release(state={"status": "done"})   # LeaseLost if we were fenced
```

### Snapshots & retention (scheduled jobs)

```bash
export CAIRNDB_STORAGE_TYPE=s3 CAIRNDB_S3_BUCKET=my-bucket

cairndb snapshot --handlers myapp.projections:registry \
                  --init-schema myapp.projections:init_schema
cairndb gc --keep-snapshots 3            # add --prune-log to drop covered history
```

See [docs/QUICKSTART.md](docs/QUICKSTART.md) for the full walkthrough,
including the lower-level `Committer`/`HandlerRegistry` API the facade is
built on.

## Guarantees & Trade-offs

| Property | Guarantee |
|---|---|
| Durability | An acked write is in the bucket; there is no ack-before-durable window |
| Ordering | Total order per log, arbitrated by the bucket (no clocks) |
| Concurrency | Any number of writers; optimistic races with retry; epoch-fenced leases |
| Transactions | Optimistic multi-key atomicity; commit point is a log record; conflicting transactions abort |
| Read consistency | Eventual (poll interval); per-session read-your-writes via `wait_for` |
| Throughput ceiling | ~1 commit per storage round-trip per log (batching multiplies events/commit; shard across named logs) |
| Recovery | Everything outside the bucket is disposable and rebuilt by replay |

## License

MIT
