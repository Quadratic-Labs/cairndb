# CairnDB

**Event-sourced, serverless database on blob storage with SQLite projections**

CairnDB turns an object-storage bucket into a very cheap event-sourced
database. There are no servers anywhere: the bucket itself serializes
writers via conditional (put-if-absent) writes, readers query a local
SQLite projection kept fresh by polling, and the only scheduled compute
is a snapshot job.

- **Writes** are a library call: `await committer.append(event)` — durable
  and totally ordered the moment it returns.
- **Reads** are plain SQLite/SQLAlchemy against a local, read-only
  projection rebuilt deterministically from the event log.
- **Cost** is storage plus pennies of request fees; idle systems cost
  almost nothing.
- **Bonus**: the same conditional-write machinery is exposed as a generic
  object API (`get_object`/`put_object` with `if_match`/`if_absent`), giving
  other systems etag-guarded compare-and-swap documents on any supported
  backend — see "Generic Conditional Object Store" in
  [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Use Cases

- Small to medium datasets (up to ~10 GB per projection)
- Low/medium write volume (tens of commits per second) with complex read queries
- Strong auditability and determinism requirements
- Event-sourced architectures, time-travel and audit logs
- "I want a database but refuse to run or rent a database server"

## How It Works

```
writers ──PUT log/N+1 (if-absent)──▶  ┌────────────────────────┐
                                      │  Blob storage bucket   │
readers ◀──GET log/N+1 (poll)──────── │  log/000000000042.msgpack
                                      │  snapshots/v1/...sqlite │
snapshot job (cron) ◀──replay──────▶  └────────────────────────┘
```

1. Every write batch becomes an immutable, numbered commit object. The
   bucket accepts exactly one writer per number (`If-None-Match`), so the
   log is dense, gap-free, and totally ordered — no clocks, no leases,
   no sequencer service.
2. Clients replay commits through registered event handlers into a local
   SQLite file, swapped atomically so readers never see partial state.
3. A scheduled job replays the log into snapshot files that bound
   client startup time; garbage collection prunes what snapshots cover.

Multiple concurrent writers are safe by construction: a writer that loses
the race for commit N+1 fetches the winner's commits, optionally
revalidates its events against them, and retries at N+2.

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

### Write events

```python
from cairndb import Committer, Event, EventType, SchemaVersion, Timestamp
from cairndb.storage.filesystem import FilesystemStorage

storage = FilesystemStorage("./data")   # or S3Storage / GCSStorage / AzureBlobStorage

async with Committer(storage) as committer:
    sequence = await committer.append(
        Event(
            event_type=EventType("user.created"),
            timestamp=Timestamp.now(),
            payload={"id": 1, "name": "Alice"},
            schema_version=SchemaVersion("1.0.0"),
        )
    )
    # Durable and globally ordered as soon as append() returns
    print(sequence)  # 000000000001:000000
```

### Project and query

```python
from sqlalchemy import text
from cairndb.client import CairnDBClient, ClientConfig, HandlerRegistry

registry = HandlerRegistry()

@registry.handler("user.created")
async def handle_user_created(db, entry):
    await db.execute(
        "INSERT INTO users (id, name) VALUES (?, ?)",
        (entry.payload["id"], entry.payload["name"]),
    )

config = ClientConfig(
    storage_type="filesystem",
    storage_path="./data",
    db_path="./projection.db",
)

client = CairnDBClient(config, registry)
await client.start()  # background polling begins

# Read-your-writes when you need it
await client.wait_for_sequence(str(sequence))

async with client.get_session() as session:   # read-only SQLAlchemy session
    result = await session.execute(text("SELECT name FROM users"))
```

### Snapshots & retention (scheduled jobs)

```bash
export CAIRNDB_STORAGE_TYPE=s3 CAIRNDB_S3_BUCKET=my-bucket

cairndb snapshot --handlers myapp.projections:registry \
                  --init-schema myapp.projections:init_schema
cairndb gc --keep-snapshots 3            # add --prune-log to drop covered history
```

See [docs/QUICKSTART.md](docs/QUICKSTART.md) for the full walkthrough and
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design and its
invariants.

## Guarantees & Trade-offs

| Property | Guarantee |
|---|---|
| Durability | An acked write is in the bucket; there is no ack-before-durable window |
| Ordering | Total order across all writers, arbitrated by the bucket (no clocks) |
| Concurrency | Any number of writers; optimistic per-commit races with retry |
| Read consistency | Eventual (poll interval); per-session read-your-writes via `wait_for_sequence` |
| Throughput ceiling | ~1 commit per storage round-trip (batching multiplies events/commit) |
| Recovery | Everything outside the bucket is disposable and rebuilt by replay |

## License

MIT
