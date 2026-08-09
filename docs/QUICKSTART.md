# CairnDB Quick Start

Get a working event-sourced database — with zero servers — in 5 minutes.

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

### 1. Define events and handlers

Events are facts; handlers project them into SQLite tables.

```python
# myapp/projections.py
import aiosqlite
from cairndb.client import HandlerRegistry

registry = HandlerRegistry()

async def init_schema(db_path: str) -> None:
    """Create projection tables on a fresh database."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL DEFAULT ''
            )
        """)
        await db.commit()

@registry.handler("user.created")
async def handle_user_created(db, entry):
    # entry is a SequencedEvent: .sequence, .payload, .timestamp, .metadata
    await db.execute(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
        (entry.payload["id"], entry.payload["name"], entry.payload.get("email", "")),
    )
```

Handlers must be deterministic: same events in, same rows out. Replay is
how everything (clients, snapshots, recovery) works.

### 2. Write events

```python
from cairndb import Committer, Event, EventType, SchemaVersion, Timestamp
from cairndb.storage import StorageConfig

storage = StorageConfig.from_env().create_storage()

async with Committer(storage) as committer:
    seq = await committer.append(Event(
        event_type=EventType("user.created"),
        timestamp=Timestamp.now(),
        payload={"id": 1, "name": "Alice"},
        schema_version=SchemaVersion("1.0.0"),
    ))
# `seq` = (commit, index): a total order shared by all writers.
# append() returning IS the durability guarantee.
```

Run as many writer processes as you like — the bucket serializes them.
If your events carry invariants that other writers could invalidate
(uniqueness, balance checks), pass a `revalidate` hook to `Committer`;
it runs whenever another writer's commits interleave with yours.

### 3. Read with SQLAlchemy

```python
from cairndb.client import CairnDBClient, ClientConfig
from myapp.projections import registry, init_schema

config = ClientConfig.from_env()
client = CairnDBClient(config, registry, init_schema=init_schema)
await client.start()   # polls the log; one GET per interval when idle

async with client.get_session() as session:  # read-only: writes raise
    ...

# Read-your-writes for a specific flow:
await client.wait_for_sequence(str(seq), timeout=10)
```

### 4. Schedule snapshots and retention

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
