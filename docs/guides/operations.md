# Operations

CairnDB has no server to operate. What remains is a handful of
**cron-shaped jobs**, each idempotent and safe to overlap:

| Job | Why | Typical schedule |
|---|---|---|
| `cairndb snapshot` | bound client startup time and replay length | nightly, or hourly for busy logs |
| `cairndb gc` | cap the number of snapshots, and optionally prune old log history | weekly |
| `db.recover_transactions()` | finish transactions that crashed between commit and apply | at process startup, and/or every few minutes |
| your sweepers | reclaim expired leases, and so on | as your workload requires |

The [Deployment](../deployment/index.md) pages show how to schedule them
on each platform.

## Sharing handlers between your app and the jobs

The snapshot job replays the log with the **same handlers and schema** as
your application's projection. Otherwise snapshots would not match what
clients build themselves. Keep them in one importable module, around a
module-level {class}`~cairndb.client.HandlerRegistry`. The CLI loads the
registry by reference, and the application passes the same object to
`db.projection(..., registry=...)`:

```python
# myapp/projections.py
import aiosqlite
from cairndb.client import HandlerRegistry

async def init_schema(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
        await conn.commit()

# For the CLI:  --handlers myapp.projections:registry
registry = HandlerRegistry()

@registry.handler("user.created")
async def on_user_created(conn, entry):
    await conn.execute("INSERT INTO users (id, name) VALUES (?, ?)",
                       (entry.payload["id"], entry.payload["name"]))

# For the application:
def users_projection(db):
    return db.projection("users_view", log="users",
                         init_schema=init_schema, registry=registry)
```

`proj.registry` is read-only. Replay is wired to the registry when the
projection is built, so pass the registry to `db.projection` rather than
assigning it afterwards.

## Snapshots

```bash
export CAIRNDB_STORAGE_TYPE=s3 CAIRNDB_S3_BUCKET=myapp

cairndb snapshot --handlers myapp.projections:registry \
                 --init-schema myapp.projections:init_schema
```

The job replays the whole log from commit 1 into a temporary SQLite file.
It then uploads the file with put-if-absent as
`snapshots/v{schema-version}/{last-commit}.sqlite`. Two jobs racing
produce the same object, so overlap is harmless. Pass
`--end-at <commit>` to build a point-in-time snapshot.

:::{important}
**`--schema-version` must equal the projection's `version`.** Both
default to `"1"`, so with defaults they agree. If you declare
`db.projection(..., version="2")`, pass `--schema-version 2` to the
snapshot and GC jobs. With mismatched values the job succeeds, but
clients never find its snapshots and silently replay from the start.
:::

Because each snapshot job replays the full log, its duration grows with
history. That is fine for the workloads CairnDB targets. It is also why
snapshots, not the log, are what clients bootstrap from.

## Named logs

Each CLI job acts on **one log per invocation**: the root log by
default, or the named log given by `--log` (or the `CAIRNDB_LOG`
environment variable, which suits container jobs):

```bash
cairndb snapshot --log orders --handlers myapp.projections:orders_registry
cairndb gc --log orders --keep-snapshots 3
CAIRNDB_LOG=orders cairndb rebuild --handlers myapp.projections:orders_registry
```

Run one scheduled snapshot job and one GC job per log that has a
projection. From Python, pass `db.log("orders").storage` (or
`cairndb.engine.log_storage(storage, "orders")`) to `SnapshotBuilder` or
`collect_garbage` (see
[Running the jobs from Python](lower-level-api.md#running-the-jobs-from-python)).

## Garbage collection

```bash
cairndb gc --keep-snapshots 3               # prune old snapshots only
cairndb gc --keep-snapshots 3 --prune-log   # also delete covered commits
```

- Snapshots: GC keeps the newest `--keep-snapshots` for the given schema
  version and deletes older ones.
- Log: only with `--prune-log`, GC deletes commits at or below the
  **oldest kept** snapshot. Every kept snapshot can still bootstrap, and
  history from that point stays replayable.

:::{warning}
`--prune-log` destroys history: time travel and audits before the oldest
kept snapshot become impossible. Log pruning considers one schema version
at a time. If several projection versions are live, prune only for the
version whose oldest kept snapshot is lowest, or not at all. Keeping full
history is usually cheap in cold storage tiers, and history is often the
point.
:::

Keys outside the engine's prefixes, such as claims, leases, and
documents, are never touched by GC. Expire them with your own sweeper, or
with bucket lifecycle rules scoped to their prefixes. Never apply
lifecycle rules to `log/`, `logs/`, `snapshots/`, or `txapplied/`.

## Transaction recovery

```python
recovered = await db.recover_transactions()
```

This re-applies committed transactions whose apply step never finished.
It is idempotent, and safe to run concurrently with live transactions.
Call it when a process starts. If your processes are short-lived, also
run it from a scheduled job.

## Rebuilding a projection

A projection is disposable. To rebuild one:

- **Delete the file.** The next refresh bootstraps from the latest
  snapshot and replays the tail.
- Or run `cairndb rebuild`, which deletes `CAIRNDB_DB_PATH` and does the
  same thing in one step (useful in ops scripts and containers):

  ```bash
  CAIRNDB_DB_PATH=./users.sqlite CAIRNDB_SCHEMA_VERSION=1 \
    cairndb rebuild --handlers myapp.projections:registry \
                    --init-schema myapp.projections:init_schema
  ```

To check determinism, compare a long-running client's projection with a
freshly built snapshot through the same commit (`cairndb snapshot
--end-at N`). The snapshot job always replays from commit 1. The two
must contain the same rows.

## Logging

CairnDB logs with [structlog](https://www.structlog.org/): structured
events such as `commit_won`, `lease_lost`, `projection_updated`, and
`snapshot_build_completed`, each with key/value context. Without
configuration, structlog prints human-readable lines. The CLI configures
JSON output with ISO timestamps, which suits log collectors. Configure
structlog in your application for the same:

```python
import structlog

structlog.configure(processors=[
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.add_log_level,
    structlog.processors.JSONRenderer(),
])
```

Signals worth alerting on:

| Event | Meaning |
|---|---|
| `lease_lost` | a worker was fenced; expected occasionally, alarming if frequent (ttl too short?) |
| `no_handler_for_event` | the projection skipped an event type; expected if intentional |
| failed jobs (non-zero exit) | snapshots are going stale; client bootstrap slows down |

## Backups and disaster recovery

The bucket is the whole database. Protect it with the provider's tools:
object versioning, soft delete, and cross-region replication. Everything
else, including projections, local files, and running processes, can be
rebuilt from the bucket. The logs, `txapplied/` markers, and snapshots
are immutable, which makes incremental copies straightforward.
