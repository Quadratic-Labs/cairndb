# Layer 3 — Projections

A projection is CairnDB's equivalent of an index or a materialized view.
Registered handlers fold a log's events into a **local SQLite database**
that your application queries with ordinary SQL. The projection is
derived state: you can delete it at any time, and replay rebuilds it.

```python
proj = db.projection(
    "orders_view",
    log="orders",                  # default: the root log
    version="2",                   # projection schema version (default "1")
    db_path="./orders.sqlite",     # default: ./{name}.v{version}.sqlite
    poll_interval=5.0,             # background polling interval, seconds
    init_schema=create_tables,     # async (db_path) -> None, runs on a fresh file
)

@proj.on("order.placed")
async def on_placed(conn, entry):              # aiosqlite connection, SequencedEvent
    await conn.execute("INSERT INTO orders VALUES (?, ?)",
                       (entry.payload["id"], entry.payload["total"]))

await proj.refresh()                # catch up now (one GET when idle)
await proj.start()                  # or: poll in the background
await proj.wait_for(seq, timeout=10)    # read-your-writes for one flow
with proj.connect() as conn:        # sqlite3, read-only (mode=ro)
    conn.execute("SELECT count(*) FROM orders").fetchone()
path = await proj.as_of(commit=1500)    # time travel (see below)
await proj.stop()
```

## How updates work

Each update is an **atomic swap**:

1. **Build a new file.** Copy the current projection to `…sqlite.new`,
   using a copy-on-write reflink when the filesystem supports one. On a
   fresh start, download the latest snapshot instead. If there is no
   snapshot either, create an empty database (metadata table plus
   `init_schema`).
2. **Replay.** Apply the new commits in order, **one SQLite transaction
   per commit**.
3. **Swap.** Rename `…sqlite.new` over the projection.

Readers therefore never observe a half-applied commit. The last applied
sequence is stored inside the projection itself (table
`_cairndb_metadata`), so a restarted process resumes exactly where it
left off. Updates are serialized per projection, so a background poll and
a manual `refresh()` never apply a commit twice.

:::{note}
The copy step makes each update proportional to the projection size.
Copy-on-write filesystems (XFS, Btrfs, APFS) make it nearly free. On
others, keep projections moderate (up to about 10 GB) and poll less
often.
:::

## Handlers

Handlers are `async def handler(conn, entry)`. `conn` is an `aiosqlite`
connection inside the commit's transaction, and `entry` is a
{class}`~cairndb.SequencedEvent`. Rules:

- **Deterministic.** The same events must produce the same rows. Take
  times, ids, and randomness from the event, never from the environment.
- **Pure SQL.** No network calls and no side effects outside the
  connection. A handler runs again on every rebuild, on every client, and
  in every snapshot job.
- **Selective by design.** Events whose type has no handler are skipped,
  with a `no_handler_for_event` warning in the logs. A projection folds
  only the event types it cares about. If a handler raises, replay aborts
  that commit's transaction with `ReplayError`, and the projection stays
  at the previous commit.
- **Versioned.** Handlers can read `entry.schema_version` to interpret
  older payload shapes.

## Snapshots

A snapshot is a fully built projection database through a given commit,
stored immutably in the bucket under
`[logs/{name}/]snapshots/v{version}/{commit}.sqlite`. Fresh clients
bootstrap from the latest snapshot for their `version` and then replay
only the tail. So snapshots bound both startup time and replay length.

A [scheduled job](../guides/operations.md#snapshots) builds snapshots.
Building is idempotent and race-safe: replay is deterministic and the
upload is put-if-absent, so overlapping jobs are harmless. A snapshot is
never diffed or patched. It is always rebuilt by replay.

## Schema versioning

Two things are versioned independently:

1. **Event schema version**: the shape of an event's payload, recorded on
   every event. Handlers use it to interpret payloads, so old events stay
   readable forever.
2. **Projection schema version**: the SQLite tables and indexes. It is the
   projection's `version`, and it selects the snapshot prefix
   `snapshots/v{version}/`.

To change a projection's tables, bump `version`. Clients on the new
version bootstrap from the new prefix, or replay from the start of the
log when no snapshot exists yet. Clients on the old version keep working
unchanged. Build new-version snapshots with the job's `--schema-version`
flag. Old and new versions coexist for as long as you need.

## Time travel

`as_of(commit)` builds a **throwaway** projection of the log *through*
`commit`. It bootstraps from the latest snapshot at or before that commit
and replays the rest. It returns the path of the new file:

```python
path = await proj.as_of(commit=1500)
with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
    ...
```

Time travel works as far back as the retained history allows. If garbage
collection pruned the log before your target, and no snapshot at or
before it remains, the build fails.

## Consistency

Projections are **eventually consistent**, trailing the log by up to one
poll interval. They are **monotonic** per client: a projection only moves
forward. **Read-your-writes** is opt-in per flow. `append()` returns a
sequence number, and `proj.wait_for(seq)` blocks until the projection has
applied it (or `timeout` expires, returning `False`).
