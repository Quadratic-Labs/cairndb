# Lower-level API

The `CairnDB` facade composes a few components that you can use directly
when you need more control. The facade adds no storage semantics of its
own, so these components give the same guarantees.

| Component | Role | Facade equivalent |
|---|---|---|
| {class}`~cairndb.storage.BlobStorage` | a backend: commit, snapshot, and conditional-object primitives | `db.storage` |
| {class}`~cairndb.Committer` | the write path: commit protocol, group commit, durable ack | `db.log(name).append` |
| {class}`~cairndb.client.HandlerRegistry` | event type → handler mapping | `proj.on(...)` |
| {class}`~cairndb.client.CairnDBClient` | polling projection + read-only **SQLAlchemy** sessions | `db.projection(...)` |
| {class}`~cairndb.jobs.snapshot.SnapshotBuilder`, {func}`~cairndb.jobs.gc.collect_garbage` | the scheduled jobs | `cairndb snapshot` / `cairndb gc` |

## Creating a storage backend

```python
from cairndb.storage import StorageConfig, S3StorageConfig, create_storage

storage = S3StorageConfig(bucket="myapp", region="eu-west-1").create_storage()
storage = StorageConfig.from_dict({"type": "gcs", "bucket": "myapp"}).create_storage()
storage = StorageConfig.from_env().create_storage()          # CAIRNDB_* variables
storage = create_storage("filesystem", path="./data")

db = CairnDB(storage)                                        # the facade over an existing backend
```

See [Configuration](../reference/configuration.md) for every option.

## Writing with the Committer

```python
from cairndb import Committer, CommitterConfig

config = CommitterConfig(
    max_events_per_commit=500,     # cap on events per commit object
    max_batch_wait_seconds=0.02,   # linger to grow batches (0 = commit immediately)
)
async with Committer(storage, config) as committer:
    seq = await committer.append(event)            # durable when it returns
    seqs = await committer.append_many(events)     # relative order preserved
```

A `Committer` writes to whichever log its storage points at. Plain
storage means the root log. To write to a named log with the same
settings, use `Log(storage, "orders", committer_config=config)` from
`cairndb.engine`, or select the log's storage view with
{func}`~cairndb.engine.log_storage`:

```python
from cairndb.engine import log_storage

orders = log_storage(storage, "orders")      # logs/orders/…; None selects the root log
async with Committer(orders, config) as committer:
    ...
```

### Revalidate hooks

A revalidate hook re-checks pending events after the committer loses a
race, against the commits that won. It returns one decision per pending
event: the event (possibly modified) to commit it, or `None` to reject
it. A rejected event raises `EventRejectedError` from its `append()`.

```python
from cairndb import Commit, Event

async def unique_usernames(pending: list[Event], interleaved: list[Commit]) -> list[Event | None]:
    taken = {
        e.payload["username"]
        for commit in interleaved
        for e in commit.events
        if e.event_type == "user.registered"
    }
    return [
        None if e.event_type == "user.registered" and e.payload["username"] in taken else e
        for e in pending
    ]

committer = Committer(storage, revalidate=unique_usernames)
```

The hook sees only the commits that interleaved with *this* attempt. It
cannot see the whole history. Check invariants against the full history
before appending, for example with a projection query or a
[claim](../concepts/coordination.md#claim-unique-constraint) on the
username. The hook closes the race window between that check and the
commit.

## Reading with CairnDBClient and SQLAlchemy

`CairnDBClient` keeps a projection up to date in the background and hands
out **read-only SQLAlchemy `AsyncSession`s**. It reconnects transparently
after each atomic swap. Use it when you want an ORM over the projection:

```python
from sqlalchemy import select
from cairndb.client import CairnDBClient, ClientConfig, HandlerRegistry
from cairndb.storage import FilesystemStorageConfig

registry = HandlerRegistry()

@registry.handler("user.created")
async def on_user_created(conn, entry):
    await conn.execute("INSERT INTO users (id, name) VALUES (?, ?)",
                       (entry.payload["id"], entry.payload["name"]))

config = ClientConfig(
    storage=FilesystemStorageConfig(path="./data"),
    db_path="./projection.db",
    poll_interval_seconds=2.0,
    schema_version="1",
)
client = CairnDBClient(config, registry, init_schema=init_schema)
await client.start()

await client.wait_for_sequence(str(seq), timeout=10)     # read-your-writes
async with client.get_session() as session:
    users = (await session.execute(select(User))).scalars().all()

await client.stop()
```

`CairnDBClient` reads the root log of its configured storage. For a named
log, use `db.projection(..., log="orders")`, or build the lower-level
`Projector` / `BackgroundUpdater` on `log_storage(storage, "orders")`. `ClientConfig.from_env()` builds the same
configuration from environment variables (see
[Configuration](../reference/configuration.md#client-variables)).

The repository's `examples/simple_app` is a complete users-and-posts
application on this API, with SQLAlchemy ORM models:

```bash
python -m examples.simple_app.writer    # terminal 1: append events
python -m examples.simple_app.client    # terminal 2: query the projection
```

## Running the jobs from Python

The CLI is a thin wrapper. You can call the jobs directly, for example
from your own scheduler:

```python
from cairndb.jobs.gc import collect_garbage
from cairndb.jobs.snapshot import SnapshotBuilder

builder = SnapshotBuilder(storage, registry, init_schema=init_schema, schema_version="1")
commit = await builder.build()                          # snapshot through the current tail

result = await collect_garbage(storage, "1", keep_snapshots=3, prune_log=False)
```

For a named log, pass `log_storage(storage, "orders")`, or equivalently
`db.log("orders").storage`, as `storage`.
