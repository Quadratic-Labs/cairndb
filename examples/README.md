# CairnDB Examples

Example applications built on the current, serverless architecture:
a `Committer` writes events directly to storage, and a `CairnDBClient`
reads them back through a local SQLite projection. There is no server.

For a guided walkthrough, start with [`docs/QUICKSTART.md`](../docs/QUICKSTART.md).

## simple_app

A minimal end-to-end application: users and posts.

**Files:**

- `simple_app/models.py` — application-owned SQLAlchemy ORM models
- `simple_app/handlers.py` — event handlers projecting events into SQLite
- `simple_app/writer.py` — appends events via the `Committer`
- `simple_app/client.py` — queries the projection via `CairnDBClient`

**Running it** (from the repo root, after `pip install -e ".[dev]"`):

```bash
# Terminal 1 — write some events (durable once append returns):
python -m examples.simple_app.writer

# Terminal 2 — query the projection:
python -m examples.simple_app.client
```

Both default to `./data` for storage and `./projection.db` for the
projection. The projection file is a disposable cache — delete it any
time; it is rebuilt from the log.

## Future Examples

- Schema migration (versioned snapshots + application-defined migrations)
- Multi-writer contention with a `RevalidateHook`
- Cloud storage backends (S3, GCS, Azure)
- Performance benchmarking
