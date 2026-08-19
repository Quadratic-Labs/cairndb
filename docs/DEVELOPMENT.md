# CairnDB Development Guide

## Prerequisites

- Python 3.14+ (uses `uuid`-free integer sequences, modern typing)
- Git

## Setup

```bash
python3.14 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,cli]"
```

`[dev]` includes all storage SDKs (boto3, google-cloud-storage,
azure-storage-blob) so every backend's unit tests can run.

## Running Tests

```bash
pytest                                    # everything
pytest tests/unit                         # fast unit tests
pytest tests/integration                  # replay/projector/end-to-end
pytest --cov=cairndb --cov-report=term-missing
```

Notes on the test design:

- **Filesystem backend is the race simulator.** Its put-if-absent is a
  real atomic `os.link`, so concurrency tests (committer races, the
  end-to-end acceptance test) exercise genuine contention without cloud
  credentials.
- **Cloud backends are unit-tested against mocked SDK clients**, pinning
  the exact conditional-write parameters (`IfNoneMatch="*"`,
  `if_generation_match=0`, `overwrite=False`) and error mappings.
- `tests/integration/test_end_to_end.py` is the acceptance test:
  3 concurrent writers, a mid-run snapshot, a killed-and-replaced
  committer, and a fresh reader that must see every acked event exactly
  once over a dense, gap-free log.
- `tests/integration/test_snapshot_bootstrap.py` is the regression suite
  for the v1 bug where fresh clients silently lost pre-snapshot history.

### Testing against real object stores

The unit tests never hit the network. To smoke-test a real backend:

```bash
# MinIO (needs a version with conditional-write support, late 2024+)
docker run -d -p 9000:9000 minio/minio server /data

export CAIRNDB_STORAGE_TYPE=s3 \
       CAIRNDB_S3_BUCKET=cairndb-test \
       CAIRNDB_S3_ENDPOINT_URL=http://localhost:9000 \
       AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin

python -m cairndb.client.run
```

If a backend does not support `If-None-Match` on PUT, `put_commit` will
overwrite silently and CairnDB's ordering guarantees are void — verify
support before production use.

## Code Layout

```
src/cairndb/
├── committer.py        # write path: group commit, durable ack, races
├── cli.py              # cairndb snapshot | gc | rebuild
├── core/
│   ├── types.py        # SequenceNumber (commit, index), Timestamp
│   ├── log.py          # Event, Commit, SequencedEvent (msgpack)
│   └── exceptions.py
├── storage/
│   ├── base.py         # interface + key naming (log/, snapshots/)
│   ├── config.py       # per-backend StorageConfig dataclasses (env/dict-driven)
│   └── filesystem|s3|gcs|azure.py
├── client/
│   ├── connection.py   # CairnDBClient (read-only SQLAlchemy sessions)
│   ├── updater.py      # background polling loop
│   ├── projector.py    # atomic-swap updates, snapshot bootstrap
│   ├── replay.py       # commit replay, one transaction per commit
│   ├── discovery.py    # snapshot lookup + tail poll (one GET)
│   └── registry.py     # event handler registry
└── jobs/
    ├── snapshot.py     # scheduled snapshot builder
    └── gc.py           # retention for snapshots and the log
```

## Formatting & Linting

```bash
black src tests
ruff check src tests
mypy src
```

## Invariants (do not break)

See `docs/ARCHITECTURE.md`. In particular:

1. `put_commit` must be put-if-absent on every backend — the bucket is
   the only arbiter of ordering.
2. An `append()` future may only resolve after its PUT succeeded.
3. Replay must be deterministic; handlers see events in sequence order.
4. Projection updates are atomic swaps; readers never see partial state.
5. Commit numbers are dense: never create one except via a won
   conditional PUT, never delete except strictly below a kept snapshot.

## Release

```bash
python -m build
docker build -t cairndb-jobs .   # jobs image (cairndb CLI entrypoint)
```

Infrastructure templates for Azure (storage account, ACR, scheduled
Container Apps Jobs for snapshot/GC) live in `infra/`.
