# Installation

## Requirements

- **Python 3.14** or later.
- A storage backend. The local **filesystem** backend needs nothing else
  and is the right choice for development and tests. For production, use a
  bucket on Amazon S3, Google Cloud Storage, or Azure Blob Storage, or an
  S3-compatible store that supports conditional writes (see
  [Choosing a backend](../deployment/index.md#choosing-a-storage-backend)).

## Install from PyPI

The core package ships only the filesystem backend. Cloud SDKs and the
`cairndb` command-line tool are optional extras:

```bash
pip install cairndb              # core + filesystem backend
pip install "cairndb[s3]"        # + Amazon S3 / S3-compatible (boto3)
pip install "cairndb[gcs]"       # + Google Cloud Storage
pip install "cairndb[azure]"     # + Azure Blob Storage (with azure-identity)
pip install "cairndb[cli]"       # + the `cairndb` CLI (snapshot, gc, rebuild jobs)
```

Extras combine: `pip install "cairndb[s3,cli]"`.

| Extra | Pulls in | Needed for |
|---|---|---|
| *(none)* | `msgpack`, `sqlalchemy[asyncio]`, `aiosqlite`, `structlog` | engine, filesystem backend, projections |
| `s3` | `boto3>=1.35` (conditional writes) | `{"type": "s3"}` storage |
| `gcs` | `google-cloud-storage` | `{"type": "gcs"}` storage |
| `azure` | `azure-storage-blob`, `azure-identity` | `{"type": "azure"}` storage |
| `cli` | `typer` | the `cairndb` command |
| `docs` | Sphinx toolchain | building this documentation |
| `dev` | test, lint, and type-check tools, plus every backend SDK | contributing |

## Install from a checkout

```bash
git clone https://github.com/Quadratic-Labs/cairndb.git
cd cairndb
python3.14 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Verify the install

```bash
python -c "import cairndb; print(cairndb.__version__)"
cairndb --help        # requires the cli extra
```

The fastest end-to-end check is the bundled demo. It runs a writer and a
reader against a local directory:

```bash
CAIRNDB_STORAGE_TYPE=filesystem \
CAIRNDB_STORAGE_PATH=./demo-data \
CAIRNDB_DB_PATH=./demo-projection.db \
CAIRNDB_POLL_INTERVAL=1 \
python -m cairndb.client.run
```

A `demo.ping` event is appended every few seconds, and the reader's row
count catches up on each poll. Stop it with Ctrl-C. `make demo` does the
same from a checkout.

Next: the [Quickstart](quickstart.md).
