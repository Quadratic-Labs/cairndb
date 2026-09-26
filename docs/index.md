# CairnDB

**A serverless database engine on blob storage — the bucket is the only server.**

A cairn is a stack of stones raised one by one, by many independent hands,
with no custodian — and it stands for centuries. CairnDB works the same
way: all state lives in an object-storage bucket, every writer is just a
library call, and the bucket itself arbitrates concurrency through
conditional writes. No database server, no coordinator, no idle compute.

```python
from cairndb import CairnDB

db = CairnDB.configure({"storage": {"type": "s3", "bucket": "myapp"}})

await db.objects.put("config/app.json", data)                   # conditional KV
result = await db.claim("dispatch/etl:2026-08-10", {"run": 1})  # exactly-one winner
lease  = await db.lease("state/run-1", ttl=120)                 # fenced ownership
seq    = await db.log("orders").append(event)                   # durable, ordered
async with db.transact() as tx:                                 # multi-key atomicity
    tx.put("accounts/alice", alice_bytes)
    tx.put("accounts/bob", bob_bytes)
proj   = db.projection("orders_view", log="orders")             # SQL over the log
```

## What you get

| Layer | You get | Engine analogy |
|---|---|---|
| [Objects](concepts/objects.md) | etag-guarded key-value documents (CAS, put-if-absent) | atomic page writes |
| [Coordination](concepts/coordination.md) | `claim` (unique constraint), `lease` (fenced ownership), `doc` (retrying read-modify-write) | locks & constraints |
| [Logs](concepts/logs.md) | named, append-only commit logs — dense, totally ordered, durable on ack | write-ahead log |
| [Transactions](concepts/transactions.md) | optimistic multi-key atomicity, coordinated by a system log | transaction manager |
| [Projections](concepts/projections.md) | deterministic replay into local read-only SQLite, snapshots, time travel | indexes & materialized views |

## When to use it

CairnDB fits **low-to-medium write workloads that tolerate latency** in
exchange for near-zero operating cost:

- Small to medium datasets (up to ~10 GB per projection) with rich read
  queries and modest write volume.
- Strong auditability and determinism requirements: event sourcing,
  time-travel reads, rebuild-anywhere recovery.
- Coordination state for serverless and scale-to-zero systems: workflow
  ownership, exactly-once dispatch, checkpoints.
- "I want a database, but I refuse to run or rent a database server."

It is **not** a high-throughput OLTP database, a low-latency replication
engine, a general-purpose event bus, or a multi-region active-active
system. See [Guarantees and limits](concepts/consistency.md).

## Where to go next

- **New to CairnDB?** [Install](getting-started/installation.md) it, then
  follow the [Quickstart](getting-started/quickstart.md) — ten minutes,
  no cloud account needed.
- **Understand the model** — [Concepts](concepts/index.md) explains how a
  bucket can be a database, layer by layer.
- **Solve a problem** — [Guides](guides/index.md) collect recipes for
  coordination, operations, and the lower-level API.
- **Ship it** — [Deployment](deployment/index.md) covers local setups and
  AWS, Google Cloud, and Azure.
- **Look something up** — the [Reference](reference/index.md) is generated
  from the source docstrings.

```{toctree}
:hidden:
:caption: Getting started

getting-started/installation
getting-started/quickstart
```

```{toctree}
:hidden:
:caption: Concepts

concepts/index
concepts/storage-model
concepts/objects
concepts/coordination
concepts/logs
concepts/transactions
concepts/projections
concepts/consistency
```

```{toctree}
:hidden:
:caption: Guides

guides/index
guides/coordination-patterns
guides/lower-level-api
guides/operations
guides/troubleshooting
```

```{toctree}
:hidden:
:caption: Deployment

deployment/index
deployment/local
deployment/aws
deployment/gcp
deployment/azure
```

```{toctree}
:hidden:
:caption: Reference

reference/index
reference/configuration
reference/cli
reference/api/engine
reference/api/events
reference/api/writer
reference/api/storage
reference/api/client
reference/api/jobs
reference/api/exceptions
glossary
```

```{toctree}
:hidden:
:caption: Project

project/roadmap
project/changelog
project/contributing
project/code-of-conduct
```
