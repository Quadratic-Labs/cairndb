# Storage model

## The bucket is the arbiter

Every backend CairnDB supports provides two atomic conditional writes.
Everything else is built from them:

| Primitive | Meaning | S3 | GCS | Azure Blob | Filesystem |
|---|---|---|---|---|---|
| **put-if-absent** | create only if the key does not exist | `IfNoneMatch="*"` | `if_generation_match=0` | `overwrite=False` | temp file + atomic `os.link` |
| **compare-and-swap** | replace only if the etag still matches | `IfMatch=<etag>` | `if_generation_match=<gen>` | `If-Match` etag | content MD5 under a `flock` |

A losing writer learns that it lost: the call returns `None` instead of
a new etag. It then re-reads and decides what to do. No lock is held
between attempts, so a crashed writer can never block anyone.

:::{warning}
If a backend silently ignores these preconditions, for example an old
S3-compatible server without `If-None-Match` support on PUT, writes
overwrite each other and every ordering guarantee is void. Check support
before you use such a backend in production. See
[Choosing a storage backend](../deployment/index.md#choosing-a-storage-backend).
:::

## Core principles

These invariants hold across the whole system. Every other guarantee
depends on them.

1. **Append-only source of truth.** Log writes are immutable commit
   objects. A commit is never mutated in place.
2. **Dense sequence per log, enforced by the bucket.** Within a log,
   commits are numbered 1, 2, 3, … with no gaps. A conditional
   put-if-absent write arbitrates each number. No process, lease, or
   clock does. Many named logs can coexist, each with its own sequence.
3. **Blob storage is the ledger.** Commits, snapshots, and documents live
   in the bucket. It is the ultimate source of truth.
4. **Durable ack.** A write is acknowledged only after its object is
   durably stored. No window exists in which an acknowledged write can be
   lost.
5. **SQLite is a projection, not the truth.** Local databases are
   derived, read-only views. They can always be rebuilt from snapshots
   plus the log.
6. **Deterministic replay.** Replaying the same commits always produces
   the same SQLite state.
7. **Eventual consistency for reads.** Projections trail the log by up
   to one poll interval. Read-your-writes is opt-in, per flow.

## Bucket layout

All log and snapshot objects are immutable. These prefixes are reserved
for the engine. `db.objects` rejects application keys under them:

```text
log/000000000042.msgpack             # root log: commit #42 (an ordered batch of events)
snapshots/v1/000000000040.sqlite     # root-log projection through commit #40, schema v1
logs/{name}/log/…                    # a named log: same layout, own sequence
logs/{name}/snapshots/v1/…           # snapshots of a named log's projections
logs/_tx/log/…                       # the engine's transaction log
txapplied/000000000007               # marker: transaction commit #7 applied
```

Everything else in the bucket belongs to you: claims, leases, documents,
and plain objects, under any key you choose (`dispatch/…`, `state/…`,
`accounts/…`).

Names are zero-padded to a fixed width of 12 digits, so lexicographic
order equals numeric order. Clients discover snapshots by listing the
snapshot prefix. No manifest or pointer object exists that could go
stale.

A storage **prefix** (the `prefix` option on S3, GCS, and Azure) moves
this whole layout under a sub-path. That lets several independent engines
share one bucket.

## Serialization

The wire format is fixed per plane. It is not pluggable.

- **Control plane** (claims, leases, documents): canonical JSON with
  sorted keys and tight separators, so a person can read and diff
  documents while debugging.
- **Data plane** (log commits): msgpack, for compact storage, fast bulk
  decode on replay, and native binary payloads.
- **Timestamps** in both planes serialize as canonical RFC 3339, fixed
  width and always UTC: `YYYY-MM-DDTHH:MM:SS.ffffffZ`.

## Sync and async

The cloud SDKs are synchronous underneath. So every storage and layer-0/1
operation exists in its primitive `*_sync` form, and the async method
wraps it in a worker thread: `db.claim` / `db.claim_sync`,
`db.objects.put` / `db.objects.put_sync`, `lease.renew` /
`lease.renew_sync`, and so on. Synchronous programs can use CairnDB's
coordination layer without an event loop. Logs, transactions, and
projections are async-only.
