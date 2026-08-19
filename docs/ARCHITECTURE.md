# CairnDB Architecture

CairnDB is a serverless database engine for low/medium workloads, built
directly on blob storage. There are no long-running servers: writing is a
library call arbitrated by the bucket itself, reading is a local SQLite
file, and the only scheduled compute is a snapshot job.

This document describes the storage substrate and its invariants — the
commit log, snapshots, projections, and the conditional object store. The
engine layers built on top of it (coordination, named logs, transactions,
declarative projections, behind the `CairnDB` facade) are specified in
[ENGINE_API.md](ENGINE_API.md); everything there reduces to the two write
primitives defined here.

---

## Core Principles

These principles are invariants and must not be violated by implementations.

1. **Append-only source of truth**
   - All writes are immutable commit objects in the log.
   - No in-place mutation of data ever occurs.

2. **Dense sequence per log, enforced by the bucket**
   - Within a log, commits are numbered densely (1, 2, 3, …) with no gaps.
   - Ordering is total and authoritative, arbitrated by conditional
     (put-if-absent) writes — never by a process, a lease, or a clock.
   - There may be many named logs, each with its own sequence; ordering is
     only defined within a log, and sharding across logs is the scaling
     mechanism.

3. **Blob storage is the ledger**
   - Commit objects and snapshots are stored in blob/object storage.
   - Blob storage is the ultimate source of truth.

4. **Durable ack**
   - A write is acknowledged only after its commit object is durably stored.
   - There is no window in which an acknowledged write can be lost.

5. **SQLite is a projection, not the truth**
   - SQLite databases are *derived*, read-only views.
   - They can always be rebuilt from snapshots + the commit log.

6. **Deterministic replay**
   - Replaying the same commits always produces the same SQLite state.
   - Projection logic must be idempotent and versioned.

7. **Eventual consistency**
   - Clients are eventually consistent with the ledger (polling).
   - Read-your-writes is available per session via `wait_for_sequence`.

---

## High-Level Architecture

```mermaid
flowchart TD
    Writer[Writer App\nCommitter library]
    App[Reader App\n(SQLAlchemy)]
    SQLite[SQLite Projection\n• Derived state\n• Read-only]
    Updater[Client Updater\n• Polls log tail\n• Replays commits\n• Atomic swap]
    Blob[Blob Storage\n• log/N commit objects\n• snapshots/vX/N.sqlite]
    Job[Snapshot Job\n• Scheduled, serverless\n• Replays log → snapshot]

    Writer -->|PUT log/N+1 if-absent| Blob
    App -->|SELECT queries only| SQLite
    Updater -->|transactional rebuild| SQLite
    Blob -->|snapshots + commits| Updater
    Blob <-->|read log / write snapshot| Job
```

There is no server between writers and the bucket. Any process holding
credentials and the committer library is a writer; the bucket serializes them.

---

## Storage Layout

All log/snapshot objects are immutable. Reserved prefixes:

```
log/000000000042.msgpack          # root log, commit #42: one msgpack object
                                  # per commit (an ordered batch of events)
snapshots/v1/000000000040.sqlite  # root-log projection state through commit
                                  # #40, for projection schema version 1
logs/{name}/log/…                 # named logs: same layout per log
logs/{name}/snapshots/v1/…        # snapshots of a named log's projections
logs/_tx/log/…                    # the engine's transaction log
txapplied/000000000007            # marker: tx commit #7 applied to objects
```

Names are zero-padded fixed width so lexicographic order equals numeric order.
Every log is **dense**: commit N+1 is only ever created by a successful
put-if-absent, so there are never gaps. Snapshots are discovered by listing
their prefix; no manifest or pointer object exists.

**Serialization convention** — the wire format is fixed per plane, not
pluggable: control-plane objects (claims, leases, coordination documents) are
canonical JSON (sorted keys, tight separators) so they can be inspected and
diffed by hand when debugging; the data-plane log is msgpack for size, decode
speed on bulk replay, and native binary payloads. Timestamps serialize in
canonical RFC 3339 (`YYYY-MM-DDTHH:MM:SS.ffffffZ`, fixed width, always UTC) in
both.

Consumers of the generic object API (below) may keep key-addressed objects
under any *other* prefix in the same store; the engine rejects application
keys under the reserved prefixes.

---

## Components

### 1. Committer (Writer Library)

The write path is a client library, embeddable in any process (an app, a
scale-to-zero container, a batch job).

**Commit protocol**

1. Hold the last known commit number N.
2. Serialize the pending batch of events as commit object N+1.
3. `PUT log/{N+1}` with put-if-absent
   (S3 `IfNoneMatch="*"`, GCS `if_generation_match=0`,
   Azure `overwrite=False`, filesystem `O_CREAT|O_EXCL`).
4. Success → the batch is durable; acknowledge every event with its
   sequence number `(commit, index)`.
5. Precondition failure → another writer won N+1. Fetch the winning
   commit(s), advance N, optionally revalidate the batch (application
   hook), and retry.

**Group commit**: events submitted while a PUT is in flight accumulate into
the next commit. Callers await a future resolved only after the PUT succeeds.
This preserves durable ack while amortizing storage round-trips.

**Non-responsibilities**: no SQL, no reads from projections, no client
tracking, no push notifications. The committer is intentionally simple.

### 2. Commit Objects

A commit object is:
- immutable
- an ordered batch of events (`event_type`, effective timestamp, payload,
  event schema version, metadata)
- msgpack-serialized
- the **only write path** in the system

An event's global sequence number is its position: `(commit_number, index)`.
Ordering never depends on wall clocks; timestamps are informational.

### 3. Snapshots

A snapshot is a fully built SQLite database representing the projection
through a given commit number, stored as an immutable blob under its
projection schema version prefix.

Snapshots are produced by a **scheduled batch job** (e.g. nightly). Building
is idempotent and race-safe: replay is deterministic and the upload is
put-if-absent, so duplicate jobs are harmless. Snapshots bound client startup
time and log-replay length. They are never diffed — always rebuilt by replay.

### 4. Client Updater (Reader Library)

Each reading client runs a background updater that:

- bootstraps from the latest snapshot for its schema version, then replays
  the log tail on top
- polls for new commits with a single `GET log/{last+1}` (404 = up to date;
  the dense log makes this exact and cheap)
- applies commits in order via registered event handlers
- updates the projection with an **atomic swap** (copy → apply → rename),
  so readers never see partial state
- tracks the last applied sequence inside the projection

### 5. SQLite Projection

- local to the client, read-only for application code
- fully derived; can be deleted and rebuilt at any time
- SQLAlchemy connects read-only (`mode=ro`); from its perspective this is a
  normal SQLite database

### 6. Generic Conditional Object Store

`BlobStorage` also exposes the conditional-write machinery directly, for
consumers that need **mutable, etag-guarded documents** next to the immutable
logs. This is the second write primitive of the system: the engine's
coordination layer (`claim`, `lease`, `doc` — see
[ENGINE_API.md](ENGINE_API.md)) is built entirely on it, and it is how
[Flowlet](https://github.com/Quadratic-Labs/flowlet) implements run-state
ownership transfer on blob storage:

- `get_object(key)` → data + current etag
- `put_object(key, data, if_absent=True)` → put-if-absent
- `put_object(key, data, if_match=etag)` → compare-and-swap; fails (returns
  `None`) if the object changed or was deleted since the etag was read
- `delete_object(key)` / `list_objects(prefix)`

Etags are opaque and backend-native: Azure blob etags (`If-Match`), S3 etags
(`IfMatch`), GCS generation numbers (`if_generation_match`), and content MD5
under an flock-serialised compare-and-replace on the filesystem backend. Each
method has a synchronous `*_sync` twin — the primitive form — so synchronous
callers don't need an event loop.

The commit log uses none of this beyond put-if-absent: log objects stay
immutable.

### 7. Engine Facade

Everything above is exposed through the `CairnDB` facade
([ENGINE_API.md](ENGINE_API.md)), which composes the two write primitives
into higher-level machinery without adding any new storage semantics:

- **Named logs** reroute the commit protocol under `logs/{name}/` via the
  generic object API — same Committer, one sequencer per log.
- **Coordination** (`claim`/`lease`/`doc`) is put-if-absent and CAS on
  documents, with epochs for fencing.
- **Transactions** use a dedicated log (`logs/_tx/`) as the commit point
  and fold committed mutations into the object store idempotently.
- **Projections** wrap the client updater declaratively, per log.

---

## Consistency Model

- **Writes**: linearizable — the bucket serializes commits; an acked write
  is durable and totally ordered within its log.
- **Objects**: linearizable per key (backend conditional writes); claims
  have exactly one winner; leases exclude concurrent holders up to clock
  skew, and fencing by epoch + CAS makes even a paused holder's late
  writes fail.
- **Transactions**: optimistic serializability between transactions; the
  commit point is a durable record in `logs/_tx/`, and conflicting
  transactions abort (`TransactionConflict`).
- **Reads**: eventually consistent, seconds of lag by design (poll interval).
- **Per-client reads**: monotonic (the projection only moves forward).
- **Read-your-writes**: opt-in per flow — the write returns `(commit, index)`;
  `wait_for_sequence` blocks until the projection has applied it.
- **Failure recovery**: replay from snapshot + log. Crashes, lost races, and
  restarts are all handled by re-reading the log; there is no state to lose
  outside the bucket.

**Concurrent writers** are safe by construction: every commit is a
put-if-absent on the exact next number, so a stale or "zombie" writer's PUT
is rejected by the storage itself (fencing). Cross-writer application
invariants (e.g. uniqueness) are the application's responsibility via the
committer's revalidate hook.

---

## Schema & Versioning

Two independently versioned layers:

1. **Event schema version** — structure of events; recorded per commit;
   used by handlers to interpret payloads.
2. **Projection schema version** — SQLite tables/indices; encoded in the
   snapshot prefix (`snapshots/v2/…`). Bumping it means new snapshots are
   built under the new prefix by replaying the same log; old and new can
   coexist during migration.

---

## Cost Model

- Write: 1 conditional PUT per commit (group commit batches events).
- Read poll: 1 GET per interval per client (404 when idle) — cents/month.
- Snapshot job: scheduled serverless container, minutes/day.
- No idle compute anywhere. Storage is the only always-on cost.

Throughput ceiling: one commit per storage round-trip (~10–30 commits/s)
*per log*, multiplied by group-commit batching. This is a deliberate
trade-off; named logs are the sharding mechanism when a single log's
ceiling is exceeded.

---

## Non-Goals

CairnDB explicitly does **not** aim to be:
- a high-throughput database
- a low-latency replication engine
- a general-purpose event bus
- a multi-region active-active system

---

## Mental Model Summary

- **Blob storage is the ledger — and the sequencer, and the lock manager**
- **Commits are truth; the ack follows the PUT**
- **Two write primitives: put-if-absent (logs, claims) and CAS (documents)**
- **Snapshots are checkpoints**
- **SQLite is a cache**
- **SQLAlchemy is a reader**
- **Replay fixes everything**

Any implementation must preserve these invariants.
