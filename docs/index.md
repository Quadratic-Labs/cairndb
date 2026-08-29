# CairnDB

CairnDB is a serverless database engine on blob storage.
There are no long-running servers: the storage bucket is the arbiter.
CairnDB is for low and medium workloads that accept higher latency.
In return, CairnDB costs very little to operate.

<!-- Before TOC, provide quick links to strategic parts: usecases, quickstart, ... -->

---

<!-- TODO : TOC -->

---

<!-- This should really be the end of the document -->

This document is the overview of CairnDB. It describes the storage
substrate and its invariants: the commit log, snapshots, projections, and
the conditional object store. It also summarizes the engine layers built on
that substrate: coordination, named logs, transactions, and declarative
projections. The `CairnDB` facade exposes those layers, and
[ENGINE_API.md](ENGINE_API.md) specifies them in full. Every engine
operation reduces to the two write primitives defined here: put-if-absent
and compare-and-swap (CAS).

---

## Non-Goals

CairnDB explicitly does **not** aim to be:

- a high-throughput database
- a low-latency replication engine
- a general-purpose event bus
- a multi-region active-active system
---

A database engine decomposes into a WAL, a concurrency-control layer,
derived state, and vacuum. CairnDB exposes each as a client-library
primitive:

| Layer | Primitive | Engine analogy |
|---|---|---|
| 0 | Conditional objects (`objects`) | atomic page writes |
| 1 | Coordination (`claim`, `lease`, `doc`) | unique constraints, row locks, RMW |
| 2 | Logs (`log`, `transact`) | WAL, transaction coordinator |
| 3 | Projections (`projection`) | indexes / materialized views |
| 4 | Lifecycle & watch (`wait_for`, `tail`, jobs) | vacuum, change feeds |

---

## Core Principles

These principles are invariants. Implementations must not violate them.

1. **Append-only source of truth**
   - All writes are immutable commit objects in the log.
   - In-place mutation of data never occurs.

2. **Dense sequence per log, enforced by the bucket**
   - Within a log, commits are numbered densely (1, 2, 3, …), with no gaps.
   - Ordering is total and authoritative. Conditional (put-if-absent)
     writes arbitrate it — never a process, a lease, or a clock.
   - Many named logs can exist, each with its own sequence. Ordering is
     defined only within a log. Sharding across logs is the scaling
     mechanism.

3. **Blob storage is the ledger**
   - Commit objects and snapshots are stored in blob/object storage.
   - Blob storage is the ultimate source of truth.

4. **Durable ack**
   - A write is acknowledged only after its commit object is durably
     stored.
   - There is no window in which an acknowledged write can be lost.

5. **SQLite is a projection, not the truth**
   - SQLite databases are *derived*, read-only views.
   - They can always be rebuilt from snapshots plus the commit log.

6. **Deterministic replay**
   - Replay of the same commits always produces the same SQLite state.
   - Projection logic must be idempotent and versioned.

7. **Eventual consistency**
   - Clients are eventually consistent with the ledger (polling).
   - Read-your-writes is available per session, via `wait_for_sequence`.

---

## Schema & Versioning

Two layers are versioned independently:

1. **Event schema version** — the structure of events. It is recorded per
   commit. Handlers use it to interpret payloads.
2. **Projection schema version** — the SQLite tables and indices. It is
   encoded in the snapshot prefix (`snapshots/v2/…`). After a version bump,
   new snapshots are built under the new prefix, by replay of the same log.
   Old and new versions can coexist during migration.

