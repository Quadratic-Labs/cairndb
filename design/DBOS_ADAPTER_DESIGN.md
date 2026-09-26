# DBOS on CairnDB — design discussion record

*Recorded 2026-08-06 from a design conversation. Status: design/assessment only —
no code written yet. This document is the resume point.*

## Goal

Durable, auditable, **serverless** workflow state for [DBOS](https://github.com/dbos-inc/dbos-transact-py)
(Python durable-execution framework), replacing its local SQLite system database
with blob storage via CairnDB — no database server anywhere.

Deployment model: multiple **ephemeral serverless container workers** (triggered
by user interface or crontabs), potentially many concurrent, realistically low
load.

## Findings about DBOS internals (inspected `dbos` 2.29.0 wheel)

- The **system database** (workflow status, step checkpoints, results — the
  durable-execution state) is `SQLiteSystemDatabase` in `dbos/_sys_db_sqlite.py`,
  using a single **sync** SQLAlchemy engine. It forces `isolation_level =
  "IMMEDIATE"` on connect to serialize local writers.
- Factory choke point: `SystemDatabase.create()` in `dbos/_sys_db.py:539` picks
  the SQLite or Postgres subclass from the database URL. **Monkeypatch here** —
  no fork needed.
- The **async engine** is the app-side datasource (`dbos/_datasource_sqlite.py:61`,
  aiosqlite) — application data from transaction steps, separate from the sys DB.
- Write methods on base `SystemDatabase` (`_sys_db.py`) are already domain
  events: `_insert_workflow_status`, `update_workflow_outcome`,
  `record_operation_result` (~line 2521), `record_get_result`,
  `record_child_workflow`, `record_sleep`, queue updates.
- Exactly-once step recording relies on a unique constraint on
  `(workflow_id, function_id)`.
- Schema: `dbos/_schemas/system_database.py`; DDL in `dbos/_migration.py`.
- Caveat: `_sys_db` is a **private** module and DBOS releases fast — pin the
  DBOS version and keep a small test suite over the wrapped methods.

## Interception decision

Rejected: network proxy (SQLite has no wire protocol), LD_PRELOAD/VFS/WAL-tailing
(unnecessary — DBOS is pure Python), raw row-level CDC (loses intent).

**Chosen: wrap DBOS's own `SystemDatabase` write methods** (via a monkeypatched
`SystemDatabase.create` returning a wrapped/subclassed instance). This yields
semantic events (`workflow.started`, `step.completed {workflow_id, step_id,
output}`) — genuine event sourcing, not CDC. Fallback (more version-stable,
less semantic): SQLAlchemy `before_cursor_execute` events capturing
(statement, params); for the async datasource attach via
`async_engine.sync_engine`.

**Async append is acceptable for DBOS** — key insight: DBOS's recovery model is
at-least-once from the last checkpoint, so losing an unshipped tail of events on
crash just rewinds a few checkpoints and re-runs those steps — exactly the
failure mode DBOS already tolerates. No need to put a blob PUT on every step's
critical path. Optionally await the append synchronously for chosen transitions
(e.g. workflow completion).

## Architecture: hybrid of CairnDB's two layers

DBOS write traffic has three classes with different concurrency profiles, and a
single global totally-ordered log serializes traffic that is ~90% partitionable
per workflow ("one log = one lock"). Instead, use both layers CairnDB already
exposes:

| Traffic class | Volume | Primitive |
|---|---|---|
| Step checkpoints (`record_operation_result` etc.) | high, workflow-local | **put-if-absent** at `workflows/{id}/steps/{function_id}` |
| Workflow lifecycle (insert status, outcome) | ~2–3 events/workflow | **CairnDB commit log** (total order, SQLite projection, audit spine) |
| Queues / notifications | low, contended | log events, or CAS'd queue doc (`put_object` with `if_match`) holding in-flight counters |

Why put-if-absent for steps is the elegant core: the storage primitive *is*
DBOS's checkpoint contract. A duplicate/recovered executor re-runs a step, loses
the PUT to the existing object, reads back the winner's result, and continues on
the identical deterministic path — **duplicate execution converges**. Zero
cross-workflow contention; throughput scales with workflow count instead of
being capped at the log's tens of commits/sec. Standard DBOS caveat unchanged:
external side effects of a step can fire twice, steps should be idempotent.

Global audit/full projection of steps becomes an **async fold**: the snapshot
job lists `workflows/*/steps/*` for active workflows and merges them into the
SQLite projection. Trade-off accepted: bit-exact global time-travel only holds
for lifecycle/queue state; step history is ordered per-workflow.

Keeping steps out of the log also keeps **cold starts bounded**: a booting
container fetches the latest snapshot and tails only lifecycle/queue events.

## Multi-worker additions (the genuinely new design work)

Serverless containers break DBOS's assumption that an executor restarts with a
stable ID and reclaims its own PENDING workflows. Required: an
**ownership/lease/fencing layer** on blob-storage CAS:

- Workflow status doc carries `{state, executor_id, epoch}`; claiming (at start
  or recovery) is an `if_match` CAS that bumps the **epoch**.
- Workers heartbeat `workers/{executor_id}` (CAS timestamp ~every 15s);
  generous lease windows (~60s) to absorb clock skew.
- **Reaper as a scheduled job** (same operational shape as the CairnDB snapshot
  job): scan PENDING workflows in the projection, check owner heartbeat,
  CAS-reclaim expired ones. Recovery-as-cron — nothing needs to be resident.
- Stale worker overlap: its step PUTs are harmless (convergence), and it detects
  the stale epoch on its next status-doc CAS and abandons the workflow.
- **Cron trigger dedup for free**: put-if-absent keyed by
  `(schedule_name, fire_time)` makes exactly-once-per-tick a storage guarantee.

## Known limits (sized, accepted)

- **Queues**: CAS'd queue doc serializes dequeue at a few claims/sec per queue —
  fine at low load; hot queues are the first thing to outgrow blob storage
  (nothing here matches Postgres `SKIP LOCKED`).
- **Per-step latency**: each checkpoint is a PUT (~20–80ms standard S3/GCS);
  chatty 50-step workflows pay seconds. Mitigation if needed: S3 Express One
  Zone (single-digit ms, supports conditional writes) — same design.
- If durability were the *only* goal, litestream-style WAL shipping would be far
  less code; CairnDB earns its place via the auditable event stream and
  rebuild-anywhere recovery.

## Agreed plan

1. **Phase 1**: `cairndb-dbos` shim package — patch `SystemDatabase.create`,
   wrap write methods, map to typed CairnDB events, ship via background
   committer with per-commit batching. Everything through the log,
   single-worker assumption. Include the `epoch` field in the status doc from
   day one (costs nothing).
2. **Phase 2**: move step checkpoints to keyed put-if-absent objects; keep
   lifecycle + queues on the log; async fold of steps into the projection.
   Replay handlers rebuild a SQLite file matching DBOS's system schema; recovery
   CLI: rebuild projection → launch DBOS against it.
3. **Phase 3**: ownership layer — leases, epochs, heartbeats, reaper cron job.

App-side datasource (aiosqlite engine) is a later phase via SQLAlchemy events.

## Resume pointers

- DBOS source inspected at: `pip download dbos --no-deps` → wheel 2.29.0
  (was unpacked in a session scratchpad; re-download to re-inspect).
- CairnDB primitives used: `Committer.append` (log) and the generic conditional
  object store `get_object`/`put_object` with `if_match`/`if_absent`
  (see the [storage model](../docs/concepts/storage-model.md)).
