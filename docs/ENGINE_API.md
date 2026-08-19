# CairnDB Engine API

*Design recorded 2026-08-09 from the DBOS/Flowlet convergence discussion
(see [DBOS_ADAPTER_DESIGN.md](DBOS_ADAPTER_DESIGN.md)). Status: v1 implemented
in `cairndb.engine`.*

CairnDB started as "a commit log with SQLite projections". This document
widens its identity: a **serverless database-engine kernel over blob
storage / distributed filesystems**. The bucket is the arbiter; there is no
resident process. A database engine decomposes into a WAL, a
concurrency-control layer, derived state, and vacuum — CairnDB exposes each
as a client-library primitive:

| Layer | Primitive | Engine analogy | Provenance |
|---|---|---|---|
| 0 | Conditional objects (`objects`) | atomic page writes | existed (`BlobStorage` generic API) |
| 1 | Coordination (`claim`, `lease`, `doc`) | unique constraints, row locks, RMW | extracted from [Flowlet](https://github.com/Quadratic-Labs/flowlet)'s control plane |
| 2 | Logs (`log`, `transact`) | WAL, transaction coordinator | log existed; named logs + transactions new |
| 3 | Projections (`projection`) | indexes / materialized views | existed (replay/updater), made declarative |
| 4 | Lifecycle & watch (`wait_for`, `tail`, jobs) | vacuum, change feeds | jobs existed; watch helpers new |

All invariants of [ARCHITECTURE.md](ARCHITECTURE.md) hold unchanged. The
engine is a facade over existing machinery, not a rewrite.

---

## Entry point

```python
from cairndb import CairnDB

db = CairnDB.configure({
    "storage": {"type": "azure", "container": "myapp", ...},   # StorageConfig fields
})
# or, with an existing backend instance:
db = CairnDB(storage)

...
await db.close()          # or: async with CairnDB.configure({...}) as db:
```

One engine = one bucket (or one prefix of one bucket). Everything hangs off
`db`. Every layer-0/1 method has a `*_sync` twin (the primitive form, as in
`BlobStorage`); layers 2–3 are async-only in v1.

---

## Layer 0 — Objects

The raw conditional KV, namespaced as `db.objects`. Exactly the semantics of
the generic conditional object store in ARCHITECTURE.md:

```python
obj  = await db.objects.get("config/app.json")            # StoredObject(data, etag) | None
etag = await db.objects.put("config/app.json", data,
                            if_match=obj.etag)             # CAS; None = precondition failed
etag = await db.objects.put("init/marker", b"",
                            if_absent=True)                # put-if-absent; None = lost
await db.objects.delete(key)
keys = await db.objects.list("state/")

# Watch: poll until a key exists / changes. Cheap: one GET per interval.
obj = await db.objects.wait_for("signals/run-1/approval",
                                timeout=300, poll_interval=2.0,
                                changed_from=old_etag)      # None = wait for existence
```

Reserved prefixes a client must not write through `objects`:
`log/`, `snapshots/`, `logs/`, `txapplied/`.

## Layer 1 — Coordination

Three primitives, extracted from the patterns Flowlet proved on this
substrate (`repository/state.py`, `lease.py`, `repository/dispatch.py`).

### claim — unique constraint

Put-if-absent with read-back of the winner. Duplicate callers **converge**:
the loser gets the winner's value. This one call is Flowlet's dispatch keys,
the DBOS design's step checkpoints, and cron-tick dedup.

```python
result = await db.claim("dispatch/daily-etl:2026-08-09", {"run_id": rid})
result.won      # True iff this call created the object
result.value    # yours if won, the winner's if not
result.etag
```

Claims are immutable once won; there is no "unclaim".

### lease — expiring ownership with fencing

```python
lease = await db.lease("state/etl/run-123", ttl=120,
                       holder=worker_id, steal_if_expired=True)
# None => actively held by someone else (not expired)

lease.epoch                                # monotonic fence token, bumped per acquisition
await lease.renew()                        # CAS; raises LeaseLost if fenced
await lease.write({"progress": 0.5})       # guarded payload replace (discards signals)
await lease.update_state(lambda s: {**s, "progress": 0.5})  # guarded RMW (preserves signals)
await lease.release(state={"status": "completed"})

# from outside the lease — cooperative signal to the holder, no fencing:
await db.signal("state/etl/run-123", lambda s: {**(s or {}), "cancel_requested": True})
```

Semantics:

- The lease document carries `{epoch, holder, deadline_at, state}` and is
  only ever replaced by CAS. Acquiring — fresh, after release, or by stealing
  an expired lease — **bumps `epoch`**.
- Every write through the lease is etag-guarded. A holder that was fenced
  (its lease expired and was stolen) gets `LeaseLost` on its next write and
  must discard its outcome — Flowlet's worker contract, verbatim.
- `db.signal(key, fn)` CAS-updates only the `state` field, preserving epoch,
  holder and deadline — the holder is *signaled*, not fenced. Its next
  `renew`/`update_state` absorbs the fresh state before writing, so the
  signal shows up on `lease.state`. A heartbeat of `renew` + `lease.state`
  checks is the holder side of a cooperative cancel protocol; plain
  `write` replaces state unconditionally and discards unread signals.
- Expiry is judged against the holder-written `deadline_at`; clocks only
  need to agree to within the ttl (choose generous ttls).

### doc — typed document with RMW retry

The CAS read-modify-write loop everyone hand-rolls:

```python
counters = db.doc("queues/default", model=QueueState)   # model class (pydantic-compatible), or raw dict
state = await counters.update(lambda q: q.bump())        # retries on conflict
value_etag = await counters.get()                        # (value, etag) | None
await counters.delete()
```

`update(fn, create=initial)` reads, applies `fn`, CAS-writes; on conflict it
re-reads and retries (bounded attempts). `fn` must be pure — it may run
several times.

## Layer 2 — Logs and transactions

### Named logs

```python
orders = db.log("orders")          # named log
root   = db.log()                  # the original root log (back-compat: log/)
seq  = await orders.append(Event("order.placed", ...))     # durable when returned
seqs = await orders.append_many([...])                     # group commit, as today

async for entry in orders.read(after=0):                   # replay: SequencedEvents until the tail
    ...
async for entry in orders.tail(poll_interval=1.0):         # follow forever (GET next; 404 = idle)
    ...
```

A named log lives at `logs/{name}/log/{N:012d}.msgpack` with snapshots under
`logs/{name}/snapshots/v{schema}/`, and is implemented by routing the
existing commit protocol through the generic conditional-object API
(`NamespacedStorage`). Same dense numbering, same put-if-absent arbitration,
same Committer — one sequencer *per log*. Sharding across logs is the
scaling story; ordering is only total *within* a log.

The root log keeps its original `log/` + `snapshots/` layout, so existing
deployments are untouched.

### Transactions — multi-key atomicity via the log

The one thing raw blob CAS cannot do: change several keyed objects
atomically. The engine uses **a dedicated system log (`logs/_tx/`) as the
transaction coordinator**: the appended transaction record is the commit
point; applying its mutations to the object store is an idempotent fold.

```python
async with db.transact() as tx:
    bal = await tx.get("accounts/alice")        # reads record etags (the read set)
    tx.put("accounts/alice", new_alice)
    tx.put("accounts/bob", new_bob)
    tx.note("transfer.completed", {"amount": 10})   # audit payload inside the tx record
# exiting commits; raises TransactionConflict if the read set went stale
```

Protocol:

1. **Validate** — re-read the read set; any etag change → `TransactionConflict`.
2. **Commit point** — append one `cairndb.tx` event (mutations + read/write
   sets) to the `_tx` log. Conflict detection is completed by the committer's
   revalidate hook: if the append loses a race, interleaved tx records are
   inspected and the transaction is rejected when any of its *read* keys is
   in an interleaved transaction's *write* set. Because the log is dense, any
   transaction committing between our validation and our PUT forces exactly
   such a lost race — so transactions in the same engine are serialized by
   the log, with optimistic-concurrency aborts.
3. **Apply** — perform the puts/deletes, then mark
   `txapplied/{commit:012d}` (put-if-absent).
4. **Recovery** — `await db.recover_transactions()` re-applies any committed
   tx records lacking an applied marker, in log order. Apply is
   deterministic and last-writer-wins by log order, so duplicate application
   is harmless. Run it at engine start or from a cron, like the snapshot job.

Honest limits (v1): isolation holds between *transactions*; direct
`db.objects.put` writers bypass conflict detection (they can still never be
lost — applies are plain object writes — but they can be overwritten).
Transactions serialize on the `_tx` log (~10–30 tx/s ceiling). Cross-log
atomic appends are not supported; `tx.note` embeds audit events in the tx
record itself, which projections of the `_tx` log can fold.

## Layer 3 — Projections

The existing replay/updater machinery, made declarative and per-log:

```python
proj = db.projection("orders_view", version="2",
                     db_path="./orders_view.sqlite",       # default: ./{name}.v{version}.sqlite
                     log="orders",                          # default: the root log
                     init_schema=create_tables)             # optional DDL callback

@proj.on("order.placed")
async def apply_placed(conn, entry):                        # aiosqlite conn, SequencedEvent
    await conn.execute("INSERT ...", (entry.payload["id"],))

await proj.refresh()                       # catch up now (one GET when idle)
await proj.start(); await proj.stop()      # background polling updater
await proj.wait_for(seq)                   # read-your-writes
with proj.connect() as conn:               # sqlite3, mode=ro — readers never block replay
    conn.execute("SELECT ...")
path = await proj.as_of(commit=1500)       # time travel: build a throwaway db through commit 1500
```

Snapshots for a projection of a named log live under that log's namespace
and are produced by the same scheduled job machinery. `as_of` bootstraps
from the latest snapshot at or before the target commit (subject to GC
retention) and replays up to it.

## Layer 4 — Lifecycle

Unchanged, cron-shaped: the snapshot job and GC (`cairndb` CLI) operate per
log. Watch primitives are `objects.wait_for` and `log.tail()` (both polling;
etag-conditional GETs make idle polls nearly free). Retention policies as
declarative per-prefix rules are future work.

---

## Consistency summary

- **Objects**: linearizable per key (backend conditional writes).
- **Claims**: exactly-one winner, losers converge on the winner's value.
- **Leases**: mutual exclusion within clock skew ≪ ttl; fencing by epoch +
  CAS makes even a paused holder safe (its writes fail).
- **Logs**: linearizable appends, total order per log, durable ack.
- **Transactions**: optimistic serializable w.r.t. other transactions in the
  same engine; commit point = tx record durable in `logs/_tx/`.
- **Projections**: eventually consistent, monotonic per client, opt-in
  read-your-writes via `wait_for`.

## What Flowlet looks like on this API

```python
claim = await db.claim(f"dispatch/{key}", {"run_id": rid})          # dedup submission
lease = await db.lease(f"state/{flow}/{rid}", ttl=opts.timeout,
                       steal_if_expired=True)                       # claim or reclaim the run
try:
    result = flow_fn(**kwargs)                                      # heartbeats = lease.renew()
    await lease.release(state=completed(result))
except LeaseLost:
    return                                                          # fenced — discard outcome
```

The sweeper is `db.objects.list("state/")` + expired-lease steals; DBOS step
checkpoints are `db.claim(f"workflows/{id}/steps/{fid}", output)`.
