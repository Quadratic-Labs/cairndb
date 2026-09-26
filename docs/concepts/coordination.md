# Layer 1 — Coordination

Three primitives cover most of what serverless workers need to cooperate:
a unique constraint, fenced ownership, and a safe read-modify-write.
Each is a thin protocol over layer-0 conditional writes. Documents are stored as canonical JSON, so values must be
JSON-serializable.

## claim: unique constraint

```python
result = await db.claim("dispatch/daily-etl:2026-08-09", {"run_id": rid})
result.won      # True iff this call created the object
result.value    # yours if you won, the winner's if you lost
result.etag
```

A claim is a put-if-absent write followed by a read-back of the winner.
Exactly one caller wins. Every loser receives the winner's value. So
duplicate callers **converge**: a retried or recovered caller reads back
the original result and continues on the same path, instead of failing.

Claims are immutable once won. There is no "unclaim". Typical uses:

- **Idempotent dispatch**: key the claim on the submission identity.
- **Cron-tick dedup**: key it on `(schedule, fire_time)`, and
  exactly-once-per-tick becomes a storage guarantee.
- **Step checkpoints**: key it on `(workflow, step)` and store the step's
  output. A re-executed step converges on the first result.

## lease: expiring ownership with fencing

```python
lease = await db.lease("state/etl/run-123", ttl=120, holder=worker_id)
# None => actively held by someone else (and not expired)

lease.epoch                                       # fence token, bumped on every acquisition
lease.state                                       # the payload the lease protects
await lease.renew()                               # heartbeat: extend deadline by ttl
await lease.update_state(lambda s: {**s, "progress": 0.5})   # guarded read-modify-write
await lease.write({"progress": 0.5})              # guarded replace
await lease.release(state={"status": "completed"})
```

### The lease document

A lease is a JSON document `{epoch, holder, deadline_at, state}` that is
only ever replaced by compare-and-swap. Acquiring it bumps `epoch`,
whether the lease is fresh, was released, or is being stolen after
expiry. `epoch` is therefore a monotonic **fence token**, and the triple
`(key, epoch, holder)` identifies one ownership period.

### Fencing

Every write through a lease handle is guarded by the etag the handle last
observed. When the guard fails, the handle re-reads the document and
classifies the failure:

- **The epoch advanced, another holder is recorded, or the document was
  deleted**: the lease was *lost*. The call raises {class}`~cairndb.LeaseLost`, and the holder must
  discard its outcome.
- **Same epoch, same holder**: someone made a *cooperative write* (see
  below). The handle absorbs the fresh state and retries its write, so
  the cooperative write is observed rather than clobbered.

Because the storage itself rejects stale writes, even a holder that was
paused for minutes, by GC, a suspended container, or a network partition,
cannot corrupt state after someone else takes over. Its next write fails.

### Expiry and clocks

Expiry is judged against the holder-written `deadline_at`. Another worker
may steal the lease once the deadline has passed (`steal_if_expired=True`,
the default). Clocks only need to agree to within the ttl, so choose
generous ttls: tens of seconds to minutes, not milliseconds.

Nothing *enforces* expiry against the holder. A holder that overruns its
deadline can optimistically carry on, and if nobody stole the lease
meanwhile, its next write succeeds and it still owns the lease.

### Acquiring with a state transition

`state_fn` makes the acquisition itself a transition. It receives the
current state (`None` on fresh creation), and its result is written
atomically with the acquisition:

```python
def claim_if_pending(state):
    if state and state.get("status") == "completed":
        raise AlreadyDone()          # aborts the acquisition; nothing is written
    return {**(state or {}), "status": "running"}

lease = await db.lease("state/etl/run-123", ttl=120, holder=me, state_fn=claim_if_pending)
```

`state_fn` must be pure, because a lost race calls it again.

### Cooperative writes

`db.cooperative_write` changes a lease's `state` from **outside** the
lease, without fencing the holder. It preserves epoch, holder, and
deadline:

```python
await db.cooperative_write("state/etl/run-123",
                           lambda s: {**(s or {}), "cancel_requested": True})
```

The holder sees the new state on its next `renew` or `update_state`. A
heartbeat loop of `renew()` plus a check of `lease.state` is the holder
side of a cooperative-cancel protocol. Note that `lease.write(...)`
replaces state unconditionally, so it discards cooperative writes it has
not read yet. Holders taking part in such a protocol should use
`update_state`.

### Re-attaching to your own lease

`db.attach_lease` rebuilds a handle onto an ownership period that is
**already yours**. It performs one read, no write, no epoch bump, and no
deadline change:

```python
same = await db.attach_lease("state/etl/run-123", holder=worker_id, ttl=120)
# None => absent, released, held by someone else, or expired
```

`lease` *begins* an ownership period and must fence the previous one.
`attach_lease` *re-enters* the current period, so it must not. Two
handles on one period converge on each other's writes instead of fencing
each other. That lets a lease outlive the process that took it: an HTTP
relay renewing for a remote client, a supervisor acting for a worker it
spawned, or a restarted process that remembered its holder id.

An expired lease is never re-attached. Resume it with `lease`, which
takes a fresh epoch, because a sweeper may already have declared the
holder dead.

:::{admonition} The holder string is a credential
:class: warning
Whoever knows a holder string can attach, and can therefore renew, write,
and release. Inside one deployment this changes nothing, since access to
the bucket is already authority. Where the string crosses a trust
boundary, mint it with `secrets.token_urlsafe()` rather than anything
guessable.
:::

### Release

`release(state=...)` records an optional final state and sets the holder
to `None`. The document stays in place, so the epoch keeps increasing
across re-acquisitions. Any further write through a released handle
raises `LeaseLost`.

## doc: typed document with a retry loop

`db.doc` wraps one object as a document and supplies the
compare-and-swap loop that everyone otherwise writes by hand:

```python
counters = db.doc("metrics/daily")
await counters.update(lambda c: {**c, "runs": c["runs"] + 1}, create={"runs": 0})
value, etag = await counters.get()       # or None if absent
await counters.delete()
```

`update(fn, create=...)` works like this:

1. It reads the document and its etag.
2. It applies `fn` to the value, or to `create` if the document is
   absent.
3. It writes the result with compare-and-swap (or put-if-absent),
   guarded by the etag from step 1.
4. On conflict, it goes back to step 1, up to `max_attempts` times
   (default 10).

`fn` must be pure, because it can run several times. With no `create`, an
absent document is an error.

Pass `model=` to get typed values: any class exposing
`model_validate_json()` and `model_dump_json()`, such as a pydantic
`BaseModel`, works:

```python
queue = db.doc("queues/default", model=QueueState)
await queue.update(lambda q: q.bump())
```

## Guarantees at a glance

| Primitive | Guarantee |
|---|---|
| `claim` | exactly one winner; losers converge on the winner's value |
| `lease` | mutual exclusion while clock skew ≪ ttl; epoch + CAS fencing makes a paused holder's late writes fail |
| `doc` | each `update` is an atomic read-modify-write on one key |

For end-to-end recipes (dispatch dedup, worker heartbeats, cooperative
cancel, sweepers), see [Coordination patterns](../guides/coordination-patterns.md).
