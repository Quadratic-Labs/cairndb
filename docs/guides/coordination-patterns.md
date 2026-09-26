# Coordination patterns

Recipes built from `claim`, `lease`, and `doc`. Each one survives crashes,
retries, and duplicate deliveries, because its correctness comes from the
bucket's conditional writes rather than from any process staying alive.

## Exactly-once dispatch

Many triggers are *at-least-once*: a UI double-click, a retried webhook,
two cron replicas. A claim turns "at least once" into "exactly one run":

```python
import uuid

async def submit(db, job: str, idempotency_key: str) -> str:
    result = await db.claim(f"dispatch/{job}:{idempotency_key}",
                            {"run_id": str(uuid.uuid4())})
    # Winner and losers all get the same run_id back.
    return result.value["run_id"]
```

For scheduled jobs, key the claim on the fire time. Every replica then
computes the same key, and exactly one replica wins each tick:

```python
tick = scheduled_time.strftime("%Y-%m-%dT%H:%M")
if (await db.claim(f"cron/nightly-report:{tick}", {"by": worker_id})).won:
    await run_report()
```

## A worker that owns a run

A lease gives one worker ownership of a unit of work. The ttl bounds how
long a crashed worker blocks others. Renewing between steps keeps
ownership, and `LeaseLost` tells a slow worker to stop:

```python
import secrets
from cairndb import LeaseLost

async def work(db, run_id: str, steps) -> None:
    holder = secrets.token_urlsafe(16)
    lease = await db.lease(f"state/{run_id}", ttl=120, holder=holder)
    if lease is None:
        return                                   # someone else is on it

    try:
        for step in steps:
            await step()
            await lease.renew()                  # heartbeat; raises LeaseLost if fenced
            await lease.update_state(lambda s: {**(s or {}), "done": step.__name__})
        await lease.release(state={"status": "completed"})
    except LeaseLost:
        return                                   # fenced: another worker took over; discard
```

Pick the ttl to be several times your longest step plus the clock skew
you expect. A ttl that is too short causes needless steals. A ttl that is
too long delays recovery after a crash.

:::{tip}
Side effects outside the bucket, such as emails, payments, or API calls,
can still happen twice if a worker is fenced right after performing one.
Make steps idempotent, or guard each external effect with its own claim.
:::

## Refusing to acquire finished work

Use `state_fn` to make acquisition conditional on the lease's state. The
check and the acquisition happen in one atomic write:

```python
class AlreadyDone(Exception):
    pass

def start_unless_done(state):
    if state and state.get("status") == "completed":
        raise AlreadyDone
    return {**(state or {}), "status": "running", "attempt": (state or {}).get("attempt", 0) + 1}

try:
    lease = await db.lease(f"state/{run_id}", ttl=120, holder=me, state_fn=start_unless_done)
except AlreadyDone:
    lease = None
```

## Cooperative cancellation

An operator, or another service, requests cancellation **without**
fencing the worker, so the worker can shut down cleanly:

```python
# Anywhere: request cancellation.
await db.cooperative_write(f"state/{run_id}",
                           lambda s: {**(s or {}), "cancel_requested": True})

# In the worker's heartbeat loop:
await lease.renew()                              # absorbs the cooperative write
if (lease.state or {}).get("cancel_requested"):
    await lease.release(state={**lease.state, "status": "cancelled"})
    return
```

Use `lease.update_state(fn)` rather than `lease.write(value)` in workers
that take part in this protocol. `write` replaces the state and would
discard a cancel request it has not seen yet.

## Sweeping expired work

A sweeper finds runs whose worker died and restarts them. It is just
another cron job:

```python
import json

async def sweep(db) -> None:
    for key in await db.objects.list("state/"):
        obj = await db.objects.get(key)
        if obj is None:
            continue
        doc = json.loads(obj.data)                  # {epoch, holder, deadline_at, state}
        if doc["holder"] is None or (doc["state"] or {}).get("status") == "completed":
            continue
        lease = await db.lease(key, ttl=120, holder=my_holder)   # steals only if expired
        if lease is not None:
            await resume(lease)
```

`db.lease` returns `None` for leases that are still alive. So a sweeper
racing a healthy worker, or another sweeper, is harmless: the storage
decides.

## Handing a lease to another process

A lease can outlive the process that acquired it. The acquirer persists
`(key, holder)`, and any process that knows both can re-attach, without
bumping the epoch:

```python
lease = await db.attach_lease(f"state/{run_id}", holder=holder, ttl=120)
if lease is None:
    ...   # expired, released, or taken over: re-acquire with db.lease() or give up
else:
    await lease.renew()
```

Typical cases are an HTTP relay that renews on behalf of a remote client,
a supervisor that acts for the worker it spawned, and a process that
restarts and resumes its own lease. Treat the holder string as a secret.

## Step checkpoints

For durable workflows, record each step's output with a claim keyed on
`(workflow, step)`. A re-executed workflow converges on the recorded
outputs instead of recomputing them:

```python
async def step(db, workflow_id: str, step_id: int, fn):
    key = f"workflows/{workflow_id}/steps/{step_id}"
    existing = await db.objects.get(key)
    if existing is not None:
        return json.loads(existing.data)
    result = await fn()
    return (await db.claim(key, result)).value      # first writer's result wins
```

Checkpoints of different workflows never contend with each other, so
this scales with the number of workflows, not with a log's commit rate.

## Counters and small shared state

`db.doc(...).update(fn)` retries on conflict, so it is the simplest safe
way to maintain small shared documents:

```python
await db.doc("quotas/tenant-7").update(
    lambda q: {**q, "used": q["used"] + 1} if q["used"] < q["limit"] else q,
    create={"used": 0, "limit": 1000},
)
```

Keep documents small and contention modest. Every conflicting writer
re-reads the whole document and retries, and `update` gives up after
`max_attempts` (10 by default).
