# Data Model and Concurrency

CairnDB has no server that coordinates writers. Concurrent processes
cooperate through conditional writes to shared blob storage. The storage
backend accepts or rejects each conditional write atomically. This one
mechanism is sufficient to build the usual parts of a database engine: a
write-ahead log (WAL), atomic writes, transactions, uniqueness constraints,
locks, indexes, and materialized views. The layers below expose each part
as a client-library primitive.

## Layer 0 — Objects

Layer 0 is a conditional key-value store of mutable objects. Each stored
object has an opaque etag. The etag changes on every write. The store
implements three write semantics:

- *Put* — write unconditionally.
- *Put-If-Match* — CAS write only if the given etag matches the stored etag.
  The caller receives the new etag, or `None` if the write lost a race.
- *Put-If-Absent* — write only if the key does not exist. The caller
  receives the new etag, or `None` if the write lost a race.

Objects are linearizable per key: a client immediately reads its own
acknowledged writes. All higher layers build on objects:

- `doc` (layer 1) wraps one object as a typed, mutable document.
- Logs (layer 2) store immutable records in a global, dense order.

## Layer 1 — Coordination

### claim — unique constraint

A claim is a put-if-absent write, followed by a read-back of the winner's
value. Duplicate callers **converge**: each loser receives the winner's
value. Claims are immutable after they are won. There is no "unclaim".

### lease — expiring ownership with fencing

A lease is an ownership document with expiry and fencing. The document
contains `{epoch, holder, deadline_at, state}`. Each acquisition increases
`epoch`, so `epoch` is a monotonic fence token. The lease is evidence of
ownership while the epoch in the arbiter's lease object matches the
holder's epoch.

Every write through the lease is a CAS, guarded by the document's etag.
When a guarded write fails, the epoch classifies the failure:

- The etag changed, but the epoch and the holder still match: another
  process made a *cooperative write* to the lease. The holder absorbs the
  new state and continues.
- The epoch advanced: the lease was *lost*. The holder must discard its
  outcome.

Note: nothing enforces lease expiry. The holder can optimistically continue
after the deadline. If no other process acquired the lease, the holder in
effect keeps ownership.

### doc — typed document with RMW retry

A doc wraps one stored object as a typed document (a model class or a raw
dict). It supplies the read-modify-write (RMW) retry loop that callers
usually write by hand:

1. Read the document and its etag.
2. Apply the caller's update function to the value.
3. Write the result with Put-If-Match, guarded by the etag from step 1.
4. If the write fails, read the document again and retry, up to a bounded
   number of attempts.

The update function must be pure, because it can run more than one time.

## Layer 2 — Logs and transactions

### (Named) Logs

A log is a sequence of immutable commit objects with a dense, total order:
commit N+1 exists only after commit N, and gaps never occur. Any writer
appends to a log through the commit protocol (see the Committer below).
There is no sequencer process. The bucket itself sequences each log: an
append is a put-if-absent write of the exact next commit number, and the
storage accepts exactly one winner. Order is total only within one log.
Sharding across named logs is the scaling mechanism.

### Transactions — multi-key atomicity via the log

Raw blob CAS cannot change several keyed objects atomically. Transactions
add this ability. The engine uses a dedicated system log as the transaction
coordinator. The appended transaction record is the commit point.

<!-- TODO: protocol details go into a different section -->
The protocol has four steps:

1. *Validate* — read the read set again. If any etag changed, raise
   `TransactionConflict`.
2. *Commit* — append one transaction event to the transaction log. The event
   contains the mutations and the read and write sets. The committer's
   revalidate hook completes conflict detection: if the append loses a
   race, the hook examines the interleaved transaction records. The hook
   rejects the transaction when the write set of an interleaved transaction
   contains one of its read keys. The log is dense, so each transaction
   that commits between the validation and the append forces exactly such a
   lost race. The log therefore serializes all transactions in the same
   engine, with optimistic-concurrency aborts.
3. *Apply* — perform the puts and deletes. Then write the transaction applied
   marker with put-if-absent.
4. *Recover* — retry applies, in log order, each committed transaction record
   that has no applied marker. Apply is deterministic, and the last writer in
   log order wins, so duplicate application causes no harm. Run recovery at engine
   start, or from a cron job, like the snapshot job.

Known limits: isolation holds only between *transactions*. Writers
that call `db.objects.put` directly bypass conflict detection. Their writes
can never be lost, because applies are plain object writes, but their
writes can be overwritten. Transactions serialize on the `_tx` log, which
has a ceiling of approximately 10–30 transactions per second. Atomic
appends across logs are not supported. `tx.note` embeds audit events in the
transaction record itself, and projections of the `_tx` log can fold these
events.

## Layer 3 — Projections

A projection over a log is the equivalent of a materialized view.
Registered event handlers fold log commits into a local SQLite database.
Snapshots memoize projections: a snapshot is a fully built projection
through a given commit. The replay and updater machinery builds and
consumes snapshots. A point-in-time view bootstraps from a snapshot, then
replays the log up to the recovery point.

## Layer 4 — Lifecycle

Lifecycle operations are cron-shaped sweepers. The snapshot job and garbage
collection operate per named log. The watch primitives `objects.wait_for` and
`log.tail()`. Both poll, and etag-conditional GET requests make idle polls almost free.

<!-- TODO: this goes into a roadmap section -->
Retention policies as declarative per-prefix rules are future work.

---

## Consistency Model

- *Objects*: linearizable per key. The backend's conditional writes
  enforce this.
- *Claims*: exactly one winner. Losers converge on the winner's value.
- *Leases*: mutual exclusion, while clock skew is much smaller than the
  ttl. Fencing by epoch plus CAS makes even a paused holder safe: its late
  writes fail.
- *Logs*: linearizable appends and a total order per log. An acknowledged
  write is durable and can never be lost.
- *Transactions*: optimistic serializability between transactions in the
  same engine. The commit point is a durable transaction log.
  Conflicting transactions abort with `TransactionConflict`.
- *Reads*: eventually consistent, with seconds of lag by design (the
  poll interval). Reads are monotonic per client: the projection only moves
  forward.
- *Read-your-writes*: opt-in per flow. A write returns `(commit, index)`,
  and `wait_for_sequence` blocks until the projection has applied it.
- *Failure recovery*: replay from a snapshot plus the log. Re-reading the
  log repairs crashes, lost races, and restarts. There is no state to lose
  outside the bucket.

Concurrent writers are safe by construction. Every commit is a
put-if-absent write on the exact next number, so the storage itself rejects
the PUT of a stale or "zombie" writer (fencing). Cross-writer application
invariants, for example uniqueness, are the application's responsibility,
through the committer's revalidate hook.

---

## Mental Model Summary

- Blob storage is the ledger — and the sequencer, and the lock manager
- Commits are truth; the ack follows the PUT
- Two write primitives: put-if-absent (logs, claims) and CAS (documents)
- Snapshots are checkpoints
- SQLite is a cache
- SQLAlchemy is a reader
- Replay fixes everything

Any implementation must preserve these invariants.

---

## Non-Goals

CairnDB explicitly does **not** aim to be:

- a high-throughput database
- a low-latency replication engine
- a general-purpose event bus
- a multi-region active-active system

