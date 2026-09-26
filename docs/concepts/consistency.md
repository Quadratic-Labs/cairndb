# Guarantees and limits

## Consistency model

| Layer | Guarantee |
|---|---|
| **Objects** | Linearizable per key. The backend's conditional writes enforce this. |
| **Claims** | Exactly one winner. Losers converge on the winner's value. |
| **Leases** | Mutual exclusion, while clock skew is much smaller than the ttl. Epoch fencing plus CAS makes even a paused holder safe: its late writes fail. |
| **Logs** | Linearizable appends and a total order per log. An acknowledged append is durable and can never be lost. |
| **Transactions** | Optimistic serializability between transactions of the same bucket. The commit point is a durable record in `logs/_tx/`. Conflicting transactions abort with `TransactionConflict`. |
| **Projections** | Eventually consistent, with lag up to the poll interval. Monotonic per client. Read-your-writes is opt-in via `wait_for`. |
| **Recovery** | Replay from a snapshot plus the log. Nothing outside the bucket needs to survive a crash. |

Concurrent writers are safe by construction. The storage itself rejects
a stale writer's PUT: a log append targets a number that already exists,
and a lease write carries an etag that no longer matches. Invariants that
span writers, such as uniqueness of a value inside events, are the
application's to enforce: use a [claim](coordination.md#claim-unique-constraint),
a [transaction](transactions.md), or a
[revalidate hook](logs.md#revalidation-invariants-across-writers).

## Durability

An acknowledged write is in the bucket, and there is no
ack-before-durable window. Durability is exactly your storage backend's
durability. On the filesystem backend, that is your disk. On cloud object
stores, it is the provider's replication (for example, S3's eleven
nines). Replicate or back up the bucket with the provider's tools
(versioning, cross-region replication) if you need more.

## Cost model

- **Write**: one conditional PUT per commit. Group commit batches events.
- **Read poll**: one GET per interval per client (a 404 when idle). That
  is a few cents a month.
- **Coordination**: a few GETs and PUTs per claim, lease operation, or
  document update.
- **Snapshot job**: a scheduled serverless container, running for minutes
  a day.
- **No idle compute anywhere.** Storage is the only always-on cost.

## Performance envelope

| Dimension | Typical figure | Scales with |
|---|---|---|
| Commit latency | one storage round-trip: ~20–80 ms on standard S3/GCS/Azure | faster storage tiers (e.g. S3 Express One Zone) |
| Commits per second, per log | ~10–30 | more named logs (sharding) |
| Events per second, per log | commits/s × events per commit | group-commit batching |
| Transactions per second | ~10–30 per bucket | — (single `_tx` log) |
| Read freshness | the poll interval (default 5 s) | shorter intervals; `wait_for` |
| Projection size | up to ~10 GB comfortable | copy-on-write filesystems |

These figures describe the design envelope, not benchmarks of your
deployment. Measure your own workload before you rely on them.

## Non-goals

CairnDB explicitly does **not** aim to be:

- a high-throughput OLTP database;
- a low-latency replication engine;
- a general-purpose event bus or message queue;
- a multi-region active-active system.

Hot, contended counters and queues are the first things to outgrow blob
storage. Nothing here matches PostgreSQL's `SELECT … FOR UPDATE SKIP
LOCKED`. If your workload looks like that, CairnDB is the wrong tool.

## Honest limits

- Transaction isolation covers transactions only. Direct `objects.put`
  writes to transactional keys can be overwritten by later applies.
- Transactions cannot atomically append to other logs.
- The CLI jobs operate on one log (and one schema version) per
  invocation. Schedule one job per log (`--log`). See
  [Operations](../guides/operations.md#named-logs).
- Retention policies as declarative per-prefix rules are future work.
  Today, retention is the `gc` job plus your bucket's lifecycle rules.
