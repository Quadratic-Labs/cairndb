# Glossary

```{glossary}
Bucket
  The object-storage container (S3 bucket, GCS bucket, Azure container,
  or local directory) that holds all of an engine's state. One engine is
  one bucket, or one prefix of one.

Claim
  A put-if-absent write with read-back of the winner. It implements a
  unique constraint: exactly one caller wins, and losers converge on the
  winner's value.

Commit
  An immutable object in a log holding an ordered batch of events. It is
  numbered densely: commit N+1 exists only after commit N.

Compare-and-swap
CAS
  A conditional write that succeeds only if the object still carries the
  etag the writer last observed.

Cooperative write
  A write into a lease's `state` from outside the lease, which does not
  fence the holder. The holder observes it on its next renew or update.

Epoch
  A lease's monotonic fence token, incremented on every acquisition.
  Together with the key and the holder, it identifies one ownership period.

Etag
  An opaque, backend-native version identifier of an object that changes
  on every write: an S3 or Azure etag, a GCS generation, or a content MD5
  on the filesystem.

Fencing
  Rejecting writes from a holder whose ownership has ended. CairnDB fences
  through storage conditions (etag plus epoch), so even a paused process
  cannot write stale state.

Group commit
  Batching the events appended while a PUT is in flight into the next
  commit. It amortizes round-trips without weakening the durable ack.

Lease
  An expiring, fenced ownership document `{epoch, holder, deadline_at,
  state}`.

Log
  A named, append-only, densely numbered sequence of commits: CairnDB's
  write-ahead log. Order is total within a log.

Projection
  A local, read-only SQLite database derived from a log by deterministic
  replay through handlers.

Put-if-absent
  A conditional write that succeeds only if the key does not exist yet.

Revalidate hook
  A committer callback that re-checks pending events against the commits
  that won a race, and keeps, modifies, or rejects each event.

Schema version
  Either an event's payload version (`Event.schema_version`) or a
  projection's table layout version (`Projection` `version`, which
  selects `snapshots/v{version}/`).

Sequence number
  An event's position in its log: `(commit, index)`, rendered
  canonically as `000000000042:000003`.

Snapshot
  A fully built projection database through a given commit, stored
  immutably in the bucket to bound client startup and replay.
```
