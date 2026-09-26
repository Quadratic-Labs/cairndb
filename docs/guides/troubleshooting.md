# Troubleshooting

## Clients take long to start, even though snapshots exist

**Cause:** the snapshots are under a different schema version than the
one the clients read. The projection's `version` selects
`snapshots/v{version}/`, and `cairndb snapshot --schema-version` selects
where the job writes. Both default to `"1"`, so a mismatch comes from
setting only one of them. It can also come from a client built with
`ClientConfig(schema_version="1.0.0")`, the default in CairnDB 0.4.0 and
earlier.

**Fix:** pass `--schema-version` equal to the projection's `version`.
List the bucket's `snapshots/` (or `logs/{name}/snapshots/`) prefix to see
what exists. Clients log `no_snapshots_available schema=…` when they find
nothing.

## Snapshots for a named log never appear

**Cause:** the job ran against the root log. CLI jobs act on the root log
unless you name one.

**Fix:** pass `--log {name}` or set `CAIRNDB_LOG={name}`. See
[Operations → Named logs](operations.md#named-logs).

## `ValueError: key '…' is under the reserved prefix`

Application keys cannot start with `log/`, `snapshots/`, `logs/`, or
`txapplied/`. Choose another prefix, such as `app/logs/…`.

## `LeaseLost` on renew

Another worker acquired the lease after it expired, the lease was
released, or its document was deleted. Discard the current outcome: the
new holder owns the work. If this happens often, the ttl is too short for
your step durations, or the renew calls are too far apart.

## `db.lease()` always returns `None`

The lease is actively held and its deadline has not passed yet. Wait for
expiry, or ask the holder to release it through a cooperative write. If
the holder crashed, the lease becomes stealable once `deadline_at`
passes. With `steal_if_expired=False`, an expired but unreleased lease is
never taken.

## `TransactionConflict`

Something this transaction read changed before it committed: a direct
object write, or another transaction that wrote one of its read keys.
Retry the whole transaction, re-reading inside the new attempt. Frequent
conflicts mean hot keys: split the data, or rethink the keys.

## `CairnDBError: update of '…' kept conflicting`

A `doc.update()` lost the compare-and-swap race `max_attempts` times in a
row. The document is too hot for optimistic retries. Shard it, reduce the
number of writers, or raise `max_attempts`.

## `EventRejectedError` from `append()`

Your revalidate hook rejected the event after a lost race. That is by
design: surface it to the caller as a business-rule violation.

## `ReplayError: Handler failed for …`

A projection handler raised. The commit's SQLite transaction was rolled
back, and the projection stays at the previous commit. Fix the handler.
Because handlers are deterministic, the same commit fails for everyone,
including the snapshot job. Deploy a handler that copes with the event,
then refresh.

## Projection writes fail with `attempt to write a readonly database`

`proj.connect()` opens the projection read-only on purpose. Projections
are derived state, and only replay writes to them. Persist changes by
appending events instead.

## Duplicate or reordered commits on an S3-compatible store

The store does not enforce `If-None-Match` on PUT, so concurrent writers
overwrite each other. CairnDB cannot be correct on such a store. Upgrade
it (MinIO has supported conditional writes since late 2024) or use
another backend. See
[Choosing a storage backend](../deployment/index.md#choosing-a-storage-backend).

## Filesystem backend: errors on Windows or network shares

The filesystem backend relies on POSIX `flock` and atomic `os.link`. Use
it on a local POSIX filesystem, for development, tests, and
single-machine deployments. Do not use it on network filesystems whose
locking semantics you have not verified.
