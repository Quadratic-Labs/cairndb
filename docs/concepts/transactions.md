# Layer 2 — Transactions

Raw blob compare-and-swap changes **one** key atomically. Transactions
change several keys atomically, with optimistic concurrency control. The
engine uses a dedicated system log, `logs/_tx/`, as the transaction
coordinator: the appended transaction record *is* the commit point, and
applying its mutations to the object store is an idempotent fold.

```python
import json
from cairndb import TransactionConflict

try:
    async with db.transact() as tx:
        raw = await tx.get("accounts/alice")          # joins the read set
        alice = json.loads(raw)
        tx.put("accounts/alice", json.dumps({**alice, "balance": alice["balance"] - 10}).encode())
        tx.put("accounts/bob", bob_bytes)
        tx.delete("holds/transfer-17")
        tx.note("transfer.completed", {"amount": 10})  # audit payload in the record
    print(tx.sequence)                                 # the record's position in _tx
except TransactionConflict:
    ...  # the read set was written concurrently: retry or report
```

- `tx.get(key)` reads the current bytes (or `None`) and records the
  observed etag in the **read set**. After a staged `put` or `delete` of
  the same key, it returns the staged value (read-your-writes).
- `tx.put` and `tx.delete` stage mutations. Nothing touches storage until
  commit.
- `tx.note(event_type, payload)` embeds an audit event in the transaction
  record. Notes are not appended to any other log.
- Leaving the block normally commits. An exception discards everything
  staged. A transaction with nothing staged commits nothing, and
  `tx.sequence` stays `None`.

## Protocol

1. **Validate.** Re-read the read set. If any etag changed, raise
   `TransactionConflict`. Then scan transaction records committed since
   the transaction began. If any of them wrote a key this transaction
   read, raise `TransactionConflict`.
2. **Commit point.** Append one `cairndb.tx` event to the `_tx` log,
   holding the mutations plus the read and write sets. The committer's
   revalidate hook completes conflict detection. If the append loses a
   race, the hook inspects the interleaved transaction records and rejects
   this transaction when any of its *read* keys is in an interleaved
   *write* set. Because the log is dense, any transaction that commits
   between validation and the PUT forces exactly such a lost race.
   Transactions are therefore serialized by the log, with optimistic
   aborts.
3. **Apply.** Perform the puts and deletes on the object store. Then
   write the marker `txapplied/{commit}` with put-if-absent.
4. **Recover.** `await db.recover_transactions()` re-applies, in log
   order, every committed record that lacks an applied marker. Applies are
   deterministic and last-writer-wins in log order, so applying twice is
   harmless. It returns the number of commits it re-applied.

A crash between steps 2 and 3 leaves a committed but unapplied
transaction. **Run `recover_transactions()` at startup, or on a
schedule**, the same way you run the snapshot job. It is idempotent and
safe to run concurrently with live traffic.

## Limits

- **Isolation holds between transactions only.** Writers that call
  `db.objects.put` directly on the same keys bypass conflict detection.
  Their writes are never lost, since applies are plain object writes,
  but a later transaction apply can overwrite them. Route every write to
  transactional keys through transactions.
- **Throughput.** All transactions of an engine serialize on the single
  `_tx` log, with one record per commit. That puts the ceiling around
  10–30 transactions per second.
- **No cross-log atomic appends.** A transaction cannot atomically append
  to `db.log("orders")`. Use `tx.note` to embed audit events in the
  transaction record, and fold them with a projection of the `_tx` log.
- **Values are bytes.** Encode them yourself, for example as JSON.
