# Exceptions

Every CairnDB exception derives from `CairnDBError`. The two that
applications routinely handle, `LeaseLost` and `TransactionConflict`, are
also exported from the top-level package.

| Exception | Raised when | What to do |
|---|---|---|
| `LeaseLost` | a lease write finds the lease fenced, released, or deleted | discard the outcome; someone else owns the work |
| `TransactionConflict` | a transaction's read set was written concurrently | retry the transaction |
| `EventRejectedError` | a revalidate hook rejected an event after a lost race | surface it as a business-rule violation |
| `CommitError` | a batch could not be committed within `max_commit_attempts` | retry later; check storage health |
| `ReplayError` | a projection handler raised, or there was nothing to snapshot | fix the handler; the projection stays at the previous commit |
| `StorageError` | a backend operation failed (other than a lost precondition) | retry; check credentials and connectivity |
| `ConfigurationError` | unknown storage type | fix the configuration |

```{eval-rst}
.. automodule:: cairndb.core.exceptions
   :members:
   :show-inheritance:
```
