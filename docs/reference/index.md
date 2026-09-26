# Reference

- [Configuration](configuration.md): storage options, environment
  variables, and client and committer settings.
- [Command-line interface](cli.md): `cairndb snapshot | gc | rebuild`.
- API reference, generated from the source docstrings:
  - [Engine](api/engine.md): `CairnDB` and every layer (`Objects`,
    `ClaimResult`, `Lease`, `Document`, `Log`, `Transaction`,
    `Projection`).
  - [Events and types](api/events.md): `Event`, `SequencedEvent`,
    `Commit`, `SequenceNumber`, `Timestamp`.
  - [Writer](api/writer.md): `Committer`, `CommitterConfig`,
    `RevalidateHook`.
  - [Storage](api/storage.md): `StorageConfig` and the backends.
  - [Client](api/client.md): `CairnDBClient`, `ClientConfig`,
    `HandlerRegistry`, and the replay internals.
  - [Jobs](api/jobs.md): `SnapshotBuilder`, `collect_garbage`.
  - [Exceptions](api/exceptions.md).
- [Glossary](../glossary.md).

Everything most applications need is importable from the top-level
package:

```python
from cairndb import (
    CairnDB,                                        # the engine
    Event, EventType, SchemaVersion, Timestamp,     # writing events
    SequenceNumber, SequencedEvent, Commit,         # reading them
    LeaseLost, TransactionConflict,                 # expected exceptions
)
```
