# Layer 0 — Objects

`db.objects` is a conditional key-value store of mutable objects. Each
stored object has an opaque, backend-native **etag** that changes on every
write. Every higher layer is built on these operations.

```python
obj  = await db.objects.get("config/app.json")         # StoredObject(data, etag) | None
etag = await db.objects.put("config/app.json", data)   # unconditional write
etag = await db.objects.put("config/app.json", data,
                            if_match=obj.etag)          # compare-and-swap
etag = await db.objects.put("init/marker", b"",
                            if_absent=True)             # put-if-absent
await db.objects.delete("config/app.json")             # no-op if missing
keys = await db.objects.list("config/")                # ascending
```

## Write semantics

| Call | Succeeds when | Returns on failure |
|---|---|---|
| `put(key, data)` | always | — |
| `put(key, data, if_match=etag)` | the stored etag still equals `etag` (a deleted object fails too) | `None` |
| `put(key, data, if_absent=True)` | the key does not exist | `None` |

`if_match` and `if_absent` are mutually exclusive. A `None` result is not
an error: it means "you lost the race". Re-read and decide what to do.

Data is raw `bytes`. Encode it however you like. If you want JSON
documents with a retry loop, the [`doc`](coordination.md#doc-typed-document-with-a-retry-loop)
primitive does the encoding for you.

Objects are **linearizable per key**: once a write is acknowledged,
every subsequent read, from any process, observes it.

## Watching a key

`wait_for` polls until a key appears, or until it changes from a known
etag:

```python
# Wait for an approval object to exist.
approval = await db.objects.wait_for("signals/run-1/approval", timeout=300, poll_interval=2.0)

# Wait until a document changes from the version we last saw.
newer = await db.objects.wait_for("config/app.json", changed_from=obj.etag, timeout=60)
```

It costs one GET per interval and raises `TimeoutError` when time runs
out. Polling *is* the correctness mechanism: there is no push channel that
could drop a notification.

## Reserved prefixes

`log/`, `snapshots/`, `logs/`, and `txapplied/` belong to the engine.
`get`, `put`, and `delete` raise `ValueError` for keys under them, so
application objects can never corrupt a log.

## Sync twins

Each method has a `*_sync` twin (`get_sync`, `put_sync`, `delete_sync`,
`list_sync`) for code without an event loop.
