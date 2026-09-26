# Writer

The commit protocol behind `Log.append`. Use it directly for fine
control over batching and revalidation (see
[Lower-level API](../../guides/lower-level-api.md)).

```{eval-rst}
.. autoclass:: cairndb.Committer
   :no-show-inheritance:

.. autoclass:: cairndb.CommitterConfig
   :no-show-inheritance:

.. py:data:: cairndb.RevalidateHook

   ``Callable[[list[Event], list[Commit]], Awaitable[list[Event | None]]]``

   Called after a lost commit race with the pending events and the commits
   that interleaved. It returns one entry per pending event: the event to
   commit in its place (possibly modified), or ``None`` to reject it. A
   rejected event fails its ``append()`` with
   :class:`~cairndb.core.exceptions.EventRejectedError`.
```
