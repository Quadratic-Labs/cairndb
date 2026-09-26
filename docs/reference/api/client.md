# Client

The lower-level read path: a polling projection with SQLAlchemy sessions,
and the replay machinery that `Projection` and the jobs are built on. See
[Lower-level API](../../guides/lower-level-api.md).

## Public API

```{eval-rst}
.. autoclass:: cairndb.client.CairnDBClient
   :no-show-inheritance:

.. autoclass:: cairndb.client.ClientConfig
   :no-show-inheritance:

.. autoclass:: cairndb.client.HandlerRegistry
   :no-show-inheritance:

.. autodata:: cairndb.client.EventHandler
   :no-value:
```

## Internals

These classes are stable enough to use, but most applications never
need them directly.

```{eval-rst}
.. autoclass:: cairndb.client.projector.Projector
   :no-show-inheritance:

.. autoclass:: cairndb.client.updater.BackgroundUpdater
   :no-show-inheritance:

.. autoclass:: cairndb.client.replay.ReplayEngine
   :no-show-inheritance:

.. autoclass:: cairndb.client.DiscoveryService
   :no-show-inheritance:
```
