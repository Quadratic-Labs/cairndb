# Engine

The `CairnDB` facade and the objects its methods return. Import them from
the top-level package (`from cairndb import CairnDB, Lease, ...`). See
[Concepts](../../concepts/index.md) for the semantics behind each layer.

## CairnDB

```{eval-rst}
.. autoclass:: cairndb.CairnDB
   :no-show-inheritance:
```

## Layer 0 — Objects

```{eval-rst}
.. autoclass:: cairndb.Objects
   :no-show-inheritance:

.. autodata:: cairndb.engine.objects.RESERVED_PREFIXES
   :no-value:
```

## Layer 1 — Coordination

```{eval-rst}
.. autoclass:: cairndb.ClaimResult
   :no-show-inheritance:

.. autoclass:: cairndb.Lease
   :no-show-inheritance:

.. autoclass:: cairndb.Document
   :no-show-inheritance:
```

## Layer 2 — Logs and transactions

```{eval-rst}
.. autoclass:: cairndb.Log
   :no-show-inheritance:

.. autoclass:: cairndb.Transaction
   :no-show-inheritance:
   :exclude-members: to_event
```

## Layer 3 — Projections

```{eval-rst}
.. autoclass:: cairndb.Projection
   :no-show-inheritance:
```
