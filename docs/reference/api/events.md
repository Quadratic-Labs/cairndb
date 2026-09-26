# Events and types

The data model of the logs: what writers append and what handlers
receive. See [Logs](../../concepts/logs.md).

## Events

```{eval-rst}
.. autoclass:: cairndb.Event
   :no-show-inheritance:
   :exclude-members: to_dict, from_dict

.. autoclass:: cairndb.SequencedEvent
   :no-show-inheritance:

.. autoclass:: cairndb.Commit
   :no-show-inheritance:
   :exclude-members: to_msgpack, from_msgpack
```

## Types

```{eval-rst}
.. autoclass:: cairndb.SequenceNumber
   :no-show-inheritance:

.. autoclass:: cairndb.Timestamp
   :no-show-inheritance:

.. py:data:: cairndb.EventType

   ``NewType("EventType", str)``: an event's type name, for example
   ``EventType("order.placed")``. It selects the projection handler.

.. py:data:: cairndb.SchemaVersion

   ``NewType("SchemaVersion", str)``: the version of an event payload's
   shape, for example ``SchemaVersion("1")``.
```
