"""CairnDB engine: the serverless database-engine facade.

See docs/ENGINE_API.md for the design. Layers:

- objects: conditional key-value store (Objects)
- coordination: claim / Lease / Document
- logs: named commit logs (Log) and multi-key transactions (Transaction)
- projection: declarative SQLite projections (Projection)
"""

from cairndb.engine.coordination import ClaimResult, Document, Lease
from cairndb.engine.core import CairnDB
from cairndb.engine.logs import Log, NamespacedStorage
from cairndb.engine.objects import Objects
from cairndb.engine.projection import Projection
from cairndb.engine.transactions import Transaction, TransactionManager

__all__ = [
    "CairnDB",
    "ClaimResult",
    "Document",
    "Lease",
    "Log",
    "NamespacedStorage",
    "Objects",
    "Projection",
    "Transaction",
    "TransactionManager",
]
