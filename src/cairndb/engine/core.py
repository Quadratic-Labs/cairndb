"""The engine facade: one object exposing every layer of the kernel.

    from cairndb import CairnDB

    db = CairnDB.configure({"storage": {"type": "filesystem", "path": "./ledger"}})

    await db.objects.put("config/app", b"{}")          # layer 0: conditional KV
    result = await db.claim("dispatch/etl:2026-08-09", {"run": 1})   # layer 1
    lease = await db.lease("state/run-1", ttl=60)
    seq = await db.log("orders").append(event)          # layer 2: named logs
    async with db.transact() as tx: ...                 #          transactions
    proj = db.projection("orders_view", log="orders")   # layer 3: projections

    await db.close()

One engine = one bucket (or one prefix of one bucket). See
docs/ENGINE_API.md for the full design.
"""

from typing import Any

import structlog

from cairndb.core.types import SequenceNumber
from cairndb.storage.base import BlobStorage
from cairndb.storage.config import StorageConfig

from cairndb.engine import coordination
from cairndb.engine.coordination import ClaimResult, Document, Lease
from cairndb.engine.logs import Log
from cairndb.engine.objects import Objects
from cairndb.engine.projection import Projection, SchemaInitializer
from cairndb.engine.transactions import Transaction, TransactionManager

logger = structlog.get_logger(__name__)


class CairnDB:
    """Serverless database engine on blob storage.

    All state lives in the bucket; instances are cheap, stateless views of
    it and any number of them — across any number of processes — may
    operate on the same bucket concurrently.
    """

    def __init__(self, storage: BlobStorage):
        self.storage = storage
        self.objects = Objects(storage)
        self._logs: dict[str | None, Log] = {}
        self._tx: TransactionManager | None = None
        self._projections: list[Projection] = []

    @classmethod
    def configure(cls, config: dict[str, Any] | StorageConfig) -> "CairnDB":
        """Build an engine from configuration.

        Accepts either a StorageConfig, or a dict with a ``"storage"`` key
        holding StorageConfig fields::

            CairnDB.configure({"storage": {"type": "s3", "bucket": "myapp"}})
        """
        if isinstance(config, StorageConfig):
            storage_config = config
        else:
            storage_config = StorageConfig(**config.get("storage", config))
        return cls(storage_config.create_storage())

    # ------------------------------------------------------------------
    # Layer 1 — coordination
    # ------------------------------------------------------------------

    async def claim(self, key: str, value: Any) -> ClaimResult:
        """Claim `key` put-if-absent; losers converge on the winner's value."""
        return await coordination.claim(self.storage, key, value)

    def claim_sync(self, key: str, value: Any) -> ClaimResult:
        """Sync twin of :meth:`claim`."""
        return coordination.claim_sync(self.storage, key, value)

    async def lease(
        self,
        key: str,
        *,
        ttl: float,
        holder: str | None = None,
        steal_if_expired: bool = True,
    ) -> Lease | None:
        """Acquire the lease on `key`, or None if it is actively held."""
        return await coordination.acquire(
            self.storage, key, ttl=ttl, holder=holder, steal_if_expired=steal_if_expired
        )

    def lease_sync(
        self,
        key: str,
        *,
        ttl: float,
        holder: str | None = None,
        steal_if_expired: bool = True,
    ) -> Lease | None:
        """Sync twin of :meth:`lease`."""
        return coordination.acquire_sync(
            self.storage, key, ttl=ttl, holder=holder, steal_if_expired=steal_if_expired
        )

    def doc(self, key: str, model: type | None = None) -> Document:
        """A typed, etag-guarded document with a read-modify-write loop."""
        return Document(self.storage, key, model)

    # ------------------------------------------------------------------
    # Layer 2 — logs and transactions
    # ------------------------------------------------------------------

    def log(self, name: str | None = None) -> Log:
        """The named commit log `name`, or the root log when omitted.

        Logs are cached per name; the same Log (and its committer) is
        returned for repeated calls.
        """
        if name not in self._logs:
            self._logs[name] = Log(self.storage, name)
        return self._logs[name]

    def transact(self) -> Transaction:
        """Begin a multi-key transaction (use as ``async with``)."""
        return self._transactions.transaction()

    async def recover_transactions(self) -> int:
        """Re-apply committed transactions that never reached the object
        store (crash between commit point and apply). Idempotent."""
        return await self._transactions.recover()

    @property
    def _transactions(self) -> TransactionManager:
        if self._tx is None:
            self._tx = TransactionManager(self.storage)
        return self._tx

    # ------------------------------------------------------------------
    # Layer 3 — projections
    # ------------------------------------------------------------------

    def projection(
        self,
        name: str,
        *,
        version: str = "1",
        db_path: str | None = None,
        log: str | None = None,
        poll_interval: float = 5.0,
        init_schema: SchemaInitializer | None = None,
    ) -> Projection:
        """A declarative SQLite projection of one log (root by default)."""
        projection = Projection(
            self.log(log).storage,
            name,
            version=version,
            db_path=db_path,
            poll_interval=poll_interval,
            init_schema=init_schema,
        )
        self._projections.append(projection)
        return projection

    # ------------------------------------------------------------------
    # Read-your-writes across layers
    # ------------------------------------------------------------------

    async def wait_for_sequence(
        self, projection: Projection, sequence: SequenceNumber | str, timeout: float = 30.0
    ) -> bool:
        """Convenience: block until `projection` has applied `sequence`."""
        return await projection.wait_for(sequence, timeout)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Stop projection updaters and drain/close every log committer."""
        for projection in self._projections:
            await projection.stop()
        for log in self._logs.values():
            await log.close()
        if self._tx is not None:
            await self._tx.close()
            self._tx = None
        logger.info("engine_closed")

    async def __aenter__(self) -> "CairnDB":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()
