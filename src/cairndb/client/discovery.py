"""Discovery: finding snapshots and detecting new commits.

The dense commit log makes discovery trivial and cheap:
- "Anything new?" is one GET of the next commit number (404 = up to date).
- Bootstrap finds the latest snapshot with one prefix LIST.
No manifest exists.
"""

import structlog

from cairndb.core.types import SequenceNumber
from cairndb.storage.base import BlobStorage

logger = structlog.get_logger(__name__)


class DiscoveryService:
    """Helpers for clients to locate their starting point and poll the tail."""

    def __init__(self, storage: BlobStorage):
        """
        Initialize the discovery service.

        Args:
            storage: Blob storage backend
        """
        self.storage = storage
        logger.info("discovery_service_initialized")

    async def find_latest_snapshot(self, schema: str) -> int | None:
        """
        Find the latest snapshot for a projection schema version.

        Returns:
            The snapshot's commit number, or None if no snapshot exists.
        """
        number = await self.storage.find_latest_snapshot(schema)
        if number is not None:
            logger.info("latest_snapshot_found", schema=schema, number=number)
        else:
            logger.info("no_snapshots_available", schema=schema)
        return number

    async def has_new_commits(self, current: SequenceNumber | None) -> bool:
        """
        Check whether the log extends past the current position.

        This is the steady-state poll: a single GET on the next commit
        number. 404 means fully caught up.

        Args:
            current: Current projection position, or None if empty

        Returns:
            True if at least one new commit exists
        """
        next_number = 1 if current is None else current.commit + 1
        data = await self.storage.get_commit(next_number)
        has_new = data is not None

        logger.debug(
            "polled_for_new_commits",
            next_number=next_number,
            has_new=has_new,
        )
        return has_new
