"""Garbage collection: retention for snapshots and the commit log.

Deletion is the only mutation in the system besides put-if-absent, so GC
is deliberately conservative:

- Snapshots: keep the newest `keep_snapshots` for the given schema version.
- Log: only when `prune_log` is set, delete commits at or below the OLDEST
  KEPT snapshot — every kept snapshot can still bootstrap, and full history
  from that point remains replayable.

Caveat: log pruning considers a single schema version. If multiple
projection schema versions are active, run prune_log only for the version
whose oldest kept snapshot is lowest.
"""

from dataclasses import dataclass

import structlog

from cairndb.storage.base import BlobStorage

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class GCResult:
    """What a garbage-collection run removed."""

    snapshots_deleted: int
    commits_deleted: int
    oldest_kept_snapshot: int | None


async def collect_garbage(
    storage: BlobStorage,
    schema_version: str,
    keep_snapshots: int = 3,
    prune_log: bool = False,
) -> GCResult:
    """
    Apply retention policy.

    Args:
        storage: Blob storage backend
        schema_version: Projection schema version whose snapshots to prune
        keep_snapshots: Number of most recent snapshots to keep (>= 1)
        prune_log: Also delete commits fully covered by the oldest kept
            snapshot. Leave False to retain full event history (cheap in
            archive-tier storage, and history is usually the point).

    Returns:
        GCResult with deletion counts.
    """
    if keep_snapshots < 1:
        raise ValueError("keep_snapshots must be >= 1")

    numbers = await storage.list_snapshots(schema_version)

    if len(numbers) <= keep_snapshots:
        oldest_kept = numbers[0] if numbers else None
        snapshots_deleted = 0
    else:
        oldest_kept = numbers[-keep_snapshots]
        snapshots_deleted = await storage.delete_snapshots_before(
            schema_version, oldest_kept
        )

    commits_deleted = 0
    if prune_log and oldest_kept is not None:
        # Commits 1..oldest_kept are covered by the oldest kept snapshot
        commits_deleted = await storage.delete_commits_before(oldest_kept + 1)

    result = GCResult(
        snapshots_deleted=snapshots_deleted,
        commits_deleted=commits_deleted,
        oldest_kept_snapshot=oldest_kept,
    )
    logger.info(
        "gc_completed",
        schema_version=schema_version,
        snapshots_deleted=result.snapshots_deleted,
        commits_deleted=result.commits_deleted,
        oldest_kept_snapshot=result.oldest_kept_snapshot,
    )
    return result
