"""End-to-end acceptance test for the v2 commit-log architecture.

Scenario (from the refactor plan):
- 3 concurrent committer tasks x N events each, with jitter
- one snapshot built mid-run
- one committer killed (closed mid-stream) and replaced
- a fresh reader bootstraps from snapshot + tail and must observe every
  acked event exactly once, in a total order consistent with each
  writer's submission order, over a dense gap-free log.
"""

import asyncio
import random

from cairndb.client.projector import Projector
from cairndb.client.registry import HandlerRegistry
from cairndb.client.replay import ReplayEngine
from cairndb.committer import Committer, CommitterConfig
from cairndb.core.log import Commit
from cairndb.core.types import SequenceNumber
from cairndb.storage.filesystem import FilesystemStorage
from tests.conftest import make_event

N_WRITERS = 3
N_EVENTS = 30  # per writer


def event_registry() -> HandlerRegistry:
    """Handlers projecting every event into a generic events table."""
    registry = HandlerRegistry()

    async def record(db, entry):
        await db.execute(
            "INSERT INTO events (sequence, writer, n) VALUES (?, ?, ?)",
            (str(entry.sequence), entry.payload["writer"], entry.payload["n"]),
        )

    for writer_id in range(N_WRITERS):
        registry.register(f"writer.{writer_id}", record)
    return registry


async def init_events_projection(db_path: str) -> None:
    import aiosqlite

    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS _cairndb_metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                sequence TEXT PRIMARY KEY,
                writer INTEGER NOT NULL,
                n INTEGER NOT NULL
            )
        """)
        await db.commit()


async def test_end_to_end_concurrent_writers_snapshot_and_fresh_reader(temp_dir):
    ledger = temp_dir / "ledger"
    config = CommitterConfig(lost_race_backoff_seconds=0.001)
    registry = event_registry()
    acked: dict[int, list[SequenceNumber]] = {w: [] for w in range(N_WRITERS)}

    async def writer(writer_id: int) -> None:
        """Write N_EVENTS; writer 0 is killed and replaced mid-run."""
        committer = Committer(FilesystemStorage(ledger), config)
        for n in range(N_EVENTS):
            seq = await committer.append(
                make_event(f"writer.{writer_id}", {"writer": writer_id, "n": n})
            )
            acked[writer_id].append(seq)

            if writer_id == 0 and n == N_EVENTS // 2:
                # Kill this committer mid-run; continue with a fresh one
                # (fresh storage instance = fresh tail view)
                await committer.close()
                committer = Committer(FilesystemStorage(ledger), config)

            if random.random() < 0.3:
                await asyncio.sleep(random.random() * 0.005)
        await committer.close()

    async def snapshotter() -> None:
        """Build one snapshot mid-run from whatever the log holds."""
        await asyncio.sleep(0.05)
        storage = FilesystemStorage(ledger)
        snap_path = str(temp_dir / "snap-build.db")
        await init_events_projection(snap_path)

        engine = ReplayEngine(storage, registry)
        last = await engine.replay(snap_path, after=0)
        if last is not None:
            with open(snap_path, "rb") as f:
                await storage.put_snapshot("1.0.0", last.commit, f.read())

    await asyncio.gather(*(writer(w) for w in range(N_WRITERS)), snapshotter())

    storage = FilesystemStorage(ledger)

    # --- Log-level invariants -------------------------------------------
    commits = await storage.list_commits()
    assert commits == list(range(1, max(commits) + 1)), "log has gaps"

    total_events = 0
    for number in commits:
        total_events += len(Commit.from_msgpack(await storage.get_commit(number)).events)
    assert total_events == N_WRITERS * N_EVENTS, "event lost or duplicated in log"

    # Every ack points at a real, distinct position
    all_acked = [s for seqs in acked.values() for s in seqs]
    assert len(set(all_acked)) == N_WRITERS * N_EVENTS

    # --- Fresh reader bootstraps (snapshot + tail) -----------------------
    from cairndb.client.config import ClientConfig

    reader_config = ClientConfig(
        storage_type="filesystem",
        storage_path=str(ledger),
        db_path=str(temp_dir / "reader.db"),
        poll_interval_seconds=0.1,
    )
    projector = Projector(
        reader_config, storage, registry, init_schema=init_events_projection
    )
    updated, last_seq = await projector.apply_updates()

    assert updated is True
    assert last_seq == Commit.from_msgpack(
        await storage.get_commit(commits[-1])
    ).last_sequence

    import aiosqlite

    async with aiosqlite.connect(reader_config.db_path) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM events")
        assert (await cursor.fetchone())[0] == N_WRITERS * N_EVENTS

        # Per-writer order in the projection matches submission order
        for writer_id in range(N_WRITERS):
            cursor = await db.execute(
                "SELECT n FROM events WHERE writer = ? ORDER BY sequence",
                (writer_id,),
            )
            ns = [row[0] for row in await cursor.fetchall()]
            assert ns == list(range(N_EVENTS)), f"writer {writer_id} order broken"

        # Acked sequences all present
        cursor = await db.execute("SELECT sequence FROM events")
        present = {row[0] for row in await cursor.fetchall()}
        assert {str(s) for s in all_acked} == present
