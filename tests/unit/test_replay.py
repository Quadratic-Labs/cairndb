"""Unit tests for the replay engine's metadata tracking and reporting."""

from structlog.testing import capture_logs

from cairndb.client.registry import HandlerRegistry
from cairndb.client.replay import ReplayEngine
from cairndb.core.types import SequenceNumber
from tests.conftest import (
    commit_events,
    init_users_projection,
    make_user_registry,
    user_created,
)


async def test_replay_reports_exact_counts(storage, temp_dir):
    db_path = str(temp_dir / "projection.db")
    await init_users_projection(db_path)
    # Commit 1 carries two events, commit 2 one — the completion report
    # is the module's observability contract.
    await commit_events(storage, user_created(1, "a"), user_created(2, "b"))
    await commit_events(storage, user_created(3, "c"))

    engine = ReplayEngine(storage, make_user_registry())
    with capture_logs() as logs:
        last = await engine.replay(db_path, after=0)

    assert last == SequenceNumber(2, 0)
    done = next(entry for entry in logs if entry["event"] == "replay_completed")
    assert done["commits_applied"] == 2
    assert done["events_applied"] == 3


async def test_empty_replay_reports_nothing(storage, temp_dir):
    db_path = str(temp_dir / "projection.db")
    await init_users_projection(db_path)

    engine = ReplayEngine(storage, make_user_registry())
    with capture_logs() as logs:
        assert await engine.replay(db_path, after=0) is None
    assert not [entry for entry in logs if entry["event"] == "replay_completed"]


async def test_update_last_applied_sequence_roundtrip(storage, temp_dir):
    engine = ReplayEngine(storage, HandlerRegistry())
    db_path = str(temp_dir / "meta.db")
    await engine.initialize_metadata_table(db_path)
    assert await engine.get_last_applied_sequence(db_path) is None

    await engine.update_last_applied_sequence(db_path, SequenceNumber(3, 1))
    assert await engine.get_last_applied_sequence(db_path) == SequenceNumber(3, 1)
