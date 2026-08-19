"""Unit tests for the background updater's loop, counters, and waiting."""

import asyncio
from dataclasses import replace

from structlog.testing import capture_logs

from cairndb.client.updater import BackgroundUpdater
from tests.conftest import (
    commit_events,
    init_users_projection,
    make_user_registry,
    user_created,
)


async def _wait_until(condition, timeout: float = 5.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while not condition():
        assert asyncio.get_event_loop().time() < deadline, "condition never became true"
        await asyncio.sleep(0.01)


def _updater(client_config, storage, poll: float = 0.02) -> BackgroundUpdater:
    config = replace(client_config, poll_interval_seconds=poll)
    return BackgroundUpdater(
        config, storage, make_user_registry(), init_schema=init_users_projection
    )


async def test_background_loop_counts_and_stops_cleanly(storage, client_config):
    updater = _updater(client_config, storage)
    assert updater.update_count == 0
    assert updater.is_running is False

    with capture_logs() as logs:
        await updater.start()
        await commit_events(storage, user_created(1, "a"))
        await _wait_until(lambda: updater.update_count >= 1)
        assert updater.update_count == 1  # one batch of news = one update

        await commit_events(storage, user_created(2, "b"))
        await _wait_until(lambda: updater.update_count >= 2)
        assert updater.update_count == 2
        await updater.stop()

    assert updater.is_running is False
    events = [entry["event"] for entry in logs]
    assert "update_loop_stopped" in events
    assert "update_loop_error" not in events


async def test_update_loop_survives_errors(storage, client_config, monkeypatch):
    updater = _updater(client_config, storage)
    attempts = {"n": 0}
    real_apply = updater.projector.apply_updates

    async def flaky():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("transient")
        return await real_apply()

    monkeypatch.setattr(updater.projector, "apply_updates", flaky)

    await commit_events(storage, user_created(1, "a"))
    await updater.start()
    await _wait_until(lambda: updater.update_count >= 1)
    await updater.stop()
    assert updater.update_count == 1


async def test_trigger_update_returns_sequence_string(storage, client_config):
    updater = _updater(client_config, storage)
    (seq,) = await commit_events(storage, user_created(1, "a"))

    updated, sequence_str = await updater.trigger_update()
    assert updated is True
    assert sequence_str == str(seq)


async def test_wait_for_sequence_times_out_at_exactly_the_default(
    storage, client_config, monkeypatch
):
    updater = _updater(client_config, storage)

    times = iter([0.0, 30.0, 0.0, 10.0, 30.5])
    slept = []

    class _FakeLoop:
        def time(self):
            return next(times)

    class _FakeAsyncio:
        def get_event_loop(self):
            return _FakeLoop()

        async def sleep(self, delay):
            slept.append(delay)

    monkeypatch.setattr("cairndb.client.updater.asyncio", _FakeAsyncio())

    triggers = []

    async def spy_trigger():
        triggers.append(1)
        return False, None

    monkeypatch.setattr(updater, "trigger_update", spy_trigger)

    # elapsed == timeout is already a timeout: no extra polling round.
    assert await updater.wait_for_sequence("000000000005:000000") is False
    assert triggers == []

    # Before the deadline, the wait polls once per 0.1s pacing sleep.
    assert await updater.wait_for_sequence("000000000005:000000") is False
    assert triggers == [1]
    assert slept == [0.1]
