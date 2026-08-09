"""Tests for the Committer (commit protocol)."""

import asyncio
import shutil
import tempfile
from pathlib import Path

import pytest

from cairndb.committer import Committer, CommitterConfig
from cairndb.core.exceptions import CommitError, EventRejectedError, StorageError
from cairndb.core.log import Commit, Event
from cairndb.core.types import EventType, SchemaVersion, SequenceNumber, Timestamp
from cairndb.storage.filesystem import FilesystemStorage


def make_event(n: int = 0, event_type: str = "test.event") -> Event:
    return Event(
        event_type=EventType(event_type),
        timestamp=Timestamp.now(),
        payload={"n": n},
        schema_version=SchemaVersion("1.0.0"),
    )


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d)


@pytest.fixture
def storage(temp_dir):
    return FilesystemStorage(temp_dir)


async def read_all_events(storage) -> list[tuple[SequenceNumber, Event]]:
    """Read every event in the log with its sequence."""
    result = []
    for number in await storage.list_commits():
        commit = Commit.from_msgpack(await storage.get_commit(number))
        for sequenced in commit.sequenced_events():
            result.append((sequenced.sequence, sequenced.event))
    return result


class TestBasicAppend:
    @pytest.mark.asyncio
    async def test_append_returns_sequence_and_is_durable(self, storage):
        async with Committer(storage) as committer:
            seq = await committer.append(make_event(1))

        assert seq == SequenceNumber(1, 0)
        # Durable: readable directly from storage
        commit = Commit.from_msgpack(await storage.get_commit(1))
        assert commit.events[0].payload == {"n": 1}

    @pytest.mark.asyncio
    async def test_sequential_appends_are_dense(self, storage):
        async with Committer(storage) as committer:
            seqs = [await committer.append(make_event(i)) for i in range(5)]

        assert [s.commit for s in seqs] == [1, 2, 3, 4, 5]
        assert await storage.list_commits() == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_append_many_preserves_order(self, storage):
        async with Committer(storage) as committer:
            seqs = await committer.append_many([make_event(i) for i in range(10)])

        assert seqs == sorted(seqs)
        events = await read_all_events(storage)
        assert [e.payload["n"] for _, e in events] == list(range(10))

    @pytest.mark.asyncio
    async def test_append_many_empty(self, storage):
        async with Committer(storage) as committer:
            assert await committer.append_many([]) == []

    @pytest.mark.asyncio
    async def test_resumes_from_existing_tail(self, storage):
        async with Committer(storage) as committer:
            await committer.append(make_event(1))

        # A brand-new committer discovers the tail and continues densely
        async with Committer(storage) as committer2:
            seq = await committer2.append(make_event(2))

        assert seq.commit == 2

    @pytest.mark.asyncio
    async def test_append_after_close_raises(self, storage):
        committer = Committer(storage)
        await committer.close()

        with pytest.raises(CommitError):
            await committer.append(make_event())

    @pytest.mark.asyncio
    async def test_close_drains_pending(self, storage):
        committer = Committer(storage)
        # Fire off appends and close immediately; close must drain, not drop
        tasks = [asyncio.ensure_future(committer.append(make_event(i))) for i in range(5)]
        await asyncio.sleep(0)  # let appends enqueue
        await committer.close()

        seqs = await asyncio.gather(*tasks)
        assert len(seqs) == 5
        assert len(await read_all_events(storage)) == 5


class TestDurableAck:
    @pytest.mark.asyncio
    async def test_no_ack_before_storage_put(self, storage, monkeypatch):
        """The append future must not resolve before put_commit succeeds."""
        put_started = asyncio.Event()
        release_put = asyncio.Event()
        real_put = storage.put_commit

        async def slow_put(number, data):
            put_started.set()
            await release_put.wait()
            return await real_put(number, data)

        monkeypatch.setattr(storage, "put_commit", slow_put)

        committer = Committer(storage)
        task = asyncio.ensure_future(committer.append(make_event()))

        await put_started.wait()
        await asyncio.sleep(0.05)
        assert not task.done()  # put in flight -> no ack

        release_put.set()
        seq = await task
        assert seq == SequenceNumber(1, 0)
        await committer.close()

    @pytest.mark.asyncio
    async def test_storage_error_fails_the_append(self, storage, monkeypatch):
        async def failing_put(number, data):
            raise StorageError("bucket on fire")

        monkeypatch.setattr(storage, "put_commit", failing_put)

        committer = Committer(storage)
        with pytest.raises(StorageError):
            await committer.append(make_event())
        await committer.close()


class TestGroupCommit:
    @pytest.mark.asyncio
    async def test_events_arriving_during_put_batch_into_next_commit(
        self, storage, monkeypatch
    ):
        """Appends submitted while a put is in flight share the next commit."""
        release_first_put = asyncio.Event()
        put_count = 0
        real_put = storage.put_commit

        async def gated_put(number, data):
            nonlocal put_count
            put_count += 1
            if put_count == 1:
                await release_first_put.wait()
            return await real_put(number, data)

        monkeypatch.setattr(storage, "put_commit", gated_put)

        committer = Committer(storage)
        first = asyncio.ensure_future(committer.append(make_event(0)))
        await asyncio.sleep(0.05)  # first put now in flight

        # These accumulate while the first put is blocked
        rest = [asyncio.ensure_future(committer.append(make_event(i))) for i in range(1, 6)]
        await asyncio.sleep(0.05)
        release_first_put.set()

        first_seq = await first
        rest_seqs = await asyncio.gather(*rest)

        assert first_seq.commit == 1
        # All five later events landed together in commit 2 (group commit)
        assert {s.commit for s in rest_seqs} == {2}
        assert [s.index for s in rest_seqs] == [0, 1, 2, 3, 4]
        await committer.close()

    @pytest.mark.asyncio
    async def test_max_events_per_commit_splits_batches(self, storage):
        config = CommitterConfig(max_events_per_commit=3)
        async with Committer(storage, config) as committer:
            seqs = await committer.append_many([make_event(i) for i in range(7)])

        assert [s.commit for s in seqs] == [1, 1, 1, 2, 2, 2, 3]


class TestLostRaces:
    @pytest.mark.asyncio
    async def test_lost_race_advances_and_retries(self, storage):
        """A committer whose tail is stale advances past interleaved commits."""
        # Another writer owns commits 1-3 (sequential appends = one commit each)
        other = Committer(storage)
        for i in range(3):
            await other.append(make_event(i, "other.event"))
        await other.close()

        # This committer believes the log is empty (stale tail_hint bypasses
        # discovery), so its first put targets commit 1 and loses
        committer = Committer(storage)
        committer._tail = 0

        seq = await committer.append(make_event(99))

        assert seq.commit == 4  # advanced past 1..3, won 4
        await committer.close()

    @pytest.mark.asyncio
    async def test_attempts_exhausted_raises(self, storage, monkeypatch):
        """If the put can never be won, appends fail with CommitError."""

        async def always_lose(number, data):
            return False

        monkeypatch.setattr(storage, "put_commit", always_lose)

        config = CommitterConfig(max_commit_attempts=2, lost_race_backoff_seconds=0.0)
        committer = Committer(storage, config)

        with pytest.raises(CommitError, match="attempts"):
            await committer.append(make_event())
        await committer.close()

    @pytest.mark.asyncio
    async def test_conflict_without_visible_winner_backs_off_and_retries(
        self, storage, monkeypatch
    ):
        """A 409-style rejection with no visible object retries the same number."""
        real_put = storage.put_commit
        calls = 0

        async def flaky_put(number, data):
            nonlocal calls
            calls += 1
            if calls == 1:
                return False  # rejected, but nothing visible at this number
            return await real_put(number, data)

        monkeypatch.setattr(storage, "put_commit", flaky_put)

        config = CommitterConfig(lost_race_backoff_seconds=0.0)
        async with Committer(storage, config) as committer:
            seq = await committer.append(make_event())

        assert seq.commit == 1
        assert calls == 2


class TestRevalidation:
    @pytest.mark.asyncio
    async def test_hook_sees_interleaved_commits(self, storage):
        # Other writer owns commits 1-2 (sequential appends = one commit each)
        other = Committer(storage)
        for i in range(2):
            await other.append(make_event(i, "other.event"))
        await other.close()

        seen: dict = {}

        async def hook(events, interleaved):
            seen["interleaved"] = [c.number for c in interleaved]
            return list(events)  # keep everything

        committer = Committer(storage, revalidate=hook)
        committer._tail = 0  # stale view forces a lost race

        seq = await committer.append(make_event(99))

        assert seq.commit == 3
        assert seen["interleaved"] == [1, 2]
        await committer.close()

    @pytest.mark.asyncio
    async def test_hook_rejection_fails_that_event_only(self, storage):
        other = Committer(storage)
        await other.append(make_event(0, "other.event"))
        await other.close()

        async def reject_odd(events, interleaved):
            return [None if e.payload["n"] % 2 else e for e in events]

        committer = Committer(storage, revalidate=reject_odd)
        committer._tail = 0  # force one lost race

        ok_task = asyncio.ensure_future(committer.append(make_event(2)))
        bad_task = asyncio.ensure_future(committer.append(make_event(3)))

        seq = await ok_task
        with pytest.raises(EventRejectedError):
            await bad_task

        assert seq.commit == 2
        events = await read_all_events(storage)
        assert [e.payload["n"] for _, e in events] == [0, 2]
        await committer.close()


class TestConcurrentCommitters:
    @pytest.mark.asyncio
    async def test_dense_gap_free_log_under_contention(self, storage, temp_dir):
        """N concurrent committers on one log: no loss, no gaps, total order."""
        n_writers, n_events = 4, 25
        config = CommitterConfig(lost_race_backoff_seconds=0.001)

        async def writer(writer_id: int) -> list[SequenceNumber]:
            # Separate storage instance per writer = separate tail views
            committer = Committer(FilesystemStorage(temp_dir), config)
            seqs = []
            for i in range(n_events):
                seqs.append(await committer.append(make_event(i, f"writer.{writer_id}")))
                if i % 7 == writer_id % 7:
                    await asyncio.sleep(0.001)  # jitter
            await committer.close()
            return seqs

        all_seqs = await asyncio.gather(*(writer(w) for w in range(n_writers)))

        # Every append was acked with a distinct sequence
        flat = [s for seqs in all_seqs for s in seqs]
        assert len(set(flat)) == n_writers * n_events

        # The log is dense: no gaps
        commits = await storage.list_commits()
        assert commits == list(range(1, max(commits) + 1))

        # No event lost, none duplicated
        events = await read_all_events(storage)
        assert len(events) == n_writers * n_events

        # Per-writer submission order is preserved in the global order
        for writer_id, seqs in enumerate(all_seqs):
            assert seqs == sorted(seqs)
            positions = [
                e.payload["n"]
                for _, e in events
                if e.event_type == f"writer.{writer_id}"
            ]
            assert positions == list(range(n_events))
