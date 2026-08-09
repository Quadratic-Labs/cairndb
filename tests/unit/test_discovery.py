"""Tests for the discovery service."""

import pytest

from cairndb.client.discovery import DiscoveryService
from cairndb.core.types import SequenceNumber

from tests.conftest import commit_events, make_event


@pytest.fixture
def discovery(storage):
    return DiscoveryService(storage)


class TestFindLatestSnapshot:
    async def test_no_snapshots(self, discovery):
        assert await discovery.find_latest_snapshot("1.0.0") is None

    async def test_returns_highest_number(self, storage, discovery):
        await storage.put_snapshot("1.0.0", 10, b"old")
        await storage.put_snapshot("1.0.0", 40, b"new")

        assert await discovery.find_latest_snapshot("1.0.0") == 40

    async def test_schema_isolation(self, storage, discovery):
        await storage.put_snapshot("1.0.0", 40, b"v1")

        assert await discovery.find_latest_snapshot("2.0.0") is None


class TestHasNewCommits:
    async def test_empty_log_from_scratch(self, discovery):
        assert await discovery.has_new_commits(None) is False

    async def test_new_commits_from_scratch(self, storage, discovery):
        await commit_events(storage, make_event())

        assert await discovery.has_new_commits(None) is True

    async def test_caught_up(self, storage, discovery):
        await commit_events(storage, make_event())  # commit 1

        assert await discovery.has_new_commits(SequenceNumber(1, 0)) is False

    async def test_behind_the_tail(self, storage, discovery):
        await commit_events(storage, make_event())  # commit 1
        await commit_events(storage, make_event())  # commit 2

        assert await discovery.has_new_commits(SequenceNumber(1, 0)) is True
