"""Tests for commit log structures and serialization."""

import pytest

from cairndb.core.log import Event, Commit
from cairndb.core.types import EventType, SchemaVersion, SequenceNumber, Timestamp


def make_event(n: int = 0) -> Event:
    """Create a sample event."""
    return Event(
        event_type=EventType("user.created"),
        timestamp=Timestamp.now(),
        payload={"user_id": n, "name": f"user-{n}"},
        schema_version=SchemaVersion("1.0.0"),
        metadata={"source": "test"},
    )


class TestEvent:
    """Tests for Event serialization."""

    def test_dict_round_trip(self):
        """Events survive dict serialization."""
        event = make_event()
        restored = Event.from_dict(event.to_dict())

        assert restored == event

    def test_from_dict_defaults_metadata(self):
        """Missing metadata deserializes as empty dict."""
        data = make_event().to_dict()
        del data["metadata"]

        restored = Event.from_dict(data)
        assert restored.metadata == {}

    def test_immutable(self):
        """Events are frozen."""
        event = make_event()
        with pytest.raises(AttributeError):
            event.payload = {}  # type: ignore


class TestCommit:
    """Tests for Commit structure and serialization."""

    def test_msgpack_round_trip(self):
        """Commits survive msgpack serialization."""
        commit = Commit(
            number=42,
            created_at=Timestamp.now(),
            events=tuple(make_event(i) for i in range(3)),
        )

        restored = Commit.from_msgpack(commit.to_msgpack())

        assert restored.number == commit.number
        assert restored.events == commit.events
        assert restored.created_at.to_iso() == commit.created_at.to_iso()

    def test_sequences_are_positional(self):
        """Each event's sequence is (commit number, position)."""
        commit = Commit(
            number=7,
            created_at=Timestamp.now(),
            events=tuple(make_event(i) for i in range(3)),
        )

        assert commit.sequences() == [
            SequenceNumber(7, 0),
            SequenceNumber(7, 1),
            SequenceNumber(7, 2),
        ]
        assert commit.last_sequence == SequenceNumber(7, 2)

    def test_sequenced_events_pairs_in_order(self):
        """sequenced_events yields (sequence, event) pairs in order."""
        events = tuple(make_event(i) for i in range(3))
        commit = Commit(number=7, created_at=Timestamp.now(), events=events)

        pairs = list(commit.sequenced_events())

        assert [p.sequence for p in pairs] == commit.sequences()
        assert tuple(p.event for p in pairs) == events

    def test_rejects_empty_commit(self):
        """A commit must contain at least one event."""
        with pytest.raises(ValueError):
            Commit(number=1, created_at=Timestamp.now(), events=())

    def test_rejects_invalid_number(self):
        """Commit numbers start at 1."""
        with pytest.raises(ValueError):
            Commit(number=0, created_at=Timestamp.now(), events=(make_event(),))
