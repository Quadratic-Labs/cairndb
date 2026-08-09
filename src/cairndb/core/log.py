"""Commit log structures and serialization.

The commit log is the only write path in the system. Each commit object
holds an ordered batch of events; an event's global sequence number is its
position: SequenceNumber(commit_number, index).
"""

from dataclasses import dataclass, field
from typing import Any, Iterator

import msgpack

from cairndb.core.types import SequenceNumber, EventType, Timestamp, SchemaVersion


@dataclass(frozen=True)
class Event:
    """
    A single immutable event.

    Events carry no sequence number of their own — their global order is
    given by their position in a committed Commit object.
    """

    event_type: EventType
    timestamp: Timestamp
    payload: dict[str, Any]
    schema_version: SchemaVersion
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict suitable for msgpack serialization."""
        return {
            "event_type": self.event_type,
            "timestamp": self.timestamp.to_iso(),
            "payload": self.payload,
            "schema_version": self.schema_version,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        """Reconstruct typed fields from serialized representation."""
        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=Timestamp.from_iso(data["timestamp"]),
            payload=data["payload"],
            schema_version=SchemaVersion(data["schema_version"]),
            metadata=data.get("metadata", {}),
        )


@dataclass(frozen=True)
class SequencedEvent:
    """An event paired with its global sequence number.

    This is what event handlers receive. Event fields are exposed directly
    (entry.payload, entry.event_type, ...) for handler ergonomics.
    """

    sequence: SequenceNumber
    event: Event

    @property
    def event_type(self) -> EventType:
        return self.event.event_type

    @property
    def timestamp(self) -> Timestamp:
        return self.event.timestamp

    @property
    def payload(self) -> dict[str, Any]:
        return self.event.payload

    @property
    def schema_version(self) -> SchemaVersion:
        return self.event.schema_version

    @property
    def metadata(self) -> dict[str, Any]:
        return self.event.metadata


@dataclass(frozen=True)
class Commit:
    """
    An immutable commit object: an ordered batch of events.

    Stored as log/{number:012d}.msgpack. The commit number is dense —
    commit N exists only if a writer won the put-if-absent race for it,
    so the log never has gaps.
    """

    number: int
    created_at: Timestamp
    events: tuple[Event, ...]

    def __post_init__(self) -> None:
        if self.number < 1:
            raise ValueError(f"commit number must be >= 1, got {self.number}")
        if not self.events:
            raise ValueError("a commit must contain at least one event")

    def sequences(self) -> list[SequenceNumber]:
        """Sequence numbers of this commit's events, in order."""
        return [SequenceNumber(self.number, i) for i in range(len(self.events))]

    def sequenced_events(self) -> Iterator[SequencedEvent]:
        """Yield each event paired with its global sequence number."""
        for i, event in enumerate(self.events):
            yield SequencedEvent(SequenceNumber(self.number, i), event)

    @property
    def last_sequence(self) -> SequenceNumber:
        """Sequence number of the last event in this commit."""
        return SequenceNumber(self.number, len(self.events) - 1)

    def to_msgpack(self) -> bytes:
        """Serialize to MessagePack binary format."""
        data = {
            "number": self.number,
            "created_at": self.created_at.to_iso(),
            "events": [e.to_dict() for e in self.events],
        }
        return msgpack.packb(data, use_bin_type=True)

    @classmethod
    def from_msgpack(cls, data: bytes) -> "Commit":
        """Deserialize from MessagePack binary format."""
        unpacked = msgpack.unpackb(data, raw=False)
        return cls(
            number=unpacked["number"],
            created_at=Timestamp.from_iso(unpacked["created_at"]),
            events=tuple(Event.from_dict(e) for e in unpacked["events"]),
        )
