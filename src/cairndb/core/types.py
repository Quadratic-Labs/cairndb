"""Core type definitions for CairnDB."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import NewType, overload

_SEQUENCE_RE = re.compile(r"^(\d{12}):(\d{6})$")


@dataclass(frozen=True, order=True)
class SequenceNumber:
    """
    Global sequence number: position of an event in the commit log.

    An event is totally ordered by (commit, index):
    - commit: the dense, monotonically increasing commit object number,
      assigned by the bucket via put-if-absent
    - index: the event's position within its commit

    Ordering never depends on clocks or process identity.
    """

    commit: int
    index: int

    def __post_init__(self) -> None:
        if self.commit < 1:
            raise ValueError(f"commit must be >= 1, got {self.commit}")
        if self.index < 0:
            raise ValueError(f"index must be >= 0, got {self.index}")

    @classmethod
    def from_string(cls, s: str) -> SequenceNumber:
        """Parse from the canonical '{commit:012d}:{index:06d}' form."""
        match = _SEQUENCE_RE.match(s)
        if not match:
            raise ValueError(f"Invalid sequence number string: {s!r}")
        return cls(commit=int(match.group(1)), index=int(match.group(2)))

    def __str__(self) -> str:
        """Canonical zero-padded form; sorts lexicographically as it sorts numerically."""
        return f"{self.commit:012d}:{self.index:06d}"

    def __repr__(self) -> str:
        return f"SequenceNumber(commit={self.commit}, index={self.index})"


# Type aliases for semantic clarity
EventType = NewType("EventType", str)
SchemaVersion = NewType("SchemaVersion", str)


@dataclass(frozen=True, order=True)
class Timestamp:
    """
    Immutable UTC timestamp value type.

    Every construction path normalizes to UTC: naive datetimes are assumed
    UTC, aware ones are converted. Instances order among themselves,
    subtract to a ``timedelta``, and shift by a ``timedelta`` — so deadline
    arithmetic never has to unwrap ``value``.

    Timestamps are informational (effective/valid-from time); record
    ordering in the log never depends on them — use identifiers for that.
    """

    value: datetime

    def __post_init__(self) -> None:
        if self.value.tzinfo is None:
            object.__setattr__(self, "value", self.value.replace(tzinfo=UTC))
        else:
            object.__setattr__(self, "value", self.value.astimezone(UTC))

    @classmethod
    def now(cls) -> Timestamp:
        """Get current UTC timestamp."""
        return cls(datetime.now(UTC))

    @classmethod
    def from_datetime(cls, dt: datetime) -> Timestamp:
        """Create from datetime; the constructor normalizes to UTC."""
        return cls(dt)

    @classmethod
    def from_iso(cls, iso_string: str) -> Timestamp:
        """Parse from ISO 8601 format string."""
        return cls(datetime.fromisoformat(iso_string))

    def __add__(self, other: object) -> Timestamp:
        if not isinstance(other, timedelta):
            return NotImplemented
        return Timestamp(self.value + other)

    __radd__ = __add__

    @overload
    def __sub__(self, other: timedelta) -> Timestamp: ...
    @overload
    def __sub__(self, other: Timestamp) -> timedelta: ...

    def __sub__(self, other: object) -> Timestamp | timedelta:
        if isinstance(other, timedelta):
            return Timestamp(self.value - other)
        if isinstance(other, Timestamp):
            return self.value - other.value
        return NotImplemented

    def to_iso(self) -> str:
        """Canonical RFC 3339 form: fixed-width microseconds, 'Z' suffix.

        Fixed-width so the form sorts lexicographically as it sorts
        chronologically. Parsing (`from_iso`) stays lenient: any ISO 8601
        offset, 'Z', or a naive string (assumed UTC) is accepted.
        """
        return self.value.isoformat(timespec="microseconds").replace("+00:00", "Z")

    def __str__(self) -> str:
        """String representation in ISO format."""
        return self.to_iso()

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"Timestamp({self.to_iso()})"
