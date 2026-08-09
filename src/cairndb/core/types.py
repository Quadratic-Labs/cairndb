"""Core type definitions for CairnDB."""

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import NewType

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
    def from_string(cls, s: str) -> "SequenceNumber":
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


@dataclass(frozen=True)
class Timestamp:
    """
    Immutable timestamp wrapper with timezone awareness.

    All timestamps are stored in UTC. Timestamps are informational
    (effective/valid-from time); ordering never depends on them.
    """

    value: datetime

    @classmethod
    def now(cls) -> "Timestamp":
        """Get current UTC timestamp."""
        return cls(datetime.now(timezone.utc))

    @classmethod
    def from_datetime(cls, dt: datetime) -> "Timestamp":
        """Create from datetime, converting to UTC if needed."""
        if dt.tzinfo is None:
            # Assume UTC if no timezone specified
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            # Convert to UTC
            dt = dt.astimezone(timezone.utc)
        return cls(dt)

    @classmethod
    def from_iso(cls, iso_string: str) -> "Timestamp":
        """Parse from ISO 8601 format string."""
        dt = datetime.fromisoformat(iso_string)
        return cls.from_datetime(dt)

    def to_iso(self) -> str:
        """Convert to ISO 8601 format string."""
        return self.value.isoformat()

    def __str__(self) -> str:
        """String representation in ISO format."""
        return self.to_iso()

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"Timestamp({self.to_iso()})"
