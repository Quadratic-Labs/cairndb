"""Core type definitions for CairnDB."""

import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import NewType, overload
from uuid import UUID

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
        if not isinstance(self.value, datetime):
            raise TypeError(
                f"Timestamp value must be a datetime, got {type(self.value).__name__}"
            )
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

    @classmethod
    def from_uuid7(cls, u: UUID) -> Timestamp:
        """Extract the embedded millisecond timestamp from a UUIDv7."""
        if u.version != 7:
            raise ValueError(f"Not a UUIDv7: {u}")
        # Top 48 bits = Unix timestamp in milliseconds
        ts_ms = (int(u) >> 80) & 0xFFFF_FFFF_FFFF
        return cls(datetime.fromtimestamp(ts_ms / 1000, tz=UTC))

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

    def to_uuid7(self) -> UUID:
        """Encode as a standard UUIDv7 (RFC 9562, ascending lexicographic order).

        The 80 non-timestamp bits are random, so each call yields a distinct
        UUID; only the millisecond timestamp round-trips via ``from_uuid7``.

        Layout (128 bits):
            127..80  unix_ts_ms  48-bit millisecond timestamp
             79..76  0x7         version
             75..64  rand_a      12 random bits
             63..62  0b10        RFC 4122 variant
             61..0   rand_b      62 random bits
        """
        ts_ms = int(self.value.timestamp() * 1000)
        rand_a = secrets.randbits(12)
        rand_b = secrets.randbits(62)
        return UUID(int=(
              (ts_ms  << 80)
            | (0x7    << 76)
            | (rand_a << 64)
            | (0b10   << 62)
            |  rand_b
        ))

    def __str__(self) -> str:
        """String representation in ISO format."""
        return self.to_iso()

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"Timestamp({self.to_iso()})"
