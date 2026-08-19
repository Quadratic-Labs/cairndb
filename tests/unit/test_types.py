"""Tests for core type definitions."""

from datetime import UTC, datetime

import pytest

from cairndb.core.types import SequenceNumber, Timestamp


class TestSequenceNumber:
    """Tests for SequenceNumber type."""

    def test_ordering_by_commit_then_index(self):
        """Sequence numbers order by commit first, then index."""
        assert SequenceNumber(1, 0) < SequenceNumber(1, 1)
        assert SequenceNumber(1, 999999) < SequenceNumber(2, 0)
        assert SequenceNumber(41, 5) < SequenceNumber(42, 0)
        assert SequenceNumber(42, 0) == SequenceNumber(42, 0)

    def test_string_conversion_round_trip(self):
        """Canonical string form parses back to an equal value."""
        seq1 = SequenceNumber(42, 3)
        seq2 = SequenceNumber.from_string(str(seq1))

        assert seq1 == seq2

    def test_string_form_is_zero_padded(self):
        """String form is fixed-width so text sort equals numeric sort."""
        assert str(SequenceNumber(42, 3)) == "000000000042:000003"

        sequences = [SequenceNumber(100, 0), SequenceNumber(2, 5), SequenceNumber(2, 50)]
        by_value = sorted(sequences)
        by_string = sorted(sequences, key=str)
        assert by_value == by_string

    def test_from_string_rejects_invalid(self):
        """Malformed strings are rejected."""
        for bad in ["", "42:3", "abc", "000000000042", "000000000042:03"]:
            with pytest.raises(ValueError):
                SequenceNumber.from_string(bad)

    def test_rejects_invalid_values(self):
        """Commit numbers start at 1; indices at 0."""
        with pytest.raises(ValueError):
            SequenceNumber(0, 0)
        with pytest.raises(ValueError):
            SequenceNumber(1, -1)

    def test_repr(self):
        """Developer representation includes both components."""
        seq = SequenceNumber(42, 3)
        assert "SequenceNumber" in repr(seq)
        assert "42" in repr(seq)
        assert "3" in repr(seq)

    def test_immutable(self):
        """Sequence numbers are frozen."""
        seq = SequenceNumber(1, 0)
        with pytest.raises(AttributeError):
            seq.commit = 2  # type: ignore


class TestTimestamp:
    """Tests for Timestamp type."""

    def test_now(self):
        """Test creating current timestamp."""
        ts = Timestamp.now()

        # Should be recent (within last few seconds)
        now = datetime.now(UTC)
        delta = now - ts.value
        assert delta.total_seconds() < 1

    def test_from_datetime_with_timezone(self):
        """Test creating from timezone-aware datetime."""
        dt = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
        ts = Timestamp.from_datetime(dt)

        assert ts.value == dt

    def test_from_datetime_naive_assumes_utc(self):
        """Test that naive datetimes are treated as UTC."""
        dt = datetime(2024, 1, 15, 12, 30, 45)  # noqa: DTZ001 — naive on purpose
        ts = Timestamp.from_datetime(dt)

        assert ts.value.tzinfo == UTC
        assert ts.value.year == 2024
        assert ts.value.month == 1
        assert ts.value.day == 15

    def test_iso_format_round_trip(self):
        """Test ISO format serialization and parsing."""
        ts1 = Timestamp.now()
        iso_str = ts1.to_iso()

        ts2 = Timestamp.from_iso(iso_str)

        # Should be equal (within microsecond precision)
        assert abs((ts1.value - ts2.value).total_seconds()) < 0.000001

    def test_string_representation(self):
        """Test string representation is ISO format."""
        ts = Timestamp.now()
        str_repr = str(ts)

        # Should be parseable as ISO format
        parsed = Timestamp.from_iso(str_repr)
        assert abs((ts.value - parsed.value).total_seconds()) < 0.000001

    def test_iso_form_is_canonical(self):
        """Same instant, any input form → one fixed-width 'Z' string."""
        forms = [
            "2024-01-15T12:00:00Z",
            "2024-01-15T12:00:00+00:00",
            "2024-01-15T13:00:00+01:00",
            "2024-01-15T12:00:00",  # naive, assumed UTC
        ]
        outputs = {Timestamp.from_iso(f).to_iso() for f in forms}
        assert outputs == {"2024-01-15T12:00:00.000000Z"}
        assert len(Timestamp.now().to_iso()) == len("2024-01-15T12:00:00.000000Z")

    def test_timestamp_immutable(self):
        """Test that timestamps are immutable."""
        ts = Timestamp.now()

        with pytest.raises(AttributeError):
            ts.value = datetime.now(UTC)  # type: ignore
