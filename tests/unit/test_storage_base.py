"""Tests for storage key naming helpers."""

from cairndb.storage.base import (
    commit_key,
    parse_commit_key,
    parse_snapshot_key,
    snapshot_key,
    snapshot_prefix,
)


class TestKeyNaming:
    def test_commit_key_is_zero_padded(self):
        assert commit_key(42) == "log/000000000042.msgpack"

    def test_commit_keys_sort_lexicographically(self):
        numbers = [1, 9, 10, 99, 100, 42_000_000]
        keys = [commit_key(n) for n in numbers]
        assert sorted(keys) == keys

    def test_parse_commit_key_round_trip(self):
        assert parse_commit_key(commit_key(42)) == 42
        # Also works on fully-qualified keys with a bucket prefix
        assert parse_commit_key("some/prefix/" + commit_key(7)) == 7

    def test_parse_commit_key_rejects_non_commit(self):
        assert parse_commit_key("log/other.txt") is None
        assert parse_commit_key("log/42.msgpack") is None  # not zero-padded

    def test_snapshot_key_includes_schema_version(self):
        assert snapshot_key("1.0.0", 40) == "snapshots/v1.0.0/000000000040.sqlite"
        assert snapshot_prefix("1.0.0") == "snapshots/v1.0.0/"

    def test_parse_snapshot_key_round_trip(self):
        assert parse_snapshot_key(snapshot_key("1.0.0", 40)) == 40
        assert parse_snapshot_key("prefix/" + snapshot_key("2", 3)) == 3

    def test_parse_snapshot_key_rejects_non_snapshot(self):
        assert parse_snapshot_key("snapshots/v1/other.txt") is None
