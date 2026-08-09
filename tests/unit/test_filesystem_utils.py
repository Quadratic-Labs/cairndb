"""Tests for filesystem utilities."""

import pytest
import tempfile
from pathlib import Path
import os

from cairndb.utils.filesystem import copy_database, atomic_swap, get_file_mtime, ensure_directory


class TestCopyDatabase:
    """Tests for copy_database function."""

    def test_copy_database_basic(self):
        """Test basic database copy."""
        with tempfile.TemporaryDirectory() as temp_dir:
            src = Path(temp_dir) / "source.db"
            dst = Path(temp_dir) / "dest.db"

            # Create source file
            src.write_text("test database content")

            # Copy
            copy_database(src, dst, use_reflink=False)

            # Verify
            assert dst.exists()
            assert dst.read_text() == "test database content"

    def test_copy_database_source_not_found(self):
        """Test that copying non-existent source raises error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            src = Path(temp_dir) / "nonexistent.db"
            dst = Path(temp_dir) / "dest.db"

            with pytest.raises(FileNotFoundError):
                copy_database(src, dst)

    def test_copy_database_with_reflink(self):
        """Test copy with reflink attempt (may fall back)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            src = Path(temp_dir) / "source.db"
            dst = Path(temp_dir) / "dest.db"

            # Create source file
            src.write_bytes(b"binary database content" * 1000)

            # Copy with reflink (will fall back if not supported)
            copy_database(src, dst, use_reflink=True)

            # Verify
            assert dst.exists()
            assert dst.read_bytes() == src.read_bytes()

    def test_copy_database_preserves_size(self):
        """Test that copy preserves file size."""
        with tempfile.TemporaryDirectory() as temp_dir:
            src = Path(temp_dir) / "source.db"
            dst = Path(temp_dir) / "dest.db"

            # Create source file
            content = b"x" * 10000
            src.write_bytes(content)

            # Copy
            copy_database(src, dst)

            # Verify size
            assert src.stat().st_size == dst.stat().st_size


class TestAtomicSwap:
    """Tests for atomic_swap function."""

    def test_atomic_swap_basic(self):
        """Test basic atomic swap."""
        with tempfile.TemporaryDirectory() as temp_dir:
            src = Path(temp_dir) / "new.db"
            dst = Path(temp_dir) / "current.db"

            # Create files
            src.write_text("new content")
            dst.write_text("old content")

            # Swap
            atomic_swap(src, dst)

            # Verify
            assert not src.exists()
            assert dst.exists()
            assert dst.read_text() == "new content"

    def test_atomic_swap_destination_not_exist(self):
        """Test swap when destination doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            src = Path(temp_dir) / "new.db"
            dst = Path(temp_dir) / "current.db"

            # Create only source
            src.write_text("new content")

            # Swap (should create dst)
            atomic_swap(src, dst)

            # Verify
            assert not src.exists()
            assert dst.exists()
            assert dst.read_text() == "new content"

    def test_atomic_swap_source_not_found(self):
        """Test that swapping non-existent source raises error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            src = Path(temp_dir) / "nonexistent.db"
            dst = Path(temp_dir) / "current.db"

            with pytest.raises(FileNotFoundError):
                atomic_swap(src, dst)


class TestGetFileMtime:
    """Tests for get_file_mtime function."""

    def test_get_file_mtime(self):
        """Test getting file modification time."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test.db"
            file_path.write_text("content")

            mtime = get_file_mtime(file_path)

            assert isinstance(mtime, float)
            assert mtime > 0

    def test_get_file_mtime_not_found(self):
        """Test that getting mtime of non-existent file raises error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "nonexistent.db"

            with pytest.raises(FileNotFoundError):
                get_file_mtime(file_path)


class TestEnsureDirectory:
    """Tests for ensure_directory function."""

    def test_ensure_directory_creates(self):
        """Test that directory is created."""
        with tempfile.TemporaryDirectory() as temp_dir:
            new_dir = Path(temp_dir) / "new" / "nested" / "dir"

            assert not new_dir.exists()

            ensure_directory(new_dir)

            assert new_dir.exists()
            assert new_dir.is_dir()

    def test_ensure_directory_idempotent(self):
        """Test that calling twice is safe."""
        with tempfile.TemporaryDirectory() as temp_dir:
            new_dir = Path(temp_dir) / "new_dir"

            ensure_directory(new_dir)
            ensure_directory(new_dir)  # Should not raise

            assert new_dir.exists()


class TestIntegration:
    """Integration tests for filesystem utilities."""

    def test_copy_and_swap_workflow(self):
        """Test typical copy-modify-swap workflow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            current = Path(temp_dir) / "projection.db"
            new = Path(temp_dir) / "projection_new.db"

            # Initial state
            current.write_text("version 1")

            # Copy
            copy_database(current, new)

            # Modify copy
            new.write_text("version 2")

            # Swap
            atomic_swap(new, current)

            # Verify
            assert not new.exists()
            assert current.read_text() == "version 2"

    def test_multiple_swaps(self):
        """Test multiple swap operations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            current = Path(temp_dir) / "db.db"
            new = Path(temp_dir) / "db_new.db"

            current.write_text("v1")

            for i in range(2, 6):
                # Copy
                copy_database(current, new)

                # Update
                new.write_text(f"v{i}")

                # Swap
                atomic_swap(new, current)

                # Verify
                assert current.read_text() == f"v{i}"
                assert not new.exists()
