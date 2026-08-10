"""Filesystem utilities for copy-on-write and atomic operations."""

import os
import shutil
import subprocess
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)


def copy_database(src: str | Path, dst: str | Path, use_reflink: bool = True) -> None:
    """
    Copy a database file, using copy-on-write (reflink) when available.

    On filesystems that support reflinks (btrfs, XFS, ZFS), this creates
    an instant copy that shares blocks with the source. This is much faster
    than a full copy and uses no additional disk space initially.

    Falls back to regular copy if reflink is not available.

    Args:
        src: Source database path
        dst: Destination database path
        use_reflink: Whether to attempt reflink copy

    Raises:
        FileNotFoundError: If source file doesn't exist
        PermissionError: If insufficient permissions
        OSError: If copy operation fails
    """
    src_path = Path(src)
    dst_path = Path(dst)

    if not src_path.exists():
        raise FileNotFoundError(f"Source database not found: {src}")

    # Try copy-on-write first (if enabled)
    if use_reflink:
        try:
            # Use cp with --reflink=auto
            # This will use reflink if available, otherwise falls back to regular copy
            result = subprocess.run(
                ["cp", "--reflink=auto", str(src_path), str(dst_path)],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode == 0:
                logger.info(
                    "database_copied_with_reflink",
                    src=str(src_path),
                    dst=str(dst_path),
                    size_bytes=src_path.stat().st_size,
                )
                return

            # If cp failed, fall through to regular copy
            logger.debug(
                "reflink_copy_failed_falling_back",
                error=result.stderr,
            )

        except FileNotFoundError:
            # cp command not available (Windows?)
            logger.debug("cp_command_not_available")

    # Fall back to regular copy
    shutil.copy2(src_path, dst_path)

    logger.info(
        "database_copied_regular",
        src=str(src_path),
        dst=str(dst_path),
        size_bytes=src_path.stat().st_size,
    )


def atomic_swap(src: str | Path, dst: str | Path) -> None:
    """
    Atomically swap a file by renaming.

    On POSIX systems, os.rename() is atomic. This ensures that readers
    either see the old file or the new file, never a partial update.

    Args:
        src: Source file (must exist)
        dst: Destination file (will be replaced)

    Raises:
        FileNotFoundError: If source file doesn't exist
        OSError: If rename operation fails
    """
    src_path = Path(src)
    dst_path = Path(dst)

    if not src_path.exists():
        raise FileNotFoundError(f"Source file not found: {src}")

    # Atomic rename
    os.rename(src_path, dst_path)

    logger.info(
        "file_swapped_atomically",
        src=str(src_path),
        dst=str(dst_path),
    )


def get_file_mtime(path: str | Path) -> float:
    """
    Get the modification time of a file.

    Args:
        path: File path

    Returns:
        Modification time (timestamp)

    Raises:
        FileNotFoundError: If file doesn't exist
    """
    return os.stat(path).st_mtime


def ensure_directory(path: str | Path) -> None:
    """
    Ensure a directory exists, creating it if necessary.

    Args:
        path: Directory path
    """
    Path(path).mkdir(parents=True, exist_ok=True)
