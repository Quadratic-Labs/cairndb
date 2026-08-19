"""Smoke test for the standalone demo (`python -m cairndb.client.run`).

The demo is user-facing documentation; this guards that it starts from
env config, commits ping events, projects them, and stops on SIGINT.
Its modules are excluded from mutation testing (see [tool.mutmut]) —
this is a regression test, not a mutant-killer.
"""

import asyncio
import os
import signal
import sqlite3

from cairndb.client.run import run_demo
from cairndb.storage.filesystem import FilesystemStorage


async def test_demo_ticks_and_stops_on_sigint(temp_dir, monkeypatch):
    monkeypatch.setenv("CAIRNDB_STORAGE_TYPE", "filesystem")
    monkeypatch.setenv("CAIRNDB_STORAGE_PATH", str(temp_dir / "data"))
    monkeypatch.setenv("CAIRNDB_DB_PATH", str(temp_dir / "demo.db"))
    monkeypatch.setenv("CAIRNDB_POLL_INTERVAL", "0.05")

    task = asyncio.ensure_future(run_demo())
    try:
        await asyncio.sleep(0.4)  # at least one tick and one poll
    finally:
        os.kill(os.getpid(), signal.SIGINT)
    await asyncio.wait_for(task, timeout=10)

    # The writer committed at least one ping...
    storage = FilesystemStorage(temp_dir / "data")
    assert await storage.get_commit(1) is not None

    # ...and the reader projected it into the demo table.
    with sqlite3.connect(temp_dir / "demo.db") as conn:
        count = conn.execute("SELECT COUNT(*) FROM demo_events").fetchone()[0]
    assert count >= 1
