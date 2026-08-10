"""Concurrency tests around atomic projection swaps.

Verifies that:
- A session opened before a swap keeps a consistent (old) view.
- The next session after a swap sees the new, complete state.
- Concurrent readers during updates never observe partial state.
"""

import asyncio

from sqlalchemy import text

from cairndb.client.connection import CairnDBClient
from cairndb.client.projector import Projector
from tests.conftest import (
    commit_events,
    init_users_projection,
    query_users,
    user_created,
)


async def test_next_session_after_swap_sees_new_state(
    storage, user_registry, client_config
):
    """CairnDBClient reconnects (mtime change) and serves the new projection."""
    await init_users_projection(client_config.db_path)
    projector = Projector(client_config, storage, user_registry)
    client = CairnDBClient(client_config, user_registry)

    await commit_events(storage, user_created(1, "Alice"))
    await projector.apply_updates()

    async with client.get_session() as session:
        result = await session.execute(text("SELECT COUNT(*) FROM users"))
        assert result.scalar() == 1

    # New commit + atomic swap while the client engine is idle
    await asyncio.sleep(0.02)  # distinct mtime
    await commit_events(storage, user_created(2, "Bob"))
    await projector.apply_updates()

    async with client.get_session() as session:
        result = await session.execute(text("SELECT COUNT(*) FROM users"))
        assert result.scalar() == 2

    await client.stop()


async def test_concurrent_readers_never_see_partial_state(
    storage, user_registry, client_config
):
    """Readers racing an update always see a commit-consistent row count.

    Each commit inserts a *pair* of users, so any reader observing an odd
    count has seen a partially applied commit.
    """
    await init_users_projection(client_config.db_path)
    projector = Projector(client_config, storage, user_registry)
    client = CairnDBClient(client_config, user_registry)

    async def reader() -> None:
        for _ in range(30):
            try:
                async with client.get_session() as session:
                    result = await session.execute(text("SELECT COUNT(*) FROM users"))
                    count = result.scalar()
                    assert count % 2 == 0, f"partial commit visible: {count} users"
            except FileNotFoundError:
                pass  # projection not created yet
            await asyncio.sleep(0.001)

    async def writer() -> None:
        for i in range(10):
            await commit_events(
                storage,
                user_created(2 * i + 1, f"user-{2 * i + 1}"),
                user_created(2 * i + 2, f"user-{2 * i + 2}"),
            )
            await projector.apply_updates()
            await asyncio.sleep(0.002)

    await asyncio.gather(writer(), *(reader() for _ in range(3)))

    assert len(await query_users(client_config.db_path)) == 20
    await client.stop()


async def test_sequential_updates_each_consistent(
    storage, user_registry, client_config
):
    """Every intermediate projection state is a valid commit boundary."""
    await init_users_projection(client_config.db_path)
    projector = Projector(client_config, storage, user_registry)

    observed = []
    for i in range(1, 6):
        await commit_events(storage, user_created(i, f"user-{i}"))
        await projector.apply_updates()
        observed.append(len(await query_users(client_config.db_path)))

    assert observed == [1, 2, 3, 4, 5]
