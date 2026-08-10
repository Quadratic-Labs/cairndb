"""The commit protocol: CairnDB's write path.

The Committer turns `append(event)` calls into conditional puts of commit
objects. The bucket arbitrates concurrency: whoever wins the put-if-absent
for log/{N+1} owns commit N+1; losers advance past the winner's commits and
retry. An append() only returns once its commit object is durably stored —
there is no window in which an acknowledged write can be lost.

Group commit: events appended while a put is in flight accumulate and are
written together in the next commit, amortizing storage round-trips without
weakening the durability guarantee.
"""

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Self

import structlog
from pydantic import BaseModel, Field

from cairndb.core.exceptions import CommitError, EventRejectedError
from cairndb.core.log import Commit, Event
from cairndb.core.types import SequenceNumber, Timestamp
from cairndb.storage.base import BlobStorage

logger = structlog.get_logger(__name__)

# Called after a lost race with (pending events, commits that interleaved).
# Returns a list of the same length: element i is the event to commit in
# place of pending[i] (possibly modified), or None to reject it. Rejected
# events fail their append() with EventRejectedError.
RevalidateHook = Callable[
    [list[Event], list[Commit]], Awaitable[list[Event | None]]
]


class CommitterConfig(BaseModel):
    """Committer configuration."""

    max_events_per_commit: int = Field(
        default=1000,
        ge=1,
        le=100_000,
        description="Maximum number of events batched into one commit object",
    )

    max_batch_wait_seconds: float = Field(
        default=0.0,
        ge=0.0,
        le=10.0,
        description=(
            "Extra time to wait for more events before committing a batch. "
            "0 commits immediately; group commit still batches whatever "
            "arrives while a put is in flight."
        ),
    )

    max_commit_attempts: int = Field(
        default=20,
        ge=1,
        description="Attempts per batch before failing appends with CommitError",
    )

    lost_race_backoff_seconds: float = Field(
        default=0.05,
        ge=0.0,
        le=5.0,
        description=(
            "Backoff before retrying when a put was rejected but the winning "
            "commit is not visible yet (concurrent-conditional-write conflict)"
        ),
    )

    tail_hint: int = Field(
        default=0,
        ge=0,
        description=(
            "Cold-start optimization: commits at or below this number are "
            "known to exist (e.g. from a snapshot), so tail discovery only "
            "lists commits after it"
        ),
    )


class _Pending:
    """An event waiting to be committed, with the future its caller awaits."""

    __slots__ = ("event", "future")

    def __init__(self, event: Event, future: asyncio.Future[SequenceNumber]):
        self.event = event
        self.future = future


class Committer:
    """
    Durable, concurrent-safe writer for the commit log.

    Usage::

        committer = Committer(storage)
        seq = await committer.append(event)   # durable once this returns
        ...
        await committer.close()

    Or as an async context manager::

        async with Committer(storage) as committer:
            await committer.append(event)

    Any number of Committer instances may write to the same log from any
    number of processes; the storage layer's put-if-absent serializes them.
    """

    def __init__(
        self,
        storage: BlobStorage,
        config: CommitterConfig | None = None,
        revalidate: RevalidateHook | None = None,
    ):
        """
        Initialize the committer.

        Args:
            storage: Blob storage backend
            config: Committer configuration (defaults are sensible)
            revalidate: Optional hook to re-check events against commits that
                interleaved after a lost race. Omit for pure-fact events.
        """
        self.storage = storage
        self.config = config or CommitterConfig()
        self._revalidate = revalidate

        self._pending: deque[_Pending] = deque()
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._closed = False
        self._tail: int | None = None  # highest commit number known to exist

        logger.info(
            "committer_initialized",
            max_events_per_commit=self.config.max_events_per_commit,
            max_batch_wait_seconds=self.config.max_batch_wait_seconds,
            revalidate=revalidate is not None,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def append(self, event: Event) -> SequenceNumber:
        """
        Append one event to the log.

        Returns once the event's commit object is durably stored.

        Returns:
            The event's global sequence number.

        Raises:
            CommitError: If the commit could not be won within
                max_commit_attempts, or the committer is closed.
            EventRejectedError: If the revalidate hook rejected the event.
            StorageError: If storage failed with a non-race error.
        """
        sequences = await self.append_many([event])
        return sequences[0]

    async def append_many(self, events: list[Event]) -> list[SequenceNumber]:
        """
        Append several events, preserving their relative order.

        Events are packed into as few commit objects as possible. Returns
        once every event is durable.
        """
        if self._closed:
            raise CommitError("Committer is closed")
        if not events:
            return []

        self._ensure_running()

        loop = asyncio.get_running_loop()
        futures: list[asyncio.Future[SequenceNumber]] = []
        for event in events:
            future = loop.create_future()
            self._pending.append(_Pending(event, future))
            futures.append(future)
        self._wake.set()

        results = await asyncio.gather(*futures, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result
        return results  # type: ignore[return-value]

    async def close(self) -> None:
        """Drain pending events (committing them), then stop."""
        if self._closed:
            return
        self._closed = True
        self._wake.set()

        if self._task:
            await self._task
            self._task = None

        logger.info("committer_closed")

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    @property
    def tail(self) -> int | None:
        """Highest commit number this committer knows to exist (None = unknown)."""
        return self._tail

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _ensure_running(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        """Flusher loop: drain pending events into commits, one put at a time."""
        while True:
            while not self._pending and not self._closed:
                await self._wake.wait()
                self._wake.clear()

            if not self._pending:
                return  # closed and drained

            if self.config.max_batch_wait_seconds > 0:
                await asyncio.sleep(self.config.max_batch_wait_seconds)

            batch = [
                self._pending.popleft()
                for _ in range(
                    min(len(self._pending), self.config.max_events_per_commit)
                )
            ]

            try:
                await self._commit_batch(batch)
            except Exception as e:  # noqa: BLE001 — defensive: never kill the flusher loop
                logger.error("commit_batch_unexpected_error", error=str(e))
                for pending in batch:
                    if not pending.future.done():
                        pending.future.set_exception(
                            CommitError(f"Unexpected commit failure: {e}")
                        )

    async def _commit_batch(self, batch: list[_Pending]) -> None:
        """Run the commit protocol for one batch until it wins, fails, or empties."""
        pendings = batch

        for attempt in range(1, self.config.max_commit_attempts + 1):
            number = await self._next_number()
            commit = Commit(
                number=number,
                created_at=Timestamp.now(),
                events=tuple(p.event for p in pendings),
            )

            try:
                won = await self.storage.put_commit(number, commit.to_msgpack())
            except Exception as e:  # noqa: BLE001 — transferred to the callers' futures
                for pending in pendings:
                    pending.future.set_exception(e)
                return

            if won:
                self._tail = number
                for index, pending in enumerate(pendings):
                    pending.future.set_result(SequenceNumber(number, index))
                logger.info(
                    "commit_won",
                    number=number,
                    event_count=len(pendings),
                    attempt=attempt,
                )
                return

            # Lost the race: advance past the winner's commits
            interleaved = await self._advance_tail(from_number=number)

            if not interleaved:
                # Put was rejected but the winning object is not visible yet
                # (e.g. S3 409 between concurrent conditional writers).
                await asyncio.sleep(self.config.lost_race_backoff_seconds)
                continue

            if self._revalidate:
                pendings = await self._apply_revalidation(pendings, interleaved)
                if not pendings:
                    return  # every event was rejected

        error = CommitError(
            f"Could not win a commit after {self.config.max_commit_attempts} attempts"
        )
        for pending in pendings:
            pending.future.set_exception(error)
        logger.error("commit_attempts_exhausted", attempts=self.config.max_commit_attempts)

    async def _next_number(self) -> int:
        """Next commit number to try; discovers the tail on first use."""
        if self._tail is None:
            commits = await self.storage.list_commits(after=self.config.tail_hint)
            self._tail = max(commits, default=self.config.tail_hint)
            logger.info("tail_discovered", tail=self._tail)
        return self._tail + 1

    async def _advance_tail(self, from_number: int) -> list[Commit]:
        """Fetch commits from `from_number` forward until a gap; update tail."""
        interleaved: list[Commit] = []
        number = from_number
        while (data := await self.storage.get_commit(number)) is not None:
            interleaved.append(Commit.from_msgpack(data))
            number += 1

        if interleaved:
            self._tail = number - 1
            logger.debug(
                "lost_race_advanced",
                from_number=from_number,
                new_tail=self._tail,
                interleaved=len(interleaved),
            )
        return interleaved

    async def _apply_revalidation(
        self, pendings: list[_Pending], interleaved: list[Commit]
    ) -> list[_Pending]:
        """Run the revalidate hook; fail rejected events, keep the rest."""
        events = [p.event for p in pendings]
        decisions = await self._revalidate(events, interleaved)  # type: ignore[misc]

        if len(decisions) != len(events):
            raise CommitError(
                f"revalidate hook returned {len(decisions)} decisions "
                f"for {len(events)} events"
            )

        kept: list[_Pending] = []
        for pending, decision in zip(pendings, decisions):
            if decision is None:
                pending.future.set_exception(
                    EventRejectedError(
                        f"Event {pending.event.event_type} rejected by revalidation"
                    )
                )
            else:
                pending.event = decision
                kept.append(pending)

        if len(kept) < len(pendings):
            logger.info(
                "events_rejected_by_revalidation",
                rejected=len(pendings) - len(kept),
                kept=len(kept),
            )
        return kept
