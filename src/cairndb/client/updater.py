"""Background updater: polls the log and applies new commits."""

import asyncio

import structlog

from cairndb.client.config import ClientConfig
from cairndb.client.projector import Projector
from cairndb.client.registry import HandlerRegistry
from cairndb.storage.base import BlobStorage

logger = structlog.get_logger(__name__)


class BackgroundUpdater:
    """
    Background service keeping the projection eventually consistent.

    Polls the log tail every poll_interval_seconds (a single GET when idle)
    and applies new commits via the projector's atomic-swap update. Polling
    is the correctness mechanism; there is no push channel to miss.
    """

    def __init__(
        self,
        config: ClientConfig,
        storage: BlobStorage,
        registry: HandlerRegistry,
        init_schema=None,
    ):
        """
        Initialize the background updater.

        Args:
            config: Client configuration
            storage: Blob storage backend
            registry: Event handler registry
            init_schema: Optional async callback creating application tables
                on a fresh projection (see Projector)
        """
        self.config = config
        self.projector = Projector(config, storage, registry, init_schema=init_schema)
        self._task: asyncio.Task | None = None  # pragma: no mutate (start() assigns first)
        self._running = False
        self._update_count = 0

        logger.info(
            "background_updater_initialized",
            poll_interval_seconds=config.poll_interval_seconds,
        )

    async def start(self) -> None:
        """Start the background update loop."""
        if self._running:
            logger.warning("updater_already_running")
            return

        self._running = True
        self._task = asyncio.create_task(self._update_loop())

        logger.info("background_updater_started")

    async def stop(self) -> None:
        """Stop the background update loop and wait for completion."""
        if not self._running:
            return

        self._running = False

        if self._task:
            self._task.cancel()

            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("background_updater_stopped", updates_performed=self._update_count)

    async def _update_loop(self) -> None:
        """Poll and apply until stopped. Errors are logged, never fatal."""
        logger.info("update_loop_started")

        while self._running:
            try:
                updated, new_sequence = await self.projector.apply_updates()
                if updated:
                    self._update_count += 1
                    logger.info(
                        "update_applied",
                        new_sequence=str(new_sequence) if new_sequence else None,
                        total_updates=self._update_count,
                    )

                await asyncio.sleep(self.config.poll_interval_seconds)

            except asyncio.CancelledError:
                break

            except Exception as e:  # noqa: BLE001 — resilience loop: log and retry
                logger.error(
                    "update_loop_error",
                    error=str(e),
                    retrying_in=self.config.poll_interval_seconds,
                )
                await asyncio.sleep(self.config.poll_interval_seconds)

        logger.info("update_loop_stopped")

    async def trigger_update(self) -> tuple[bool, str | None]:
        """
        Apply updates immediately, outside the normal poll interval.

        Useful for tests and read-your-writes flows.

        Returns:
            Tuple of (updated, new_sequence_str)
        """
        logger.info("manual_update_triggered")

        updated, new_sequence = await self.projector.apply_updates()

        return updated, str(new_sequence) if new_sequence else None

    @property
    def is_running(self) -> bool:
        """Check if the updater is currently running."""
        return self._running

    @property
    def update_count(self) -> int:
        """Get the total number of updates performed."""
        return self._update_count

    async def wait_for_sequence(
        self, target_sequence: str, timeout: float = 30.0
    ) -> bool:
        """
        Wait until the projection reaches a target sequence.

        Read-your-writes flow:
        1. `seq = await committer.append(event)`
        2. `await updater.wait_for_sequence(str(seq))`
        3. Query the projection

        Args:
            target_sequence: Sequence string as returned by the committer
            timeout: Maximum time to wait in seconds

        Returns:
            True if sequence reached, False if timeout

        Raises:
            ValueError: If invalid sequence format
        """
        from cairndb.core.types import SequenceNumber

        target = SequenceNumber.from_string(target_sequence)

        logger.info(
            "waiting_for_sequence",
            target_sequence=target_sequence,
            timeout=timeout,
        )

        start_time = asyncio.get_event_loop().time()

        while True:
            current = await self.projector.get_current_sequence()

            if current and current >= target:
                logger.info(
                    "sequence_reached",
                    target_sequence=target_sequence,
                    current_sequence=str(current),
                )
                return True

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                logger.warning(
                    "sequence_wait_timeout",
                    target_sequence=target_sequence,
                    current_sequence=str(current) if current else None,
                    timeout=timeout,
                )
                return False

            await self.trigger_update()
            await asyncio.sleep(0.1)
