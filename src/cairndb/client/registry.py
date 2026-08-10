"""Event handler registration system."""

from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from cairndb.core.exceptions import HandlerNotFoundError
from cairndb.core.log import SequencedEvent

logger = structlog.get_logger(__name__)

# Type alias for handler functions
EventHandler = Callable[[Any, SequencedEvent], Awaitable[None]]


class HandlerRegistry:
    """
    Registry for event handlers.

    Applications register handlers for specific event types. Handlers are
    async functions that take a database connection and a SequencedEvent
    (event fields plus its global sequence number).

    Example:
        registry = HandlerRegistry()

        @registry.handler("user.created")
        async def handle_user_created(db, entry):
            await db.execute(
                "INSERT INTO users (id, name) VALUES (?, ?)",
                (entry.payload["id"], entry.payload["name"])
            )
    """

    def __init__(self):
        """Initialize the handler registry."""
        self._handlers: dict[str, EventHandler] = {}
        logger.info("handler_registry_initialized")

    def handler(self, event_type: str) -> Callable[[EventHandler], EventHandler]:
        """
        Decorator to register an event handler.

        Args:
            event_type: The event type to handle (e.g., "user.created")

        Returns:
            Decorator function

        Example:
            @registry.handler("user.created")
            async def handle_user_created(db, entry):
                ...
        """

        def decorator(func: EventHandler) -> EventHandler:
            self.register(event_type, func)
            return func

        return decorator

    def register(self, event_type: str, handler: EventHandler) -> None:
        """
        Register an event handler.

        Args:
            event_type: The event type to handle
            handler: Async function that processes the event

        Raises:
            ValueError: If handler is already registered for this event type
        """
        if event_type in self._handlers:
            raise ValueError(f"Handler already registered for event type: {event_type}")

        self._handlers[event_type] = handler

        logger.info(
            "handler_registered",
            event_type=event_type,
            handler_name=handler.__name__,
        )

    def get_handler(self, event_type: str) -> EventHandler:
        """
        Get the handler for an event type.

        Args:
            event_type: The event type

        Returns:
            The registered handler function

        Raises:
            HandlerNotFoundError: If no handler is registered
        """
        if event_type not in self._handlers:
            raise HandlerNotFoundError(f"No handler registered for event type: {event_type}")

        return self._handlers[event_type]

    def has_handler(self, event_type: str) -> bool:
        """
        Check if a handler is registered for an event type.

        Args:
            event_type: The event type to check

        Returns:
            True if handler exists, False otherwise
        """
        return event_type in self._handlers

    def unregister(self, event_type: str) -> None:
        """
        Unregister a handler.

        Args:
            event_type: The event type to unregister

        Raises:
            HandlerNotFoundError: If no handler is registered
        """
        if event_type not in self._handlers:
            raise HandlerNotFoundError(f"No handler registered for event type: {event_type}")

        del self._handlers[event_type]

        logger.info("handler_unregistered", event_type=event_type)

    def list_handlers(self) -> list[str]:
        """
        List all registered event types.

        Returns:
            List of event types with handlers
        """
        return list(self._handlers.keys())

    def clear(self) -> None:
        """Clear all registered handlers."""
        self._handlers.clear()
        logger.info("handlers_cleared")

    def __len__(self) -> int:
        """Get number of registered handlers."""
        return len(self._handlers)

    def __contains__(self, event_type: str) -> bool:
        """Check if event type has a handler (supports 'in' operator)."""
        return event_type in self._handlers
