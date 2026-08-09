"""Tests for event handler registry."""

import pytest

from cairndb.core.types import SequenceNumber, EventType, Timestamp, SchemaVersion
from cairndb.core.log import Event, SequencedEvent
from cairndb.core.exceptions import HandlerNotFoundError
from cairndb.client.registry import HandlerRegistry


class TestHandlerRegistry:
    """Tests for HandlerRegistry."""

    def test_register_handler(self):
        """Test registering a handler function."""
        registry = HandlerRegistry()

        async def my_handler(db, entry):
            pass

        registry.register("test.event", my_handler)

        assert registry.has_handler("test.event")
        assert registry.get_handler("test.event") == my_handler

    def test_register_handler_decorator(self):
        """Test registering a handler using decorator."""
        registry = HandlerRegistry()

        @registry.handler("user.created")
        async def handle_user_created(db, entry):
            pass

        assert registry.has_handler("user.created")
        assert len(registry) == 1

    def test_register_duplicate_handler(self):
        """Test that registering duplicate handler raises error."""
        registry = HandlerRegistry()

        async def handler1(db, entry):
            pass

        async def handler2(db, entry):
            pass

        registry.register("test.event", handler1)

        with pytest.raises(ValueError, match="Handler already registered"):
            registry.register("test.event", handler2)

    def test_get_handler_not_found(self):
        """Test that getting non-existent handler raises error."""
        registry = HandlerRegistry()

        with pytest.raises(HandlerNotFoundError, match="No handler registered"):
            registry.get_handler("missing.event")

    def test_has_handler(self):
        """Test checking if handler exists."""
        registry = HandlerRegistry()

        assert not registry.has_handler("test.event")

        async def my_handler(db, entry):
            pass

        registry.register("test.event", my_handler)

        assert registry.has_handler("test.event")

    def test_unregister_handler(self):
        """Test unregistering a handler."""
        registry = HandlerRegistry()

        async def my_handler(db, entry):
            pass

        registry.register("test.event", my_handler)
        assert registry.has_handler("test.event")

        registry.unregister("test.event")
        assert not registry.has_handler("test.event")

    def test_unregister_not_found(self):
        """Test unregistering non-existent handler raises error."""
        registry = HandlerRegistry()

        with pytest.raises(HandlerNotFoundError):
            registry.unregister("missing.event")

    def test_list_handlers(self):
        """Test listing all registered handlers."""
        registry = HandlerRegistry()

        async def handler1(db, entry):
            pass

        async def handler2(db, entry):
            pass

        registry.register("event.1", handler1)
        registry.register("event.2", handler2)

        handlers = registry.list_handlers()

        assert len(handlers) == 2
        assert "event.1" in handlers
        assert "event.2" in handlers

    def test_clear_handlers(self):
        """Test clearing all handlers."""
        registry = HandlerRegistry()

        async def handler(db, entry):
            pass

        registry.register("event.1", handler)
        registry.register("event.2", handler)

        assert len(registry) == 2

        registry.clear()

        assert len(registry) == 0

    def test_len(self):
        """Test registry length."""
        registry = HandlerRegistry()

        assert len(registry) == 0

        async def handler(db, entry):
            pass

        registry.register("event.1", handler)
        assert len(registry) == 1

        registry.register("event.2", handler)
        assert len(registry) == 2

    def test_contains(self):
        """Test 'in' operator."""
        registry = HandlerRegistry()

        async def handler(db, entry):
            pass

        assert "test.event" not in registry

        registry.register("test.event", handler)

        assert "test.event" in registry

    @pytest.mark.asyncio
    async def test_handler_execution(self):
        """Test that registered handler can be executed."""
        registry = HandlerRegistry()

        executed = []

        @registry.handler("test.event")
        async def handle_test(db, entry):
            executed.append((db, entry.event_type))

        # Create a test entry
        entry = SequencedEvent(
            sequence=SequenceNumber(1, 0),
            event=Event(
                event_type=EventType("test.event"),
                timestamp=Timestamp.now(),
                payload={},
                schema_version=SchemaVersion("1.0.0"),
            ),
        )

        # Execute handler
        handler = registry.get_handler("test.event")
        await handler("mock_db", entry)

        assert len(executed) == 1
        assert executed[0][0] == "mock_db"
        assert executed[0][1] == "test.event"

    def test_multiple_event_types(self):
        """Test registering handlers for multiple event types."""
        registry = HandlerRegistry()

        @registry.handler("user.created")
        async def handle_user_created(db, entry):
            pass

        @registry.handler("user.updated")
        async def handle_user_updated(db, entry):
            pass

        @registry.handler("order.placed")
        async def handle_order_placed(db, entry):
            pass

        assert len(registry) == 3
        assert registry.has_handler("user.created")
        assert registry.has_handler("user.updated")
        assert registry.has_handler("order.placed")
