"""Exception hierarchy for CairnDB."""


class CairnDBError(Exception):
    """Base exception for all CairnDB errors."""
    pass


class StorageError(CairnDBError):
    """Errors related to blob storage operations."""
    pass


class SequenceError(CairnDBError):
    """Errors related to sequence number generation or ordering."""
    pass


class ReplayError(CairnDBError):
    """Errors during WAL replay operations."""
    pass


class SchemaVersionError(CairnDBError):
    """Errors related to schema version mismatches."""
    pass


class CommitError(CairnDBError):
    """Errors during the commit protocol (write path)."""
    pass


class EventRejectedError(CommitError):
    """Event rejected by the revalidate hook after a lost commit race."""
    pass


class ConfigurationError(CairnDBError):
    """Errors related to invalid configuration."""
    pass


class HandlerNotFoundError(CairnDBError):
    """Event handler not found for given event type."""
    pass


class LeaseLost(CairnDBError):
    """The lease was fenced: another holder acquired it (epoch advanced).

    The former holder must discard any outcome it has not yet written —
    writes through a lost lease never reach storage.
    """
    pass


class TransactionConflict(CairnDBError):
    """Optimistic transaction aborted: its read set was written concurrently."""
    pass
