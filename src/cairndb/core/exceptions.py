"""Exception hierarchy for CairnDB."""


class CairnDBError(Exception):
    """Base exception for all CairnDB errors."""


class StorageError(CairnDBError):
    """Errors related to blob storage operations."""


class SequenceError(CairnDBError):
    """Errors related to sequence number generation or ordering."""


class ReplayError(CairnDBError):
    """Errors during WAL replay operations."""


class SchemaVersionError(CairnDBError):
    """Errors related to schema version mismatches."""


class CommitError(CairnDBError):
    """Errors during the commit protocol (write path)."""


class EventRejectedError(CommitError):
    """Event rejected by the revalidate hook after a lost commit race."""


class ConfigurationError(CairnDBError):
    """Errors related to invalid configuration."""


class HandlerNotFoundError(CairnDBError):
    """Event handler not found for given event type."""


class LeaseLost(CairnDBError):
    """The lease was fenced: another holder acquired it (epoch advanced).

    The former holder must discard any outcome it has not yet written —
    writes through a lost lease never reach storage.
    """


class TransactionConflict(CairnDBError):
    """Optimistic transaction aborted: its read set was written concurrently."""
