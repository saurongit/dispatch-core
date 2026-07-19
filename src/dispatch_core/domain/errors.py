class DomainError(ValueError):
    """A business invariant was violated."""


class InvalidTransition(DomainError):
    """The requested lifecycle transition is not allowed."""


class EvidenceMissing(DomainError):
    """A completion report does not satisfy the configured requirements."""


class ConcurrencyConflict(RuntimeError):
    """An aggregate changed after it was read."""


class NotFound(LookupError):
    """A requested aggregate does not exist."""
