class IdempotencyConflict(ValueError):
    """The same idempotency key was used with a different payment payload."""


class InvalidEvent(ValueError):
    """The received event does not match a persisted payment event."""


class LeaseLost(RuntimeError):
    """Another worker owns the task, or its lease has expired."""
