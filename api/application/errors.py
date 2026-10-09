class EntityNotFoundError(LookupError):
    """The requested entity does not exist."""


class BusinessRuleViolationError(ValueError):
    """A domain rule prevents the requested operation."""


class ConcurrentModificationError(BusinessRuleViolationError):
    """The entity changed after it was read."""
