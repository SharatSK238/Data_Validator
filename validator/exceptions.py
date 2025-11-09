"""Custom exception types for the data validation framework."""


class InvalidConfigError(Exception):
    """Raised when the validation configuration is malformed or invalid."""


class UnknownValidatorError(Exception):
    """Raised when a configuration references a validator that is not registered."""