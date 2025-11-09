"""Expose validator implementations and trigger their registration.

Importing this module causes all built-in validators to register
themselves with the `ValidatorRegistry`.  Additional validators can be
added to this package following the same pattern: define a class that
inherits from `BaseValidator`, implement the required methods and apply
the `@ValidatorRegistry.register` decorator.
"""

# Import each validator module so that its class decorator executes
from .range_validator import RangeValidator
from .null_check_validator import NullCheckValidator
from .regex_validator import RegexValidator
from .type_validator import TypeValidator
from .custom_validator import CustomFunctionValidator

__all__ = [
    "RangeValidator",
    "NullCheckValidator",
    "RegexValidator",
    "TypeValidator",
    "CustomFunctionValidator",
]