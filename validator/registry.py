"""Validator registry for managing validator plugins.

The registry stores a mapping from validator names to their classes.
Validators register themselves via the `@ValidatorRegistry.register`
decorator.  Consumers can look up a validator by name and create
instances at runtime based on configuration.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Type

from .base import BaseValidator


class ValidatorRegistry:
    """Registry for managing validator plugins."""

    _validators: Dict[str, Type[BaseValidator]] = {}

    @classmethod
    def register(cls, validator_class: Type[BaseValidator]) -> Type[BaseValidator]:
        """Register a validator class and return it.

        This method is intended to be used as a decorator on validator
        subclasses.  The validator is instantiated with an empty
        configuration in order to inspect its name.  The resulting name
        is used as the key in the registry.  If a validator with the
        same name is already registered, it will be overwritten.

        Args:
            validator_class: The subclass of `BaseValidator` to register.

        Returns:
            The same validator class, so the decorator can be used on
            class definitions.
        """
        try:
            instance = validator_class(config={})  # type: ignore[arg-type]
        except Exception:
            # If instantiation fails we cannot determine the name
            raise RuntimeError(
                f"Failed to instantiate validator '{validator_class}' while registering."
            )
        name = instance.name
        cls._validators[name] = validator_class
        return validator_class

    @classmethod
    def get_validator(cls, name: str) -> Optional[Type[BaseValidator]]:
        """Return a validator class by its registered name.

        Args:
            name: The name of the validator (e.g. "range").

        Returns:
            The corresponding validator class, or None if not found.
        """
        return cls._validators.get(name)

    @classmethod
    def create_validator(cls, name: str, config: Dict[str, Any]) -> Optional[BaseValidator]:
        """Create a validator instance given its name and configuration.

        Args:
            name: Name of the validator to create.
            config: Configuration dictionary to pass to the validator.

        Returns:
            An instance of the validator, or None if the name is not registered.
        """
        validator_class = cls.get_validator(name)
        if validator_class is None:
            return None
        return validator_class(config)

    @classmethod
    def list_validators(cls) -> List[str]:
        """Return a list of all registered validator names."""
        return sorted(cls._validators.keys())