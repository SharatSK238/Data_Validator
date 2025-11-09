"""Base classes for validators.

The data validation framework relies on an abstract base class
(`BaseValidator`) that defines the interface for all validators.  Each
concrete validator must implement the required methods and register
itself with the `ValidatorRegistry`.  A `ValidationResult` dataclass is
used to return structured information about each validation run.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class ValidationResult:
    """Result of a validation operation.

    Attributes:
        validator_name: Name of the validator (e.g. "range").
        passed: True if the validation succeeded (no failures).
        total_records: Total number of records examined.
        failed_records: Number of records that failed validation.
        error_details: List of dictionaries describing each failure.  Each
            item should at least contain a row index and reason.
        message: Human-readable summary describing the result.
        duration_seconds: Optional runtime of the validation in seconds.
    """

    validator_name: str
    passed: bool
    total_records: int
    failed_records: int
    error_details: List[Dict[str, Any]]
    message: str
    duration_seconds: Optional[float] = None


class BaseValidator(ABC):
    """Abstract base class for all validators.

    Validators are initialised with a configuration dictionary.  They
    implement a name, description, parameter schema and a validation
    function.  Subclasses must register themselves with the
    `ValidatorRegistry` using the provided decorator.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialise the validator with a configuration.

        Args:
            config: Mapping of parameter names to their values.  Missing
                parameters should fall back to sensible defaults.
        """
        self.config: Dict[str, Any] = config

    @property
    @abstractmethod
    def name(self) -> str:
        """Short name of the validator used for registration and lookup."""

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description of what the validator does."""

    @abstractmethod
    def validate(self, df: pd.DataFrame, column: str) -> ValidationResult:
        """Validate a column of a DataFrame.

        Args:
            df: The DataFrame containing the data to validate.
            column: Name of the column to validate.

        Returns:
            A `ValidationResult` object summarising the outcome.
        """

    @abstractmethod
    def get_required_params(self) -> Dict[str, Dict[str, Any]]:
        """Return a parameter specification for this validator.

        The specification maps each parameter name to a dictionary
        containing the expected type, optional default value,
        description and whether the parameter is required.  This schema
        is used by the configuration loader to validate incoming
        configuration before instantiating the validator.

        Returns:
            A mapping from parameter name to parameter metadata.
        """
        # Each subclass must implement this
        raise NotImplementedError