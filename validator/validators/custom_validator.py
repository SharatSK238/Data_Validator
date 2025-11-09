"""CustomFunctionValidator allows users to supply their own validation function."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from ..base import BaseValidator, ValidationResult
from ..registry import ValidatorRegistry


@ValidatorRegistry.register
class CustomFunctionValidator(BaseValidator):
    """Validate values using a user‑provided callable.

    The callable must accept a single value and return a truthy value if
    the value passes validation.  A falsy return value indicates failure.
    """

    @property
    def name(self) -> str:
        return "custom_function"

    @property
    def description(self) -> str:
        return "Validate values using a user-provided function"

    def get_required_params(self) -> Dict[str, Dict[str, Any]]:
        return {
            "validation_func": {
                "type": Callable,
                "default": None,
                "description": "Callable that takes a single value and returns bool",
                "required": True,
            },
            "error_message": {
                "type": str,
                "default": None,
                "description": "Custom error message for failures",
                "required": False,
            },
        }

    def validate(self, df: pd.DataFrame, column: str) -> ValidationResult:
        func: Optional[Callable[[Any], Any]] = self.config.get("validation_func")
        error_message: Optional[str] = self.config.get("error_message")
        if func is None:
            raise ValueError("'validation_func' parameter is required for CustomFunctionValidator")
        
        total = len(df)
        failed_details: List[Dict[str, Any]] = []
        series = df[column]
        for idx, val in series.items():
            # Treat nulls as failures unless the function specifically handles them
            try:
                result = func(val)
            except Exception as e:
                failed_details.append(
                    {
                        "row": idx,
                        "value": val,
                        "reason": f"Exception in validation function: {e}",
                    }
                )
                continue

            if not result:
                reason = error_message or "Custom validation failed"
                failed_details.append(
                    {"row": idx, "value": val, "reason": reason}
                )
        
        failed_count = len(failed_details)
        passed = failed_count == 0
        if passed:
            message = f"All {total} records passed"
        else:
            message = f"{failed_count}/{total} records failed custom validation"

        return ValidationResult(
            validator_name=self.name,
            passed=passed,
            total_records=total,
            failed_records=failed_count,
            error_details=failed_details,
            message=message,
        )