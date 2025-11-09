"""NullCheckValidator ensures that missing values are within an acceptable threshold."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from ..base import BaseValidator, ValidationResult
from ..registry import ValidatorRegistry


@ValidatorRegistry.register
class NullCheckValidator(BaseValidator):
    """Validate that a column has no nulls or within an allowable percentage."""

    @property
    def name(self) -> str:
        return "null_check"

    @property
    def description(self) -> str:
        return "Validate that null values are absent or within a threshold"

    def get_required_params(self) -> Dict[str, Dict[str, Any]]:
        return {
            "allow_null": {
                "type": bool,
                "default": False,
                "description": "Whether null values are allowed",
                "required": False,
            },
            "max_null_percent": {
                "type": float,
                "default": None,
                "description": "Maximum percentage (0-100) of nulls allowed",
                "required": False,
            },
        }

    def validate(self, df: pd.DataFrame, column: str) -> ValidationResult:
        allow_null: bool = self.config.get("allow_null", False)
        max_null_percent: Optional[float] = self.config.get("max_null_percent")
        series = df[column]
        total = len(series)
        null_mask = series.isna()
        num_null = int(null_mask.sum())
        failed_details: List[Dict[str, Any]] = []

        if not allow_null:
            # Nulls are not allowed at all
            for idx, is_null in null_mask.items():
                if is_null:
                    failed_details.append(
                        {"row": idx, "value": None, "reason": "Null value not allowed"}
                    )
        else:
            # Nulls are allowed up to a certain percentage
            if max_null_percent is not None:
                percent = (num_null / total) * 100 if total else 0.0
                if percent > max_null_percent:
                    # record all null rows as failures
                    for idx, is_null in null_mask.items():
                        if is_null:
                            failed_details.append(
                                {
                                    "row": idx,
                                    "value": None,
                                    "reason": f"Null percentage {percent:.2f}% exceeds limit {max_null_percent}%",
                                }
                            )
                else:
                    # records the rows even if the null values do not exceed the max percentage allowed
                    for idx, is_null in null_mask.items():
                        if is_null:
                            failed_details.append(
                                {
                                    "row": idx,
                                    "value": None,
                                    "reason": f"Null present but within allowed {max_null_percent}% limit"
                                }
                            )
        failed_count = len(failed_details)
        passed = failed_count == 0
        if passed:
            message = f"All {total} records passed"
        else:
            message = f"{failed_count}/{total} records failed null check"
        return ValidationResult(
            validator_name=self.name,
            passed=passed,
            total_records=total,
            failed_records=failed_count,
            error_details=failed_details,
            message=message,
        )