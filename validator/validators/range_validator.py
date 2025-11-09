"""RangeValidator ensures numeric values fall within a specified range."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from ..base import BaseValidator, ValidationResult
from ..registry import ValidatorRegistry


@ValidatorRegistry.register
class RangeValidator(BaseValidator):
    """Validate that values are within an inclusive or exclusive numeric range."""

    @property
    def name(self) -> str:
        return "range"

    @property
    def description(self) -> str:
        return "Validate that numeric values fall within a specified range"

    def get_required_params(self) -> Dict[str, Dict[str, Any]]:
        return {
            "min_value": {
                "type": (int, float),
                "default": None,
                "description": "Minimum allowed value (optional)",
                "required": False,
            },
            "max_value": {
                "type": (int, float),
                "default": None,
                "description": "Maximum allowed value (optional)",
                "required": False,
            },
            "inclusive": {
                "type": bool,
                "default": True,
                "description": "Whether range bounds are inclusive",
                "required": False,
            },
        }

    def validate(self, df: pd.DataFrame, column: str) -> ValidationResult:
        # Extract configuration parameters with defaults
        min_value: Optional[float] = self.config.get("min_value")
        max_value: Optional[float] = self.config.get("max_value")
        inclusive: bool = self.config.get("inclusive", True)

        total = len(df)
        failed_details: List[Dict[str, Any]] = []

        series = df[column]
        for idx, val in series.items():
            # Treat missing values as failures
            if pd.isna(val):
                failed_details.append(
                    {"row": idx, "value": val, "reason": "Null value"}
                )
                continue
            # Attempt to interpret as numeric
            try:
                numeric_val = float(val)
            except Exception:
                failed_details.append(
                    {
                        "row": idx,
                        "value": val,
                        "reason": "Value is not numeric",
                    }
                )
                continue
            # Check lower bound
            if min_value is not None:
                if inclusive:
                    if numeric_val < min_value:
                        failed_details.append(
                            {
                                "row": idx,
                                "value": val,
                                "reason": f"Value below minimum {min_value}",
                            }
                        )
                        continue
                else:
                    if numeric_val <= min_value:
                        failed_details.append(
                            {
                                "row": idx,
                                "value": val,
                                "reason": f"Value not greater than minimum {min_value}",
                            }
                        )
                        continue
            # Check upper bound
            if max_value is not None:
                if inclusive:
                    if numeric_val > max_value:
                        failed_details.append(
                            {
                                "row": idx,
                                "value": val,
                                "reason": f"Value above maximum {max_value}",
                            }
                        )
                        continue
                else:
                    if numeric_val >= max_value:
                        failed_details.append(
                            {
                                "row": idx,
                                "value": val,
                                "reason": f"Value not less than maximum {max_value}",
                            }
                        )
                        continue
        failed_count = len(failed_details)
        passed = failed_count == 0
        if passed:
            message = f"All {total} records passed"
        else:
            message = f"{failed_count}/{total} records failed range validation"
        return ValidationResult(
            validator_name=self.name,
            passed=passed,
            total_records=total,
            failed_records=failed_count,
            error_details=failed_details,
            message=message,
        )