"""TypeValidator ensures values match an expected data type."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
from datetime import datetime

from ..base import BaseValidator, ValidationResult
from ..registry import ValidatorRegistry


@ValidatorRegistry.register
class TypeValidator(BaseValidator):
    """Validate that column values match a specified data type."""

    @property
    def name(self) -> str:
        return "type"

    @property
    def description(self) -> str:
        return "Validate that values are of a specified type"

    def get_required_params(self) -> Dict[str, Dict[str, Any]]:
        return {
            "expected_type": {
                "type": str,
                "default": None,
                "description": "Expected type ('int', 'float', 'string', 'datetime', 'bool')",
                "required": True,
            },
            "strict": {
                "type": bool,
                "default": False,
                "description": "Whether to require exact type without conversion",
                "required": False,
            },
        }

    def validate(self, df: pd.DataFrame, column: str) -> ValidationResult:
        expected_type: str = str(self.config.get("expected_type")).lower()
        if not expected_type:
            raise ValueError("'expected_type' parameter is required for TypeValidator")
        strict: bool = self.config.get("strict", False)

        valid_types = {"int", "float", "string", "datetime", "bool"}
        if expected_type not in valid_types:
            raise ValueError(
                f"Unsupported expected_type: '{expected_type}'.\nSupported types: {sorted(valid_types)}"
            )
        series = df[column]
        total = len(series)
        failed_details: List[Dict[str, Any]] = []

        for idx, val in series.items():
            # Nulls are considered invalid for type check
            if pd.isna(val):
                failed_details.append(
                    {"row": idx, "value": val, "reason": "Null value"}
                )
                continue
            try:
                if expected_type == "int":
                    if strict:
                        if not isinstance(val, int):
                            raise TypeError(
                                f"Expected int but got {type(val).__name__}"
                            )
                    else:
                        # Attempt to convert
                        int_val = int(float(val))  # convert via float to handle numeric strings
                        # ensure that conversion is lossless
                        if abs(float(val) - int_val) > 0:
                            raise ValueError("Non-integer value cannot be coerced to int")
                elif expected_type == "float":
                    if strict:
                        if not isinstance(val, float):
                            raise TypeError(
                                f"Expected float but got {type(val).__name__}"
                            )
                    else:
                        float(val)  # attempt conversion
                elif expected_type == "string":
                    if strict:
                        if not isinstance(val, str):
                            raise TypeError(
                                f"Expected string but got {type(val).__name__}"
                            )
                    else:
                        # any value can be converted to string
                        str(val)
                elif expected_type == "datetime":
                    if strict:
                        # check pandas Timestamp or datetime.datetime
                        if not isinstance(val, (pd.Timestamp, datetime)):
                            raise TypeError(
                                f"Expected datetime but got {type(val).__name__}"
                            )
                    else:
                        # attempt to parse to datetime
                        pd.to_datetime(val)
                elif expected_type == "bool":
                    if strict:
                        if not isinstance(val, bool):
                            raise TypeError(
                                f"Expected bool but got {type(val).__name__}"
                            )
                    else:
                        # attempt to coerce
                        if isinstance(val, bool):
                            pass
                        elif isinstance(val, (int, float)) and val in (0, 1):
                            pass
                        elif isinstance(val, str) and val.lower() in {"true", "false", "1", "0"}:
                            pass
                        else:
                            raise ValueError("Cannot coerce value to bool")
            except Exception as e:
                failed_details.append(
                    {"row": idx, "value": val, "reason": str(e)}
                )
                continue
        
        failed_count = len(failed_details)
        passed = failed_count == 0

        if passed:
            message = f"All {total} records passed"
        else:
            message = f"{failed_count}/{total} records failed type validation"

        return ValidationResult(
            validator_name=self.name,
            passed=passed,
            total_records=total,
            failed_records=failed_count,
            error_details=failed_details,
            message=message,
        )