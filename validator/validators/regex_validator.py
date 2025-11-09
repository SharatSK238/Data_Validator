"""RegexValidator ensures string values match a given regular expression pattern."""

from __future__ import annotations

import re
from typing import Any, Dict, List

import pandas as pd

from ..base import BaseValidator, ValidationResult
from ..registry import ValidatorRegistry


@ValidatorRegistry.register
class RegexValidator(BaseValidator):
    """Validate that string values match a specified regular expression."""

    @property
    def name(self) -> str:
        return "regex"

    @property
    def description(self) -> str:
        return "Validate that string values match a regular expression pattern"

    def get_required_params(self) -> Dict[str, Dict[str, Any]]:
        return {
            "pattern": {
                "type": str,
                "default": None,
                "description": "Regular expression pattern",
                "required": True,
            },
            "case_sensitive": {
                "type": bool,
                "default": True,
                "description": "Whether pattern matching is case sensitive",
                "required": False,
            },
        }

    def validate(self, df: pd.DataFrame, column: str) -> ValidationResult:
        pattern_value = self.config.get("pattern")
        if pattern_value is None:
            raise ValueError("'pattern' parameter is required for RegexValidator")
        pattern: str = str(pattern_value)

        case_sensitive: bool = self.config.get("case_sensitive", True)
        flags = 0 if case_sensitive else re.IGNORECASE

        try:
            regex = re.compile(pattern, flags)
        except re.error as e:
            raise ValueError(f"Invalid regular expression pattern: {e}")
        
        total = len(df)
        failed_details: List[Dict[str, Any]] = []
        series = df[column]

        for idx, val in series.items():
            if pd.isna(val):
                failed_details.append(
                    {"row": idx, "value": None, "reason": "Null value cannot match pattern"}
                )
                continue
            text = str(val)
            if not regex.match(text):
                failed_details.append(
                    {
                        "row": idx,
                        "value": text,
                        "reason": "Does not match pattern",
                    }
                )
        
        failed_count = len(failed_details)
        passed = failed_count == 0

        if passed:
            message = f"All {total} records passed"
        else:
            message = f"{failed_count}/{total} records failed regex validation"
        
        return ValidationResult(
            validator_name=self.name,
            passed=passed,
            total_records=total,
            failed_records=failed_count,
            error_details=failed_details,
            message=message,
        )