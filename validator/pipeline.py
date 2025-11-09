"""Validation pipeline and associated configuration/result classes."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import ValidationResult
from .exceptions import InvalidConfigError, UnknownValidatorError
from .registry import ValidatorRegistry


logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for the validation pipeline.

    Attributes:
        validations: A list of validation specifications.  Each item is a
            dictionary with keys `column` (str), `validator` (str) and
            `params` (dict of parameters for the validator).
    """

    validations: List[Dict[str, Any]]


@dataclass
class PipelineResult:
    """Result of running the entire validation pipeline."""

    passed: bool
    total_validations: int
    passed_validations: int
    failed_validations: int
    results: List[ValidationResult]
    duration_seconds: Optional[float] = None


class ValidationPipeline:
    """Pipeline orchestrating a sequence of validators on a DataFrame."""

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self._validator_specs: List[Dict[str, Any]] = []

        # Prepare validator instances according to config
        for idx, spec in enumerate(self.config.validations):
            if not isinstance(spec, dict):
                raise InvalidConfigError(f"Validation entry at index {idx} must be a dict.")
            
            column = spec.get("column")
            name = spec.get("validator")
            params = spec.get("params", {})
            if column is None or name is None:
                raise InvalidConfigError(
                    f"Validation entry at index {idx} must contain 'column' and 'validator' keys."
                )
            
            validator_class = ValidatorRegistry.get_validator(name)
            if validator_class is None:
                raise UnknownValidatorError(f"Unknown validator '{name}' in validation entry {idx}.")
            
            # Create validator instance with parameters
            validator = validator_class(params)
            self._validator_specs.append({"column": column, "validator": validator})
        
        self.last_result: Optional[PipelineResult] = None
    

    def run(self, df: pd.DataFrame) -> PipelineResult:
        """Execute the pipeline on the given DataFrame.

        Args:
            df: The DataFrame to validate.

        Returns:
            A `PipelineResult` summarising the outcome.
        """
        start_time = time.perf_counter()
        results: List[ValidationResult] = []
        passed_count = 0
        failed_count = 0
        total = len(self._validator_specs)

        for index, spec in enumerate(self._validator_specs, start=1):
            col = spec["column"]
            validator = spec["validator"]
            # Check if column exists
            if col not in df.columns:
                message = f"Column '{col}' not found in input data."
                logger.error(message)
                raise InvalidConfigError(message)
            logger.debug(
                "Running validator '%s' on column '%s' (%d/%d)",
                validator.name,
                col,
                index,
                total,
            )

            val_start = time.perf_counter()
            result = validator.validate(df, col)
            val_end = time.perf_counter()
            result.duration_seconds = val_end - val_start
            results.append(result)
            if result.passed:
                passed_count += 1
            else:
                failed_count += 1
        
        end_time = time.perf_counter()
        overall_passed = failed_count == 0
        pipeline_result = PipelineResult(
            passed=overall_passed,
            total_validations=total,
            passed_validations=passed_count,
            failed_validations=failed_count,
            results=results,
            duration_seconds=end_time - start_time,
        )

        self.last_result = pipeline_result
        return pipeline_result

    def get_summary(self) -> str:
        """Return a human‑readable summary of the most recent pipeline run.

        Raises:
            RuntimeError: If the pipeline has not yet been run.
        """
        if self.last_result is None:
            raise RuntimeError("Pipeline has not been run yet.")
        
        lines: List[str] = []
        lines.append("Validation Results:")
        lines.append("==================")
        for idx, (spec, result) in enumerate(zip(self._validator_specs, self.last_result.results), start=1):
            status = "✓ PASSED" if result.passed else "✗ FAILED"
            lines.append(f"\n{idx}. Validator: {result.validator_name} (column: {spec['column']})")
            lines.append(f"   Status: {status}")
            lines.append(f"   Message: {result.message}")
            if not result.passed:
                lines.append("   Failed Records:")
                for detail in result.error_details:
                    # Convert row index to 1‑based row number for display
                    row_num = detail.get("row")
                    if isinstance(row_num, int):
                        row_num_display = row_num + 1
                    else:
                        row_num_display = row_num
                    value = detail.get("value")
                    reason = detail.get("reason")
                    lines.append(f"     - Row {row_num_display}: value={value!r}, reason={reason}")
        lines.append("\n==================")

        overall = "PASSED" if self.last_result.passed else "FAILED"
        lines.append(f"Overall Result: {overall}")
        lines.append(
            f"Passed: {self.last_result.passed_validations}/{self.last_result.total_validations} validations"
        )
        lines.append(
            f"Failed: {self.last_result.failed_validations}/{self.last_result.total_validations} validations"
        )

        if self.last_result.duration_seconds is not None:
            lines.append(
                f"Total validation time: {self.last_result.duration_seconds:.3f} seconds."
            )
        
        return "\n".join(lines)