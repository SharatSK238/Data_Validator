"""Unit tests for validators and pipeline using pytest."""

import pandas as pd

from validator.registry import ValidatorRegistry
from validator.pipeline import PipelineConfig, ValidationPipeline
import validator.validators
from .conftest import log_result, log_pass_fail_rows, log_failed_rows_full


def test_range_validator_valid_cases() -> None:
    df = pd.DataFrame({"value": [1, 5, 9]})
    validator = ValidatorRegistry.create_validator(
        "range", {"min_value": 0, "max_value": 10, "inclusive": True}
    )

    if validator is None:
        raise ValueError("Range validator not found in registry")
    result = validator.validate(df, "value")
    log_result(result)
    log_failed_rows_full(result, df, max_show=100)
    assert result.passed
    assert result.failed_records == 0


def test_range_validator_invalid_cases() -> None:
    df = pd.DataFrame({"value": [5, -1, 11]})
    validator = ValidatorRegistry.create_validator(
        "range", {"min_value": 0, "max_value": 10, "inclusive": True}
    )

    if validator is None:
        raise ValueError("Range validator not found in registry")
    result = validator.validate(df, "value")
    log_result(result)
    log_failed_rows_full(result, df, max_show=100)
    assert not result.passed
    # two values outside the range
    assert result.failed_records == 2
    # ensure correct rows reported (indexes 1 and 2)
    failed_rows = {detail["row"] for detail in result.error_details}
    assert failed_rows == {1, 2}


def test_range_validator_edge_inclusive_false() -> None:
    df = pd.DataFrame({"value": [0, 5, 10]})
    validator = ValidatorRegistry.create_validator(
        "range", {"min_value": 0, "max_value": 10, "inclusive": False}
    )

    if validator is None:
        raise ValueError("Range validator not found in registry")
    result = validator.validate(df, "value")
    log_result(result)
    log_failed_rows_full(result, df, max_show=100)
    assert not result.passed
    # 0 and 10 should fail when exclusive
    assert result.failed_records == 2


def test_regex_validator_valid() -> None:
    df = pd.DataFrame({"text": ["abc", "xyz"]})
    validator = ValidatorRegistry.create_validator(
        "regex", {"pattern": r"^[a-z]+$", "case_sensitive": True}
    )

    if validator is None:
        raise ValueError("Range validator not found in registry")
    result = validator.validate(df, "text")
    log_result(result)
    log_failed_rows_full(result, df, max_show=100)
    assert result.passed
    assert result.failed_records == 0


def test_regex_validator_invalid_and_case_sensitive() -> None:
    df = pd.DataFrame({"text": ["abc", "Xyz", ""]})
    validator = ValidatorRegistry.create_validator(
        "regex", {"pattern": r"^[a-z]+$", "case_sensitive": True}
    )

    if validator is None:
        raise ValueError("Range validator not found in registry")
    result = validator.validate(df, "text")
    log_result(result)
    log_failed_rows_full(result, df, max_show=100)
    assert not result.passed
    # two failures: capitalised and empty string
    assert result.failed_records == 2


def test_regex_validator_case_insensitive() -> None:
    df = pd.DataFrame({"text": ["ABC", "xyz"]})
    validator = ValidatorRegistry.create_validator(
        "regex", {"pattern": r"^[a-z]+$", "case_sensitive": False}
    )

    if validator is None:
        raise ValueError("Range validator not found in registry")
    result = validator.validate(df, "text")
    log_result(result)
    log_failed_rows_full(result, df, max_show=100)
    assert result.passed


def test_pipeline_multiple_validations() -> None:
    df = pd.DataFrame(
        {
            "num": [5, 0, 10, 15],
            "word": ["good", "Bad", "ok", "excellent"],
        }
    )
    config = PipelineConfig(
        validations=[
            {
                "column": "num",
                "validator": "range",
                "params": {"min_value": 0, "max_value": 10},
            },
            {
                "column": "word",
                "validator": "regex",
                "params": {"pattern": r"^[a-z]+$", "case_sensitive": False},
            },
        ]
    )
    pipeline = ValidationPipeline(config)
    result = pipeline.run(df)
    log_result(result)
    for res in result.results:
        log_failed_rows_full(res, df, max_show=100)
    # Range validator should fail on index 3 (value 15)
    assert not result.passed
    assert result.total_validations == 2
    assert result.passed_validations == 1
    assert result.failed_validations == 1