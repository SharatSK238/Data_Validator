"""Demonstration of using the data validator framework programmatically."""

import pandas as pd

from validator.registry import ValidatorRegistry
from validator.pipeline import ValidationPipeline, PipelineConfig
import validator.validators  # registers validators


def main() -> None:
    # Load sample data
    df = pd.read_csv('examples/sample_data.csv')
    print(f"Loaded {len(df)} records")

    # Option 1: Use validators directly
    print("\n=== Direct Validator Usage ===")
    range_validator = ValidatorRegistry.create_validator('range', {
        'min_value': 0,
        'max_value': 120,
        'inclusive': True,
    })
    if range_validator is None:
        raise ValueError("The 'range' validator is not registered.")
    result = range_validator.validate(df, 'age')
    print(f"Validator: {result.validator_name}")
    print(f"Passed: {result.passed}")
    print(f"Failed Records: {result.failed_records}/{result.total_records}")
    print(f"Message: {result.message}")

    # Option 2: Use validation pipeline from config
    print("\n=== Pipeline Usage ===")
    config = PipelineConfig(validations=[
        {
            "column": "age",
            "validator": "range",
            "params": {"min_value": 0, "max_value": 120},
        },
        {
            "column": "email",
            "validator": "regex",
            "params": {"pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"},
        },
    ])
    pipeline = ValidationPipeline(config)
    pipeline_result = pipeline.run(df)
    print("\nPipeline Result:")
    print(f"Overall Passed: {pipeline_result.passed}")
    print(f"Total Validations: {pipeline_result.total_validations}")
    print(f"Passed: {pipeline_result.passed_validations}")
    print(f"Failed: {pipeline_result.failed_validations}")
    print("\nSummary:\n")
    print(pipeline.get_summary())


if __name__ == '__main__':
    main()