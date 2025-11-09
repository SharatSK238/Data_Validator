# Data Validator Framework

## Overview

This project provides a flexible **data validation framework** built with
Python 3.8 and pandas.  It allows you to define reusable validation rules
as plugins, chain multiple validators together and run them against
arbitrary pandas data frames.  The framework is inspired by the
requirements of the take‑home assignment and supports core and optional
features such as a command‑line interface, strong configuration
validation, logging, unit tests, YAML configuration loading and basic
performance metrics.

The code lives under the `validator/` package.  Validators are
discovered and registered via a simple decorator and can be composed in
a pipeline.  A small CLI (`validator_cli.py`) reads CSV files and a
validation config file, runs the pipeline and writes a detailed JSON
report.  Example data, an example config and a minimal usage script are
provided under the `examples/` folder.

## Installation

1. **Clone the repository or unzip the provided archive**.
2. Ensure you have Python 3.8 or later installed.
3. Create a virtual environment (recommended) and install
   dependencies:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`requirements.txt` lists all required packages: `pandas` for data
manipulation, `pyyaml` for optional YAML support, and `pytest` for
running the unit tests.

## Usage

### Running the CLI

The primary interface for end users is the CLI script.  It accepts an
input CSV file, a validation config (JSON or YAML) and an output
location for the report:

```sh
python validator_cli.py \
    --input examples/sample_data.csv \
    --config examples/validation_config.json \
    --output validation_report.json
```

Additional options:

- `--verbose` or `-v` – enable detailed debug logging.

When executed, the CLI loads the CSV into a pandas DataFrame, loads and
validates the configuration, runs the validation pipeline and then
prints a human‑readable summary along with the overall pass/fail
status.  A detailed JSON report is also written to the path specified
via `--output`.

### Example Usage Script

An example script located at `examples/example_usage.py` demonstrates
how to use the framework programmatically.  It imports the registry
explicitly to register all validators, loads a sample data frame,
creates individual validator instances, and then runs a pipeline
configuration:

```python
import pandas as pd
from validator.registry import ValidatorRegistry
from validator.pipeline import ValidationPipeline, PipelineConfig
import validator.validators  # registers all validators

# Load sample data
df = pd.read_csv('examples/sample_data.csv')
print(f"Loaded {len(df)} records")

# Direct validator usage
range_validator = ValidatorRegistry.create_validator('range', {
    'min_value': 0,
    'max_value': 120,
    'inclusive': True
})
result = range_validator.validate(df, 'age')
print(result)

# Pipeline usage
config = PipelineConfig(validations=[
    {
        "column": "age",
        "validator": "range",
        "params": {"min_value": 0, "max_value": 120}
    },
    {
        "column": "email",
        "validator": "regex",
        "params": {"pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"}
    }
])
pipeline = ValidationPipeline(config)
pipeline_result = pipeline.run(df)
print(pipeline.get_summary())
```

### Running Tests

Unit tests are provided for the `RangeValidator`, `RegexValidator` and
the `ValidationPipeline` in the `tests/` directory.  To execute the
tests, run:

```sh
pytest -q tests
```

The tests cover typical cases, edge cases and error conditions to
ensure the validators behave correctly.

## Design Decisions

The design follows a plugin architecture.  Each validator subclass
derives from the `BaseValidator` abstract base class and registers
itself with the global `ValidatorRegistry` via a decorator.  The
registry maps validator names to classes and provides factory methods
for creating instances.  The `ValidationPipeline` takes a
`PipelineConfig` dataclass containing a list of validation
specifications (column name, validator name and parameters).  During
initialisation it instantiates the validators and verifies the
configuration.  When run on a DataFrame, the pipeline executes each
validator sequentially, collects results, handles exceptions and
aggregates a high‑level summary.

Error handling is explicit and user friendly.  The configuration loader
raises custom exceptions (e.g. `InvalidConfigError`, `UnknownValidatorError`)
when the configuration is malformed or references unknown validators.
Validators themselves raise exceptions when columns are missing or when
parameters are invalid.  The CLI catches these exceptions, logs them
appropriately and exits with an informative message.

Logging is configured centrally in `validator_cli.py` using Python’s
`logging` module.  A `--verbose` flag allows switching between
`INFO` and `DEBUG` levels.  Logs are emitted when loading data and
configuration, starting and ending the pipeline and when each validator
runs.

The optional YAML support is implemented transparently in the
configuration loader by detecting `.yaml` or `.yml` extensions.  If
`pyyaml` is unavailable, an informative error is raised.  Performance
metrics are collected using simple timestamps; each validation result
includes a `duration_seconds` field and the pipeline summary prints the
total duration.

## Assumptions

- **Null handling:** type and regex validators treat null values as
  failures.  Users can explicitly allow nulls via the `NullCheckValidator`.
- **Custom functions:** the `CustomFunctionValidator` expects a Python
  callable in the configuration.  Such callables cannot be defined in
  JSON/YAML, so custom validation functions are typically passed
  programmatically.
