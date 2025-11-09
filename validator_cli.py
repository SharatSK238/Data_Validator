#!/usr/bin/env python3
"""Command line interface for the data validator framework."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any, Dict

import pandas as pd

from validator.config_loader import InvalidConfigError, UnknownValidatorError, load_config
from validator.pipeline import ValidationPipeline


def configure_logging(verbose: bool) -> None:
    """Configure global logging settings.

    Args:
        verbose: If True, set log level to DEBUG; otherwise INFO.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="[%(levelname)s] %(message)s",
    )


def parse_args(args: Any = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run data validation pipeline on a CSV file.")
    parser.add_argument(
        "--input", "-i", required=True, help="Path to the input CSV file to validate"
    )
    parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="Path to the validation config file (JSON or YAML)",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Path to write the JSON validation report",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose debug logging",
    )
    return parser.parse_args(args)


def serialise_result(result: Any) -> Any:
    """Recursively serialise dataclasses and other values to JSON serialisable types."""
    if result is None:
        return None
    if hasattr(result, "__dict__"):
        return {k: serialise_result(v) for k, v in result.__dict__.items() if not k.startswith("_")}
    if isinstance(result, list):
        return [serialise_result(item) for item in result]
    if isinstance(result, dict):
        return {k: serialise_result(v) for k, v in result.items()}
    # For pandas Timestamp or other non‑serialisable types convert to string
    try:
        json.dumps(result)
        return result
    except Exception:
        return str(result)


def main(argv: Any = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # Load data
    logger.info("Loading data from %s...", args.input)
    try:
        df = pd.read_csv(args.input)
    except FileNotFoundError:
        logger.error("Input file not found: %s", args.input)
        return 1
    except Exception as e:
        logger.error("Failed to read input CSV: %s", e)
        return 1
    logger.info("Loaded %d records with %d columns", len(df), len(df.columns))

    # Load and validate configuration
    logger.info("Loading validation config from %s...", args.config)
    try:
        pipeline_config = load_config(args.config)
    except (InvalidConfigError, UnknownValidatorError) as e:
        logger.error("Configuration error: %s", e)
        return 1
    except Exception as e:
        logger.error("Failed to load config: %s", e)
        return 1
    logger.info("Found %d validations to run", len(pipeline_config.validations))

    # Run pipeline
    logger.info("Running validation pipeline...")
    pipeline = ValidationPipeline(pipeline_config)
    try:
        result = pipeline.run(df)
    except (InvalidConfigError, UnknownValidatorError) as e:
        logger.error("Validation error: %s", e)
        return 1
    except Exception as e:
        logger.exception("Unexpected error during validation: %s", e)
        return 1
    
    # Print summary to stdout
    print()
    summary = pipeline.get_summary()
    print(summary)

    # Serialise detailed report to JSON
    report_data: Dict[str, Any] = {
        "pipeline": serialise_result(result),
    }
    try:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, default=str)
        logger.info("Detailed report saved to: %s", args.output)
    except Exception as e:
        logger.error("Failed to write report to %s: %s", args.output, e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())