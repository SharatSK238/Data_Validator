# tests/conftest.py

import logging
from typing import Iterable, List, Set, Any
from logging.handlers import RotatingFileHandler
from pathlib import Path
import pytest
import pandas as pd

def pretty_result(res) -> str:
    """
    Build a compact one-line summary for ValidationResult/PipelineResult.
    """
    name = getattr(res, "validator_name", None)
    passed = getattr(res, "passed", None)
    total = getattr(res, "total_records", None)
    failed = getattr(res, "failed_records", None)
    total_validations = getattr(res, "total_validations", None)
    passed_validations = getattr(res, "passed_validations", None)
    failed_validations = getattr(res, "failed_validations", None)

    if name is not None:
        return (f"[ValidationResult] name={name} passed={passed} "
                f"total={total} failed={failed}")
    
    return (f"[PipelineResult] passed={passed} total={total_validations} "
            f"passed_validations={passed_validations} failed_validations={failed_validations}")

def log_result(res, logger_name: str = "tests"):
    """
    Log the result using the root test logger at INFO level.
    """
    logger = logging.getLogger(logger_name)
    logger.info(pretty_result(res))


def _extract_failed_rows(result: Any) -> List[int]:
    """
    Pull row indices from result.error_details.
    We assume each error detail looks like: {"row": <index>, ...}
    """
    rows: List[int] = []
    details = getattr(result, "error_details", []) or []

    for item in details:
        row = item.get("row")
        if row is not None:
            rows.append(int(row))
    
    return rows


def log_pass_fail_rows(
    result: Any,
    df_index: Iterable[int],
    *,
    logger_name: str = "tests",
    max_show: int = 20,
) -> None:
    """
    Log a concise summary of passed/failed row indices for a single ValidationResult.

    Parameters
    ----------
    result : Any
        A ValidationResult-like object with fields:
        - error_details: List[dict] containing "row" for failed rows.
        - passed / failed_records / total_records (optional; only for info).
    df_index : Iterable[int]
        The DataFrame's index (so we can compute 'passed' = all - failed).
    logger_name : str
        Name of the logger to emit to (so it shows up under --log-cli-level=INFO).
    max_show : int
        How many row indices to display at most (keeps output readable).
    """
    logger = logging.getLogger(logger_name)

    failed_rows: List[int] = sorted(set(_extract_failed_rows(result)))
    failed_set: Set[int] = set(failed_rows)

    all_rows: List[int] = [int(x) for x in df_index]

    passed_rows: List[int] = [r for r in all_rows if r not in failed_set]

    # Helper to limit long lists but still show content
    def _limit(seq: List[int]) -> List[int | str]:
        if len(seq) <= max_show:
            return seq + [""]
        else:
            return seq[:max_show] + ["..."]
    
    name = getattr(result, "validator_name", None)
    total_records = getattr(result, "total_records", len(all_rows))
    failed_records = getattr(result, "failed_records", len(failed_rows))
    passed_flag = getattr(result, "passed", None)

    logger.info(
        "[%s] passed=%s total=%d failed=%d",
        name or "ValidationResult",
        passed_flag,
        total_records,
        failed_records,
    )

    logger.info("Failed rows (up to %d): %s", max_show, _limit(failed_rows))
    logger.info("Passed rows (up to %d): %s", max_show, _limit(passed_rows))


@pytest.fixture(scope="session", autouse=True)
def _setup_rotating_log_file() -> None:
    """
    Create a rotating file handler for test logs and attach it to the root logger.
    This runs once per test session (autouse=True).
    """
    logs_dir = Path("tests/logs")
    logs_dir.mkdir(exist_ok=True)

    handler = RotatingFileHandler(
        logs_dir / "test.log",
        maxBytes=1_000_000,
        backupCount=5,
        encoding="utf-8",
        delay=True
    )

    formatter = logging.Formatter(
        fmt="%(levelname)s %(name)s - %(message)s",
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.addHandler(handler)

    root.setLevel(logging.INFO)


def _extract_failed_row_indices(result: Any) -> List[int]:
    """
    Pull failed row indices from result.error_details,
    assuming each entry looks like {"row": <index>, ...}.
    """
    rows: List[int] = []
    details = getattr(result, "error_details", []) or []
    for item in details:
        row = item.get("row")
        if row is not None:
            rows.append(int(row))
            
    return sorted(set(rows))


def log_failed_rows_full(
    result: Any,
    df: pd.DataFrame,
    *,
    columns: Iterable[str] | None = None,
    logger_name: str = "tests",
    max_show: int = 50,
) -> None:
    """
    Log the COMPLETE failed rows to the test log file, one row per line as a dict.

    Parameters
    ----------
    result : Any
        A ValidationResult-like object with .error_details containing {"row": idx, ...}.
    df : pd.DataFrame
        The DataFrame that was validated (used to fetch the full rows).
    columns : Iterable[str] | None
        Optional subset of columns to include in the log (if None, include all).
    logger_name : str
        The logger name used for emission; it will go to your existing file handler.
    max_show : int
        Cap on how many failed rows to log (helps keep logs readable).
    """
    logger = logging.getLogger(logger_name)

    failed_indices = _extract_failed_row_indices(result)

    failed_count = len(failed_indices)
    total_records = getattr(result, "total_records", len(df))
    validator_name = getattr(result, "validator_name", "unknown-validator")

    logger.info(
        "[%s] failed_rows=%d / total=%d",
        validator_name,
        failed_count,
        total_records,
    )

    if failed_count == 0:
        logger.info("No failed rows to display.")
        return
    
    idx_in_df = [i for i in failed_indices if i in df.index]
    if not idx_in_df:
        logger.info("No matching failed indices found in DataFrame index.")
        return

    failed_df = df.loc[idx_in_df]
    if columns is not None:
        failed_df = failed_df.loc[:, list(columns)]
    
    rows_as_dicts = failed_df.to_dict(orient="index")

    to_show = list(rows_as_dicts.items())[:max_show]
    logger.info("Printing up to %d failed row(s):", max_show)

    for idx, row_dict in to_show:
        logger.info("Failed row %s: %s", idx, row_dict)
    
    if failed_count > len(to_show):
        logger.info("... %d more failed row(s) not shown", failed_count - len(to_show))