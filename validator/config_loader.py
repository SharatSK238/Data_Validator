"""Utility for loading and validating pipeline configuration files."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List

from .exceptions import InvalidConfigError, UnknownValidatorError
from .pipeline import PipelineConfig
from .registry import ValidatorRegistry

logger = logging.getLogger(__name__)


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_yaml(path: str) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError as e:
        raise ImportError(
            "pyyaml is required for loading YAML configuration files. "
            "Install it via `pip install pyyaml`."
        ) from e
    
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_config(path: str) -> PipelineConfig:
    """Load a validation configuration from a JSON or YAML file.

    Args:
        path: Path to the configuration file (.json, .yaml or .yml).

    Returns:
        A `PipelineConfig` instance containing the list of validation specs.

    Raises:
        InvalidConfigError: If the configuration is malformed or missing.
        UnknownValidatorError: If an unknown validator is referenced.
    """
    if not os.path.exists(path):
        raise InvalidConfigError(f"Configuration file not found: {path}")
    ext = os.path.splitext(path)[1].lower()

    if ext == ".json":
        config_data = _load_json(path)
    elif ext in {".yaml", ".yml"}:
        config_data = _load_yaml(path)
    else:
        raise InvalidConfigError(
            f"Unsupported configuration file extension '{ext}'. Use .json or .yaml/.yml"
        )
    
    if not isinstance(config_data, dict):
        raise InvalidConfigError("Top level of config file must be a JSON/YAML object")
    
    # Perform strong validation and normalisation
    normalized_validations = _validate_and_normalise_config(config_data)
    return PipelineConfig(validations=normalized_validations)


def _validate_and_normalise_config(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Validate the structure of the configuration and normalise parameters.

    Returns the list of validation specifications in a canonical form with
    parameters filled with defaults where appropriate.
    """
    if "validations" not in config:
        raise InvalidConfigError("Configuration must contain a 'validations' list")
    
    validations = config["validations"]
    if not isinstance(validations, list):
        raise InvalidConfigError("'validations' must be a list")
    
    normalized: List[Dict[str, Any]] = []
    for idx, entry in enumerate(validations):
        if not isinstance(entry, dict):
            raise InvalidConfigError(f"Validation entry at index {idx} must be a dict")
        column = entry.get("column")
        validator_name = entry.get("validator")
        params = entry.get("params", {}) or {}
        if column is None or validator_name is None:
            raise InvalidConfigError(
                f"Validation entry at index {idx} must contain 'column' and 'validator' keys"
            )
        
        # Ensure validator exists
        validator_class = ValidatorRegistry.get_validator(validator_name)
        if validator_class is None:
            raise UnknownValidatorError(
                f"Unknown validator '{validator_name}' in validation entry {idx}"
            )
        
        # Validate parameters against validator schema
        schema = validator_class({}).get_required_params()
        # Fill in defaults and check required params
        normalized_params: Dict[str, Any] = {}
        for param_name, meta in schema.items():
            default = meta.get("default")
            required = meta.get("required", False)
            if param_name in params:
                value = params[param_name]
            else:
                if required and default is None:
                    raise InvalidConfigError(
                        f"Missing required parameter '{param_name}' for validator '{validator_name}'"
                        f" in entry {idx} (column '{column}')."
                    )
                value = default
            
            # Type check if value is not None
            expected_type = meta.get("type")
            if value is not None and expected_type is not None:
                if isinstance(expected_type, tuple):
                    if not any(isinstance(value, t) for t in expected_type):
                        raise InvalidConfigError(
                            f"Parameter '{param_name}' for validator '{validator_name}'"
                            f" must be of type {expected_type}, got {type(value).__name__}"
                        )
                else:
                    import collections.abc

                    if expected_type == collections.abc.Callable:
                        if not callable(value):
                            raise InvalidConfigError(
                                f"Parameter '{param_name}' for validator '{validator_name}' must be callable"
                            )
                    elif not isinstance(value, expected_type):
                        raise InvalidConfigError(
                            f"Parameter '{param_name}' for validator '{validator_name}'"
                            f" must be of type {expected_type.__name__}, got {type(value).__name__}"
                        )
            normalized_params[param_name] = value
        
        # Check for unknown parameters
        for param_name in params:
            if param_name not in schema:
                raise InvalidConfigError(
                    f"Unknown parameter '{param_name}' for validator '{validator_name}' in entry {idx}"
                )
        
        normalized.append({"column": column, "validator": validator_name, "params": normalized_params})
    return normalized