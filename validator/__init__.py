"""Top level package for the data validation framework.

Importing this module automatically registers all built‑in validators via
the validator registry.  End users should import from this package to
gain access to the registry and pipeline classes.
"""

from .registry import ValidatorRegistry
from .pipeline import ValidationPipeline, PipelineConfig, PipelineResult
from .config_loader import load_config, InvalidConfigError, UnknownValidatorError

# Import validators to trigger decorator‑based registration
from . import validators  # noqa: F401

__all__ = [
    "ValidatorRegistry",
    "ValidationPipeline",
    "PipelineConfig",
    "PipelineResult",
    "load_config",
    "InvalidConfigError",
    "UnknownValidatorError",
]