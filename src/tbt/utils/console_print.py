"""Utility module for conditional console printing alongside logging."""

import os
import yaml
from pathlib import Path


def _load_logging_config():
    """Load logging configuration from logging.yml."""
    try:
        # Try to find the logging.yml file
        config_paths = [
            Path(__file__).parents[3] / "conf" / "base" / "logging.yml",
            Path("conf") / "base" / "logging.yml",
        ]

        for config_path in config_paths:
            if config_path.exists():
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f)

        # If file not found, return default config
        return {"enable_console_prints": True}
    except Exception:
        # On any error, default to enabled
        return {"enable_console_prints": True}


def conditional_print(message: str, *args, **kwargs):
    """
    Print message to console if enabled in logging.yml configuration.

    This function checks the 'enable_console_prints' setting in conf/base/logging.yml
    and prints the message only if it's set to True.

    Args:
        message: The message to print (can contain format placeholders)
        *args: Arguments to format the message
        **kwargs: Keyword arguments (currently unused, for future compatibility)

    Example:
        conditional_print("Processing %d items", 42)
        conditional_print("Status: %s", "complete")
    """
    config = _load_logging_config()

    if config.get("enable_console_prints", True):
        if args:
            # Format the message with provided arguments
            try:
                formatted_message = message % args
            except (TypeError, ValueError):
                # If formatting fails, print as-is
                formatted_message = str(message) + " " + " ".join(str(arg) for arg in args)
        else:
            formatted_message = message

        print(formatted_message)

