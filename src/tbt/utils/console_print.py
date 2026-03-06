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


def _conditional_print_base(prefix: str, message: str, *args, **kwargs):
    """
    Base function for conditional printing with level prefix.

    Args:
        prefix: Level prefix (e.g., "INFO", "WARNING", "ERROR")
        message: The message to print (can contain format placeholders)
        *args: Arguments to format the message
        **kwargs: Keyword arguments (currently unused, for future compatibility)
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

        if prefix:
            print(f"[{prefix}] {formatted_message}")
        else:
            print(formatted_message)


def conditional_print(message: str, *args, **kwargs):
    """
    Print INFO level message to console if enabled in logging.yml configuration.

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
    _conditional_print_base("INFO", message, *args, **kwargs)


def conditional_print_warning(message: str, *args, **kwargs):
    """
    Print WARNING level message to console if enabled in logging.yml configuration.

    Args:
        message: The message to print (can contain format placeholders)
        *args: Arguments to format the message
        **kwargs: Keyword arguments (currently unused, for future compatibility)

    Example:
        conditional_print_warning("Resource usage high: %d%%", 95)
    """
    _conditional_print_base("WARNING", message, *args, **kwargs)


def conditional_print_error(message: str, *args, **kwargs):
    """
    Print ERROR level message to console if enabled in logging.yml configuration.

    Args:
        message: The message to print (can contain format placeholders)
        *args: Arguments to format the message
        **kwargs: Keyword arguments (currently unused, for future compatibility)

    Example:
        conditional_print_error("Failed to process: %s", error_msg)
    """
    _conditional_print_base("ERROR", message, *args, **kwargs)


def conditional_print_debug(message: str, *args, **kwargs):
    """
    Print DEBUG level message to console if enabled in logging.yml configuration.

    Args:
        message: The message to print (can contain format placeholders)
        *args: Arguments to format the message
        **kwargs: Keyword arguments (currently unused, for future compatibility)

    Example:
        conditional_print_debug("Variable value: %s", debug_var)
    """
    _conditional_print_base("DEBUG", message, *args, **kwargs)


def conditional_print_critical(message: str, *args, **kwargs):
    """
    Print CRITICAL level message to console if enabled in logging.yml configuration.

    Args:
        message: The message to print (can contain format placeholders)
        *args: Arguments to format the message
        **kwargs: Keyword arguments (currently unused, for future compatibility)

    Example:
        conditional_print_critical("System failure: %s", critical_error)
    """
    _conditional_print_base("CRITICAL", message, *args, **kwargs)


