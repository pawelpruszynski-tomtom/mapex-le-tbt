"""Utility module for conditional console printing alongside logging."""

import os
import sys


def _is_console_prints_enabled() -> bool:
    """
    Check if console prints are enabled via the ENABLE_CONSOLE_PRINTS env variable.

    Reads the ``ENABLE_CONSOLE_PRINTS`` environment variable (set in the root
    ``.env`` file).  Any value other than ``"false"``, ``"0"`` or ``"no"``
    (case-insensitive) is treated as *enabled*.  Defaults to ``True`` when the
    variable is not set at all.
    """
    raw = os.environ.get("ENABLE_CONSOLE_PRINTS", "true").strip().lower()
    return raw not in ("false", "0", "no")


def _conditional_print_base(prefix: str, message: str, *args, **kwargs):
    """
    Base function for conditional printing with level prefix.

    Writes directly to ``sys.__stdout__`` (the original stdout, bypassing any
    redirection that Celery or Kedro may apply to ``sys.stdout``) and flushes
    immediately so the line appears in docker-compose logs without delay.

    Args:
        prefix: Level prefix (e.g., "INFO", "WARNING", "ERROR")
        message: The message to print (can contain format placeholders)
        *args: Arguments to format the message
        **kwargs: Keyword arguments (currently unused, for future compatibility)
    """
    if not _is_console_prints_enabled():
        return

    if args:
        try:
            formatted_message = message % args
        except (TypeError, ValueError):
            formatted_message = str(message) + " " + " ".join(str(arg) for arg in args)
    else:
        formatted_message = str(message)

    line = f"[{prefix}] {formatted_message}" if prefix else formatted_message

    # Use sys.__stdout__ to bypass any Celery/Kedro stdout redirection.
    # Fall back to sys.stdout if __stdout__ is None (e.g. in some test envs).
    out = sys.__stdout__ or sys.stdout
    out.write(line + "\n")
    out.flush()


def conditional_print(message: str, *args, **kwargs):
    """
    Print INFO level message to console if enabled via ENABLE_CONSOLE_PRINTS env variable.

    This function checks the ``ENABLE_CONSOLE_PRINTS`` environment variable
    (defined in the root ``.env`` file) and prints the message only if it is
    set to a truthy value (anything other than ``false``, ``0`` or ``no``).

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
    Print WARNING level message to console if enabled via ENABLE_CONSOLE_PRINTS env variable.

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
    Print ERROR level message to console if enabled via ENABLE_CONSOLE_PRINTS env variable.

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
    Print DEBUG level message to console if enabled via ENABLE_CONSOLE_PRINTS env variable.

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
    Print CRITICAL level message to console if enabled via ENABLE_CONSOLE_PRINTS env variable.

    Args:
        message: The message to print (can contain format placeholders)
        *args: Arguments to format the message
        **kwargs: Keyword arguments (currently unused, for future compatibility)

    Example:
        conditional_print_critical("System failure: %s", critical_error)
    """
    _conditional_print_base("CRITICAL", message, *args, **kwargs)


