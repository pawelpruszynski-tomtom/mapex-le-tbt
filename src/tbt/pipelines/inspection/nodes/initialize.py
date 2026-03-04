"""Node: initialize_inspection_data — pre-inspection data initialization."""

import logging
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)

_SCRIPTS_DIR = Path(__file__).parents[5] / "scripts"

_INIT_SCRIPTS = [
    "generate_empty_inspection_routes.py",
    "generate_empty_inspection_metadata.py",
    "generate_empty_inspection_critical_sections.py",
    "generate_empty_inspection_critical_sections_with_mcp_feedback.py",
]


def initialize_inspection_data(cleanup_done: bool) -> bool:
    """Creates empty parquet files in ``data/tbt/inspection/`` by running
    ``scripts/generate_empty_inspection_*.py`` scripts via PySpark.

    Depends on ``tbt_cleanup_done`` (output of ``clean_data_directories``) to
    ensure this step always runs after the directory has been cleared.

    :param cleanup_done: Flag produced by the ``clean_data_directories`` node.
    :return: ``True`` when all scripts have been executed successfully.
    :raises RuntimeError: If any of the initialization scripts exits with a
        non-zero return code.
    """
    for script_name in _INIT_SCRIPTS:
        script_path = _SCRIPTS_DIR / script_name
        if not script_path.exists():
            raise FileNotFoundError(
                f"Initialization script not found: {script_path}"
            )

        log.info("Running %s ...", script_name)
        result = subprocess.run(
            ["python", str(script_path)],
            capture_output=True,
            text=True,
        )

        if result.stdout:
            log.info(result.stdout.strip())
        if result.stderr:
            log.warning(result.stderr.strip())

        if result.returncode != 0:
            raise RuntimeError(
                f"Script {script_name} failed with exit code {result.returncode}.\n"
                f"stderr: {result.stderr}"
            )

    return True

