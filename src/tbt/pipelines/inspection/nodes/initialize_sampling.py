"""Node: initialize_sampling_data — pre-inspection sampling data initialization."""

import logging
import os
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)

_SCRIPTS_DIR = Path(__file__).parents[5] / "scripts"


def initialize_sampling_data(init_done: bool, tbt_options: dict) -> bool:
    """Creates sampling parquet files in ``data/tbt/sampling/`` by running
    the database fetching script.

    The script reads route data from the database using pipeline_id from tbt_options.
    Falls back to GeoJSON file if pipeline_id is not provided.

    Depends on ``tbt_init_done`` (output of ``initialize_inspection_data``) to
    ensure correct execution order.

    :param init_done: Flag produced by the ``initialize_inspection_data`` node.
    :param tbt_options: Dictionary containing pipeline configuration including pipeline_id.
    :return: ``True`` when the script has been executed successfully.
    :raises FileNotFoundError: If the script is missing or data source not found.
    :raises RuntimeError: If the script exits with a non-zero return code.
    """
    sample_id = tbt_options.get("sample_id", "")
    pipeline_id = tbt_options.get("pipeline_id")

    # If pipeline_id is provided, use it as sample_id (they are the same)
    if pipeline_id:
        sample_id = pipeline_id

    if pipeline_id:
        # Use database source
        log.info("Using database source for sampling data with pipeline_id/sample_id: %s", sample_id)
        script_path = _SCRIPTS_DIR / "generate_sampling_from_db.py"

        if not script_path.exists():
            raise FileNotFoundError(f"Database sampling script not found: {script_path}")

        log.info("Running generate_sampling_from_db.py with sample_id=%s ...", sample_id)
        result = subprocess.run(
            ["python3", str(script_path), sample_id],
            capture_output=True,
            text=True,
        )

        if result.stdout:
            log.info(result.stdout.strip())
        if result.stderr:
            log.warning(result.stderr.strip())

        if result.returncode != 0:
            raise RuntimeError(
                f"Database sampling script failed with exit code {result.returncode}.\n"
                f"stderr: {result.stderr}"
            )
    else:
        # Fallback to GeoJSON file (legacy mode)
        log.info("Using GeoJSON file for sampling data (legacy mode)")
        geojson_path = Path(__file__).parents[5] / "li_input" / "geojson" / "Routes2check.geojson"

        if not geojson_path.exists():
            raise FileNotFoundError(
                f"GeoJSON source file not found: {geojson_path}\n"
                "Run scripts/convert_routes2check_to_geojson.py first or provide pipeline_id."
            )

        sampling_scripts = [
            "generate_sampling_samples.py",
            "generate_sampling_metadata.py",
        ]

        for script_name in sampling_scripts:
            script_path = _SCRIPTS_DIR / script_name
            if not script_path.exists():
                raise FileNotFoundError(f"Sampling script not found: {script_path}")

            log.info("Running %s ...", script_name)
            result = subprocess.run(
                ["python3", str(script_path)],
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

