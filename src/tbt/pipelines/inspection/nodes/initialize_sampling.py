"""Node: initialize_sampling_data — pre-inspection sampling data initialization."""

import logging
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)

_SCRIPTS_DIR = Path(__file__).parents[5] / "scripts"
_GEOJSON_PATH = Path(__file__).parents[5] / "li_input" / "geojson" / "Routes2check.geojson"

_SAMPLING_SCRIPTS = [
    "generate_sampling_samples.py",
    "generate_sampling_metadata.py",
]


def initialize_sampling_data(init_done: bool) -> bool:
    """Creates sampling parquet files in ``data/tbt/sampling/`` by running
    ``scripts/generate_sampling_*.py`` scripts.

    Both scripts read route data from
    ``li_input/geojson/Routes2check.geojson``.

    Depends on ``tbt_init_done`` (output of ``initialize_inspection_data``) to
    ensure correct execution order.

    :param init_done: Flag produced by the ``initialize_inspection_data`` node.
    :return: ``True`` when all scripts have been executed successfully.
    :raises FileNotFoundError: If the GeoJSON source file or a script is missing.
    :raises RuntimeError: If any script exits with a non-zero return code.
    """
    if not _GEOJSON_PATH.exists():
        raise FileNotFoundError(
            f"GeoJSON source file not found: {_GEOJSON_PATH}\n"
            "Run scripts/convert_routes2check_to_geojson.py first."
        )

    for script_name in _SAMPLING_SCRIPTS:
        script_path = _SCRIPTS_DIR / script_name
        if not script_path.exists():
            raise FileNotFoundError(
                f"Sampling script not found: {script_path}"
            )

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

