"""Node: clean_data_directories — pre-inspection cleanup."""

import logging
import shutil
from pathlib import Path

log = logging.getLogger(__name__)

_DIRS_TO_CLEAN = [
    "data/tbt/inspection",
    "data/tbt/sampling",
]


def clean_data_directories(tbt_options: dict) -> bool:
    """Removes all files and subdirectories inside the inspection and sampling data folders.

    Directories themselves are preserved; only their contents are deleted.
    The cleanup is skipped when ``tbt_options["skip_cleanup"]`` is ``True``
    (defaults to ``False`` when the key is absent).

    :param tbt_options: Options provided to the pipeline through
        ``conf/base/parameters/tbt.yml``.
    :return: ``True`` when cleanup succeeded (or was skipped).
    """
    if tbt_options.get("skip_cleanup", False):
        log.info("Cleanup skipped (skip_cleanup=True).")
        return True

    for dir_path_str in _DIRS_TO_CLEAN:
        dir_path = Path(dir_path_str)
        if not dir_path.exists():
            log.warning("Directory does not exist, skipping: %s", dir_path)
            continue

        removed_count = 0
        for child in dir_path.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
            removed_count += 1

        log.info("Cleaned %d item(s) from %s", removed_count, dir_path)

    return True

