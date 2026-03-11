"""Node: check_duplicates — pre-inspection validation.
Also contains: deduplicate_stretch — post-inspection deduplication of the stretch column.
"""

import logging

import pandas as pd

from tbt.utils.console_print import conditional_print

log = logging.getLogger(__name__)


def deduplicate_stretch(
    inspection_routes: pd.DataFrame,
    inspection_critical_sections: pd.DataFrame,
    critical_sections_with_mcp_feedback: pd.DataFrame,
    error_logs: pd.DataFrame,
    inspection_metadata: pd.DataFrame,
    tbt_options: dict,
) -> tuple:
    """Deduplicate rows based on the ``stretch`` column.

    Applies ``DataFrame.drop_duplicates(subset=['stretch'])`` to DataFrames
    that contain the ``stretch`` column:
    - ``inspection_critical_sections``
    - ``critical_sections_with_mcp_feedback``
    - ``error_logs``

    DataFrames without the ``stretch`` column (``inspection_routes``,
    ``inspection_metadata``) are passed through unchanged.

    :param inspection_routes: Routes DataFrame.
    :param inspection_critical_sections: Critical sections DataFrame.
    :param critical_sections_with_mcp_feedback: Critical sections with MCP feedback DataFrame.
    :param error_logs: Error logs DataFrame.
    :param inspection_metadata: Inspection metadata DataFrame.
    :param tbt_options: Pipeline options dict (unused; kept for pipeline signature consistency).
    :return: Tuple of (deduplicated) DataFrames in the same order as the inputs (without tbt_options).
    """
    def _drop_duplicates_stretch(df: pd.DataFrame, name: str) -> pd.DataFrame:
        if "stretch" not in df.columns:
            log.warning("Column 'stretch' not found in '%s' — skipping deduplication.", name)
            return df
        before = len(df)
        df = df.drop_duplicates(subset=["stretch"])
        removed = before - len(df)
        log.info("Deduplication '%s': removed %d duplicate stretch rows (%d → %d).", name, removed, before, len(df))
        conditional_print(f"Deduplication '{name}': removed {removed} duplicate stretch rows ({before} → {len(df)}).")
        return df

    inspection_critical_sections = _drop_duplicates_stretch(
        inspection_critical_sections, "inspection_critical_sections"
    )
    critical_sections_with_mcp_feedback = _drop_duplicates_stretch(
        critical_sections_with_mcp_feedback, "critical_sections_with_mcp_feedback"
    )
    error_logs = _drop_duplicates_stretch(error_logs, "error_logs")

    return (
        inspection_routes,
        inspection_critical_sections,
        critical_sections_with_mcp_feedback,
        error_logs,
        inspection_metadata,
    )



def check_duplicates(tbt_options: dict, inspection_metadata: pd.DataFrame) -> bool:
    """Checks for duplicate entries in the inspection metadata.

    :param tbt_options: Options provided to the pipeline through ``conf/base/parameters/tbt.yml``,
        with at least the following fields: ``sample_id``, ``provider``, ``competitor``, ``endpoint``, ``product``
    :param inspection_metadata: DataFrame containing metadata of the inspections.
    :raises ValueError: If a duplicate entry is found when ``avoid_duplicates`` is True.
    :return: True if no duplicates are found or if ``avoid_duplicates`` is False.
    """
    if not tbt_options["avoid_duplicates"]:
        return True

    sample_id = tbt_options["sample_id"]
    provider = tbt_options["provider"]
    competitor = tbt_options["competitor"]
    product = tbt_options["product"]

    duplicates = (
        (inspection_metadata["provider"] == provider)
        & (inspection_metadata["sample_id"] == sample_id)
        & (inspection_metadata["product"] == product)
        & (inspection_metadata["competitor"] == competitor)
    ).any()

    if duplicates:
        log.info(
            "Inspection already exists for this sample_id, provider, competitor and product"
        )
        conditional_print(
            "Inspection already exists for this sample_id, provider, competitor and product"
        )
        raise ValueError(
            "Inspection already exists for this sample_id, provider, competitor and product"
        )

    return True

