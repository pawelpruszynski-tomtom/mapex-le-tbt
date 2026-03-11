"""Node: check_duplicates — pre-inspection validation.
Also contains: deduplicate_stretch — post-inspection deduplication of the stretch column.
"""

import logging
import re

import pandas as pd

from tbt.utils.console_print import conditional_print

log = logging.getLogger(__name__)


def round_coordinates(stretch_str: str) -> str:
    """Round coordinates in a WKT LINESTRING to 2 decimal places.

    :param stretch_str: WKT LINESTRING string, e.g. ``'LINESTRING (1.23456 4.56789, ...)'``.
    :return: WKT LINESTRING with coordinates rounded to 2 decimal places.
    """
    match = re.search(r'LINESTRING \((.*)\)', stretch_str)
    if not match:
        return stretch_str

    coords_str = match.group(1)
    coord_pairs = coords_str.split(', ')

    rounded_coords = []
    for pair in coord_pairs:
        lon, lat = map(float, pair.split())
        rounded_coords.append(f"{round(lon, 2)} {round(lat, 2)}")

    return f"LINESTRING ({', '.join(rounded_coords)})"


def deduplicate_stretch(
    inspection_routes: pd.DataFrame,
    inspection_critical_sections: pd.DataFrame,
    critical_sections_with_mcp_feedback: pd.DataFrame,
    error_logs: pd.DataFrame,
    inspection_metadata: pd.DataFrame,
    tbt_options: dict,
) -> tuple:
    """Deduplicate rows based on the ``stretch`` column and then on ``stretch_rounded``.

    Step 1 — exact deduplication:
        Applies ``DataFrame.drop_duplicates(subset=['stretch'])`` to every
        DataFrame that contains the ``stretch`` column.

    Step 2 — rounded-coordinates deduplication:
        Creates a temporary ``stretch_rounded`` column by applying
        :func:`round_coordinates` (coordinates rounded to 2 decimal places)
        and calls ``drop_duplicates(subset=['stretch_rounded'])``.
        The temporary column is dropped afterwards.

    Both steps are applied to:
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

        # Step 1: exact deduplication on 'stretch'
        before = len(df)
        df = df.drop_duplicates(subset=["stretch"])
        removed = before - len(df)
        log.info("Deduplication '%s' [exact]: removed %d rows (%d → %d).", name, removed, before, len(df))
        conditional_print(f"Deduplication '{name}' [exact]: removed {removed} rows ({before} → {len(df)}).")

        # Step 2: rounded-coordinates deduplication on 'stretch_rounded'
        before = len(df)
        df["stretch_rounded"] = df["stretch"].apply(round_coordinates)
        df = df.drop_duplicates(subset=["stretch_rounded"])
        df = df.drop(columns=["stretch_rounded"])
        removed = before - len(df)
        log.info("Deduplication '%s' [rounded]: removed %d rows (%d → %d).", name, removed, before, len(df))
        conditional_print(f"Deduplication '{name}' [rounded]: removed {removed} rows ({before} → {len(df)}).")

        return df.reset_index(drop=True)

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

