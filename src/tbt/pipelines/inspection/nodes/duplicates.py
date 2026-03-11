"""Node: check_duplicates — pre-inspection validation.
Also contains: deduplicate_stretch — post-inspection deduplication of the stretch column.
"""

import logging
import re

import numpy as np
import pandas as pd
from shapely.wkt import loads

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


def find_similar_linestrings_all(
    df: pd.DataFrame,
    max_distance: float = 0.01,
    max_point_diff: int = 2,
) -> pd.DataFrame:
    """Find groups of similar LINESTRINGs using Hausdorff distance.

    Rows that belong to a group receive a ``group_id`` >= 0.
    Ungrouped rows get ``group_id`` == -1.
    The result is sorted by ``group_id`` descending (groups first).

    :param df: DataFrame containing a ``stretch_rounded`` column with WKT LINESTRING values.
    :param max_distance: Maximum Hausdorff distance between two geometries to be considered similar.
    :param max_point_diff: Maximum allowed difference in the number of coordinate points.
    :return: Copy of ``df`` with added ``group_id`` and ``point_count`` columns.
    """
    n = len(df)
    geometries = []
    point_counts = []

    for _, row in df.iterrows():
        geom = loads(row["stretch_rounded"])
        geometries.append(geom)
        point_counts.append(len(geom.coords))

    df_result = df.copy()
    df_result["group_id"] = -1
    df_result["point_count"] = point_counts

    group_counter = 0
    used: set = set()

    for i in range(n):
        if i in used:
            continue

        similar_indices = [i]

        for j in range(i + 1, n):
            if j in used:
                continue

            if abs(point_counts[i] - point_counts[j]) > max_point_diff:
                continue

            hausdorff_dist = geometries[i].hausdorff_distance(geometries[j])
            if hausdorff_dist <= max_distance:
                similar_indices.append(j)

        if len(similar_indices) > 1:
            for idx in similar_indices:
                df_result.iloc[idx, df_result.columns.get_loc("group_id")] = group_counter
            used.update(similar_indices)
            group_counter += 1

    return df_result.sort_values("group_id", ascending=False)


def deduplicate_stretch(
    inspection_routes: pd.DataFrame,
    inspection_critical_sections: pd.DataFrame,
    critical_sections_with_mcp_feedback: pd.DataFrame,
    error_logs: pd.DataFrame,
    inspection_metadata: pd.DataFrame,
    tbt_options: dict,
) -> tuple:
    """Deduplicate rows based on the ``stretch`` column in three steps.

    Step 1 — exact deduplication:
        ``drop_duplicates(subset=['stretch'])``

    Step 2 — rounded-coordinates deduplication:
        Creates a temporary ``stretch_rounded`` column via :func:`round_coordinates`
        (coordinates rounded to 2 decimal places) and calls
        ``drop_duplicates(subset=['stretch_rounded'])``.

    Step 3 — near-duplicate geometry deduplication:
        Uses :func:`find_similar_linestrings_all` (Hausdorff distance ≤ 0.005,
        max point diff = 1) to find groups of nearly-identical LINESTRINGs,
        keeps the first record from each group and all ungrouped records.
        Temporary columns ``stretch_rounded``, ``group_id`` and ``point_count``
        are dropped afterwards.

    All three steps are applied to:
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
        removed = before - len(df)
        log.info("Deduplication '%s' [rounded]: removed %d rows (%d → %d).", name, removed, before, len(df))
        conditional_print(f"Deduplication '{name}' [rounded]: removed {removed} rows ({before} → {len(df)}).")

        # Step 3: near-duplicate geometry deduplication via Hausdorff distance
        before = len(df)
        similar_groups = find_similar_linestrings_all(df, max_distance=0.005, max_point_diff=1)
        ungrouped = similar_groups[similar_groups["group_id"] == -1]
        grouped_first = (
            similar_groups[similar_groups["group_id"] != -1]
            .groupby("group_id")
            .first()
            .reset_index()
        )
        df = (
            pd.concat([ungrouped, grouped_first])
            .sort_values("group_id", ascending=False)
            .reset_index(drop=True)
        )
        df = df.drop(columns=["stretch_rounded", "group_id", "point_count"], errors="ignore")
        removed = before - len(df)
        log.info("Deduplication '%s' [hausdorff]: removed %d rows (%d → %d).", name, removed, before, len(df))
        conditional_print(f"Deduplication '{name}' [hausdorff]: removed {removed} rows ({before} → {len(df)}).")

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

