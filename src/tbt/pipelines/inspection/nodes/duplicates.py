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
    """Deduplicate rows based on the ``stretch`` column using multiple strategies.

    Strategies are applied in order and are controlled by ``tbt_options["deduplication"]``:
      - ``"first"``       — keep the first occurrence of each stretch value (default).
      - ``"last"``        — keep the last occurrence.
      - ``"min_length"``  — keep the row with the minimum ``provider_route_length`` per stretch.
      - ``"max_length"``  — keep the row with the maximum ``provider_route_length`` per stretch.

    The active strategy is read from ``tbt_options.get("deduplication", {}).get("strategy", "first")``.

    :param inspection_routes: Routes DataFrame.
    :param inspection_critical_sections: Critical sections DataFrame.
    :param critical_sections_with_mcp_feedback: Critical sections with MCP feedback DataFrame.
    :param error_logs: Error logs DataFrame.
    :param inspection_metadata: Inspection metadata DataFrame.
    :param tbt_options: Pipeline options dict.
    :return: Tuple of deduplicated DataFrames in the same order as the inputs (without tbt_options).
    """
    dedup_cfg = tbt_options.get("deduplication", {})
    strategy = dedup_cfg.get("strategy", "first")

    log.info("Running stretch deduplication with strategy='%s'", strategy)
    conditional_print(f"Deduplication: strategy='{strategy}'")

    # NOTE: Replace the bodies of the helpers below with the functions you provide.
    inspection_routes = _dedup_routes(inspection_routes, strategy)
    inspection_critical_sections = _dedup_by_stretch(inspection_critical_sections, strategy)
    critical_sections_with_mcp_feedback = _dedup_by_stretch(
        critical_sections_with_mcp_feedback, strategy
    )

    return (
        inspection_routes,
        inspection_critical_sections,
        critical_sections_with_mcp_feedback,
        error_logs,
        inspection_metadata,
    )


# ---------------------------------------------------------------------------
# Internal helpers — replace these bodies with the functions you provide
# ---------------------------------------------------------------------------

def _dedup_routes(df: pd.DataFrame, strategy: str) -> pd.DataFrame:
    """Deduplicate *inspection_routes* on the ``stretch`` column.

    This is a placeholder — replace with the real implementation.
    """
    if "stretch" not in df.columns:
        log.warning("Column 'stretch' not found in inspection_routes — skipping deduplication.")
        return df

    if strategy in ("first", "last"):
        return df.drop_duplicates(subset=["stretch"], keep=strategy)
    elif strategy == "min_length":
        idx = df.groupby("stretch")["provider_route_length"].idxmin()
        return df.loc[idx].reset_index(drop=True)
    elif strategy == "max_length":
        idx = df.groupby("stretch")["provider_route_length"].idxmax()
        return df.loc[idx].reset_index(drop=True)
    else:
        log.warning("Unknown deduplication strategy '%s' — skipping.", strategy)
        return df


def _dedup_by_stretch(df: pd.DataFrame, strategy: str) -> pd.DataFrame:
    """Deduplicate a DataFrame on the ``stretch`` column (keep first/last).

    This is a placeholder — replace with the real implementation.
    """
    if "stretch" not in df.columns:
        log.warning("Column 'stretch' not found — skipping deduplication for this DataFrame.")
        return df

    keep = strategy if strategy in ("first", "last") else "first"
    return df.drop_duplicates(subset=["stretch"], keep=keep).reset_index(drop=True)


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

