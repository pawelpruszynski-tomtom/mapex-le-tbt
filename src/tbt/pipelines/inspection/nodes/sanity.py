"""Nodes: sanity_check, raise_sanity_error — output validation."""

import json
import logging

import pandas as pd
import pyspark.sql
import shapely.wkt
from pyspark.sql.functions import col

import tbt.navutils.common.decorators as decorators
from tbt.navutils.navutils.sanity_checks import SanityCheckInspectionTBT

log = logging.getLogger(__name__)


@decorators.timing
def sanity_check(
    inspection_routes: pd.DataFrame,
    inspection_critical_sections: pd.DataFrame,
    critical_sections_with_mcp_feedback: pd.DataFrame,
    error_logs: pd.DataFrame,
    inspection_metadata: pd.DataFrame,
    tbt_sampling_samples: pyspark.sql.DataFrame,
    tbt_options: dict,
    sample_metric: str,
):
    """Run all sanity checks on the inspection output."""
    sample_id = tbt_options["sample_id"]
    sampled_routes = tbt_sampling_samples.filter(col("sample_id") == sample_id).count()

    sanity_result = {}
    routes = inspection_routes.copy()
    routes["provider_route"] = [shapely.wkt.loads(x) for x in routes.provider_route]
    routes["competitor_route"] = [shapely.wkt.loads(x) for x in routes.competitor_route]
    sanity = SanityCheckInspectionTBT(routes_df=routes)

    if sample_metric == "HDR":
        routes_length_ceiling = 1_000_000  # 1000 km for HDR
    else:
        routes_length_ceiling = 10_000  # 10 km for TbT

    sanity.run_checks(
        sampled_routes=sampled_routes,
        geom_col="provider_route",
        len_col="provider_route_length",
        time_col="provider_route_time",
        routes_length_ceiling=routes_length_ceiling,
        routes_length_ceiling_ratio=0.20,
        missing_routes_ratio=0.05,
    )
    sanity_result["provider"] = {"warnings": sanity.warnings, "info": sanity.info}
    sanity.reset_output()

    sanity.run_checks(
        sampled_routes=sampled_routes,
        geom_col="competitor_route",
        len_col="competitor_route_length",
        time_col="competitor_route_time",
        routes_length_ceiling=routes_length_ceiling,
        routes_length_ceiling_ratio=0.20,
        missing_routes_ratio=0.05,
    )
    sanity_result["competitor"] = {"warnings": sanity.warnings, "info": sanity.info}
    sanity.reset_output()

    sanity.run_competitor_relative_checks(
        provider_length_col="provider_route_length",
        provider_time_col="provider_route_time",
        competitor_length_col="competitor_route_length",
        competitor_time_col="competitor_route_time",
    )
    sanity_result["provider_vs_competitor"] = {"warnings": sanity.warnings}
    sanity.reset_output()

    # Save information
    inspection_metadata["sanity_fail"] = (
        any(sanity_result["provider"]["warnings"])
        or any(sanity_result["competitor"]["warnings"])
        or any(sanity_result["provider_vs_competitor"]["warnings"])
    )
    inspection_metadata["sanity_msg"] = json.dumps(sanity_result)

    return (
        inspection_routes,
        inspection_critical_sections,
        critical_sections_with_mcp_feedback,
        error_logs,
        inspection_metadata,
    )


def raise_sanity_error(
    export_to_spark_ok: bool,
    export_to_sql_ok: bool,
    export_to_database_ok: bool,
    inspection_metadata: pd.DataFrame,
) -> bool:
    """Validate that sanity checks passed. Raises ``AssertionError`` on failure.

    :param export_to_spark_ok: Dummy input ensuring export_to_spark ran first.
    :param export_to_sql_ok: Dummy input ensuring export_to_sql ran first.
    :param export_to_database_ok: Dummy input ensuring export_to_database ran first.
    :param inspection_metadata: Metadata with sanity results.
    :return: False if no error is found.
    """
    if (
        inspection_metadata["sanity_fail"].values[0]
        and export_to_spark_ok
        and export_to_sql_ok
        and export_to_database_ok
    ):
        log.info(
            "Result of sanity checks: Failed. Please check the output of this run carefully"
        )

    assert not inspection_metadata["sanity_fail"].values[0], "Sanity check failed"
    return False

