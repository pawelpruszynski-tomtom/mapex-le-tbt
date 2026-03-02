"""Node: merge_inspection_data — aggregate all inspection outputs."""

import logging

import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import col, lit

import tbt.navutils.common.decorators as decorators
from tbt.pipelines.inspection.domain.metadata import build_inspection_metadata_record

log = logging.getLogger(__name__)


@decorators.timing
def merge_inspection_data(
    tbt_options: dict,
    run_id: str,
    provider_routes_known: pyspark.sql.DataFrame,
    inspection_critical_sections_known: pyspark.sql.DataFrame,
    routes: pyspark.sql.DataFrame,
    tbt_critical_sections_with_fcd_state: pyspark.sql.DataFrame,
    tbt_provider_routes_node_time: float,
    tbt_competitor_routes_node_time: float,
    tbt_reuse_static_routes_node_time: float,
    tbt_get_rac_state_api_calls: dict,
    tbt_get_rac_state_time: float,
    tbt_get_fcd_state_time: float,
):
    """Wrapper function to prepare the output tables."""

    # --- Inspection routes ---
    inspection_routes = (
        routes.select(
            "route_id", "country", "sample_id", "competitor",
            "provider_route", "provider_route_time", "provider_route_length",
            "origin", "destination", "provider",
            "competitor_route", "competitor_route_length", "competitor_route_time",
            "rac_state",
        )
        .union(
            provider_routes_known.select(
                "route_id", "country", "sample_id", "competitor",
                "provider_route", "provider_route_time", "provider_route_length",
                "origin", "destination", "provider",
                "competitor_route", "competitor_route_length", "competitor_route_time",
                "rac_state",
            )
        )
        .withColumn("run_id", lit(run_id))
    ).cache()
    log.info(
        "Aggregated %i inspection routes (%i new)",
        inspection_routes.count(),
        routes.count(),
    )

    # --- Inspection critical sections ---
    inspection_critical_sections = (
        tbt_critical_sections_with_fcd_state.withColumn("mcp_state", lit(None))
        .withColumn("error_subtype", lit(None))
        .withColumn("reference_case_id", lit(None))
        .select(
            "route_id", "case_id", "stretch", "stretch_length",
            "fcd_state", "pra", "prb", "prab", "lift", "tot",
            "reference_case_id", "mcp_state", "error_subtype",
        )
        .union(
            inspection_critical_sections_known.select(
                "route_id", "case_id", "stretch", "stretch_length",
                "fcd_state", "pra", "prb", "prab", "lift", "tot",
                "reference_case_id", "mcp_state", "error_subtype",
            )
        )
        .withColumn("run_id", lit(run_id))
    ).cache()

    log.info(
        "Aggregated %i critical sections (%i new)",
        inspection_critical_sections.count(),
        tbt_critical_sections_with_fcd_state.count(),
    )

    # --- Critical sections with MCP feedback ---
    tmp_filter_condition = (
        F.col("mcp_state").isNotNull() & (F.col("mcp_state") != "MBP")
    ) | ((F.col("mcp_state") == "MBP") & (F.col("error_subtype") == "NO-TEMP"))

    critical_sections_with_mcp_feedback = inspection_critical_sections.filter(
        tmp_filter_condition
    ).select(
        "run_id", "route_id", "case_id", "mcp_state", "reference_case_id", "error_subtype",
    )

    # --- Error logs ---
    error_logs = (
        inspection_critical_sections.filter(col("mcp_state").isNull())
        .filter(
            condition=~tmp_filter_condition | (F.col("fcd_state") == "potential_error")
        )
        .select("run_id", "case_id", "route_id", "stretch")
    )
    error_logs = (
        error_logs.join(
            inspection_routes.select(
                "run_id", "route_id",
                "provider_route", "competitor_route",
                "country", "provider", "competitor",
            ),
            on=["run_id", "route_id"],
            how="left",
        )
        .withColumn("product", lit(tbt_options["product"]))
        .cache()
    )
    log.info("Generated %i mcp tasks", error_logs.count())

    # --- Inspection metadata ---
    country = inspection_routes.select("country").take(1)[0][0]
    metadata_record = build_inspection_metadata_record(
        run_id=run_id,
        tbt_options=tbt_options,
        country=country,
        mcp_tasks=error_logs.count(),
        tbt_provider_routes_node_time=tbt_provider_routes_node_time,
        tbt_competitor_routes_node_time=tbt_competitor_routes_node_time,
        tbt_reuse_static_routes_node_time=tbt_reuse_static_routes_node_time,
        tbt_get_rac_state_time=tbt_get_rac_state_time,
        tbt_get_fcd_state_time=tbt_get_fcd_state_time,
        tbt_get_rac_state_api_calls=tbt_get_rac_state_api_calls,
    )

    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    inspection_metadata = spark.createDataFrame(
        [metadata_record],
        schema=T.StructType(
            [
                T.StructField("run_id", T.StringType()),
                T.StructField("sample_id", T.StringType()),
                T.StructField("provider", T.StringType()),
                T.StructField("endpoint", T.StringType()),
                T.StructField("mapdate", T.DateType()),
                T.StructField("product", T.StringType()),
                T.StructField("country", T.StringType()),
                T.StructField("mode", T.StringType()),
                T.StructField("competitor", T.StringType()),
                T.StructField("mcp_tasks", T.IntegerType()),
                T.StructField("completed", T.BooleanType()),
                T.StructField("inspection_date", T.DateType()),
                T.StructField("comment", T.StringType()),
                T.StructField("rac_elapsed_time", T.FloatType()),
                T.StructField("fcd_elapsed_time", T.FloatType()),
                T.StructField("total_elapsed_time", T.FloatType()),
                T.StructField("api_calls", T.StringType()),
            ]
        ),
    )

    return (
        inspection_routes.toPandas(),
        inspection_critical_sections.drop("mcp_state", "error_subtype").toPandas(),
        critical_sections_with_mcp_feedback.toPandas(),
        error_logs.toPandas(),
        inspection_metadata.toPandas(),
    )

