"""Nodes: export_to_csv, export_to_spark, export_to_sql — data export."""

import logging
import os

import pandas as pd
import pyspark.sql
import pyspark.sql.types as T

log = logging.getLogger(__name__)


def export_to_sql(
    inspection_routes: pd.DataFrame,
    inspection_critical_sections: pd.DataFrame,
    critical_sections_with_mcp_feedback: pd.DataFrame,
    error_logs: pd.DataFrame,
    inspection_metadata: pd.DataFrame,
):
    """Dummy function to export to SQL (pass-through for Kedro catalog)."""
    return (
        inspection_routes,
        inspection_critical_sections,
        critical_sections_with_mcp_feedback,
        error_logs,
        inspection_metadata,
        True,
    )


def export_to_csv(
    inspection_routes: pd.DataFrame,
    inspection_critical_sections: pd.DataFrame,
    critical_sections_with_mcp_feedback: pd.DataFrame,
    error_logs: pd.DataFrame,
    inspection_metadata: pd.DataFrame,
    output_dir: str = "output",
):
    """Export all inspection DataFrames to CSV files."""
    os.makedirs(output_dir, exist_ok=True)

    inspection_routes.to_csv(
        os.path.join(output_dir, "inspection_routes.csv"), index=False
    )
    inspection_critical_sections.to_csv(
        os.path.join(output_dir, "inspection_critical_sections.csv"), index=False
    )
    critical_sections_with_mcp_feedback.to_csv(
        os.path.join(output_dir, "critical_sections_with_mcp_feedback.csv"), index=False
    )
    error_logs.to_csv(
        os.path.join(output_dir, "error_logs.csv"), index=False
    )
    inspection_metadata.to_csv(
        os.path.join(output_dir, "inspection_metadata.csv"), index=False
    )

    return (
        inspection_routes,
        inspection_critical_sections,
        critical_sections_with_mcp_feedback,
        error_logs,
        inspection_metadata,
        True,
    )


def export_to_spark(
    inspection_routes: pd.DataFrame,
    inspection_critical_sections: pd.DataFrame,
    critical_sections_with_mcp_feedback: pd.DataFrame,
    error_logs: pd.DataFrame,
    inspection_metadata: pd.DataFrame,
):
    """Convert Pandas DataFrames to Spark DataFrames for data lake export via Kedro catalog."""
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    return (
        spark.createDataFrame(
            inspection_routes,
            schema=T.StructType(
                [
                    T.StructField("route_id", T.StringType()),
                    T.StructField("country", T.StringType()),
                    T.StructField("sample_id", T.StringType()),
                    T.StructField("competitor", T.StringType()),
                    T.StructField("provider_route", T.StringType()),
                    T.StructField("provider_route_time", T.FloatType()),
                    T.StructField("provider_route_length", T.FloatType()),
                    T.StructField("origin", T.StringType()),
                    T.StructField("destination", T.StringType()),
                    T.StructField("provider", T.StringType()),
                    T.StructField("competitor_route", T.StringType()),
                    T.StructField("competitor_route_length", T.FloatType()),
                    T.StructField("competitor_route_time", T.FloatType()),
                    T.StructField("rac_state", T.StringType()),
                    T.StructField("run_id", T.StringType()),
                ]
            ),
        ),
        spark.createDataFrame(
            inspection_critical_sections,
            schema=T.StructType(
                [
                    T.StructField("route_id", T.StringType()),
                    T.StructField("case_id", T.StringType()),
                    T.StructField("stretch", T.StringType()),
                    T.StructField("stretch_length", T.FloatType()),
                    T.StructField("fcd_state", T.StringType()),
                    T.StructField("pra", T.FloatType()),
                    T.StructField("prb", T.FloatType()),
                    T.StructField("prab", T.FloatType()),
                    T.StructField("lift", T.FloatType()),
                    T.StructField("tot", T.IntegerType()),
                    T.StructField("reference_case_id", T.StringType()),
                    T.StructField("run_id", T.StringType()),
                ]
            ),
        ),
        spark.createDataFrame(
            critical_sections_with_mcp_feedback,
            schema=T.StructType(
                [
                    T.StructField("run_id", T.StringType()),
                    T.StructField("route_id", T.StringType()),
                    T.StructField("case_id", T.StringType()),
                    T.StructField("mcp_state", T.StringType()),
                    T.StructField("reference_case_id", T.StringType()),
                    T.StructField("error_subtype", T.StringType()),
                ]
            ),
        ),
        spark.createDataFrame(
            error_logs,
            schema=T.StructType(
                [
                    T.StructField("run_id", T.StringType()),
                    T.StructField("route_id", T.StringType()),
                    T.StructField("case_id", T.StringType()),
                    T.StructField("stretch", T.StringType()),
                    T.StructField("provider_route", T.StringType()),
                    T.StructField("competitor_route", T.StringType()),
                    T.StructField("country", T.StringType()),
                    T.StructField("provider", T.StringType()),
                    T.StructField("competitor", T.StringType()),
                    T.StructField("product", T.StringType()),
                ]
            ),
        ),
        spark.createDataFrame(
            inspection_metadata,
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
                    T.StructField("mcp_tasks", T.StringType()),
                    T.StructField("completed", T.StringType()),
                    T.StructField("inspection_date", T.DateType()),
                    T.StructField("comment", T.StringType()),
                    T.StructField("rac_elapsed_time", T.FloatType()),
                    T.StructField("fcd_elapsed_time", T.FloatType()),
                    T.StructField("total_elapsed_time", T.FloatType()),
                    T.StructField("api_calls", T.StringType()),
                    T.StructField("sanity_fail", T.BooleanType()),
                    T.StructField("sanity_msg", T.StringType()),
                ]
            ),
        ),
        True,
    )

