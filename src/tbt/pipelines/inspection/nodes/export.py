"""Nodes: export_to_csv, export_to_spark, export_to_sql, export_to_database — data export."""

import logging
import os
from typing import Tuple

import pandas as pd
import pyspark.sql
import pyspark.sql.types as T
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from tbt.utils.console_print import conditional_print, conditional_print_error

log = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


def _wkt_point_to_lat_lon(wkt_point: str) -> str:
    """Convert WKT POINT(lon lat) to 'lat, lon' string format.

    Args:
        wkt_point: WKT string like 'POINT(9.27566 45.43851)'

    Returns:
        String like '45.43851, 9.27566', or original value if parsing fails.
    """
    if not wkt_point or not isinstance(wkt_point, str):
        return wkt_point
    try:
        coords = wkt_point.strip().replace("POINT(", "").replace(")", "")
        lon, lat = coords.split()
        return f"{float(lat)}, {float(lon)}"
    except Exception:
        return wkt_point


def export_to_database(
    inspection_routes: pd.DataFrame,
    inspection_critical_sections: pd.DataFrame,
    critical_sections_with_mcp_feedback: pd.DataFrame,
    error_logs: pd.DataFrame,
    inspection_metadata: pd.DataFrame,
    tbt_options: dict,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, bool]:
    """
    Export all inspection DataFrames to PostgreSQL database.

    Error logs are exported to 'leads' table in JSONB format.

    Database credentials are loaded from environment variables:
    - DB_HOST: Database host
    - DB_PORT: Database port
    - DB_NAME: Database name
    - DB_USER: Database username
    - DB_PASSWORD: Database password
    - DB_SCHEMA: Database schema (optional, defaults to 'public')

    Args:
        inspection_routes: Routes inspection data
        inspection_critical_sections: Critical sections inspection data
        critical_sections_with_mcp_feedback: Critical sections with MCP feedback
        error_logs: Error logs data (exported to leads table)
        inspection_metadata: Inspection metadata
        tbt_options: Pipeline options containing sample_id/pipeline_id

    Returns:
        Tuple of all input DataFrames plus success flag
    """
    try:
        # Get database credentials from environment variables
        db_host = os.getenv("AO_HOST")
        db_port = os.getenv("AO_PORT", "5432")
        db_name = os.getenv("AO_DBNAME")
        db_user = os.getenv("AO_USER")
        db_password = os.getenv("AO_PASSWORD")
        db_schema = os.getenv("AO_SCHEMA", "public")

        # Validate required credentials
        if not all([db_host, db_name, db_user, db_password]):
            raise ValueError(
                "Missing required database credentials. "
                "Please ensure DB_HOST, DB_NAME, DB_USER, and DB_PASSWORD are set in .env file"
            )

        # Create database connection string
        db_sslmode = os.getenv("DB_SSLMODE", "require")
        connection_string = (
            f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode={db_sslmode}"
        )

        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        log.info(f"Connecting to database: {db_host}:{db_port}/{db_name}")
        conditional_print(f"Connecting to database: {db_host}:{db_port}/{db_name}")

        # -----------------------------------------------------------------
        # Idempotency: delete any previously written rows for this
        # pipeline_id BEFORE inserting, so that a Kedro re-run (triggered
        # by a Celery retry / acks_late redelivery) does not produce
        # duplicate records in the database.
        # -----------------------------------------------------------------
        pipeline_id = tbt_options.get("sample_id")  # sample_id = pipeline_id in our system
        log.info("Removing existing rows for pipeline_id=%s to ensure idempotent write...", pipeline_id)
        conditional_print(f"Removing existing rows for pipeline_id={pipeline_id} (idempotent write)...")
        with engine.begin() as conn:
            for table in (
                "errorlogs",
            ):
                conn.execute(
                    text(f"DELETE FROM {db_schema}.{table} WHERE pipeline_id = :pid AND source_type = 'tbt'"),
                    {"pid": pipeline_id},
                )

        # Export each DataFrame to the database
        # Using if_exists='append' to add new data without dropping existing tables

        log.info("Exporting inspection_routes to database...")
        conditional_print("Exporting inspection_routes to database...")
        inspection_routes.to_sql(
            name="inspection_routes",
            con=engine,
            schema=db_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

        log.info("Exporting inspection_critical_sections to database...")
        conditional_print("Exporting inspection_critical_sections to database...")
        inspection_critical_sections.to_sql(
            name="inspection_critical_sections",
            con=engine,
            schema=db_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

        log.info("Exporting critical_sections_with_mcp_feedback to database...")
        conditional_print("Exporting critical_sections_with_mcp_feedback to database...")
        critical_sections_with_mcp_feedback.to_sql(
            name="critical_sections_with_mcp_feedback",
            con=engine,
            schema=db_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

        # Export error_logs to errorlogs table
        log.info("Converting error_logs to errorlogs format...")
        conditional_print("Converting error_logs to errorlogs format...")

        # Fetch 'project' and 'label' from pipelines table
        project_value = None
        location_label = None
        internal_engine = None
        try:
            internal_connection_string = (
                f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
                f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}"
                f"?sslmode={os.getenv('DB_SSLMODE', 'require')}"
            )
            internal_engine = create_engine(internal_connection_string)
            internal_schema = os.getenv("DB_SCHEMA", "public")
            with internal_engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT project, label FROM {internal_schema}.pipelines WHERE id = :pipeline_id"),
                    {"pipeline_id": pipeline_id},
                ).fetchone()
                if result:
                    project_value = result[0]
                    location_label = result[1]
                    log.info(f"Fetched project: {project_value}, location_label (label): {location_label}")
                    conditional_print(f"Fetched project: {project_value}, location_label (label): {location_label}")
                else:
                    log.warning(f"No pipeline found for pipeline_id: {pipeline_id}")
                    conditional_print(f"No pipeline found for pipeline_id: {pipeline_id}")
        except Exception as e:
            log.warning(f"Could not fetch project/label from pipelines table: {e}")
        finally:
            if internal_engine is not None:
                internal_engine.dispose()

        # Prepare errorlogs data — map to errorlogs table schema
        errorlogs_data = []
        for _, row in error_logs.iterrows():
            errorlogs_data.append({
                "case_id": row.get("case_id"),
                "route_id": row.get("route_id"),
                "country": row.get("country"),
                "provider": row.get("provider"),
                "product": row.get("product"),
                "source_type": "tbt",
                "origin": _wkt_point_to_lat_lon(row.get("origin")),
                "destination": _wkt_point_to_lat_lon(row.get("destination")),
                "stretch": row.get("stretch"),
                "run_id": row.get("run_id"),
                "pipeline_id": pipeline_id,
                "project": project_value,
                "location_label": location_label,
            })

        errorlogs_df = pd.DataFrame(errorlogs_data)

        log.info(f"Exporting {len(errorlogs_df)} error_logs to errorlogs table...")
        conditional_print(f"Exporting {len(errorlogs_df)} error_logs to errorlogs table...")
        if not errorlogs_df.empty:
            errorlogs_df.to_sql(
                name="errorlogs",
                con=engine,
                schema=db_schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )
        else:
            log.info("No error logs to export to errorlogs table")
            conditional_print("No error logs to export to errorlogs table")

        log.info("Exporting inspection_metadata to database...")
        conditional_print("Exporting inspection_metadata to database...")
        inspection_metadata.to_sql(
            name="inspection_metadata",
            con=engine,
            schema=db_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

        log.info("Successfully exported all data to database")
        conditional_print("Successfully exported all data to database")

        # Close the connection
        engine.dispose()

        return (
            inspection_routes,
            inspection_critical_sections,
            critical_sections_with_mcp_feedback,
            error_logs,
            inspection_metadata,
            True,
        )

    except Exception as e:
        log.error(f"Failed to export data to database: {str(e)}")
        conditional_print_error(f"Failed to export data to database: {str(e)}")
        raise


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
                    T.StructField("mapdate", T.StringType()),
                    T.StructField("product", T.StringType()),
                    T.StructField("country", T.StringType()),
                    T.StructField("mode", T.StringType()),
                    T.StructField("competitor", T.StringType()),
                    T.StructField("mcp_tasks", T.StringType()),
                    T.StructField("completed", T.StringType()),
                    T.StructField("inspection_date", T.StringType()),
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

