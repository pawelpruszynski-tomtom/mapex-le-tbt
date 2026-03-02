""" Functions to delete pipeline """
import logging

import pyspark.sql
import pyspark.sql.functions as F
import sqlalchemy

log = logging.getLogger(__name__)


def delete_inspection(
    inspections_df: pyspark.sql.DataFrame, options: dict
) -> pyspark.sql.DataFrame:
    """Function to remove a inspection

    :param inspections_df: DataFrame with all inspections
    :type inspections_df: pyspark.sql.DataFrame
    :param options: options with run_id_to_delete field
    :type options: dict
    :return: Same DataFrame without the inspection
    :rtype: pyspark.sql.DataFrame
    """
    run_id_to_delete = options["run_id_to_delete"]
    inspections_df = inspections_df.filter(F.col("run_id") != run_id_to_delete)
    return inspections_df


def delete_inspection_sql(options: dict, az_db_creds: dict):
    """Delete inspection from the TbT database"""
    run_id_to_delete = options["run_id_to_delete"]

    az_db = sqlalchemy.create_engine(
        f'postgresql+psycopg2://{az_db_creds["user"]}:{az_db_creds["password"]}'
        f'@{az_db_creds["host"]}:{az_db_creds["port"]}/{az_db_creds["database"]}'
    ).execution_options(autocommit=True)

    tables = [
        "critical_sections_with_mcp_feedback",
        "error_logs",
        "inspection_critical_sections",
        "inspection_metadata",
        "inspection_routes",
        "scheduled_error_logs_history",
        "scheduled_metrics",
    ]
    for table in tables:
        log.info(
            "Deleting from PSQL table %s where run_id='%s'", table, run_id_to_delete
        )
        az_db.execute(f"delete from {table} where run_id = '{run_id_to_delete}'")

    log.info("Deleted data successfully")
    return 0
