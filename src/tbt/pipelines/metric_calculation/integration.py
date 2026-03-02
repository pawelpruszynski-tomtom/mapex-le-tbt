import logging

import pandas
import sqlalchemy

log = logging.getLogger(__name__)


def generate_metric_calculation_input1(tbt_db, run_id):
    cursor = sqlalchemy.create_engine(
        f'postgresql+psycopg2://{tbt_db["user"]}:{tbt_db["password"]}'
        f'@{tbt_db["host"]}:{tbt_db["port"]}/{tbt_db["database"]}'
    ).execution_options(autocommit=True)

    # Grab data from an inspection '5732045d-3a37-4ca7-a7a6-a1a6ea5eee25' in dev.reference and change the run_id
    inspection_routes = pandas.read_sql_query(
        "select  * from tbt.inspection_routes where run_id='5732045d-3a37-4ca7-a7a6-a1a6ea5eee25'",
        cursor,
    )
    inspection_critical_sections = pandas.read_sql_query(
        "select  * from tbt.inspection_critical_sections where run_id='5732045d-3a37-4ca7-a7a6-a1a6ea5eee25'",
        cursor,
    )
    inspection_metadata = pandas.read_sql_query(
        """
        SELECT run_id::text, sample_id::text, provider, endpoint, mapdate, product, country, mode, competitor, mcp_tasks, completed, inspection_date, comment, rac_elapsed_time, fcd_elapsed_time, total_elapsed_time, sanity_msg::text
        FROM tbt.inspection_metadata 
        WHERE run_id='5732045d-3a37-4ca7-a7a6-a1a6ea5eee25'
        """,
        cursor,
    )
    error_logs = pandas.read_sql_query(
        "select  * from tbt.scheduled_error_logs_history where run_id='5732045d-3a37-4ca7-a7a6-a1a6ea5eee25'",
        cursor,
    )

    inspection_routes["run_id"] = run_id
    inspection_critical_sections["run_id"] = run_id
    inspection_metadata["run_id"] = run_id
    error_logs["run_id"] = run_id

    inspection_metadata["comment"] = (
        "Automatic end-to-end test (tbt.pipelines.metric_calculation.integration.generate_metric_calculation_input1)"
    )

    # Write into dev postgres DB
    error_logs.to_sql(
        name="error_logs", if_exists="append", index=False, con=cursor, schema="tbt"
    )
    inspection_routes.to_sql(
        name="inspection_routes",
        if_exists="append",
        index=False,
        con=cursor,
        schema="tbt",
    )
    inspection_critical_sections.to_sql(
        name="inspection_critical_sections",
        if_exists="append",
        index=False,
        con=cursor,
        schema="tbt",
    )
    inspection_metadata.to_sql(
        name="inspection_metadata",
        if_exists="append",
        index=False,
        con=cursor,
        schema="tbt",
    )

    log.info(f"Data for metric calculation appended successfully , run_id='{run_id}'")


def validate_metric_calculation_output1(tbt_db, run_id):
    cursor = sqlalchemy.create_engine(
        f'postgresql+psycopg2://{tbt_db["user"]}:{tbt_db["password"]}'
        f'@{tbt_db["host"]}:{tbt_db["port"]}/{tbt_db["database"]}'
    ).execution_options(autocommit=True)
    assert (
        pandas.read_sql_query(
            f"""
            with tb_new as (
                    select inspection_routes, errors, provider_kms, provider_hours, eph, 
                    (metrics_per_error_type -> 'oneway' -> 'eph')::text as oneway_eph,
                    (metrics_per_error_type -> 'turnrest' -> 'eph')::text as turnrest_eph,
                    (metrics_per_error_type -> 'nonnav' -> 'eph')::text as nonnav_eph,
                    fr,
                    competitor_hours,
                    competitor_kms,
                    (rac_state -> 'optimal')::text as optimal,
                    (rac_state -> 'routing_issue')::text as routing_issue,
                    (rac_state -> 'potential_error')::text as potential_error
                    from directions_shared_components.metrics where run_id='{run_id}'
                    ),
                    tb_ref as (
                    select inspection_routes, errors, provider_kms, provider_hours, eph, 
                    (metrics_per_error_type -> 'oneway' -> 'eph')::text as oneway_eph,
                    (metrics_per_error_type -> 'turnrest' -> 'eph')::text as turnrest_eph,
                    (metrics_per_error_type -> 'nonnav' -> 'eph')::text as nonnav_eph,
                    fr,
                    competitor_hours,
                    competitor_kms,
                    (rac_state -> 'optimal')::text as optimal,
                    (rac_state -> 'routing_issue')::text as routing_issue,
                    (rac_state -> 'potential_error')::text as potential_error
                    from directions_shared_components.metrics where run_id='5732045d-3a37-4ca7-a7a6-a1a6ea5eee25'
                    )
                    select count(*) from tb_new inner join tb_ref using(eph, oneway_eph, turnrest_eph, nonnav_eph, fr, competitor_hours, competitor_kms, optimal, routing_issue, potential_error)
        """,
            cursor,
        )["count"][0]
        == 1
    )
