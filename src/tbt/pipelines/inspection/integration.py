import json
import sqlalchemy
import pandas

import logging

log = logging.getLogger(__name__)

def validate_inspection_output1(tbt_db, run_id):
    cursor = sqlalchemy.create_engine(
        f'postgresql+psycopg2://{tbt_db["user"]}:{tbt_db["password"]}'
        f'@{tbt_db["host"]}:{tbt_db["port"]}/{tbt_db["database"]}'
    ).execution_options(autocommit=True)

    # 50 routes were requested to the api

    assert (
        pandas.read_sql_query(
            f"""
        select (api_calls -> 'Directions')::text::int api_directions 
        from tbt.inspection_metadata im where run_id = '{run_id}'
        """,
            cursor,
        )["api_directions"][0] == 68
    )

    # 0 routes were requested from google's api

    assert (
        pandas.read_sql_query(
            f"""
        select (api_calls -> 'Google')::text::int api_google
        from tbt.inspection_metadata im where run_id = '{run_id}'
        """,
            cursor,
        )["api_google"][0] == 0
    )

    # 0 tasks were generated

    assert (
        pandas.read_sql_query(
            f"""
        select mcp_tasks
        from tbt.inspection_metadata im where run_id = '{run_id}'
        """,
            cursor,
        )["mcp_tasks"][0] > 0
    )


    # approx. 4800 seconds total were computed

    seconds = pandas.read_sql_query(
            f"""
        select round(sum(provider_route_time/100))*100 seconds 
        from tbt.inspection_routes im where run_id = '{run_id}'
        """,
            cursor,
        )["seconds"][0]
    
    assert seconds > 4000



    
