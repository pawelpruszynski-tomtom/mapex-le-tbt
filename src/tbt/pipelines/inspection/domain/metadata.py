"""Functions to build inspection metadata records — no Spark dependency."""

import json
from datetime import datetime


def build_inspection_metadata_record(
    run_id: str,
    tbt_options: dict,
    country: str,
    mcp_tasks: int,
    tbt_provider_routes_node_time: float,
    tbt_competitor_routes_node_time: float,
    tbt_reuse_static_routes_node_time: float,
    tbt_get_rac_state_time: float,
    tbt_get_fcd_state_time: float,
    tbt_get_rac_state_api_calls: dict,
) -> dict:
    """Build a single metadata dict for the inspection run.

    :return: Dict matching the ``inspection_metadata`` schema.
    """
    if mcp_tasks < 1:
        automatically_closed = True
        comment = "Automatically closed"
    else:
        automatically_closed = False
        comment = ""

    return {
        "run_id": run_id,
        "sample_id": tbt_options["sample_id"],
        "provider": tbt_options["provider"],
        "endpoint": tbt_options["endpoint"],
        "mapdate": tbt_options["mapdate"],
        "product": tbt_options["product"],
        "country": country,
        "mode": "all_fail",
        "competitor": tbt_options["competitor"],
        "mcp_tasks": mcp_tasks,
        "completed": automatically_closed,
        "inspection_date": datetime.now(),
        "comment": comment,
        "rac_elapsed_time": tbt_get_rac_state_time,
        "fcd_elapsed_time": tbt_get_fcd_state_time,
        "total_elapsed_time": (
            tbt_provider_routes_node_time
            + tbt_competitor_routes_node_time
            + tbt_reuse_static_routes_node_time
            + tbt_get_rac_state_time
            + tbt_get_fcd_state_time
        ),
        "api_calls": json.dumps(tbt_get_rac_state_api_calls),
    }

