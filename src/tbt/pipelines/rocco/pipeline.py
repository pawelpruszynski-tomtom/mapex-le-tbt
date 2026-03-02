"""Pipeline to run ROCCO inspections"""

import kedro.pipeline
from .nodes import (
    retrieve_run_ids,
    build_query,
    compute_anomalous_routes
)


def create_pipeline() -> kedro.pipeline.Pipeline:
    """Creates the inspection pipeline

    :return: kedro pipeline
    :rtype: Pipeline
    """

    return kedro.pipeline.Pipeline(
        [
            kedro.pipeline.node(
                func=retrieve_run_ids,
                inputs=[
                    "hdr_latest_inspections",
                    "rocco_inspections"
                ],
                outputs="run_ids",
                name="rocco_retrieve_run_ids",
            ),
            kedro.pipeline.node(
                func=build_query,
                inputs="run_ids",
                outputs=[
                    "query_inspections_runs",
                    "query_inspections_routes_counts",
                    "build_query_success"
                ],
                name="rocco_build_query",
            ),
            kedro.pipeline.node(
                func=compute_anomalous_routes,
                inputs=[
                    "rocco_inspections_runs",
                    "rocco_inspections_routes_counts",
                    "build_query_success"
                ],
                outputs="rocco_metric",
                name="rocco_compute_anomalous_routes",
            )
        ],
    )