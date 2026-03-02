"""Worldwide aggretation calculation pipeline"""
from kedro.pipeline import Pipeline, node, pipeline

from .nodes import get_latest_products, get_inspections, update_agg_source, get_results, send_results


def create_pipeline() -> Pipeline:
    """Creates ww aggregation pipeline"""
    return pipeline(
        [
            node(
                func=get_latest_products,
                inputs=[
                    "params:aggregation_ww_options",
                    "latest_products_data",
                    "inspections_available_data",
                    "base_ww_aggregations_available_data"
                ],
                outputs=[
                    "run_ids",
                    "base_run_id_ww",
                ],
                name="get_latest_products",
            ),
            node(
                func=get_inspections,
                inputs=[
                    "run_ids",
                    "base_run_id_ww",
                    "tbt_inspections_data",
                    "tbt_aggregation_ww_data",
                ],
                outputs=[
                    "tbt_aggregation_ww_new_inspections",
                    "tbt_aggregation_ww_base_ww_agg_inspections",
                ],
                name="get_inspections",
            ),
            node(
                func=update_agg_source,
                inputs=[
                    "params:aggregation_ww_options",
                    "tbt_aggregation_weights",
                    "tbt_aggregation_ww_base_ww_agg_inspections",
                    "tbt_aggregation_ww_new_inspections",
                ],
                outputs="tbt_aggregation_ww_new_ww_agg_inspections",
                name="update_agg_source",
            ),
            node(
                func=get_results,
                inputs=[
                    "params:run_id",
                    "params:aggregation_ww_options",
                    "tbt_aggregation_ww_new_ww_agg_inspections",
                ],
                outputs=["new_run_id_ww", "tbt_aggregation_ww_results"],
                name="ww_aggregation",
            ),
            node(
                func=send_results,
                inputs=[
                    "params:aggregation_ww_options",
                    "params:sender_email_credentials",
                    "base_run_id_ww",
                    "new_run_id_ww",
                    "tbt_aggregation_ww_data",
                    "tbt_inspections_data",
                    "tbt_aggregation_weights",
                ],
                outputs=None,
                name="send_results",
            ),
        ]
    )  # type: ignore
