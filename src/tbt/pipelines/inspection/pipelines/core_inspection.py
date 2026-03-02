"""Core inspection sub-pipeline: business logic.

Contains the main inspection flow:
    get_provider_routes → reuse_static_routes → get_competitor_routes
    → get_rac_state → get_fcd_state → merge_inspection_data

This sub-pipeline should NOT be modified when adding pre/post steps.
"""

import kedro.pipeline

from ..nodes import (
    get_competitor_routes,
    get_fcd_state,
    get_provider_routes,
    get_rac_state,
    merge_inspection_data,
    reuse_static_routes,
)


def create_core_inspection_pipeline() -> kedro.pipeline.Pipeline:
    """Creates the core inspection pipeline (routing, RAC, FCD, merge).

    :return: Kedro Pipeline
    """
    return kedro.pipeline.Pipeline(
        [
            kedro.pipeline.node(
                func=get_provider_routes,
                inputs=[
                    "params:tbt_options",
                    "tbt_sampling_samples_sdf",
                    "tbt_sampling_metadata_sdf",
                ],
                outputs=[
                    "tbt_provider_routes",
                    "tbt_provider_routes_node_time",
                    "sample_metric",
                    "provider_api_calls",
                ],
                name="tbt_provider_routes_node",
            ),
            kedro.pipeline.node(
                func=reuse_static_routes,
                inputs=[
                    "tbt_provider_routes",
                    "tbt_inspection_routes_input",
                    "tbt_inspection_critical_sections_input",
                    "tbt_critical_sections_with_mcp_feedback_input",
                    "tbt_inspection_metadata_input",
                    "params:tbt_options",
                ],
                outputs=[
                    "tbt_provider_routes_unknown",
                    "tbt_provider_routes_known",
                    "tbt_inspection_critical_sections_known",
                    "tbt_reuse_static_routes_node_time",
                ],
                name="tbt_reuse_static_routes_node",
            ),
            kedro.pipeline.node(
                func=get_competitor_routes,
                inputs=[
                    "params:tbt_options",
                    "tbt_provider_routes_unknown",
                    "tbt_inspection_routes_input",
                    "provider_api_calls",
                ],
                outputs=[
                    "tbt_competitor_routes",
                    "tbt_competitor_routes_node_time",
                    "competitor_api_calls",
                ],
                name="tbt_competitor_routes_node",
            ),
            kedro.pipeline.node(
                func=get_rac_state,
                inputs=[
                    "params:tbt_options",
                    "tbt_provider_routes_unknown",
                    "tbt_competitor_routes",
                    "competitor_api_calls",
                ],
                outputs=[
                    "tbt_new_routes",
                    "tbt_new_critical_sections",
                    "tbt_get_rac_state_api_calls",
                    "tbt_get_rac_state_time",
                ],
                name="tbt_get_rac_state",
            ),
            kedro.pipeline.node(
                func=get_fcd_state,
                inputs=[
                    "params:tbt_options",
                    "params:run_id",
                    "tbt_new_critical_sections",
                    "params:fcd_credentials",
                    "params:ml_model_options",
                    "sample_metric",
                ],
                outputs=[
                    "run_id",
                    "tbt_critical_sections_with_fcd_state",
                    "ec_model_log_table",
                    "tbt_get_fcd_state_time",
                ],
                name="tbt_get_fcd_state",
            ),
            kedro.pipeline.node(
                func=merge_inspection_data,
                inputs=[
                    "params:tbt_options",
                    "run_id",
                    "tbt_provider_routes_known",
                    "tbt_inspection_critical_sections_known",
                    "tbt_new_routes",
                    "tbt_critical_sections_with_fcd_state",
                    "tbt_provider_routes_node_time",
                    "tbt_competitor_routes_node_time",
                    "tbt_reuse_static_routes_node_time",
                    "tbt_get_rac_state_api_calls",
                    "tbt_get_rac_state_time",
                    "tbt_get_fcd_state_time",
                ],
                outputs=[
                    "tbt_inspection_routes",
                    "tbt_inspection_critical_sections",
                    "tbt_critical_sections_with_mcp_feedback",
                    "tbt_error_logs",
                    "tbt_inspection_metadata",
                ],
                name="tbt_merge_inspection_data",
            ),
        ]
    )

