"""Post-inspection sub-pipeline: sanity checks and export.
Extension point — add nodes here (or create a new sub-pipeline) for:
- database export (PostgreSQL, etc.)
- notifications (Slack, email)
- report generation
- metric publishing
"""
import kedro.pipeline
from ..nodes import (
    export_to_csv,
    export_to_spark,
    export_to_database,
    raise_sanity_error,
    sanity_check,
)
def create_post_inspection_pipeline() -> kedro.pipeline.Pipeline:
    """Creates the post-inspection pipeline (sanity, export CSV/Spark, final check).
    :return: Kedro Pipeline
    """
    return kedro.pipeline.Pipeline(
        [
            kedro.pipeline.node(
                func=sanity_check,
                inputs=[
                    "tbt_inspection_routes",
                    "tbt_inspection_critical_sections",
                    "tbt_critical_sections_with_mcp_feedback",
                    "tbt_error_logs",
                    "tbt_inspection_metadata",
                    "tbt_sampling_samples_sdf",
                    "params:tbt_options",
                    "sample_metric",
                ],
                outputs=[
                    "tbt_inspection_routes_sc",
                    "tbt_inspection_critical_sections_sc",
                    "tbt_critical_sections_with_mcp_feedback_sc",
                    "tbt_error_logs_sc",
                    "tbt_inspection_metadata_sc",
                ],
                name="tbt_inspection_sanity_check",
            ),
            kedro.pipeline.node(
                func=export_to_csv,
                inputs=[
                    "tbt_inspection_routes_sc",
                    "tbt_inspection_critical_sections_sc",
                    "tbt_critical_sections_with_mcp_feedback_sc",
                    "tbt_error_logs_sc",
                    "tbt_inspection_metadata_sc",
                ],
                outputs=[
                    "tbt_inspection_routes_output_sql",
                    "tbt_inspection_critical_sections_output_sql",
                    "tbt_critical_sections_with_mcp_feedback_output_sql",
                    "tbt_error_logs_output_sql",
                    "tbt_inspection_metadata_output_sql",
                    "export_to_sql_ok",
                ],
                name="tbt_export_to_sql_node",
            ),
            kedro.pipeline.node(
                func=export_to_spark,
                inputs=[
                    "tbt_inspection_routes_sc",
                    "tbt_inspection_critical_sections_sc",
                    "tbt_critical_sections_with_mcp_feedback_sc",
                    "tbt_error_logs_sc",
                    "tbt_inspection_metadata_sc",
                ],
                outputs=[
                    "tbt_inspection_routes_output_spark",
                    "tbt_inspection_critical_sections_output_spark",
                    "tbt_critical_sections_with_mcp_feedback_output_spark",
                    "tbt_error_logs_output_spark",
                    "tbt_inspection_metadata_output_spark",
                    "export_to_spark_ok",
                ],
                name="tbt_export_to_spark_node",
            ),
            kedro.pipeline.node(
                func=export_to_database,
                inputs=[
                    "tbt_inspection_routes_sc",
                    "tbt_inspection_critical_sections_sc",
                    "tbt_critical_sections_with_mcp_feedback_sc",
                    "tbt_error_logs_sc",
                    "tbt_inspection_metadata_sc",
                ],
                outputs=[
                    "tbt_inspection_routes_output_db",
                    "tbt_inspection_critical_sections_output_db",
                    "tbt_critical_sections_with_mcp_feedback_output_db",
                    "tbt_error_logs_output_db",
                    "tbt_inspection_metadata_output_db",
                    "export_to_database_ok",
                ],
                name="tbt_export_to_database_node",
            ),
            kedro.pipeline.node(
                func=raise_sanity_error,
                inputs=[
                    "export_to_spark_ok",
                    "export_to_sql_ok",
                    "export_to_database_ok",
                    "tbt_inspection_metadata_sc",
                ],
                outputs="tbt_inspection_sanity_result",
                name="tbt_inspection_sanity_check_result",
            ),
        ]
    )
