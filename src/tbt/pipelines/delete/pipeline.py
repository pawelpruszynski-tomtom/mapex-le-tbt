""" Pipeline to delete inspections """
from kedro.pipeline import Pipeline, node

from .nodes import delete_inspection, delete_inspection_sql


def create_pipeline() -> Pipeline:
    """Create the delete pipeline"""
    return Pipeline(
        [
            node(
                func=delete_inspection_sql,
                inputs=["params:delete_options", "params:tbt_db"],
                outputs="delete_inspection_sql_status",
                name="tbt_delete_inspection_sql_node",
            ),
            node(
                func=delete_inspection,
                inputs=["tbt_delete_error_logs_input", "params:delete_options"],
                outputs="tbt_delete_error_logs_output_spark",
                name="tbt_delete_error_logs_node",
            ),
            node(
                func=delete_inspection,
                inputs=[
                    "tbt_delete_inspection_metadata_input",
                    "params:delete_options",
                ],
                outputs="tbt_delete_inspection_metadata_output_spark",
                name="tbt_delete_metadata_node",
            ),
            node(
                func=delete_inspection,
                inputs=["tbt_inspection_routes_input", "params:delete_options"],
                outputs="tbt_delete_inspection_routes_output_spark",
                name="tbt_delete_routes_node",
            ),
            node(
                func=delete_inspection,
                inputs=[
                    "tbt_critical_sections_with_mcp_feedback_input",
                    "params:delete_options",
                ],
                outputs="tbt_delete_critical_sections_with_mcp_feedback_output_spark",
                name="tbt_delete_critical_sections_with_mcp_feedback_node",
            ),
            node(
                func=delete_inspection,
                inputs=[
                    "tbt_inspection_critical_sections_input",
                    "params:delete_options",
                ],
                outputs="tbt_delete_inspection_critical_sections_output_spark",
                name="tbt_delete_critical_sections_node",
            ),
        ]
    )
