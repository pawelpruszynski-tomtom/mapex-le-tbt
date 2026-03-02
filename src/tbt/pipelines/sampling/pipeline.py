""" Pipeline for tbt sampling """
from kedro.pipeline import Pipeline, node

from .nodes import export_to_spark, export_to_sql, tbt_sampling_entrypoint


def create_pipeline() -> Pipeline:
    """Creates the pipeline"""
    return Pipeline(
        [
            node(
                func=tbt_sampling_entrypoint,
                inputs=[
                    "params:sample_options",
                    "tbt_m14scope_delta",
                    "params:fcd_credentials",
                    "params:run_id",
                    "params:tbt_db",
                ],
                outputs=["tbt_sampling_samples", "tbt_sampling_metadata"],
                name="tbt_sampling_node",
            ),
            node(
                func=export_to_sql,
                inputs=["tbt_sampling_samples", "tbt_sampling_metadata"],
                outputs=["tbt_sampling_samples_sql", "tbt_sampling_metadata_sql"],
                name="tbt_sampling_export_sql_node",
            ),
            node(
                func=export_to_spark,
                inputs=["tbt_sampling_samples", "tbt_sampling_metadata"],
                outputs=["tbt_sampling_samples_sdf", "tbt_sampling_metadata_sdf"],
                name="tbt_sampling_export_spark_node",
            ),
        ]
    )
