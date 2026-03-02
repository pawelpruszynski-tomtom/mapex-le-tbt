"""HDR samling pipeline"""
from kedro.pipeline import Pipeline, node

from tbt.pipelines.sampling.nodes import export_to_spark, export_to_sql

from .nodes import golden_routes_sampling_entrypoint


def create_pipeline() -> Pipeline:
    """Create the pipeline"""
    return Pipeline(
        [
            node(
                func=golden_routes_sampling_entrypoint,
                inputs=[
                    "params:tbt_db",
                    "params:hdr_sample_options",
                    "params:fcd_credentials",
                    "params:run_id",
                ],
                outputs=[
                    "hdr_sampling_samples",
                    "hdr_sampling_metadata",
                    "hdr_sampling_extra_df",
                ],
                name="hdr_sampling_node",
            ),
            node(
                func=export_to_sql,
                inputs=["hdr_sampling_samples", "hdr_sampling_metadata"],
                outputs=["tbt_sampling_samples_sql", "tbt_sampling_metadata_sql"],
                name="tbt_sampling_export_sql_node",
            ),
            node(
                func=export_to_spark,
                inputs=["hdr_sampling_samples", "hdr_sampling_metadata"],
                outputs=["tbt_sampling_samples_sdf", "tbt_sampling_metadata_sdf"],
                name="tbt_sampling_export_spark_node",
            ),
        ]
    )
