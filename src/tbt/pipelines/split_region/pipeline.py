""" Fuction to split region on m14 tiles """
from kedro.pipeline import Pipeline, node

from .nodes import convert_poly_to_wkt, create_area_m14, process_m14_data


def create_pipeline() -> Pipeline:
    """Creates the pipeline"""
    return Pipeline(
        [
            node(
                func=convert_poly_to_wkt,
                inputs="params:split_region_options",
                outputs="tbt_region_split_wkt",
                name="tbt_region_split_wkt_node",
            ),
            node(
                func=create_area_m14,
                inputs=["params:split_region_options", "tbt_region_split_wkt"],
                outputs="tbt_region_split_sdf",
                name="tbt_region_split_sdf_node",
            ),
            node(
                func=process_m14_data,
                inputs=[
                    "tbt_m14scope_delta",
                    "tbt_region_split_sdf",
                    "params:split_region_options",
                ],
                outputs="tbt_m14scope_delta_output",
                name="tbt_region_split_export_spark_node",
            ),
        ]
    )
