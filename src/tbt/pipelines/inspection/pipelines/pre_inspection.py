"""Pre-inspection sub-pipeline: validation and preparation.

Extension point — add nodes here for:
- parameter validation
- API availability checks
- data fetching from external sources
- deduplication
"""

import kedro.pipeline

from ..nodes import check_duplicates, clean_data_directories, initialize_inspection_data, initialize_sampling_data


def create_pre_inspection_pipeline() -> kedro.pipeline.Pipeline:
    """Creates the pre-inspection pipeline (validation, dedup).

    Steps:
    - ``clean_data_directories`` — removes contents of ``data/tbt/inspection``
      and ``data/tbt/sampling`` before the run starts.
    - ``initialize_inspection_data`` — creates empty parquet files in
      ``data/tbt/inspection/`` by running ``scripts/generate_empty_inspection_*.py``.
    - ``initialize_sampling_data`` — creates sampling parquet files in
      ``data/tbt/sampling/`` by running ``scripts/generate_sampling_*.py``,
      reading routes from ``li_input/geojson/Routes2check.geojson``.
    - ``check_duplicates`` — commented-out in production. To activate, uncomment
      the node below and ensure ``tbt_inspection_metadata_input_sql`` is defined
      in the catalog.

    :return: Kedro Pipeline
    """
    return kedro.pipeline.Pipeline(
        [
            kedro.pipeline.node(
                func=clean_data_directories,
                inputs="params:tbt_options",
                outputs="tbt_cleanup_done",
                name="tbt_clean_data_directories",
            ),
            kedro.pipeline.node(
                func=initialize_inspection_data,
                inputs="tbt_cleanup_done",
                outputs="tbt_init_done",
                name="tbt_initialize_inspection_data",
            ),
            kedro.pipeline.node(
                func=initialize_sampling_data,
                inputs=["tbt_init_done", "params:tbt_options"],
                outputs="tbt_sampling_init_done",
                name="tbt_initialize_sampling_data",
            ),
            # Uncomment when tbt_inspection_metadata_input_sql is available:
            # kedro.pipeline.node(
            #     func=check_duplicates,
            #     inputs=["params:tbt_options", "tbt_inspection_metadata_input_sql"],
            #     outputs="tbt_inspection_metadata_duplicates",
            #     name="tbt_check_duplicates",
            # ),
        ]
    )

