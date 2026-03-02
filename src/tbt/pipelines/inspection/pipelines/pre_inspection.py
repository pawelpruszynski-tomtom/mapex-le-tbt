"""Pre-inspection sub-pipeline: validation and preparation.

Extension point — add nodes here for:
- parameter validation
- API availability checks
- data fetching from external sources
- deduplication
"""

import kedro.pipeline

from ..nodes import check_duplicates, clean_data_directories


def create_pre_inspection_pipeline() -> kedro.pipeline.Pipeline:
    """Creates the pre-inspection pipeline (validation, dedup).

    Steps:
    - ``clean_data_directories`` — removes contents of ``data/tbt/inspection``
      and ``data/tbt/sampling`` before the run starts.
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
            # Uncomment when tbt_inspection_metadata_input_sql is available:
            # kedro.pipeline.node(
            #     func=check_duplicates,
            #     inputs=["params:tbt_options", "tbt_inspection_metadata_input_sql"],
            #     outputs="tbt_inspection_metadata_duplicates",
            #     name="tbt_check_duplicates",
            # ),
        ]
    )

