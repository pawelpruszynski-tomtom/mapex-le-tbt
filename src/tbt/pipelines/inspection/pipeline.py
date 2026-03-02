"""Pipeline to run tbt inspections.

Composed of three sub-pipelines that can be run independently:
- ``pre_inspection``  — validation, dedup (extension point for steps BEFORE)
- ``core_inspection`` — routing, RAC, FCD, merge (business logic)
- ``post_inspection`` — sanity checks, export (extension point for steps AFTER)

To add a new step (e.g. database export), create a new sub-pipeline in
``pipelines/`` and add it to the sum in ``create_pipeline()``.
"""

import kedro.pipeline

from .pipelines import (
    create_core_inspection_pipeline,
    create_post_inspection_pipeline,
    create_pre_inspection_pipeline,
)


def create_pipeline(**kwargs) -> kedro.pipeline.Pipeline:
    """Creates the full inspection pipeline = pre + core + post.

    Adding a new step (e.g. DB export) requires:
    1. Creating a new sub-pipeline file in ``pipelines/``
    2. Adding it to the sum below

    :return: kedro pipeline
    :rtype: Pipeline
    """
    return (
        create_pre_inspection_pipeline()
        + create_core_inspection_pipeline()
        + create_post_inspection_pipeline()
        # + create_db_export_pipeline()     # ← future extension
    )
