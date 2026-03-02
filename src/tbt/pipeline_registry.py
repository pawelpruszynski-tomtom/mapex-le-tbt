"""Project pipelines.
"""

from typing import Dict

from kedro.pipeline import Pipeline

import tbt.pipelines.inspection


def register_pipelines() -> Dict[str, Pipeline]:  # pragma: no cover
    """Register the project's pipelines.

    Available pipelines:
    + ``tbt_inspection``      — full inspection pipeline (pre + core + post)
    + ``tbt_inspection_pre``  — validation, dedup (extension point for steps BEFORE)
    + ``tbt_inspection_core`` — routing, RAC, FCD, merge (business logic)
    + ``tbt_inspection_post`` — sanity checks, export (extension point for steps AFTER)

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    return {
        # Full pipeline (backward compatible)
        "tbt_inspection": tbt.pipelines.inspection.create_pipeline(),
        # Sub-pipelines (for debugging, testing, or selective execution)
        "tbt_inspection_pre": tbt.pipelines.inspection.create_pre_inspection_pipeline(),
        "tbt_inspection_core": tbt.pipelines.inspection.create_core_inspection_pipeline(),
        "tbt_inspection_post": tbt.pipelines.inspection.create_post_inspection_pipeline(),
    }
