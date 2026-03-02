"""Node: check_duplicates — pre-inspection validation."""

import logging

import pandas as pd

log = logging.getLogger(__name__)


def check_duplicates(tbt_options: dict, inspection_metadata: pd.DataFrame) -> bool:
    """Checks for duplicate entries in the inspection metadata.

    :param tbt_options: Options provided to the pipeline through ``conf/base/parameters/tbt.yml``,
        with at least the following fields: ``sample_id``, ``provider``, ``competitor``, ``endpoint``, ``product``
    :param inspection_metadata: DataFrame containing metadata of the inspections.
    :raises ValueError: If a duplicate entry is found when ``avoid_duplicates`` is True.
    :return: True if no duplicates are found or if ``avoid_duplicates`` is False.
    """
    if not tbt_options["avoid_duplicates"]:
        return True

    sample_id = tbt_options["sample_id"]
    provider = tbt_options["provider"]
    competitor = tbt_options["competitor"]
    product = tbt_options["product"]

    duplicates = (
        (inspection_metadata["provider"] == provider)
        & (inspection_metadata["sample_id"] == sample_id)
        & (inspection_metadata["product"] == product)
        & (inspection_metadata["competitor"] == competitor)
    ).any()

    if duplicates:
        log.info(
            "Inspection already exists for this sample_id, provider, competitor and product"
        )
        raise ValueError(
            "Inspection already exists for this sample_id, provider, competitor and product"
        )

    return True

