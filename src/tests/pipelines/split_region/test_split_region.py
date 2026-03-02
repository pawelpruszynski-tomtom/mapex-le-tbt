"""
This module contains an example test.

Tests should be placed in ``src/tests``, in modules that mirror your
project's structure, and in files named test_*.py. They are simply functions
named ``test_*`` which test a unit of logic.

To run the tests, run ``kedro test`` from the project root directory.
"""

import pytest

import tbt.pipelines.split_region
from tbt.pipelines.split_region.nodes import convert_poly_to_wkt, create_area_m14


class TestPipelines:
    def test_tbt_split_region(self):
        pipeline = tbt.pipelines.split_region.create_pipeline()

    def test_tbt_split_region_nodes(self):
        options = {
            "poly_url": "http://download.geofabrik.de/north-america/us/colorado.poly",
            "new_region_iso": "US-CO",
            "country": "USA",
            "default_quality": "Q3",
            "grid_resolution": 128,
        }
        wkt = convert_poly_to_wkt(options)
        sdf = create_area_m14(options, wkt)
        assert sdf.count() > 0
