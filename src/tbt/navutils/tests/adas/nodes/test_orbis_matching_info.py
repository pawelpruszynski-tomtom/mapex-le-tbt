import pytest
import os
import inspect
import pandas as pd

from ....adas.map_matchers import ValhallaMapMatcher
from ....adas.nodes import orbis_matching_info
from unittest.mock import patch
import uuid

from ....adas import kedro_utils

file_path = os.path.dirname(os.path.abspath(__file__))
test_input_data_path = os.path.join(os.path.dirname(file_path), "test_input_data")


@pytest.fixture
def sample_df():
    df = pd.read_csv(os.path.join(test_input_data_path, "df_sample.csv"))
    return df


def test_recover_sample(mocker, sample_df):
    params = {
        "sample_id": "sample_test_id",
        "provider_mapmatched_db": {"sample_limit_km": 100},
    }

    from ....adas import kedro_utils

    function_name = (
        inspect.getmodule(kedro_utils.get_credentials).__name__ + ".get_credentials"
    )
    mocker.patch(
        function_name,
        return_value={
            "db-credentials-speed-limits": {
                "con": "postgresql+psycopg2://asdf:asdf@11.111.111.111:5432/asdf"
            }
        },
    )
    sample_df['date_generated'] = None # faking metadata
    mocker.patch("pandas.read_sql_query", return_value=sample_df)

    recovered, sampling_metadata = orbis_matching_info.recover_sample(params)
    assert pd.read_sql_query.call_count == 2  # 1 sample , 1 metadata

    assert len(recovered) == len(sample_df)


@pytest.mark.parametrize(
    "params,expected_len",
    [
        (
            {
                "sample_id": "sample_test_id",
                "provider_mapmatched_db": {"sample_limit_km": 20},  # 16KM
            },
            1,
        ),
        (
            {
                "sample_id": "sample_test_id",
                "provider_mapmatched_db": {"sample_limit_km": 35},  # 16KM+16KM
            },
            2,
        ),
    ],
)
def test_recover_sample_max_km(mocker, sample_df, params, expected_len):
    sample_df["geom"] = "LINESTRING (11.1625 43.45902, 11.13127 43.31037)"  # ~16KM

    function_name = (
        inspect.getmodule(kedro_utils.get_credentials).__name__ + ".get_credentials"
    )
    mocker.patch(
        function_name,
        return_value={
            "db-credentials-speed-limits": {
                "con": "postgresql+psycopg2://asdf:asdf@11.111.111.111:5432/asdf"
            }
        },
    )
    sample_df['date_generated'] = None # faking metadata
    mocker.patch("pandas.read_sql_query", return_value=sample_df)

    recovered,sampling_metadata = orbis_matching_info.recover_sample(params)
    assert pd.read_sql_query.call_count == 2  # 1 sample , 1 metadata
    assert len(recovered) == expected_len


def test_map_matching_info_provider_new(mocker, sample_df):

    # Mock get credentials
    function_name = (
        inspect.getmodule(kedro_utils.get_credentials).__name__ + ".get_credentials"
    )
    mocker.patch(
        function_name,
        return_value={
            "test_db": {
                "con": "postgresql+psycopg2://asdf:asdf@11.111.111.111:5432/asdf"
            }
        },
    )

    # Mock info existance in DB
    mocker.patch("pandas.read_sql_query", return_value=pd.DataFrame())
    # Mock info existance in DB
    mocker.patch("pandas.DataFrame.to_sql")

    params = {
        "provider_mapmatched_db": {"schema": "test", "db_con": "test_db"},
        "sample_id": "test_sample",
        "provider_product": "test_provider",
        "provider_version": "test_version",
        "provider_valhalla_url": "",
    }

    # Mock valhalla
    # map_match_df
    with patch.object(
        ValhallaMapMatcher, "map_match_df"
    ) as mock_map_match_df:
        valhalla_return_value = sample_df[["geom"]].copy()
        for col in [
            "map_match_id",
            "id",
            "lane_counts",
            "speed_limit",
            "speed_limit_with_default_value",
            "road_class",
            "use",
            "unpaved",
            "trace_id",
        ]:
            valhalla_return_value[col] = "test"

        mock_map_match_df.return_value = valhalla_return_value

        (
            map_matched,
            map_matched_metadata,
            map_info,
            map_info_metadata,
        ) = orbis_matching_info.map_matching_info_provider(sample_df, params)

        assert pd.DataFrame.to_sql.call_count == 4  # Saving 4 dataframes
        # TODO: Test correct to_sql
    assert len(map_info) == len(valhalla_return_value)


def test_map_matching_info_provider_exists(mocker):

    # Mock get credentials
    function_name = (
        inspect.getmodule(kedro_utils.get_credentials).__name__ + ".get_credentials"
    )
    mocker.patch(
        function_name,
        return_value={
            "test_db": {
                "con": "postgresql+psycopg2://asdf:asdf@11.111.111.111:5432/asdf"
            }
        },
    )

    # Mock info existance in DB
    mocker.patch(
        "pandas.read_sql_query",
        return_value=pd.json_normalize(
            {
                "map_match_id": "0b631591-22d6-47a0-8631-27aa9e41d8c2",
                "map_info_id": "36d35d00-c0ce-423c-9aa5-7f77162f94f4",
                "sample_id": "36d35d00-c0ce-423c-9aa5-7f77162f94f4",
                "product": "Orbis",
                "map_version": 8.5,
                "geom": "LINESTRING (11.1625 43.45902, 11.13127 43.31037)",  # only for test
            }
        ),
    )

    features = pd.DataFrame()

    params = {
        "provider_mapmatched_db": {"schema": "test", "db_con": "test_db"},
        "sample_id": "test_sample",
        "provider_product": "test_provider",
        "provider_version": "test_version",
    }

    (
        map_matched,
        map_matched_metadata,
        map_info,
        map_info_metadata,
    ) = orbis_matching_info.map_matching_info_provider(features, params)

    assert isinstance(map_matched_metadata, dict)
    assert isinstance(map_matched, pd.DataFrame)
    assert isinstance(map_info_metadata, dict)
    assert isinstance(map_info, pd.DataFrame)

    assert "product" in map_info_metadata.keys()
    assert "product" in map_matched_metadata.keys()
