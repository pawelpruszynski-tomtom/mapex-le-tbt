import os, sys
from mock import patch, MagicMock
import pandas as pd
import numpy as np
import inspect
from pandas._testing import assert_frame_equal

from ...adas import ggg_info

from ...adas.ggg_info import (
    select_ggg_schema,
    findTagsGGG,
    tag_processor,
    get_ggg_tags,
    get_bus_only_n,
    get_hov_n,
    get_psv_only_n,
    get_osm_lane_empty_only,
    get_osm_bus_only,
    get_osm_psv_only,
    get_osm_bidirectional,
    get_osm_suicide_lane,
    filter_osm_empty,
)


def test_when_select_ggg_schema_then_schema_is_returned(mocker):
    ggg_con = "conn_string"
    country_iso = "ESP"
    sql = (
        "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name DESC"
    )
    df_schema_mock = pd.DataFrame({"schema_name": ["schema1", "esp", "schema2"]})

    create_engine_mock = mocker.patch(
        "sqlalchemy.create_engine", return_value=MagicMock()
    )
    read_sql_mock = mocker.patch("pandas.read_sql", return_value=df_schema_mock)

    assert select_ggg_schema(ggg_con, country_iso) == "esp"

    create_engine_mock.assert_called_once_with(ggg_con)
    read_sql_mock.assert_called_once_with(sql, create_engine_mock.return_value)


def test_when_findTagsGGG_then_returns_answer(mocker):
    ggg_con = "conn_string"
    country_iso = "ESP"
    osm_way_ids = (39236608, 123456)
    function_name = (
        inspect.getmodule(ggg_info.select_ggg_schema).__name__ + ".select_ggg_schema"
    )
    select_ggg_schema_mock = mocker.patch(function_name, return_value="schema_mock")
    sql = f"""SELECT posmway.id, posmway.tags\
        FROM {select_ggg_schema_mock.return_value}.planet_osm_ways AS posmway \
        WHERE posmway.id IN ("'39236608','123456'")"""

    ggg_answer = pd.DataFrame({"id": ["id1", "id2"], "tags": ["maxspeed1", "maxspeed2"]})
    read_sql_mock = mocker.patch("pandas.read_sql", return_value=ggg_answer)

    assert_frame_equal(
        findTagsGGG(ggg_con, country_iso, osm_way_ids), read_sql_mock.return_value
    )

    select_ggg_schema_mock.assert_called_once_with(ggg_con, country_iso)
    # read_sql_mock.assert_called_once_with(read_sql_mock.return_value, ggg_con)


@patch("logging.warning")
def test_given_odd_len_tag_when_tag_processor_then_warns_and_returns(warn_mock):
    tag = ["maxspeed", "maxspeed2", "maxspeed3"]
    assert tag_processor(tag) == {"maxspeed": "maxspeed2"}
    warn_mock.assert_called_once_with("unbalanced tags processing way_id")


@patch("logging.warning")
def test_given_pair_len_tag_when_tag_processor_then_returns(warn_mock):
    tag = ["maxspeed1", "maxspeed2"]
    assert tag_processor(tag) == {"maxspeed1": "maxspeed2"}
    warn_mock.assert_not_called()


def test_when_get_ggg_tags_then_returns_df(mocker):
    ggg_con = "conn_string"
    country_iso = "ESP"
    osm_way_ids = (39236608, 123456)

    function_name = inspect.getmodule(ggg_info.findTagsGGG).__name__ + ".findTagsGGG"
    findTagsGGG_mock = mocker.patch(
        function_name,
        return_value=pd.DataFrame(
            {"id": ["id1", "id2"], "tags": [["maxspeed1", "maxspeed2"], ["maxspeed3", "maxspeed4"]]}
        ),
    )

    osm_tags_df_mock = pd.DataFrame(
        {"id": ["id1", "id2"], "maxspeed1": ["maxspeed2", np.nan], "maxspeed3": [np.nan, "maxspeed4"]}
    )

    assert_frame_equal(
        get_ggg_tags(ggg_con, country_iso, osm_way_ids), osm_tags_df_mock
    )
    findTagsGGG_mock.assert_called_once_with(
        ggg_con=ggg_con, country_iso=country_iso, osm_way_ids=osm_way_ids, return_nodes=False
    )


# check w Guillermo
def test_given_bus_eq_hov_when_get_bus_only_n_return_0():
    bus = "designated||||"
    hov = "designated||||"
    assert get_bus_only_n(bus, hov) == 0


def test_given_bus_int_hov_str_when_get_bus_only_n_return_0():
    bus = 99999
    hov = "designated|yes|yes|yes|"
    assert get_bus_only_n(bus, hov) == 0


def test_given_bus_str_hov_int_when_get_bus_only_n_return_0():
    bus = "designated||||"
    hov = 99999
    assert get_bus_only_n(bus, hov) == 1


@patch("logging.warning")
def test_given_bus_str_hov_str_diff_len_when_get_bus_only_n_return_0(warn_mock):
    bus = "designated||||"
    hov = "designated|yes|yes|"
    assert get_bus_only_n(bus, hov) == 0
    warn_mock.assert_called_once_with(
        "Something is wrong processing lanes for buses and hov"
    )


@patch("logging.warning")
def test_given_bus_str_hov_str_eq_len_when_get_bus_only_n_return_0_case1(warn_mock):
    # bus bus_l == hov_l and bus_l==pattern
    bus = "designated|test|||"
    hov = "designated||||"
    assert get_bus_only_n(bus, hov) == 0
    warn_mock.assert_not_called()


@patch("logging.warning")
def test_given_bus_str_hov_str_eq_len_when_get_bus_only_n_return_0_case2(warn_mock):
    # bus bus_l != hov_l and bus_l==pattern
    bus = "designated|designated|designated|designated|"
    hov = "||||"
    assert get_bus_only_n(bus, hov) == 4
    warn_mock.assert_not_called()


@patch("logging.warning")
def test_given_bus_str_hov_str_eq_len_when_get_bus_only_n_return_0_case3(warn_mock):
    # bus bus_l != hov_l and bus_l!=pattern
    bus = "test|test|test|test|"
    hov = "||||"
    assert get_bus_only_n(bus, hov) == 0
    warn_mock.assert_not_called()


def test_given_hov_str_when_get_hov_n_then_returns_output():
    hov = "designated|yes|yes|yes"
    assert get_hov_n(hov) == 1


def test_given_hov_int_when_get_hov_n_then_returns_output():
    hov = 1234
    assert get_hov_n(hov) == 0


def test_given_access_str_when_get_psv_only_n_then_returns_output():
    access = "no|psv|bus|taxi|yes"
    assert get_psv_only_n(access) == 4


def test_given_access_int_when_get_psv_only_n_then_returns_output():
    access = 1234
    assert get_psv_only_n(access) == 0


def test_given_df_with_lanes_column_when_get_osm_lane_empty_only_returns_df():
    osm_tags_df_mock = pd.DataFrame(
        {"id": ["id1", "id2"], "lanes": ["lane1", "lane2"], "test": ["test1", "test2"], "lanes:forward": ["forward1","forward2"], "lanes:backward": ["back1","back2"]}
    )
    output = pd.DataFrame({"id": ["id1", "id2"], "lanes": ["lane1", "lane2"], "lanes:forward": ["forward1","forward2"], "lanes:backward":  ["back1","back2"]})
    assert_frame_equal(get_osm_lane_empty_only(osm_tags_df_mock), output)


def test_given_df_without_lanes_column_when_get_osm_lane_empty_only_returns_df():
    osm_tags_df_mock = pd.DataFrame(
        {"id": ["id1", "id2"], "test2": ["lane1", "lane2"], "test": ["test1", "test2"], "lanes:forward": ["forward1","forward2"], "lanes:backward": ["back1","back2"]}
    )
    output = pd.DataFrame({"id": ["id1", "id2"], "lanes": [None, None], "lanes:forward": ["forward1","forward2"], "lanes:backward": ["back1","back2"]})
    assert_frame_equal(get_osm_lane_empty_only(osm_tags_df_mock), output)


def test_given_df_without_hov_columns_when_get_osm_bus_only_then_returns_df():
    osm_tags_df_mock = pd.DataFrame(
        {"id": ["id1", "id2"], "bus:lanes": ["lane1", "lane2"]}
    )
    output = pd.DataFrame({"id": ["id1", "id2"], "bus_only": [0, 0], "hov": [0, 0]})
    assert_frame_equal(get_osm_bus_only(osm_tags_df_mock), output)


def test_given_df_without_hov_bus_columns_when_get_osm_bus_only_then_returns_df():
    osm_tags_df_mock = pd.DataFrame({"id": ["id1", "id2"]})
    output = pd.DataFrame({"id": ["id1", "id2"], "bus_only": [0, 0]})
    assert_frame_equal(get_osm_bus_only(osm_tags_df_mock), output)


def test_given_df_with_lanes_when_get_osm_psv_only_then_returns_df():
    osm_tags_df_mock = pd.DataFrame(
        {"id": ["id1", "id2"], "access:lanes": ["lane1", "lane2"]}
    )
    output = pd.DataFrame({"id": ["id1", "id2"], "psv_only": [0, 0]})

    assert_frame_equal(get_osm_psv_only(osm_tags_df_mock), output)


def test_given_df_withput_lanes_when_get_osm_psv_only_then_returns_df():
    osm_tags_df_mock = pd.DataFrame({"id": ["id1", "id2"]})
    output = pd.DataFrame({"id": ["id1", "id2"], "psv_only": [0, 0]})

    assert_frame_equal(get_osm_psv_only(osm_tags_df_mock), output)


def test_given_oneway_in_df_when_get_osm_bidirectional_then_returns_df():
    osm_tags_df_mock = pd.DataFrame(
        {
            "id": ["id1", "id2", "id3", "id4"],
            "oneway": ["reversible", "alternating", "conditional", "test"],
        }
    )
    output = pd.DataFrame(
        {"id": ["id1", "id2", "id3"], "bidirectional": [True, True, True]}
    )
    assert_frame_equal(get_osm_bidirectional(osm_tags_df_mock), output)


def test_given_oneway_not_in_df_when_get_osm_bidirectional_then_returns_df():
    osm_tags_df_mock = pd.DataFrame({"id": ["id1", "id2", "id3", "id4"]})
    assert_frame_equal(get_osm_bidirectional(osm_tags_df_mock), pd.DataFrame())


def test_given_lanes_in_df_when_get_osm_suicide_lane_then_returns_df():
    osm_tags_df_mock = pd.DataFrame(
        {
            "id": ["id1", "id2", "id3", "id4"],
            "lanes:both_ways": [True, True, np.nan, np.nan],
        }
    )
    output = pd.DataFrame(
        {
            "id": ["id1", "id2", "id3", "id4"],
            "suicide_lane": [True, True, np.nan, np.nan],
        }
    )
    assert_frame_equal(get_osm_suicide_lane(osm_tags_df_mock), output)


def test_given_lanes_not_in_df_when_get_osm_suicide_lane_then_returns_df():
    osm_tags_df_mock = pd.DataFrame({"id": ["id1", "id2", "id3", "id4"]})
    assert_frame_equal(get_osm_suicide_lane(osm_tags_df_mock), pd.DataFrame())


def test_given_lanes_when_filter_osm_empty_then_returns_df():
    tags_df_mock = pd.DataFrame({"id": ["id1", "id2"], "lanes": [2, 3]})
    matched_df = pd.DataFrame(
        {"way_id_competitor": ["id1", "id2"], "competitor_lane_counts": [2, 3]}
    )

    output = pd.DataFrame(
        {"way_id_competitor": ["id1", "id2"], "competitor_lane_counts": [2, 3]}
    )
    assert_frame_equal(filter_osm_empty(tags_df_mock, matched_df), output)


def test_given_no_lanes_when_filter_osm_empty_then_returns_df():
    tags_df_mock = pd.DataFrame({"id": ["id1", "id2"]})
    matched_df = pd.DataFrame(
        {"way_id_competitor": ["id1", "id2"], "competitor_lane_counts": [1, 1]}
    )
    output = pd.DataFrame(
        {"way_id_competitor": ["id1", "id2"], "competitor_lane_counts": [0, 0]}
    )
    assert_frame_equal(filter_osm_empty(tags_df_mock, matched_df), output)


# pending test but idk why they are covered
