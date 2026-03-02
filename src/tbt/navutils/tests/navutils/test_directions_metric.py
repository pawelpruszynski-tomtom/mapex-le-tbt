""" Test for directions_metrics"""
from functools import partial

import pandas as pd
import pytest

from ...navutils.direction_metrics import _weighted_average, eph, failed_routes


def test_weighted_average():
    """Tests for weighted_average (both denominator sum up 1 and not)"""
    values = pd.Series(data=[1, 2, 3], index=["g_1", "g_2", "g_3"])
    weightings = {"g_1": 0.5, "g_2": 0.4, "g_3": 0.1}
    result = _weighted_average(values=values, weightings=weightings)
    assert result == 1.6

    values = pd.Series(data=[1, 2, 3], index=["g_1", "g_2", "g_3"])
    weightings = {"g_1": 0.5, "g_2": 0.5, "g_3": 0.5}
    result = _weighted_average(values=values, weightings=weightings)
    assert result == 2


def test_eph_empty():
    """Test to eph if dataframe is empty"""
    assert (
        eph(routes_df=pd.DataFrame(), get_hours=lambda _: 0.0, get_errors=lambda _: 0)
        == 0
    )


def test_eph_no_grouping():
    """Test for eph without grouping_col"""

    def mock_get_hours(routes_df, filter_query):
        return 10

    def mock_get_errors(routes_df, filter_query):
        return 5

    eph_function = partial(
        eph,
        get_hours=partial(mock_get_hours, filter_query=None),
        get_errors=partial(mock_get_errors, filter_query=None),
    )

    result = eph_function(
        routes_df=pd.DataFrame(
            data={
                "grouping_col": [1, 1, 2, 2, 3, 3],
                "other_col": [1, 2, 3, 4, 5, 6],
                "filter_col": [1, 1, 1, 1, 1, 0],
            }
        )
    )

    assert result == 0.5


def test_eph_grouping():
    """Test for eph with grouping_col and filter_query"""

    def mock_get_hours(routes_df, filter_query):
        return 10

    def mock_get_errors(routes_df, filter_query):
        return 5

    test_df = pd.DataFrame(
        data={
            "grouping_col": [1, 1, 2, 2, 3, 3],
            "other_col": [1, 2, 3, 4, 5, 6],
            "filter_col": [1, 1, 1, 1, 1, 0],
        }
    )

    with pytest.raises(ValueError):
        eph(
            routes_df=test_df,
            get_hours=partial(mock_get_hours, filter_query=None),
            get_errors=partial(mock_get_errors, filter_query=None),
            filter_query="filter_col != 0",
            grouping_col="grouping_col",
        )

    eph_function = partial(
        eph,
        get_hours=partial(mock_get_hours, filter_query=None),
        get_errors=partial(mock_get_errors, filter_query=None),
        filter_query="filter_col != 0",
        grouping_col="grouping_col",
        weightings_dict={1: 0.5, 2: 0.4, 3: 0.1},
    )
    result = eph_function(routes_df=test_df)
    assert result == 0.5


def test_failed_routes_empty():
    """Test to eph if dataframe is empty"""
    assert (
        failed_routes(
            routes_df=pd.DataFrame(),
            get_routes=lambda _: 0,
            get_routes_error=lambda _: 0,
        )
        == 0
    )


def test_failed_routes_no_grouping():
    """Test for failed_routes without grouping_col"""

    def mock_get_routes_error(routes_df, filter_query):
        return 5

    def mock_get_routes(routes_df, filter_query):
        return 10

    failed_routes_function = partial(
        failed_routes,
        get_routes_error=partial(mock_get_routes_error, filter_query=None),
        get_routes=partial(mock_get_routes, filter_query=None),
    )
    result = failed_routes_function(
        routes_df=pd.DataFrame(data={"col_1": [1, 2], "col_3": [2, 4]})
    )
    assert result == 0.5


def test_failed_routes_grouping():
    """Test for failed_routes with grouping_col and filter_query"""

    def mock_get_routes_error(routes_df, filter_query):
        return 5

    def mock_get_routes(routes_df, filter_query):
        return 10

    test_df = pd.DataFrame(
        data={
            "grouping_col": [1, 1, 2, 2, 3, 3],
            "other_col": [1, 2, 3, 4, 5, 6],
            "filter_col": [1, 1, 1, 1, 1, 0],
        }
    )

    with pytest.raises(ValueError):
        failed_routes(
            routes_df=test_df,
            get_routes_error=partial(mock_get_routes_error, filter_query=None),
            get_routes=partial(mock_get_routes, filter_query=None),
            filter_query="filter_col != 0",
            grouping_col="grouping_col",
        )

    failed_routes_function = partial(
        failed_routes,
        get_routes_error=partial(mock_get_routes_error, filter_query=None),
        get_routes=partial(mock_get_routes, filter_query=None),
        filter_query="filter_col != 0",
        grouping_col="grouping_col",
        weightings_dict={1: 0.5, 2: 0.4, 3: 0.1},
    )

    result = failed_routes_function(routes_df=test_df)
    assert result == 0.5
