""" Tests for pandas_ops """
import pandas as pd
import pytest

from ...common.pandas_ops import (
    check_column_exist,
    get_sum_column_deduplicated,
    get_column_value_counts,
    get_number_errors,
    get_number_routes_deduplicated,
    join_queries,
)
from ...common import pandas_ops

import inspect
BASE_NAME = inspect.getmodule(pandas_ops).__name__

def test_check_column_exist_ok():
    """Test check_column_exist (column exists)"""
    test_dataframe = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    check_column_exist(dataframe=test_dataframe, column="col1")


def test_check_column_exist_error():
    """Test check_column_exist (error raise)"""
    test_dataframe = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    with pytest.raises(ValueError):
        check_column_exist(dataframe=test_dataframe, column="col_unk")


def test_get_sum_column_deduplicated_no_filter(mocker):
    """Test get_sum_column_deduplicated (without filter_query)"""
    test_dataframe = pd.DataFrame(
        data={"deduplication_col": ["a", "a", "b"], "sum_col": [1, 2, 1]}
    )

    mocker.patch(f"{BASE_NAME}.check_column_exist", return_value=True)
    result = get_sum_column_deduplicated(
        routes_df=test_dataframe,
        column="sum_col",
        deduplication_column="deduplication_col",
    )
    assert result == 2


def test_get_sum_column_deduplicated_filter(mocker):
    """Test get_column_value_counts (with filter_query)"""
    test_dataframe = pd.DataFrame(
        data={
            "deduplication_col": ["a", "a", "b", "a"],
            "sum_col": [1, 2, 1, 3],
            "filter_col": ["yes", "yes", "yes", "no"],
        }
    )

    mocker.patch(f"{BASE_NAME}.check_column_exist", return_value=True)
    result = get_sum_column_deduplicated(
        routes_df=test_dataframe,
        column="sum_col",
        deduplication_column="deduplication_col",
        filter_query="filter_col=='yes'",
    )
    assert result == 2


def test_get_column_value_counts_no_filter(mocker):
    """Test get_column_value_counts (without filter_query)"""
    test_dataframe = pd.DataFrame(
        data={
            "count_col": ["a", "a", "a", "b", "b", "c"],
            "filter_col": ["yes", "yes", "yes", "yes", "no", "yes"],
        }
    )

    mocker.patch(f"{BASE_NAME}.check_column_exist", return_value=True)
    result = get_column_value_counts(
        routes_df=test_dataframe,
        column="count_col",
    )
    assert "a" in result
    assert result["a"] == 3
    assert "b" in result
    assert result["b"] == 2
    assert "c" in result
    assert result["c"] == 1


def test_get_column_value_counts_filter(mocker):
    """Test get_sum_column_deduplicated (with filter_query)"""
    test_dataframe = pd.DataFrame(
        data={
            "count_col": ["a", "a", "a", "b", "b", "c"],
            "filter_col": ["yes", "yes", "yes", "yes", "no", "yes"],
        }
    )

    mocker.patch(f"{BASE_NAME}.check_column_exist", return_value=True)
    result = get_column_value_counts(
        routes_df=test_dataframe,
        column="count_col",
        filter_query="filter_col=='yes'",
    )
    assert "a" in result
    assert result["a"] == 3
    assert "b" in result
    assert result["b"] == 1
    assert "c" in result
    assert result["c"] == 1


def test_get_number_errors_no_filter(mocker):
    """Test get_number_errors (without filter_query)"""
    test_dataframe = pd.DataFrame(
        data={
            "other_col": ["a", "a", "a", "b", "b", "c", "d"],
            "error_col": ["yes", "yes", "yes", "yes", "no", "yes", pd.NA],
        }
    )

    mocker.patch(f"{BASE_NAME}.check_column_exist", return_value=True)
    result = get_number_errors(
        routes_df=test_dataframe, error_column="error_col", non_error_values=["no"]
    )
    assert result == 5


def test_get_number_errors_filter(mocker):
    """Test get_number_errors (with filter_query)"""
    test_dataframe = pd.DataFrame(
        data={
            "other_col": ["a", "a", "a", "b", "b", "c", "d"],
            "error_col": ["yes", "yes", "yes", "yes", "no", "yes", pd.NA],
            "filter_col": ["yes", "yes", "yes", "yes", "yes", "no", pd.NA],
        }
    )

    mocker.patch(f"{BASE_NAME}.check_column_exist", return_value=True)
    result = get_number_errors(
        routes_df=test_dataframe,
        error_column="error_col",
        non_error_values=["no"],
        filter_query="filter_col=='yes'",
    )
    assert result == 4


def test_get_number_routes_deduplicated_no_filter(mocker):
    """Test get_number_routes_deduplicated (without filter_query)"""
    test_dataframe = pd.DataFrame(
        data={"deduplication_col": ["a", "a", "b"], "sum_col": [1, 2, 1]}
    )

    mocker.patch(f"{BASE_NAME}.check_column_exist", return_value=True)
    result = get_number_routes_deduplicated(
        routes_df=test_dataframe,
        deduplication_column="deduplication_col",
    )
    assert result == 2


def test_get_number_routes_deduplicated_filter(mocker):
    """Test get_column_value_counts (with filter_query)"""
    test_dataframe = pd.DataFrame(
        data={
            "deduplication_col": ["a", "a", "b", "a"],
            "sum_col": [1, 2, 1, 3],
            "filter_col": ["yes", "yes", "no", "yes"],
        }
    )

    mocker.patch(f"{BASE_NAME}.check_column_exist", return_value=True)
    result = get_number_routes_deduplicated(
        routes_df=test_dataframe,
        deduplication_column="deduplication_col",
        filter_query="filter_col=='yes'",
    )
    assert result == 1


def test_join_queries():
    """Test all cases join_queries"""
    result = join_queries(query=None, o_query=None)
    assert result is None, "Both queries None must result None"

    result = join_queries(query="query", o_query=None)
    assert result == "query", "Just second query None  must return just first query"

    result = join_queries(query=None, o_query="query")
    assert result == "query", "Just first query None must return just second query"

    result = join_queries(query="query", o_query="o_query")
    assert result == "(query) and (o_query)", "No query None must return queries joined"
