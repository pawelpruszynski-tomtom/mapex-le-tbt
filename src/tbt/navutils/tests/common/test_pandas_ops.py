from __future__ import annotations

import datetime
import json
import uuid

import pandas as pd
import pytest

from ...common import pandas_ops

from ...common.pandas_ops import (
    aggregate_json_style,
    check_column_exist,
    get_column_value_counts,
    get_number_errors,
    get_number_routes_deduplicated,
    get_sum_column_deduplicated,
    join_queries,
    marshall_data,
    marshall_to_db,
    marshall_to_db_wrap,
    unmarshall_column,
)

import inspect
BASE_NAME = inspect.getmodule(pandas_ops).__name__

def mocked_uuid4():
    for i in range(100):
        yield f"uuid_{i}"


class MockDateTime:
    @classmethod
    def now(cls):
        return datetime.datetime(2023, 10, 4)


features_df = pd.DataFrame(
        dict(
            original_trace_id=["route-0", "route-1", "route-2", "route-3"],
            feature_id=["feature-0", "feature-1", "feature-2", "feature-3"],
            feature_id_uuid=[
                "feature-uuid-0",
                "feature-uuid-1",
                "feature-uuid-2",
                "feature-uuid-3",
            ],
            geom=[
                "LINESTRING (0 0, 1 1)",
                "LINESTRING (0 0, 1 1)",
                "LINESTRING (0 0, 1 1)",
                "LINESTRING (0 0, 1 1)",
            ],
            weight=[1, 1, 1, 1],
            weight_direction=[1, 1, 1, 1],
            org=["org-1", "org-1", "org-2", "org-2"],
            trace_date=["2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01"],
            original_length=[100, 100, 100, 100],
            confidence=[0.5, 0.5, 0.5, 0.5],
            map_match_id=[1, 1, 1, 1],
            col1=[1, 1, 1, 1],
            col2=["test-0", "test-1", "test-3", "test-4"],
            category=["cat_0", "cat_0", "cat_1", "cat_0"],
        )
    )


def test_check_column_exist():
    df = pd.DataFrame(dict(col1=["test-0", "test-1"], col2=[1, 2]))
    df_without_col = pd.DataFrame(dict(col2=[1, 2, 3]))

    assert check_column_exist(dataframe=df, column="col1") is None
    with pytest.raises(ValueError):
        check_column_exist(dataframe=df_without_col, column="col1")


def test_get_sum_column_deduplicated_without_query():
    df = pd.DataFrame(
        dict(
            route_id=["route-0", "route-1", "route-2"],
            col2=[1, 2, 3],
            category=["cat_0", "cat_1", "cat_0"],
        )
    )

    expected_value = 6
    actual_value = get_sum_column_deduplicated(routes_df=df, column="col2")

    assert actual_value == expected_value


def test_get_sum_column_deduplicated_with_query():
    df = pd.DataFrame(
        dict(
            route_id=["route-0", "route-1", "route-2"],
            col2=[1, 2, 3],
            category=["cat_0", "cat_1", "cat_0"],
        )
    )

    expected_value = 4
    expression = "category == 'cat_0'"
    actual_value = get_sum_column_deduplicated(
        routes_df=df, column="col2", filter_query=expression
    )

    assert actual_value == expected_value


def test_get_sum_column_deduplicated_with_query():
    df = pd.DataFrame(
        dict(
            route_id=["route-0", "route-1", "route-2"],
            col2=[1, 2, 3],
            category=["cat_0", "cat_1", "cat_0"],
        )
    )

    expected_value = 4
    expression = "category == 'cat_0'"
    actual_value = get_sum_column_deduplicated(
        routes_df=df, column="col2", filter_query=expression
    )

    assert actual_value == expected_value


def test_get_number_errors_with_query():
    df = pd.DataFrame(
        dict(
            route_id=["route-0", "route-1", "route-2", "route-3"],
            col2=[1, 2, 3, 4],
            category=["cat_0", "cat_1", "cat_0", "cat_0"],
            mcp_state=["ERROR", "OK", "ERROR", None],
        )
    )

    non_error_values = ["OK", "CORRECT", "discard"]
    expression = "col2 > 1"

    expected_value = 1
    actual_value = get_number_errors(
        routes_df=df,
        error_column="mcp_state",
        non_error_values=non_error_values,
        filter_query=expression,
    )

    assert actual_value == expected_value


def test_get_column_value_counts_without_query():
    df = pd.DataFrame(
        dict(
            route_id=["route-0", "route-1", "route-2", "route-3"],
            col2=[1, 2, 3, 4],
            category=["cat_0", "cat_1", "cat_0", "cat_2"],
        )
    )

    expected_value = dict(
        cat_0=2,
        cat_1=1,
        cat_2=1,
    )
    actual_value = get_column_value_counts(routes_df=df, column="category")

    assert actual_value == expected_value


def test_get_column_value_counts_with_query():
    df = pd.DataFrame(
        dict(
            route_id=["route-0", "route-1", "route-2", "route-3"],
            col2=[1, 2, 3, 4],
            category=["cat_0", "cat_1", "cat_0", "cat_2"],
        )
    )

    expected_value = dict(
        cat_0=1,
        cat_1=1,
        cat_2=1,
    )
    expression = "col2 > 1"
    actual_value = get_column_value_counts(
        routes_df=df, column="category", filter_query=expression
    )

    assert actual_value == expected_value


def test_get_number_routes_deduplicated_without_query():
    df = pd.DataFrame(
        dict(
            route_id=[
                "route-0",
                "route-0",
                "route-1",
                "route-2",
                "route-3",
                "route-3",
            ],
            col2=[1, 1, 2, 3, 4, 5],
            category=["cat_0", "cat_0", "cat_1", "cat_0", "cat_2", "cat_0"],
        )
    )

    expected_value = 4
    actual_value = get_number_routes_deduplicated(routes_df=df)

    assert actual_value == expected_value


def test_get_number_routes_deduplicated_with_query():
    df = pd.DataFrame(
        dict(
            route_id=[
                "route-0",
                "route-0",
                "route-1",
                "route-2",
                "route-3",
                "route-3",
            ],
            col2=[1, 1, 2, 3, 4, 5],
            category=["cat_0", "cat_0", "cat_1", "cat_0", "cat_2", "cat_0"],
        )
    )

    expected_value = 2
    expression = "col2 > 2"
    actual_value = get_number_routes_deduplicated(routes_df=df, filter_query=expression)

    assert actual_value == expected_value


def test_get_number_routes_deduplicated_with_query():
    df = pd.DataFrame(
        dict(
            route_id=[
                "route-0",
                "route-0",
                "route-1",
                "route-2",
                "route-3",
                "route-3",
            ],
            col2=[1, 1, 2, 3, 4, 5],
            category=["cat_0", "cat_0", "cat_1", "cat_0", "cat_2", "cat_0"],
        )
    )

    expected_value = 2
    expression = "col2 > 2"
    actual_value = get_number_routes_deduplicated(routes_df=df, filter_query=expression)

    assert actual_value == expected_value


def test_join_queries_one_empty():
    query_1 = "col1 == 'test'"
    query_2 = None

    expected_value = "col1 == 'test'"
    actual_value = join_queries(query_1, query_2)

    assert actual_value == expected_value


def test_join_queries_both_empty():
    query_1 = None
    query_2 = None

    expected_value = None
    actual_value = join_queries(query_1, query_2)

    assert actual_value == expected_value


def test_aggregate_json_style():
    df = pd.DataFrame(
        dict(
            route_id=["route-0", "route-0", "route-1", "route-1"],
            col2=[1, 1, 2, 3],
            category=["cat_0", "cat_0", "cat_1", "cat_0"],
        )
    )

    expected_value = dict(category="cat_0", col2=1, route_id="route-0")
    actual_value = aggregate_json_style(df.iloc[0], df.columns.values)

    assert actual_value == expected_value


def test_marshall_to_db():
    df = pd.DataFrame(
        dict(
            route_id=["route-0", "route-0", "route-1", "route-1"],
            col2=[1, 1, 2, 3],
            category=["cat_0", "cat_0", "cat_1", "cat_0"],
        )
    )

    expected_value = pd.DataFrame(
        dict(
            route_id=["route-0", "route-0", "route-1", "route-1"],
            tags=[
                dict(
                    col2=1,
                    category="cat_0",
                ),
                dict(
                    col2=1,
                    category="cat_0",
                ),
                dict(
                    col2=2,
                    category="cat_1",
                ),
                dict(
                    col2=3,
                    category="cat_0",
                ),
            ],
        )
    )

    id_columns = ["route_id"]
    actual_value = marshall_to_db(df=df, id_columns=id_columns)

    pd.testing.assert_frame_equal(actual_value, expected_value)


def test_marshall_to_db_wrap():



    expected_tags = list(
        map(
            json.dumps,
            [
                dict(category="cat_0", col1=1, col2="test-0"),
                dict(category="cat_0", col1=1, col2="test-1"),
                dict(category="cat_1", col1=1, col2="test-3"),
                dict(category="cat_0", col1=1, col2="test-4"),
            ],
        )
    )

    expected_value = pd.DataFrame(
        dict(
            original_trace_id=["route-0", "route-1", "route-2", "route-3"],
            feature_id=["feature-0", "feature-1", "feature-2", "feature-3"],
            feature_id_uuid=[
                "feature-uuid-0",
                "feature-uuid-1",
                "feature-uuid-2",
                "feature-uuid-3",
            ],
            geom=[
                "LINESTRING (0 0, 1 1)",
                "LINESTRING (0 0, 1 1)",
                "LINESTRING (0 0, 1 1)",
                "LINESTRING (0 0, 1 1)",
            ],
            weight=[1, 1, 1, 1],
            weight_direction=[1, 1, 1, 1],
            tags=expected_tags,
        )
    )

    actual_value = marshall_to_db_wrap(features_df)

    pd.testing.assert_frame_equal(actual_value, expected_value)


def test_marshall_data(mocker):

    mocker.patch(f"{BASE_NAME}.uuid.uuid4", mocked_uuid4().__next__)
    mocker.patch(f"{BASE_NAME}.datetime", MockDateTime())

    expected_tags = list(
        map(
            json.dumps,
            [
                dict(
                    category="cat_0",
                    col1=1,
                    col2="test-0",
                    confidence=0.5,
                    feature_id_uuid="feature-uuid-0",
                    map_match_id=1,
                    org="org-1",
                    original_length=100,
                    trace_date="2021-01-01",
                ),
                dict(
                    category="cat_0",
                    col1=1,
                    col2="test-1",
                    confidence=0.5,
                    feature_id_uuid="feature-uuid-1",
                    map_match_id=1,
                    org="org-1",
                    original_length=100,
                    trace_date="2021-01-01",
                ),
                dict(
                    category="cat_1",
                    col1=1,
                    col2="test-3",
                    confidence=0.5,
                    feature_id_uuid="feature-uuid-2",
                    map_match_id=1,
                    org="org-2",
                    original_length=100,
                    trace_date="2021-01-01",
                ),
                dict(
                    category="cat_0",
                    col1=1,
                    col2="test-4",
                    confidence=0.5,
                    feature_id_uuid="feature-uuid-3",
                    map_match_id=1,
                    org="org-2",
                    original_length=100,
                    trace_date="2021-01-01",
                ),
            ],
        )
    )

    expected_features = pd.DataFrame(
        dict(
            map_info_id=["uuid_0", "uuid_0", "uuid_0", "uuid_0"],
            original_trace_id=["route-0", "route-1", "route-2", "route-3"],
            feature_id=["feature-0", "feature-1", "feature-2", "feature-3"],
            geom=[
                "LINESTRING (0 0, 1 1)",
                "LINESTRING (0 0, 1 1)",
                "LINESTRING (0 0, 1 1)",
                "LINESTRING (0 0, 1 1)",
            ],
            weight=[1, 1, 1, 1],
            weight_direction=[1, 1, 1, 1],
            tags=expected_tags,
        )
    )

    sample_metadata = dict(sample_id="sample-0", country="country-0", region="region-0")

    expected_sanity_checks = json.dumps(
        dict(
            commit_version="commit-version-0",
            tlos_utils_commit="tlos-utils-commit-0",
            km_mapmatched=627.598,
        )
    )

    expected_features_metadata = dict(
        map_info_id="uuid_0",
        sample_id="sample-0",
        provider="provider-0",
        provider_version="provider-version-0",
        date_generated="2023-10-04 00:00:00",
        elapsed_time=1,
        country="country-0",
        region="region-0",
        sanity_checks=expected_sanity_checks,
    )

    params = dict(
        provider="provider-0",
        provider_version="provider-version-0",
        commit_version="commit-version-0",
        tlos_utils_commit="tlos-utils-commit-0",
    )

    map_matching_time = "1.0"

    actual_featurtes_metadata, actual_features = marshall_data(
        features=features_df,
        sample_metadata=sample_metadata,
        map_matching_time=map_matching_time,
        params=params,
    )

    pd.testing.assert_frame_equal(actual_features, expected_features)
    assert actual_featurtes_metadata == expected_features_metadata

def test_unmarshall_column():
    df = pd.DataFrame(
        dict(
            route_id=["route-0", "route-1", "route-2", "route-3"],
            tags=[
                dict(
                    col2=1,
                    category="cat_0",
                ),
                dict(
                    col2=1,
                    category="cat_0",
                ),
                dict(
                    col2=2,
                    category="cat_1",
                ),
                dict(
                    col2=3,
                    category="cat_0",
                ),
            ],
        )
    )

    expected_value = pd.DataFrame(
        dict(
            route_id=["route-0", "route-1", "route-2", "route-3"],
            col2=[1, 1, 2, 3],
            category=["cat_0", "cat_0", "cat_1", "cat_0"],
        )
    )

    actual_value = unmarshall_column(df=df, tag_col_name="tags")

    pd.testing.assert_frame_equal(actual_value, expected_value)
