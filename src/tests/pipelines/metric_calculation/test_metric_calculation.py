"""
This module contains an example test.

Tests should be placed in ``src/tests``, in modules that mirror your
project's structure, and in files named test_*.py. They are simply functions
named ``test_*`` which test a unit of logic.

To run the tests, run ``kedro test`` from the project root directory.
"""

import json
import math

import pandas
import pyspark.sql.functions as F
import pytest
import tbt.pipelines.metric_calculation
from pandas._testing import assert_frame_equal


# READING INPUT DATA
@pytest.fixture
def completed_new_runs():
    return pandas.read_parquet(
        "src/tests/pipelines/metric_calculation/input_data/completed_new_runs.parquet"
    )


@pytest.fixture
def df_routes():
    return pandas.read_parquet(
        "src/tests/pipelines/metric_calculation/input_data/df_routes.parquet"
    )


@pytest.fixture
def metrics_per_error_type():
    with open(
        "src/tests/pipelines/metric_calculation/input_data/metrics_per_error_type.json",
        "r",
    ) as f:
        data = json.load(f)
    return data


@pytest.fixture
def metrics_per_mqs():
    with open(
        "src/tests/pipelines/metric_calculation/input_data/metrics_per_mqs.json", "r"
    ) as f:
        data = json.load(f)
    return data


@pytest.fixture
def expected_critical_sections_with_mcp_feedback_query():
    with open(
        "src/tests/pipelines/metric_calculation/output_data/critical_sections_with_mcp_feedback_query.txt",
        "r",
    ) as f:
        content = f.read()
    return content


@pytest.fixture
def expected_critical_sections_with_mcp_feedback_history_query():
    with open(
        "src/tests/pipelines/metric_calculation/output_data/critical_sections_with_mcp_feedback_history_query.txt",
        "r",
    ) as f:
        content = f.read()
    return content


@pytest.fixture
def expected_inspection_routes_query():
    with open(
        "src/tests/pipelines/metric_calculation/output_data/inspection_routes_query.txt",
        "r",
    ) as f:
        content = f.read()
    return content


# READING OUTPUT DATA
@pytest.fixture
def expected_error_logs_query():
    with open(
        "src/tests/pipelines/metric_calculation/output_data/error_logs_query.txt", "r"
    ) as f:
        content = f.read()
    return content


@pytest.fixture
def critical_sections_with_mcp_feedback():
    return pandas.read_parquet(
        "src/tests/pipelines/metric_calculation/input_data/critical_sections_with_mcp_feedback.parquet"
    )


@pytest.fixture
def critical_sections_with_mcp_feedback_history():
    return pandas.read_parquet(
        "src/tests/pipelines/metric_calculation/input_data/critical_sections_with_mcp_feedback_history.parquet"
    )


@pytest.fixture
def inspection_routes():
    return pandas.read_parquet(
        "src/tests/pipelines/metric_calculation/input_data/inspection_routes.parquet"
    )


@pytest.fixture
def expected_metric_computation_data():
    return pandas.read_parquet(
        "src/tests/pipelines/metric_calculation/output_data/metric_computation_data.parquet"
    )


@pytest.fixture
def expected_metrics():
    return pandas.read_parquet(
        "src/tests/pipelines/metric_calculation/output_data/metrics.parquet"
    )


@pytest.fixture
def expected_inspection_info_query():
    with open(
        "src/tests/pipelines/metric_calculation/output_data/inspection_info_query.txt",
        "r",
    ) as f:
        content = f.read()
    return content


@pytest.fixture
def expected_metrics_with_inspection_info():
    return pandas.read_parquet(
        "src/tests/pipelines/metric_calculation/output_data/metrics_with_inspection_info.parquet"
    )


@pytest.fixture
def inspection_info():
    inspection_info = pandas.read_parquet(
        "src/tests/pipelines/metric_calculation/input_data/inspection_info.parquet"
    )
    inspection_info["sanity_info"] = inspection_info["sanity_info"].apply(json.loads)
    return inspection_info


### SUPPORTING FUNCTIONS
@pytest.fixture
def df_route():
    return pandas.read_parquet(
        "src/tests/pipelines/metric_calculation/input_data/df_routes.parquet"
    )


@pytest.fixture
def metrics_per_error_type():
    with open(
        "src/tests/pipelines/metric_calculation/input_data/metrics_per_error_type.json",
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def metrics_per_mqs():
    with open(
        "src/tests/pipelines/metric_calculation/input_data/metrics_per_mqs.json", "r"
    ) as f:
        return json.load(f)


### TESTING
class TestPipelines:
    def test_tbt_metric_pipeline(self):
        pipeline = tbt.pipelines.metric_calculation.create_pipeline()

    def test_populate_critical_sections_with_mcp_feedback_queries(
        self,
        completed_new_runs,
        expected_error_logs_query,
        expected_critical_sections_with_mcp_feedback_query,
        expected_critical_sections_with_mcp_feedback_history_query,
    ):
        (
            completed_new_runs_from_memory,
            error_logs_query,
            critical_sections_with_mcp_feedback_query,
            critical_sections_with_mcp_feedback_history_query,
        ) = tbt.pipelines.metric_calculation.nodes.populate_critical_sections_with_mcp_feedback_queries(
            completed_new_runs=completed_new_runs
        )

        assert_frame_equal(completed_new_runs, completed_new_runs_from_memory)

        assert error_logs_query.strip() == expected_error_logs_query.strip()
        assert (
            critical_sections_with_mcp_feedback_query.strip()
            == expected_critical_sections_with_mcp_feedback_query.strip()
        )
        assert (
            critical_sections_with_mcp_feedback_history_query.strip()
            == expected_critical_sections_with_mcp_feedback_history_query.strip()
        )

    def test_get_routes_data_queries(
        self, completed_new_runs, expected_inspection_routes_query
    ):
        (inspection_routes_query, dummy) = (
            tbt.pipelines.metric_calculation.nodes.get_routes_data_queries(
                completed_new_runs_from_memory=completed_new_runs,
            )
        )

        assert (
            inspection_routes_query.strip() == expected_inspection_routes_query.strip()
        )

    def test_prepare_data_for_metric_computation(
        self,
        critical_sections_with_mcp_feedback,
        critical_sections_with_mcp_feedback_history,
        inspection_routes,
        expected_metric_computation_data,
    ):
        metric_computation_data = tbt.pipelines.metric_calculation.nodes.prepare_data_for_metric_computation(
            critical_sections_with_mcp_feedback=critical_sections_with_mcp_feedback,
            critical_sections_with_mcp_feedback_history=critical_sections_with_mcp_feedback_history,
            inspection_routes=inspection_routes,
            dummy=True,
        )

        assert_frame_equal(metric_computation_data, expected_metric_computation_data)

    def test_compute_metrics(
        self, completed_new_runs, expected_metric_computation_data, expected_metrics
    ):
        (metrics, bootstrap_sdf, dummy) = (
            tbt.pipelines.metric_calculation.nodes.compute_metrics(
                completed_new_runs_from_memory=completed_new_runs,
                metric_computation_data=expected_metric_computation_data,
            )
        )

        assert "eph_lower" in metrics.columns
        assert "eph_upper" in metrics.columns
        assert "failed_routes_lower" in metrics.columns
        assert "failed_routes_upper" in metrics.columns
        assert "metrics_per_error_type" in metrics.columns
        assert "metrics_per_mqs" in metrics.columns

        assert_frame_equal(
            metrics.drop(
                [
                    "eph_lower",
                    "eph_upper",
                    "failed_routes_lower",
                    "failed_routes_upper",
                    "metrics_per_error_type",
                    "metrics_per_mqs",
                ],
                axis=1,
            ),
            expected_metrics.drop(
                [
                    "eph_lower",
                    "eph_upper",
                    "failed_routes_lower",
                    "failed_routes_upper",
                    "metrics_per_error_type",
                    "metrics_per_mqs",
                    "publish_harold",
                    "publish_value_stream",
                ],
                axis=1,
            ),
        )

    def test_get_inspection_info_query(
        self, completed_new_runs, expected_inspection_info_query
    ):
        (inspection_info_query, dummy) = (
            tbt.pipelines.metric_calculation.nodes.get_inspection_info_query(
                completed_new_runs_from_memory=completed_new_runs,
                dummy=True,
            )
        )

        assert inspection_info_query.strip() == expected_inspection_info_query.strip()

    def test_append_inspection_info(
        self, expected_metrics_with_inspection_info, inspection_info, expected_metrics
    ):
        (metrics_with_inspection_info, dummy) = (
            tbt.pipelines.metric_calculation.nodes.append_inspection_info(
                scheduled_inspection_info=inspection_info,
                metrics=expected_metrics,
                dummy=True,
            )
        )
        assert_frame_equal(
            metrics_with_inspection_info.drop("metric_computation_date", axis=1),
            expected_metrics_with_inspection_info.drop(
                "metric_computation_date", axis=1
            ),
        )

    def test_metric_calculation(
        self, df_routes, metrics_per_error_type, metrics_per_mqs
    ):
        run_id = "db12856e-d8c7-4bca-b992-d9592e3e8154"

        (
            metrics,
            bootstrap_df_,
        ) = tbt.pipelines.metric_calculation.nodes.metric_calculation_run_id(
            run_id=run_id, df_routes=df_routes, resamples=5
        )

        bootstrap_df_ = bootstrap_df_.withColumn("run_id", F.lit(run_id))
        bootstrap_df_ = bootstrap_df_.toPandas()

        assert bootstrap_df_.routes.max() == 51
        assert bootstrap_df_.routes.min() == 51
        assert len(metrics) == 1
        assert math.isclose(metrics.eph.values[0], 4.446208541980137)
        assert math.isclose(metrics.failed_routes.values[0], 0.058823529411764705)
        assert math.isclose(metrics.provider_hours.values[0], 1.405)
        assert math.isclose(metrics.provider_km.values[0], 46.4683073425293)
        assert metrics.errors.values[0] == 5
        assert metrics.routes.values[0] == 51
        assert (
            metrics.rac_state.values[0]
            == '{"optimal": 39, "routing_issue": 9, "potential_error": 7}'
        )
        assert math.isclose(metrics.eph_lower.values[0], 2.9096224775369164)
        assert math.isclose(metrics.eph_upper.values[0], 5.895569705746665)
        assert math.isclose(metrics.failed_routes_lower.values[0], 0.027450980392156862)
        assert math.isclose(metrics.failed_routes_upper.values[0], 0.11372549019607843)

        metrics_per_error_type_result = json.loads(metrics.metrics_per_error_type[0])

        for error_type, results in metrics_per_error_type_result.items():
            for key, value in results.items():
                if isinstance(value, int):
                    assert metrics_per_error_type[error_type][key] == value
                else:
                    assert math.isclose(metrics_per_error_type[error_type][key], value)

        metrics_per_mqs_result = json.loads(metrics.metrics_per_mqs[0])

        for mqs, mqs_results in metrics_per_mqs_result.items():
            for key, value in mqs_results.items():
                if isinstance(value, int):
                    assert metrics_per_mqs[mqs][key] == value
                elif isinstance(value, float):
                    assert math.isclose(
                        metrics_per_mqs[mqs][key], value
                    ), f"failed to compare metric_per_mqs {mqs}, {key}"
                elif isinstance(value, dict):
                    for metric_key, metric_results in value.items():
                        for key_, value_ in metric_results.items():
                            if isinstance(value, int):
                                assert (
                                    metrics_per_mqs[mqs][key][metric_key][key_]
                                    == value_
                                ), f"failed to compare metric_per_mqs {mqs}, {key}, {metric_key}"
                            else:
                                assert math.isclose(
                                    metrics_per_mqs[mqs][key][metric_key][key_],
                                    value_,
                                ), f"failed to compare metric_per_mqs {mqs}, {key}, {metric_key}, {key_}"


def test_filter_routes_num_errors_ok():
    test_df = pandas.DataFrame(
        {
            "route_id": [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 4],
            "mcp_state": [
                "MMAN",
                "MMAN",
                "MMAN",
                "MMAN",
                "MMAN",
                "discard",
                "discard",
                "IMPLICIT",
                "MMAN",
                "MMAN",
                "MMAN",
                "MMAN",
                "MMAN",
            ],
        }
    )

    result = tbt.pipelines.metric_calculation.nodes.filter_routes_num_errors(
        df_routes=test_df
    )

    assert result[0] == 1
    assert result[1] == [1]
