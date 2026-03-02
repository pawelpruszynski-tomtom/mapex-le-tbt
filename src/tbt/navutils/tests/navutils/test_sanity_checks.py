""" Test for sanity checks functions """
import pandas as pd
import shapely.geometry

from ...navutils.sanity_checks import (
    CheckResult,
    check_missing_length,
    check_missing_time,
    check_empty_routes,
    check_route_length,
    check_routes_to_be_vs_calculated,
    check_routes_outcomes,
    SanityCheckInspection,
    SanityCheckInspectionTrunQA,
)


VALID_LINE = shapely.geometry.LineString([(10, 20), (50, 60), (80, 30), (140, 60)])
INVALID_GEO = shapely.geometry.Polygon([(0, 0), (1, 1), (1, 2), (1, 1), (0, 0)])
EMPTY_GEO = shapely.geometry.LineString()


def test_check_missing_length_ok():
    """Return ok to check"""
    test_routes_df = pd.DataFrame(
        {"geom_col": [EMPTY_GEO, VALID_LINE, VALID_LINE], "len_col": [0, 10, 10]}
    )

    result, key, value = check_missing_length(
        routes_df=test_routes_df, geom_col="geom_col", len_col="len_col"
    )

    assert result == CheckResult.OK
    assert key == ""
    assert value == ""


def test_check_missing_length_ok_invalid_geo():
    """Return ok to check because invalid geos"""
    test_routes_df = pd.DataFrame(
        {"geom_col": [INVALID_GEO, INVALID_GEO, INVALID_GEO], "len_col": [10, 10, 10]}
    )

    result, key, value = check_missing_length(
        routes_df=test_routes_df, geom_col="geom_col", len_col="len_col"
    )

    assert result == CheckResult.OK
    assert key == ""
    assert value == ""


def test_check_missing_length_warning():
    """Return warning to check"""
    test_routes_df = pd.DataFrame(
        {"geom_col": [EMPTY_GEO, VALID_LINE, VALID_LINE], "len_col": [0, 10, 0]}
    )

    result, key, value = check_missing_length(
        routes_df=test_routes_df, geom_col="geom_col", len_col="len_col"
    )

    assert result == CheckResult.WARNING
    assert key == "Missing length"
    assert (
        value
        == "WARNING: No route length saved in inspection_routes on 1 routes. 33.33% percent"
    )

def test_check_missing_length_info():
    """ Return info to check """
    test_routes_df = pd.DataFrame(
        {"geom_col": [EMPTY_GEO, VALID_LINE, VALID_LINE], "len_col": [0, 10, 0]}
    )

    result, key, value = check_missing_length(
        routes_df=test_routes_df, geom_col="geom_col", len_col="len_col", threshold=0.4
    )

    assert result == CheckResult.INFO
    assert key == "Missing length"
    assert (
        value
        == "INFO: No route length saved in inspection_routes on 1 routes. 33.33% percent"
    )

def check_missing_time_ok():
    """Return ok to check"""
    test_routes_df = pd.DataFrame(
        {"geom_col": [EMPTY_GEO, VALID_LINE, VALID_LINE], "time_col": [0, 10, 10]}
    )

    result, key, value = check_missing_time(
        routes_df=test_routes_df, geom_col="geom_col", time_col="time_col"
    )

    assert result == CheckResult.OK
    assert key == ""
    assert value == ""


def test_check_missing_time_ok_invalid_geo():
    """Return ok to check because invalid geos"""
    test_routes_df = pd.DataFrame(
        {"geom_col": [INVALID_GEO, INVALID_GEO, INVALID_GEO], "time_col": [0, 10, 10]}
    )

    result, key, value = check_missing_time(
        routes_df=test_routes_df,
        geom_col="geom_col",
        time_col="time_col",
    )

    assert result == CheckResult.OK
    assert key == ""
    assert value == ""


def test_check_missing_time_warning():
    """Return warning to check"""
    test_routes_df = pd.DataFrame(
        {"geom_col": [EMPTY_GEO, VALID_LINE, VALID_LINE], "time_col": [0, 10, 0]}
    )

    result, key, value = check_missing_time(
        routes_df=test_routes_df, geom_col="geom_col", time_col="time_col"
    )

    assert result == CheckResult.WARNING
    assert key == "Missing time"
    assert (
        value
        == "WARNING: No route time saved in inspection_routes on 1 routes. 33.33% percent"
    )

def test_check_missing_time_info():
    """ Return info to check """
    test_routes_df = pd.DataFrame(
        {"geom_col": [EMPTY_GEO, VALID_LINE, VALID_LINE], "time_col": [0, 10, 0]}
    )

    result, key, value = check_missing_time(
        routes_df=test_routes_df, geom_col="geom_col", time_col="time_col", threshold=0.4
    )

    assert result == CheckResult.INFO
    assert key == "Missing time"
    assert (
        value
        == "INFO: No route time saved in inspection_routes on 1 routes. 33.33% percent"
    )

def test_check_empty_routes_ok():
    """Return ok to check"""
    test_routes_df = pd.DataFrame({"geom_col": [VALID_LINE, VALID_LINE, VALID_LINE]})

    result, key, value = check_empty_routes(
        routes_df=test_routes_df, geom_col="geom_col", missing_routes_ratio=0.05
    )

    assert result == CheckResult.OK
    assert key == ""
    assert value == ""


def test_check_empty_routes_empty_dataframe():
    """Return warning because dataframe empty"""
    test_routes_df = pd.DataFrame({"geom_col": []})

    result, key, value = check_empty_routes(
        routes_df=test_routes_df, geom_col="geom_col", missing_routes_ratio=0.05
    )

    assert result == CheckResult.WARNING
    assert key == "Empty_routes"
    assert value == "WARNING: There are no routes on dataframe"


def test_check_empty_routes_warning():
    """Return warning to check"""
    test_routes_df = pd.DataFrame(
        {"geom_col": [EMPTY_GEO, EMPTY_GEO, VALID_LINE, EMPTY_GEO]}
    )

    result, key, value = check_empty_routes(
        routes_df=test_routes_df, geom_col="geom_col", missing_routes_ratio=0.2
    )

    assert result == CheckResult.WARNING
    assert key == "Empty_routes"
    assert (
        value
        == "WARNING: Too high number of empty routes in inspection_routes = 75.00%"
    )


def test_check_route_length_ok():
    """Return ok to check"""
    test_routes_df = pd.DataFrame(
        {"geom_col": [VALID_LINE, VALID_LINE, VALID_LINE], "len_col": [10, 10, 10]}
    )

    result, key, value = check_route_length(
        routes_df=test_routes_df,
        len_col="len_col",
        routes_length_ceiling=10000,
        routes_length_ceiling_ratio=0.05,
    )

    assert result == CheckResult.OK
    assert key == ""
    assert value == ""


def test_check_route_length_empty_dataframe():
    """Return warning because dataframe empty"""
    test_routes_df = pd.DataFrame({"geom_col": [], "len_col": []})

    result, key, value = check_route_length(
        routes_df=test_routes_df,
        len_col="len_col",
        routes_length_ceiling=10000,
        routes_length_ceiling_ratio=0.05,
    )

    assert result == CheckResult.WARNING
    assert key == "Long_routes_count"
    assert value == "WARNING: There are no routes on dataframe"


def test_check_route_length_info():
    """Check should return INFO"""
    test_routes_df = pd.DataFrame(
        {"geom_col": [VALID_LINE, VALID_LINE, VALID_LINE], "len_col": [5, 5, 100]}
    )

    result, key, value = check_route_length(
        routes_df=test_routes_df,
        len_col="len_col",
        routes_length_ceiling=10,
        routes_length_ceiling_ratio=0.8,
    )

    assert result == CheckResult.INFO
    assert key == "Long_routes_count"
    assert value == "INFO: Too long routes in inspection_routes = 1"


def test_check_route_length_warning():
    """Check should return WARNING"""
    test_routes_df = pd.DataFrame(
        {
            "geom_col": [VALID_LINE, VALID_LINE, VALID_LINE, VALID_LINE],
            "len_col": [5, 5, 100, 100],
        }
    )

    result, key, value = check_route_length(
        routes_df=test_routes_df,
        len_col="len_col",
        routes_length_ceiling=10,
        routes_length_ceiling_ratio=0.2,
    )

    assert result == CheckResult.WARNING
    assert key == "Long_routes_percentage"
    assert (
        value
        == "WARNING: Too high percent of long routes in inspection_routes = 50.00%"
    )


def test_check_routes_to_be_vs_calculated_ok():
    """Check return ok"""
    test_routes_df = pd.DataFrame({"geom_col": [VALID_LINE], "len_col": [10]})

    result, key, value = check_routes_to_be_vs_calculated(
        routes_df=test_routes_df,
        sampled_routes=1,
        missing_routes_ratio=0.05,
    )

    assert result == CheckResult.OK
    assert key == ""
    assert value == ""


def test_check_routes_to_be_vs_calculated_warning():
    """Check return WARNING"""
    test_routes_df = pd.DataFrame({"geom_col": [VALID_LINE], "len_col": [10]})

    result, key, value = check_routes_to_be_vs_calculated(
        routes_df=test_routes_df,
        sampled_routes=2,
        missing_routes_ratio=0.1,
    )

    assert result == CheckResult.WARNING
    assert key == "tb_vs_real"
    assert (
        value
        == "WARNING: Too high percentage of not calculated competitor routes = 50.00%"
    )


def test_run_checks():
    """Test to check the run_checks function"""
    test_routes_df = pd.DataFrame(
        {
            "geom_col": [VALID_LINE, VALID_LINE, VALID_LINE],
            "len_col": [0, 10, 1000],
            "time_col": [0, 10, 1000],
        }
    )

    sanity = SanityCheckInspection(routes_df=test_routes_df)

    sanity.run_checks(
        sampled_routes=3,
        geom_col="geom_col",
        len_col="len_col",
        time_col="time_col",
        routes_length_ceiling=100,
        routes_length_ceiling_ratio=0.05,
        missing_routes_ratio=0.05,
    )

    assert "Missing length" in sanity.warnings.keys()
    assert (
        sanity.warnings["Missing length"]
        == "WARNING: No route length saved in inspection_routes on 1 routes. 33.33% percent"
    )

    assert "Missing time" in sanity.warnings.keys()
    assert (
        sanity.warnings["Missing time"]
        == "WARNING: No route time saved in inspection_routes on 1 routes. 33.33% percent"
    )

    assert "Long_routes_percentage" in sanity.warnings.keys()
    assert (
        sanity.warnings["Long_routes_percentage"]
        == "WARNING: Too high percent of long routes in inspection_routes = 33.33%"
    )


def test_reset_output():
    """Test to check the reset_output function"""
    test_routes_df = pd.DataFrame(
        {
            "geom_col": [VALID_LINE, VALID_LINE, VALID_LINE],
            "len_col": [0, 10, 1000],
            "time_col": [0, 10, 1000],
        }
    )

    sanity = SanityCheckInspection(routes_df=test_routes_df)

    sanity.run_checks(
        sampled_routes=3,
        geom_col="geom_col",
        len_col="len_col",
        time_col="time_col",
        routes_length_ceiling=100,
        routes_length_ceiling_ratio=0.05,
        missing_routes_ratio=0.05,
    )

    sanity.reset_output()

    assert not any(sanity.warnings)  # check dict empty
    assert not any(sanity.info)  # check dict empty


def test_run_competitor_relative_checks_ok():
    """Test to check the run_competitor_relative_checks function"""
    test_routes_df = pd.DataFrame(
        {
            "geom_col": [VALID_LINE, VALID_LINE, VALID_LINE],
            "len_col": [0, 10, 1000],
            "time_col": [0, 10, 1000],
            "ref_geom_col": [VALID_LINE, VALID_LINE, VALID_LINE],
            "ref_len_col": [0, 10, 1000],
            "ref_time_col": [1, 9, 999],
        }
    )

    sanity = SanityCheckInspection(routes_df=test_routes_df)
    sanity.run_competitor_relative_checks('len_col', 'time_col', 'ref_len_col', 'ref_time_col')
    
    assert len(sanity.warnings) == 0


def test_check_routes_outcomes():
    # Sample DataFrame for routes
    routes_df = pd.DataFrame({
        'route_outcome': ['optimal length', 'suspicious', 'optimal_length', 'suspicious', 'suspicious'],
    })

    # Test case 1: Normal case where the ratio is within limits
    ratio, code, message = check_routes_outcomes(routes_df, 'suspicious', 0.6)
    assert ratio == CheckResult.OK
    assert code == ""
    assert message == ""

    # Test case 2: Ratio exceeds the limit, should return WARNING
    ratio, code, message = check_routes_outcomes(routes_df, 'suspicious', 0.5)
    assert ratio == CheckResult.WARNING
    assert code == "suspicious_route_percentage"
    assert message == "WARNING: Too high percent of suspicious routes in inspection_routes = 60.00%"

    # Test case 3: No routes in DataFrame, should return WARNING
    empty_routes_df = pd.DataFrame({'route_outcome': []})
    ratio, code, message = check_routes_outcomes(empty_routes_df, 'suspicious', 0.5)
    assert ratio == CheckResult.WARNING
    assert code == "suspicious_count"
    assert message == "WARNING: There are no routes on dataframe"


def test_run_route_outcome_checks():
    # Prepare sample DataFrame for routes
    routes_df = pd.DataFrame({
        'route_outcome': ['suspicious', 'overlap', 'suspicious', 'suspicious', 'overlap'],
    })
    
    sanity = SanityCheckInspectionTrunQA(routes_df)

    # Test case: Warnings should be generated
    sanity.run_route_outcome_checks('suspicious', 0.5)
    assert 'suspicious_route_percentage' in sanity.warnings
    assert sanity.warnings['suspicious_route_percentage'] == "WARNING: Too high percent of suspicious routes in inspection_routes = 60.00%"
