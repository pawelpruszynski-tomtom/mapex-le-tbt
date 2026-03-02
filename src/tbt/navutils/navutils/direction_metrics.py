""" Functions for directions metrics """
from functools import partial
from typing import Callable, Dict, Any, Optional

import pandas as pd

from ..common.pandas_ops import check_column_exist, join_queries

TBT_HDR_ROUTING_ERROR_TYPES = [
    "WDTRF",
    "MMAN",
    "MGSC",
    "MBP",
    "WRC",
    "SGEO",
    "WGEO",
    "IMPLICIT",
]
TBT_HDR_ROUTING_ERRORS_DICT = {}
for _err in TBT_HDR_ROUTING_ERROR_TYPES:
    TBT_HDR_ROUTING_ERRORS_DICT[_err] = [_err]
TBT_HDR_ROUTING_ERRORS_DICT["oneway"] = ["WDTRF"]
TBT_HDR_ROUTING_ERRORS_DICT["turnrest"] = ["MMAN", "MGSC", "MBP"]
TBT_HDR_ROUTING_ERRORS_DICT["nonnav"] = ["WRC", "SGEO", "WGEO"]

TBT_HDR_SUBTYPE_ERROR_SUBTYPES = [
    "MBP_TEMP",
    "MBP_NO-TEMP",
    "MMAN_U-TURN",
    "IMPLICIT_U-TURN",
]

TBT_HDR_SUBTYPE_ERROR_SUBTYPES_DICT = {}
for _err in TBT_HDR_SUBTYPE_ERROR_SUBTYPES:
    TBT_HDR_SUBTYPE_ERROR_SUBTYPES_DICT[_err] = [_err]

IMPORTANCE_LEVEL = {
    "High": [
        "primary",
        "motorway",
        "secondary",
        "tertiary",
        "trunk",
        "primary_link",
        "motorway_link",
        "secondary_link",
        "tertiary_link",
        "trunk_link",
    ],
    "Medium": ["residential", "unclassified", "road", "living_street", "track"],
    "Low": ["service"],
    "Other": ["other"],
}
IMPORTANCE_MAPPING = {}
for clave, valores in IMPORTANCE_LEVEL.items():
    for valor in valores:
        IMPORTANCE_MAPPING[valor] = clave

ROUTER_ERROR_TYPES = [
    "alignment", 
    "MGEO", 
    "SF", 
    "SBP", 
    "SMAN", 
    "SGSC", 
    "WDTRF", 
    "IOW", 
    "SOW"
]
ROUTER_ERRORS_DICT = {}
for _err in ROUTER_ERROR_TYPES:
    ROUTER_ERRORS_DICT[_err] = [_err]
ROUTER_ERRORS_DICT["oneway"] = ["WDTRF", "IOW", "SOW"]
ROUTER_ERRORS_DICT["turnrest"] = ["SBP", "SMAN", "SGSC"]
ROUTER_ERRORS_DICT["nonnav"] = ["MGEO", "SF"]

ROUTER_ERROR_DICT_v2 = {
    "mgeo_sv": ["MGEO_severe"],
    "mgeo_nonsv": ["MGEO_start_end"],
    "nonnav_sv": ["MGEO_severe", "SF_severe", "SF_start_end"]
}



def _weighted_average(*, values: pd.Series, weightings: Dict[Any, float]) -> float:
    # Convert the wightings to a pandas Serie and remove weights not present on
    weightings_serie = pd.Series(weightings)
    weightings_serie = weightings_serie[weightings_serie.index.isin(values.index)]

    return float((values * weightings_serie).sum() / weightings_serie.sum())


def eph(
    *,
    routes_df: pd.DataFrame,
    get_hours: Callable[..., float],
    get_errors: Callable[..., int],
    filter_query: Optional[str] = None,
    grouping_col: Optional[str] = None,
    weightings_dict: Optional[Dict[Any, float]] = None,
) -> float:
    """Function for eph (errors per hour) calculation

    :param routes_df: pandas dataframe with routes including times
    :type routes_df: pd.DataFrame
    :param get_hours: Function to get the number of hours
    :type get_hours: Callable[..., float]
    :param get_errors: Function to get the number of errors
    :type get_errors: Callable[..., int]
    :param filter_query: Query to filter the dataframe, defaults to None
    :type filter_query: Optional[str], optional
    :param grouping_col: Column to group for weighted average, defaults to None
    :type grouping_col: Optional[str], optional
    :param weightings: Weights for weighted average, defaults to None
    :type weightings: Optional[Dict[Any,float]], optional
    :return: The eph value
    :rtype: float
    """
    if grouping_col:
        check_column_exist(dataframe=routes_df, column=grouping_col)

    filtered_routes_df = routes_df.copy()
    if filter_query:
        filtered_routes_df = routes_df.query(expr=filter_query)

    if len(filtered_routes_df) == 0:
        return 0.0

    if grouping_col is None:
        errors_filter_query = filter_query
        if isinstance(get_errors, partial):
            errors_filter_query = get_errors.keywords.get("filter_query", None)
            # Here we are joining the filter_query of eph
            # with the filter_query already on get_errors function (if exists)
            errors_filter_query = join_queries(errors_filter_query, filter_query)
        errors = get_errors(
            routes_df=filtered_routes_df, filter_query=errors_filter_query
        )
        hours = get_hours(routes_df=routes_df)
        return errors / hours if hours else 0

    if weightings_dict is None:
        raise ValueError("Weightings must be defined when using grouping_col")

    errors = filtered_routes_df.groupby(grouping_col).apply(
        lambda group_df: get_errors(routes_df=group_df)
    )
    hours = routes_df.groupby(grouping_col).apply(
        lambda group_df: get_hours(routes_df=group_df)
    )
    eph_values: pd.Series = errors / hours
    eph_values = eph_values.sort_index()

    return _weighted_average(values=eph_values, weightings=weightings_dict)


def epk(
    *,
    routes_df: pd.DataFrame,
    get_kms: Callable[..., float],
    get_errors: Callable[..., int],
    filter_query: Optional[str] = None,
) -> float:
    """Function for epk (errors per km) calculation

    :param routes_df: pandas dataframe with routes including times
    :type routes_df: pd.DataFrame
    :param get_kms: Function to get the number of kms
    :type get_kms: Callable[..., float]
    :param get_errors: Function to get the number of errors
    :type get_errors: Callable[..., int]
    :param filter_query: Query to filter the dataframe, defaults to None
    :type filter_query: Optional[str], optional
    :return: The eph value
    :rtype: float
    """

    filtered_routes_df = routes_df.copy()
    if filter_query:
        filtered_routes_df = routes_df.query(expr=filter_query)

    if len(filtered_routes_df) == 0:
        return 0.0

    errors_filter_query = filter_query
    if isinstance(get_errors, partial):
        errors_filter_query = get_errors.keywords.get("filter_query", None)
        # Here we are joining the filter_query of eph
        # with the filter_query already on get_errors function (if exists)
        errors_filter_query = join_queries(errors_filter_query, filter_query)
    errors = get_errors(
        routes_df=filtered_routes_df, filter_query=errors_filter_query
    )
    kms = get_kms(routes_df=routes_df)
    
    return errors / kms if kms else 0


def failed_routes(
    *,
    routes_df: pd.DataFrame,
    get_routes_error: Callable[..., int],
    get_routes: Callable[..., int],
    filter_query: Optional[str] = None,
    grouping_col: Optional[str] = None,
    weightings_dict: Optional[Dict[Any, float]] = None,
):
    """Function for failed foutes metric calculation

    :param routes_df: pandas dataframe with routes including times
    :type routes_df: pd.DataFrame
    :param filter_query: Query to filter the dataframe, defaults to None
    :type filter_query: Optional[str], optional
    :param grouping_col: Column to group for weighted average, defaults to None
    :type grouping_col: Optional[str], optional
    :param weightings_dict: Weights for weighted average, defaults to None
    :type weightings_dict: Optional[List[float]], optional
    :return: The %fr value
    :rtype: float
    """
    if grouping_col:
        check_column_exist(dataframe=routes_df, column=grouping_col)

    filtered_routes_df = routes_df.copy()
    if filter_query:
        filtered_routes_df = routes_df.query(expr=filter_query)

    if len(filtered_routes_df) == 0:
        return 0.0

    if grouping_col is None:
        errors_filter_query = filter_query
        if isinstance(get_routes_error, partial):
            errors_filter_query = get_routes_error.keywords.get("filter_query", None)
            # Here we are joining the  of failed_routes
            # with the filter_query already on errors_filter_query function (if exists)
            errors_filter_query = join_queries(errors_filter_query, filter_query)

        routes_with_error = get_routes_error(
            routes_df=filtered_routes_df, filter_query=errors_filter_query
        )
        routes = get_routes(routes_df=routes_df)
        return routes_with_error / routes if routes else 0

    if weightings_dict is None:
        raise ValueError("Weightings must be defined when using grouping_col")

    routes_with_errors = filtered_routes_df.groupby(grouping_col).apply(
        lambda group_df: get_routes_error(routes_df=group_df)
    )
    routes = routes_df.groupby(grouping_col).apply(
        lambda group_df: get_routes(routes_df=group_df)
    )
    fr_values = routes_with_errors / routes
    fr_values = fr_values.sort_index()

    return _weighted_average(values=fr_values, weightings=weightings_dict)
