from functools import partial
from typing import Dict, Tuple

import pandas as pd

from tbt.navutils.common.metric_computation import Level, QualityMetric
from tbt.navutils.common.pandas_ops import (
    check_column_exist,
    get_column_value_counts,
    get_number_errors,
    get_number_routes_deduplicated,
    get_sum_column_deduplicated,
    join_columns,
)
from tbt.navutils.navutils.direction_metrics import (
    TBT_HDR_ROUTING_ERRORS_DICT,
    TBT_HDR_SUBTYPE_ERROR_SUBTYPES_DICT,
    eph,
    failed_routes,
)

TBT_WEIGHTINGS = [0.5, 0.4, 0.1]

COMMON_GLOBAL_STATS = {
    "provider_hours": partial(get_sum_column_deduplicated, column="prov_hours"),
    "provider_km": partial(get_sum_column_deduplicated, column="prov_km"),
    "competitor_hours": partial(get_sum_column_deduplicated, column="comp_hours"),
    "competitor_km": partial(get_sum_column_deduplicated, column="comp_km"),
    "routes": get_number_routes_deduplicated,
    "rac_state": partial(get_column_value_counts, column="rac_state"),  # type: ignore
}


def _create_tbt_weightings_dict(routes_df: pd.DataFrame) -> Dict[str, float]:
    qualities = sorted(routes_df.quality.unique())
    return {quality: weighting for quality, weighting in zip(qualities, TBT_WEIGHTINGS)}


def create_tbt_metric(
    routes_df, error_col: str = "mcp_state", subtype_col: str = "error_subtype"
) -> Tuple[QualityMetric, QualityMetric]:
    check_column_exist(dataframe=routes_df, column=error_col)

    # modify routes_df error_subtype column to use it on level
    routes_df[subtype_col] = routes_df.apply(
        lambda row: join_columns(row, [error_col, subtype_col]), axis=1
    )

    weightings_dict = _create_tbt_weightings_dict(routes_df)

    level_per_error_type = Level(
        name="per_error_type",
        column=f"{error_col}",
        values=TBT_HDR_ROUTING_ERRORS_DICT,
        metrics={
            "eph": partial(
                eph,
                get_errors=partial(
                    get_number_errors, error_column=f"{error_col}"  # type: ignore
                ),
                get_hours=partial(get_sum_column_deduplicated, column="prov_hours"),  # type: ignore
                grouping_col="quality",
                weightings_dict=weightings_dict,
            ),
            "failed_routes": partial(
                failed_routes,
                get_routes_error=partial(
                    get_number_routes_deduplicated,
                    filter_query=f"{error_col} not in ['discard'] and ~{error_col}.isna()",
                ),  # type: ignore
                get_routes=get_number_routes_deduplicated,  # type: ignore
                grouping_col=None, # No MQS weighting for %FR
                weightings_dict=weightings_dict,
            ),
        },
        stats={"errors": partial(get_number_errors, error_column=f"{error_col}")},
    )

    level_per_error_subtype = Level(
        name="per_error_subtype",
        column=subtype_col,
        values=TBT_HDR_SUBTYPE_ERROR_SUBTYPES_DICT,
        metrics={
            "eph": partial(
                eph,
                get_errors=partial(
                    get_number_errors, error_column=f"{error_col}"  # type: ignore
                ),
                get_hours=partial(get_sum_column_deduplicated, column="prov_hours"),  # type: ignore
                grouping_col="quality",
                weightings_dict=weightings_dict,
            ),
            "failed_routes": partial(
                failed_routes,
                get_routes_error=partial(
                    get_number_routes_deduplicated,
                    filter_query=f"{error_col} not in ['discard'] and ~{error_col}.isna()",
                ),  # type: ignore
                get_routes=get_number_routes_deduplicated,  # type: ignore
                grouping_col=None, # No MQS weighting for %FR
                weightings_dict=weightings_dict,
            ),
        },
        stats={"errors": partial(get_number_errors, error_column=f"{error_col}")},
    )

    quality_metric = QualityMetric(
        routes_df=routes_df,
        levels=[level_per_error_type, level_per_error_subtype],
        global_stats={
            **COMMON_GLOBAL_STATS,
            **{
                "errors": partial(
                    get_number_errors,
                    error_column=f"{error_col}",
                    filter_query=f"{error_col} not in ['IMPLICIT']",
                )
            },
        },
        global_metrics={
            "eph": partial(
                eph,
                get_errors=partial(
                    get_number_errors,
                    error_column=f"{error_col}",
                    filter_query=f"{error_col} not in ['IMPLICIT']",
                ),  # type: ignore
                get_hours=partial(get_sum_column_deduplicated, column="prov_hours"),  # type: ignore
                grouping_col="quality",
                weightings_dict=weightings_dict,
            ),
            "failed_routes": partial(
                failed_routes,
                get_routes_error=partial(
                    get_number_routes_deduplicated,
                    filter_query=f"{error_col} not in ['discard', 'IMPLICIT'] and ~{error_col}.isna()",
                ),  # type: ignore
                get_routes=get_number_routes_deduplicated,  # type: ignore
                grouping_col=None, # No MQS weighting for %FR
                weightings_dict=weightings_dict,
            ),
        },
        columns_on_bootstrap=[
            "provider_hours",
            "provider_km",
            "competitor_hours",
            "competitor_km",
            "errors",
            "routes",
            "eph",
            "failed_routes",
        ],
    )

    level_per_error_type_mqs = Level(
        name="metrics_per_error_type",
        column=f"{error_col}",
        values=TBT_HDR_ROUTING_ERRORS_DICT,
        metrics={
            "eph": partial(
                eph,
                get_errors=partial(
                    get_number_errors, error_column=f"{error_col}"  # type: ignore
                ),
                get_hours=partial(get_sum_column_deduplicated, column="prov_hours"),  # type: ignore
            ),
            "failed_routes": partial(
                failed_routes,
                get_routes_error=partial(
                    get_number_routes_deduplicated,
                    filter_query=f"{error_col} not in ['discard'] and ~{error_col}.isna()",
                ),  # type: ignore
                get_routes=get_number_routes_deduplicated,  # type: ignore
            ),
        },
        stats={"errors": partial(get_number_errors, error_column=f"{error_col}")},
    )

    quality_metric_mqs = QualityMetric(
        routes_df=routes_df,
        levels=[level_per_error_type_mqs],
        global_stats={
            "provider_hours": partial(get_sum_column_deduplicated, column="prov_hours"),
            "provider_km": partial(get_sum_column_deduplicated, column="prov_km"),
            "competitor_hours": partial(
                get_sum_column_deduplicated, column="comp_hours"
            ),
            "competitor_km": partial(get_sum_column_deduplicated, column="comp_km"),
            "errors": partial(
                get_number_errors,
                error_column=f"{error_col}",
                filter_query=f"{error_col} not in ['IMPLICIT']",
            ),
            "routes": get_number_routes_deduplicated,
            # "rac_state": partial(get_column_value_counts, column="rac_state"),  # type: ignore
        },
        global_metrics={
            "eph": partial(
                eph,
                get_errors=partial(
                    get_number_errors,
                    error_column=f"{error_col}",
                    filter_query=f"{error_col} not in ['IMPLICIT']",
                ),  # type: ignore
                get_hours=partial(get_sum_column_deduplicated, column="prov_hours"),  # type: ignore
            ),
            "failed_routes": partial(
                failed_routes,
                get_routes_error=partial(
                    get_number_routes_deduplicated,
                    filter_query=f"{error_col} not in ['discard', 'IMPLICIT'] and ~{error_col}.isna()",
                ),  # type: ignore
                get_routes=get_number_routes_deduplicated,  # type: ignore
            ),
        },
        columns_on_bootstrap=[
            "provider_hours",
            "provider_km",
            "competitor_hours",
            "competitor_km",
            "errors",
            "routes",
            "eph",
            "failed_routes",
        ],
    )

    return quality_metric, quality_metric_mqs


def create_hdr_metric(
    routes_df, error_col: str = "mcp_state", subtype_col: str = "error_subtype"
) -> QualityMetric:

    check_column_exist(dataframe=routes_df, column=error_col)
    # modify routes_df error_subtype column to use it on level
    routes_df[subtype_col] = routes_df.apply(
        lambda row: join_columns(row, [error_col, subtype_col]), axis=1
    )

    level_per_error_type = Level(
        name="per_error_type",
        column=error_col,
        values=TBT_HDR_ROUTING_ERRORS_DICT,
        metrics={
            "eph": partial(
                eph,
                get_errors=partial(
                    get_number_errors, error_column=f"{error_col}"  # type: ignore
                ),
                get_hours=partial(get_sum_column_deduplicated, column="prov_hours"),  # type: ignore
            ),
            "fr": partial(
                failed_routes,
                get_routes_error=partial(
                    get_number_routes_deduplicated,
                    filter_query=f"{error_col} not in ['discard'] and ~{error_col}.isna()",
                ),  # type: ignore
                get_routes=get_number_routes_deduplicated,  # type: ignore
            ),
        },
        stats={"errors": partial(get_number_errors, error_column=f"{error_col}")},
    )

    level_per_error_subtype = Level(
        name="per_error_subtype",
        column=subtype_col,
        values=TBT_HDR_SUBTYPE_ERROR_SUBTYPES_DICT,
        metrics={
            "eph": partial(
                eph,
                get_errors=partial(
                    get_number_errors, error_column=f"{error_col}"  # type: ignore
                ),
                get_hours=partial(get_sum_column_deduplicated, column="prov_hours"),  # type: ignore
            ),
            "failed_routes": partial(
                failed_routes,
                get_routes_error=partial(
                    get_number_routes_deduplicated,
                    filter_query=f"{error_col} not in ['discard'] and ~{error_col}.isna()",
                ),  # type: ignore
                get_routes=get_number_routes_deduplicated,  # type: ignore
            ),
        },
        stats={"errors": partial(get_number_errors, error_column=f"{error_col}")},
    )

    quality_metric = QualityMetric(
        routes_df=routes_df,
        levels=[level_per_error_type, level_per_error_subtype],
        global_stats={
            **COMMON_GLOBAL_STATS,
            **{
                "errors": partial(
                    get_number_errors,
                    error_column=f"{error_col}",
                    filter_query=f"{error_col} not in ['IMPLICIT']",
                )
            },
        },
        global_metrics={
            "eph": partial(
                eph,
                get_errors=partial(
                    get_number_errors,
                    error_column=f"{error_col}",
                    filter_query=f"{error_col} not in ['IMPLICIT']",
                ),  # type: ignore
                get_hours=partial(get_sum_column_deduplicated, column="prov_hours"),  # type: ignore
            ),
            "failed_routes": partial(
                failed_routes,
                get_routes_error=partial(
                    get_number_routes_deduplicated,
                    filter_query=f"{error_col} not in ['discard', 'IMPLICIT'] and ~{error_col}.isna()",
                ),  # type: ignore
                get_routes=get_number_routes_deduplicated,  # type: ignore
            ),
        },
        columns_on_bootstrap=[
            "provider_hours",
            "provider_km",
            "competitor_hours",
            "competitor_km",
            "errors",
            "routes",
            "eph",
            "failed_routes",
        ],
    )

    return quality_metric
