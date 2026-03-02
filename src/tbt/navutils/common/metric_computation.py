""" Metric computation functions """
from dataclasses import dataclass
from functools import partial
from typing import Callable, List, Dict, Any, Optional, Union, Tuple

import pandas as pd
import pyspark.sql

from ..common.pandas_ops import join_queries

from ..common.dict_ops import (
    dict_update,
    process_dict,
    flatten_dict,
    unflatten_dict,
    append_dictionary,
    NestedDict,
    NestedCallableDict,
    CallableDict,
)


def bootresample(*, routes_df: pd.DataFrame, seed: Optional[int] = 0) -> pd.DataFrame:
    """
    Generate a resample to use with bootstrap. The routes are sample based on route_id.

    :param routes_df: A pandas dataframe with all the routes
    :type routes_df: pd.DataFrame
    :param seed: seed for repetibility, defaults to 0
    :type seed: Optional[int], optional
    :return: Sampled routes from `routes_df`
    :rtype: pd.DataFrame
    """

    sampled_routes = pd.DataFrame(
        {
            "route_id": routes_df.route_id.drop_duplicates().sample(
                frac=1, replace=True, random_state=seed
            )
        }
    )
    sampled_routes["route_order"] = range(len(sampled_routes))
    sampled_routes = sampled_routes.merge(routes_df, how="left", on="route_id")
    sampled_routes["route_id"] = (
        sampled_routes.route_id + "_" + sampled_routes.route_order.astype(str)
    )

    return sampled_routes


@dataclass
class Level:
    """Metric Level"""

    name: str
    column: str
    values: Dict[str, List[Any]]
    metrics: Dict[str, Callable[..., float]]
    stats: Dict[str, Callable[..., float]]

    def __post_init__(self):
        self.expanded_metrics: Union[CallableDict, NestedCallableDict] = {
            level_name: {} for level_name in self.values.keys()
        }
        for metric, metric_function in self.metrics.items():
            for level_name, level_values in self.values.items():
                filter_query = f"{self.column} in {level_values}"
                if isinstance(metric_function, partial):
                    existant_filter_query = metric_function.keywords.get(
                        "filter_query", None
                    )
                    # Here we are joining the new filter_query for just take
                    # into account the routes from the level
                    #  with the filter_query already on the metric function (if exists)
                    filter_query = join_queries(existant_filter_query, filter_query)

                self.expanded_metrics[level_name].update(
                    {metric: partial(metric_function, filter_query=filter_query)}
                )

        self.expanded_stats: Union[CallableDict, NestedCallableDict] = {
            level_name: {} for level_name in self.values.keys()
        }
        for stat, stat_function in self.stats.items():
            for level_name, level_values in self.values.items():
                filter_query = f"{self.column} in {level_values}"
                if isinstance(stat_function, partial):
                    existant_filter_query = stat_function.keywords.get(
                        "filter_query", None
                    )
                    # Here we are joining the new filter_query for just take
                    # into account the routes from the level
                    #  with the filter_query already on the stat function (if exists)
                    filter_query = join_queries(existant_filter_query, filter_query)
                self.expanded_stats[level_name].update(
                    {stat: partial(stat_function, filter_query=filter_query)}
                )


def compute_level(*, level: Level, routes_df: pd.DataFrame) -> NestedDict:
    """
    Compute all the metrics and stats from the level

    :param routes_df: routes df valid for metric/stat computation
    :type routes_df: pd.DataFrame
    :return: dict with the results of all computations
    :rtype: NestedDict
    """
    metrics_dict = {
        level.name: process_dict(dictionary=level.expanded_metrics, routes_df=routes_df)
    }
    stats_dict = {
        level.name: process_dict(dictionary=level.expanded_stats, routes_df=routes_df)
    }
    return dict_update(metrics_dict, stats_dict)  # type: ignore


class QualityMetric:
    """Abstract class for quality_metrics TbT, TNE, HDR, RR"""

    def __init__(
        self,
        *,
        routes_df: pd.DataFrame,
        levels: List[Level],
        global_metrics: Union[CallableDict, NestedCallableDict],
        global_stats: Union[CallableDict, NestedCallableDict],
        columns_on_bootstrap: Optional[List[str]] = None,
    ) -> None:
        self.routes_df = routes_df

        self.levels = levels

        self.global_metrics = global_metrics
        self.global_stats = global_stats

        if columns_on_bootstrap is None:
            self.columns_on_bootstrap = [*global_metrics.keys(), *global_stats.keys()]
        else:
            self.columns_on_bootstrap = columns_on_bootstrap

    def _compute_levels(
        self, *, routes_df: Optional[pd.DataFrame] = None
    ) -> NestedDict:

        return_dict = {}
        for level in self.levels:
            return_dict.update(compute_level(level=level, routes_df=routes_df))  # type: ignore
        return return_dict  # type: ignore

    def compute_bootstrap(
        self,
        *,
        resamples: int = 1000,
        routes_df: Optional[pd.DataFrame] = None,
        quantiles: Optional[List[float]] = None,
        spark_context: pyspark.SparkContext,
    ) -> Tuple[NestedDict, pyspark.sql.DataFrame]:
        """Confidence intervals for metrics and returns bootstrap dataframe

        :param spark_context: spark context for pyspark
        :type spark_context: pyspark.SparkContext
        :param resamples: number of bootstrap iterations, defaults to 1000
        :type resamples: int, optional
        :param routes_df: dataframe of routes, defaults to `self.routes_df`
        :type routes_df: Optional[pd.DataFrame], optional
        :param quantiles: the quantiles for the confiendence intervals, defaults to [.05, .95]
        :type quantiles: Optional[List[float]], optional
        :return: A dict with all confidence interval
            and  a spark dataframe with `self.columns_on_bootstrap` as columns for each resample
        :rtype: Tuple[NestedDict, pyspark.sql.DataFrame]
        """
        if (
            routes_df is None
        ):  # If no routes_df is given uses the given during class creation
            routes_df = self.routes_df

        if quantiles is None:
            quantiles = [0.05, 0.95]
        elif len(quantiles) != 2:
            raise ValueError(
                "Quantiles must be a list of two values "
                f"(len(quantiles)={len(quantiles)})"
            )

        seeds_rdd = spark_context.parallelize(range(resamples), 200)

        bootstrap_rdd = (
            seeds_rdd.map(
                lambda seed: {
                    "seed": seed,
                    "routes_df": bootresample(routes_df=routes_df, seed=seed),
                }
            ).map(
                lambda row: flatten_dict(
                    {
                        **self.compute_metrics(routes_df=row["routes_df"]),  # type: ignore
                        **{"seed": row["seed"]},
                    }
                )
            )
        ).cache()

        bootstrap_df = bootstrap_rdd.toDF()  # type: ignore

        bootstrap_df_save = bootstrap_df.select(*["seed", *self.columns_on_bootstrap])

        bootstrap_df = bootstrap_df.toPandas()  # type: ignore

        metric_columns = list(self.global_metrics.keys())
        metric_columns.extend(
            [
                f"{level.name}.{key}"
                for level in self.levels
                for key in flatten_dict(level.expanded_metrics).keys()
            ]
        )

        quantiles_df = bootstrap_df[metric_columns].quantile(q=quantiles)  # type: ignore
        lower_dict = quantiles_df.loc[min(quantiles)].to_dict()
        append_dictionary(dictionary=lower_dict, suffix="lower")
        lower_dict = unflatten_dict(lower_dict)

        upper_dict = quantiles_df.loc[max(quantiles)].to_dict()
        append_dictionary(dictionary=upper_dict, suffix="upper")
        upper_dict = unflatten_dict(upper_dict)

        quantiles_dict = dict_update(lower_dict, upper_dict)

        return quantiles_dict, bootstrap_df_save

    def compute_metrics(
        self,
        *,
        routes_df: Optional[pd.DataFrame] = None,
    ) -> NestedDict:
        """Return all the values for metrics/stats global and levels for QualityMetric

        :param routes_df: A routes dataframe, defaults `self.routes_df`
        :type routes_df: Optional[pd.DataFrame], optional
        :return: A dict with all the computations
        :rtype: NestedDict
        """
        if (
            routes_df is None
        ):  # If no routes_df is given uses the given during class creation
            routes_df = self.routes_df

        global_metrics = process_dict(
            dictionary=self.global_metrics, routes_df=routes_df
        )
        global_stats = process_dict(dictionary=self.global_stats, routes_df=routes_df)

        return_dict = dict_update(global_metrics, global_stats)

        levels_dict = self._compute_levels(routes_df=routes_df)

        return_dict.update(levels_dict)

        return return_dict
