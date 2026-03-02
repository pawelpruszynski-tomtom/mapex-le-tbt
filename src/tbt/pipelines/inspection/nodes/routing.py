"""Nodes: get_provider_routes, get_competitor_routes — API calls for route computation."""

import logging
import typing
from time import time

import pyspark.sql
import pyspark.sql.types as T
from pyspark.sql.functions import col

import tbt.navutils.common.decorators as decorators
from tbt.navutils.navutils.provider import Provider
from tbt.pipelines.inspection.domain.route_computation import (
    compute_single_competitor_route,
    compute_single_provider_route,
)

log = logging.getLogger(__name__)


@decorators.timing
def get_provider_routes(
    tbt_options: dict,
    tbt_sampling_samples: pyspark.sql.DataFrame,
    tbt_sampling_metadata: pyspark.sql.DataFrame,
    tbt_inspection_metadata_duplicates: bool = False,
) -> typing.Tuple[pyspark.sql.DataFrame, float, str, dict]:
    """Computes provider routes.

    This function should be called for every new inspection (including incrementals).

    :param tbt_options: Options provided to the pipeline through ``conf/base/parameters/tbt.yml``.
    :param tbt_sampling_samples: Delta table with historic samples.
    :param tbt_sampling_metadata: Delta table with sampling metadata.
    :param tbt_inspection_metadata_duplicates: Dummy input needed to chain the Kedro DAG.
    :return: (provider_routes DataFrame, elapsed_time, sample_metric, api_calls dict)
    """
    total_time = time()

    sample_id = tbt_options["sample_id"]
    provider = tbt_options["provider"]
    competitor = tbt_options["competitor"]
    endpoint = tbt_options["endpoint"]
    product = tbt_options["product"]

    # Infer sample metric (OMA-437)
    sample_metric = (
        tbt_sampling_metadata.filter(col("sample_id") == sample_id)
        .select("metric")
        .limit(1)
        .collect()[0]["metric"]
    )
    if sample_metric != "HDR":
        sample_metric = "TbT"

    origins_destinations = tbt_sampling_samples.filter(col("sample_id") == sample_id)
    counts = origins_destinations.count()
    log.info("Computing %i routes from sample_id=%s", counts, sample_id)

    # Initialize API call accumulators
    spark_context = pyspark.sql.SparkSession.builder.getOrCreate().sparkContext

    gg_api_calls = spark_context.accumulator(0)
    tt_api_calls = spark_context.accumulator(0)
    osm_api_calls = spark_context.accumulator(0)
    here_api_calls = spark_context.accumulator(0)
    bing_api_calls = spark_context.accumulator(0)
    kakao_api_calls = spark_context.accumulator(0)
    gt_api_calls = spark_context.accumulator(0)
    mapbox_api_calls = spark_context.accumulator(0)
    mmi_api_calls = spark_context.accumulator(0)
    genesysmap_api_calls = spark_context.accumulator(0)

    prov = Provider.from_properties(
        name=provider,
        product=product,
        endpoint=endpoint,
        gg_api_count=gg_api_calls,
        directions_api_count=tt_api_calls,
        valhalla_api_count=osm_api_calls,
        here_api_count=here_api_calls,
        bing_api_count=bing_api_calls,
        kakao_api_count=kakao_api_calls,
        gt_api_count=gt_api_calls,
        mapbox_api_count=mapbox_api_calls,
        mmi_api_count=mmi_api_calls,
        genesysmap_api_count=genesysmap_api_calls,
        qps_limit=1,
    )

    def _map_provider(row):
        return compute_single_provider_route(row, prov, provider, competitor, sample_id)

    provider_routes = (
        origins_destinations.rdd.map(_map_provider)
        .filter(lambda x: x["provider_route_length"] > 0)
        .cache()
    )
    counts = provider_routes.count()
    total_time = time() - total_time

    log.info(
        "Computed %i routes for provider %s in %.2f s, API calls: %s",
        counts,
        provider,
        total_time,
        prov.api_calls,
    )

    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    provider_routes = spark.createDataFrame(
        provider_routes,
        schema=T.StructType(
            [
                T.StructField("route_id", T.StringType()),
                T.StructField("origin", T.StringType()),
                T.StructField("destination", T.StringType()),
                T.StructField("provider_route", T.StringType()),
                T.StructField("provider_route_length", T.FloatType()),
                T.StructField("provider_route_time", T.FloatType()),
                T.StructField("provider", T.StringType()),
                T.StructField("competitor", T.StringType()),
                T.StructField("country", T.StringType()),
                T.StructField("sample_id", T.StringType()),
            ]
        ),
    ).cache()

    return provider_routes, total_time, sample_metric, prov.api_calls


@decorators.timing
def get_competitor_routes(
    tbt_options: dict,
    provider_routes_unknown: pyspark.sql.DataFrame,
    tbt_inspection_routes: pyspark.sql.DataFrame,
    provider_api_calls: dict,
):
    """Compute competitor routes for those routes that are new.

    A competitor route is reused based on the ``route_id`` and the ``competitor`` name.
    """
    total_time = time()

    ignore_previous_inspections = tbt_options["ignore_previous_inspections"]
    if ignore_previous_inspections:
        log.info("Ignoring previous inspections...")
        tbt_inspection_routes = tbt_inspection_routes.limit(0)

    log.info("Retrieving competitor routes from past inspections")

    competitor_routes = (
        provider_routes_unknown.select(
            "route_id", "origin", "destination", "country", "sample_id", "competitor"
        )
        .join(
            tbt_inspection_routes.drop_duplicates(
                ["sample_id", "route_id", "competitor"]
            ).select(
                "country",
                "sample_id",
                "competitor",
                "route_id",
                "competitor_route",
                "competitor_route_length",
                "competitor_route_time",
            ),
            on=["country", "sample_id", "route_id", "competitor"],
            how="left",
        )
        .cache()
    )

    competitor_routes_known = competitor_routes.filter(
        "competitor_route is not null"
    ).drop("origin", "destination")
    competitor_routes_unknown = competitor_routes.filter("competitor_route is null")

    log.info(
        "There are %i competitor routes that can be reused from past inspections",
        competitor_routes_known.count(),
    )
    log.info("Computing %i competitor routes", competitor_routes_unknown.count())

    spark_context = pyspark.sql.SparkSession.builder.getOrCreate().sparkContext

    gg_api_calls = spark_context.accumulator(0)
    tt_api_calls = spark_context.accumulator(0)
    osm_api_calls = spark_context.accumulator(0)
    here_api_calls = spark_context.accumulator(0)
    bing_api_calls = spark_context.accumulator(0)
    kakao_api_calls = spark_context.accumulator(0)
    gt_api_calls = spark_context.accumulator(0)
    mapbox_api_calls = spark_context.accumulator(0)
    mmi_api_calls = spark_context.accumulator(0)
    genesysmap_api_calls = spark_context.accumulator(0)

    prov = Provider.from_properties(
        name=tbt_options["competitor"],
        gg_api_count=gg_api_calls,
        directions_api_count=tt_api_calls,
        valhalla_api_count=osm_api_calls,
        here_api_count=here_api_calls,
        bing_api_count=bing_api_calls,
        kakao_api_count=kakao_api_calls,
        gt_api_count=gt_api_calls,
        mapbox_api_count=mapbox_api_calls,
        mmi_api_count=mmi_api_calls,
        genesysmap_api_count=genesysmap_api_calls,
        endpoint=tbt_options["competitor_endpoint"],
    )

    log.info(f"Provider name is {prov.name}, endpoint is {prov.endpoint}")

    def _map_competitor(row):
        return compute_single_competitor_route(row, prov)

    computed_routes = competitor_routes_unknown.rdd.map(_map_competitor)

    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    computed_routes = spark.createDataFrame(
        computed_routes,
        schema=T.StructType(
            [
                T.StructField("route_id", T.StringType()),
                T.StructField("competitor_route", T.StringType()),
                T.StructField("competitor_route_length", T.FloatType()),
                T.StructField("competitor_route_time", T.FloatType()),
            ]
        ),
    )

    competitor_routes_unknown = (
        competitor_routes_unknown.select(
            "country", "sample_id", "competitor", "route_id"
        )
        .join(computed_routes, on=["route_id"], how="left")
        .cache()
    )
    log.info("Computed %i competitor routes", competitor_routes_unknown.count())

    competitor_routes = competitor_routes_unknown.select(
        "country", "sample_id", "route_id", "competitor",
        "competitor_route", "competitor_route_length", "competitor_route_time",
    ).union(
        competitor_routes_known.select(
            "country", "sample_id", "route_id", "competitor",
            "competitor_route", "competitor_route_length", "competitor_route_time",
        )
    )

    total_time = time() - total_time

    competitor_api_calls = {
        key: provider_api_calls.get(key, 0) + prov.api_calls.get(key, 0)
        for key in provider_api_calls
    }

    log.info(
        "Returning %i competitor routes in %.2f s, API calls: %s",
        competitor_routes.count(),
        total_time,
        competitor_api_calls,
    )

    return competitor_routes, total_time, competitor_api_calls


