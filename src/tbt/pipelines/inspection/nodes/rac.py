"""Node: get_rac_state — Route Assessment Classification."""

import logging
from time import time

import pyspark.sql
import pyspark.sql.types as T
from pyspark.sql.functions import col

import tbt.navutils.common.decorators as decorators
from tbt.navutils.navutils.provider import Provider
from tbt.pipelines.inspection.domain.rac_evaluation import evaluate_rac_for_route

log = logging.getLogger(__name__)


@decorators.timing
def get_rac_state(
    tbt_options: dict,
    provider_routes_unknown: pyspark.sql.DataFrame,
    competitor_routes: pyspark.sql.DataFrame,
    competitor_api_calls: dict,
):
    """Compute TbT 4.x RAC for new routes."""
    total_time = time()

    routes = provider_routes_unknown.drop(
        "competitor_route", "competitor_route_length", "competitor_route_time"
    ).join(
        competitor_routes,
        on=["route_id", "country", "sample_id", "competitor"],
        how="left",
    )

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
        name=tbt_options["provider"],
        product=tbt_options["product"],
        endpoint=tbt_options["endpoint"],
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
    )
    comp = Provider.from_properties(
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
    )

    def _map_rac(row):
        return evaluate_rac_for_route(row, comp)

    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    critical_routes = routes.rdd.flatMap(_map_rac).collect()
    rac_routes = spark.createDataFrame(
        critical_routes,
        schema=T.StructType(
            [
                T.StructField("country", T.StringType()),
                T.StructField("provider", T.StringType()),
                T.StructField("route_id", T.StringType()),
                T.StructField("case_id", T.StringType()),
                T.StructField("route", T.StringType()),
                T.StructField("stretch", T.StringType()),
                T.StructField("rac_state", T.StringType()),
            ]
        ),
    ).cache()

    total_time = time() - total_time
    api_calls = {
        key: competitor_api_calls.get(key, 0) + prov.api_calls.get(key, 0)
        for key in competitor_api_calls
    }
    log.info(
        "Computed %i RAC routes in %.2f s, API calls: %s",
        rac_routes.count(),
        total_time,
        api_calls,
    )

    routes = routes.drop("rac_state").join(
        rac_routes.select("route_id", "rac_state").drop_duplicates(),
        on="route_id",
        how="left",
    )

    return (
        routes,
        rac_routes.filter(col("rac_state") == "potential_error").drop("rac_state"),
        api_calls,
        total_time,
    )

