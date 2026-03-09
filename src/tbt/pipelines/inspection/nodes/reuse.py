"""Node: reuse_static_routes — reuse routes from previous inspections."""

import logging
from time import time

import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
import shapely.wkt
from pyspark.sql.functions import col, expr, when

import tbt.navutils.common.decorators as decorators
from tbt.utils.console_print import conditional_print

log = logging.getLogger(__name__)


@decorators.timing
def reuse_static_routes(
    provider_routes: pyspark.sql.DataFrame,
    tbt_inspection_routes: pyspark.sql.DataFrame,
    tbt_inspection_critical_sections: pyspark.sql.DataFrame,
    tbt_critical_sections_with_mcp_feedback: pyspark.sql.DataFrame,
    tbt_inspection_metadata: pyspark.sql.DataFrame,
    tbt_options: dict,
):
    """Reuse routes that have not changed with respect to a previous inspection,
    so we save API calls and computation.

    For a route to be reused the following conditions are checked
    on the original route and the new route:

    + Same ``sample_id``
    + Same ``route_id``
    + Same ``competitor`` to evaluate with RAC
    + Same route geometry

    If a route is reused we will try to reuse all the information we have on it.
    If there is an ``mcp_state`` it will also be copied.
    """
    total_time = time()

    ignore_previous_inspections = tbt_options["ignore_previous_inspections"]
    if ignore_previous_inspections:
        log.info("Ignoring previous inspections...")
        conditional_print("Ignoring previous inspections...")
        tbt_inspection_routes = tbt_inspection_routes.limit(0)
        tbt_inspection_critical_sections = tbt_inspection_critical_sections.limit(0)
        tbt_critical_sections_with_mcp_feedback = (
            tbt_critical_sections_with_mcp_feedback.limit(0)
        )

    log.info("Trying to reuse %i routes", provider_routes.count())
    conditional_print("Trying to reuse %i routes", provider_routes.count())

    # Deduplicate critical_sections_with_mcp_feedback
    tbt_critical_sections_with_mcp_feedback = (
        tbt_critical_sections_with_mcp_feedback.drop_duplicates(
            ["run_id", "route_id", "case_id"]
        )
    )

    # Match with previous inspection routes
    matchings = provider_routes.join(
        tbt_inspection_routes.select(
            "route_id",
            F.col("provider_route").alias("old_provider_route"),
            "competitor",
            "country",
            "sample_id",
            "run_id",
        ),
        how="left",
        on=["sample_id", "route_id", "competitor", "country"],
    )
    df = matchings.select(
        "run_id", "route_id", "provider_route", "old_provider_route"
    ).toPandas()
    df["provider_route"] = [
        shapely.wkt.loads(x) if x is not None else shapely.wkt.loads("LINESTRING EMPTY")
        for x in df.provider_route
    ]
    df["old_provider_route"] = [
        shapely.wkt.loads(x) if x is not None else shapely.wkt.loads("LINESTRING EMPTY")
        for x in df.old_provider_route
    ]
    df["matching"] = df.apply(
        lambda x: x["provider_route"].buffer(0.00005).contains(x["old_provider_route"])
        and x["old_provider_route"].buffer(0.00005).contains(x["provider_route"]),
        axis=1,
    )

    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    matched_routes = spark.createDataFrame(
        df[["run_id", "route_id"]][df.matching], schema="run_id string, route_id string"
    )

    provider_routes_ = (
        provider_routes.join(matched_routes, how="left", on="route_id")
        .join(
            tbt_inspection_routes.select(
                "route_id",
                "competitor_route",
                "rac_state",
                "competitor_route_length",
                "competitor_route_time",
                "competitor",
                "country",
                "sample_id",
                "run_id",
            ),
            how="left",
            on=["sample_id", "route_id", "competitor", "country", "run_id"],
        )
        .join(
            tbt_critical_sections_with_mcp_feedback.select(
                "run_id", "route_id", "case_id", "mcp_state"
            ),
            how="left",
            on=["run_id", "route_id"],
        )
        .join(
            tbt_inspection_metadata.select(["run_id", "inspection_date"]),
            how="left",
            on="run_id",
        )
        .withColumn("mcp_state_null", F.col("mcp_state").isNull())
        .orderBy(F.col("mcp_state_null"), F.col("inspection_date").desc())
        .drop_duplicates(
            [
                "sample_id",
                "route_id",
                "competitor",
                "provider_route",
                "provider_route_length",
                "country",
            ]
        )
        .drop("mcp_state")
        .drop("mcp_state_null")
        .cache()
    )

    provider_routes_known = provider_routes_.filter("rac_state is not null")
    provider_routes_unknown = provider_routes_.filter("rac_state is null")

    log.info("%i routes are new", provider_routes_unknown.count())
    conditional_print("%i routes are new", provider_routes_unknown.count())
    log.info(
        "%i routes are known. Retrieving critical section info...",
        provider_routes_known.count(),
    )
    conditional_print(
        "%i routes are known. Retrieving critical section info...",
        provider_routes_known.count(),
    )

    inspection_critical_sections_known = (
        provider_routes_known.select("route_id", "run_id")
        .join(
            tbt_inspection_critical_sections.drop("mcp_state").withColumn(
                "reference_case_id",
                when(col("reference_case_id").isNull(), col("case_id")).otherwise(
                    col("reference_case_id")
                ),
            ),
            how="inner",
            on=["run_id", "route_id"],
        )
        .join(
            tbt_critical_sections_with_mcp_feedback.drop("reference_case_id"),
            how="left",
            on=["run_id", "route_id", "case_id"],
        )
        .withColumn("case_id", expr("uuid()").cast(T.StringType()))
        .cache()
    )
    log.info(
        "Found %i critical sections already evaluated",
        inspection_critical_sections_known.count(),
    )
    conditional_print(
        "Found %i critical sections already evaluated",
        inspection_critical_sections_known.count(),
    )
    total_time = time() - total_time

    return (
        provider_routes_unknown,
        provider_routes_known,
        inspection_critical_sections_known.drop("__index_level_0__"),
        total_time,
    )

