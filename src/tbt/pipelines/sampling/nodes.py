"""
Sampling Nodes

"""

import json
import logging
import random
import typing
import uuid
from datetime import datetime, timedelta
from time import time

import pandas
import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
import requests
import shapely.geometry
import shapely.wkt

from tbt.navutils.common import decorators
from tbt.navutils.common.morton import from_degrees
from tbt.navutils.navutils.ops import country_data_query
from tbt.navutils.navutils.provider import Provider

log = logging.getLogger(__name__)
DATE_FORMAT = "%Y-%m-%d"


@decorators.timing
def tbt_sampling_entrypoint(
    sample_options: dict,
    tbt_m14scope_delta: pyspark.sql.DataFrame,
    fcd_credentials: dict,
    run_id: str,
    tbt_db: dict,
):
    """Sampling function for TbT and RouteR

    :param sample_options: _description_
    :type sample_options: dict
    :param tbt_m14scope_delta: _description_
    :type tbt_m14scope_delta: pyspark.sql.DataFrame
    :param fcd_credentials: _description_
    :type fcd_credentials: dict
    :param run_id: _description_
    :type run_id: str
    :return: _description_
    :rtype: _type_
    """
    time_start = time()
    country = sample_options["country"]
    max_routes = sample_options["max_routes"]
    trace_limit = sample_options["trace_limit"]
    endpoint = sample_options["endpoint"]

    df_routes, sampling_metadata = tbt_sampling_legacy(
        country,
        max_routes,
        tbt_m14scope_delta,
        fcd_credentials,
        trace_limit,
        run_id,
        endpoint,
        tbt_db,
    )
    df_routes["date_generated"] = datetime.today()
    sampling_metadata["date_generated"] = datetime.today()
    sampling_metadata["osm_map_version"] = sample_options["osm_map_version"]
    sampling_metadata["parameters"] = json.dumps(sample_options)
    sampling_metadata["api_calls"] = None
    sampling_metadata["sample_q"] = sample_options["sample_q"]
    sampling_metadata["final_routes"] = len(df_routes)
    time_end = time()
    sampling_metadata["elapsed_time"] = time_end - time_start
    sampling_metadata["metric"] = "TbT 3.0"

    return (
        df_routes[
            [
                "sample_id",
                "tile_id",
                "origin",
                "destination",
                "route_id",
                "quality",
                "country",
                "date_generated",
                "org",
            ]
        ],
        sampling_metadata,
    )


def export_to_sql(sampling_samples, sampling_metadata):
    """Save the sampling on the database"""
    return sampling_samples, sampling_metadata


def export_to_spark(sampling_samples, sampling_metadata):
    """Save the sampling on the DataLake"""
    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    sampling_samples_sdf = spark.createDataFrame(
        sampling_samples,
        schema=T.StructType(
            [
                T.StructField("sample_id", T.StringType()),
                T.StructField("tile_id", T.StringType()),
                T.StructField("origin", T.StringType()),
                T.StructField("destination", T.StringType()),
                T.StructField("route_id", T.StringType()),
                T.StructField("quality", T.StringType()),
                T.StructField("country", T.StringType()),
                T.StructField("date_generated", T.DateType()),
                T.StructField("org", T.StringType()),
            ]
        ),
    )

    sampling_metadata_sdf = spark.createDataFrame(
        sampling_metadata,
        schema=T.StructType(
            [
                T.StructField("sample_id", T.StringType()),
                T.StructField("country", T.StringType()),
                T.StructField("fcd_extraction_time", T.FloatType()),
                T.StructField("fcd_extracted_routes", T.IntegerType()),
                T.StructField("processed_routes", T.IntegerType()),
                T.StructField("processing_time", T.FloatType()),
                T.StructField("date_generated", T.DateType()),
                T.StructField("osm_map_version", T.StringType()),
                T.StructField("parameters", T.StringType()),
                T.StructField("api_calls", T.StringType()),
                T.StructField("sample_q", T.StringType()),
                T.StructField("final_routes", T.IntegerType()),
                T.StructField("elapsed_time", T.FloatType()),
                T.StructField("metric", T.StringType()),
            ]
        ),
    )

    return sampling_samples_sdf, sampling_metadata_sdf


def tbt_sampling_legacy(
    country: str,
    max_routes: dict,
    tbt_m14scope_delta: pyspark.sql.DataFrame,
    fcd_credentials: dict,
    trace_limit: int,
    run_id: str,
    endpoint: str,
    tbt_db: dict,
):
    if run_id == "auto":
        run_id = str(uuid.uuid4())

    log.info("Loading country polygon...")
    country_data = country_data_query(country, tbt_db)

    log.info("Loading Spark session...")
    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    log.info("Retrieving M14 scope for country %s", country)
    # M14 scope contains the tile_ids and the MQS
    m14scope = tbt_m14scope_delta.select("tile_id", "quality", "country").filter(
        F.col("country").isin([country])
    )
    tiles = pandas.DataFrame(
        m14scope.select("tile_id", "quality", "country").toPandas()
    )
    tiles = (
        tiles.sample(replace=False, frac=1)
        .sort_values("quality")
        .reset_index(drop=True)
    )
    log.info(
        "Loaded tiles: %s",
        tiles[["quality", "tile_id"]].groupby("quality").count().to_dict(),
    )

    log.info("Plan with %i tiles generated OK", len(tiles))
    qualities = list(tiles.quality.unique())
    qualities.sort(reverse=True)
    country_lowest_quality = qualities[0]

    counts = {"Q1": 0, "Q2": 0, "Q3": 0, "Q4": 0, "Q5": 0}
    tiles_sampled = 0

    df_routes = pandas.DataFrame()

    log.info("Lowest MQS for country: %s", country_lowest_quality)
    log.info("Started FCD sampling ------------------------------->")

    date_start = datetime.strftime(datetime.now() - timedelta(32), DATE_FORMAT)
    date_end = datetime.strftime(datetime.now() - timedelta(30), DATE_FORMAT)

    fcd_extraction_time = 0.0
    fcd_extracted_routes = 0
    processed_routes = 0
    processing_time = 0

    i = 0
    threads = 4

    while i < len(tiles):

        i_max = min(i + threads, len(tiles))
        if tiles["quality"].values[i] != tiles["quality"].values[i_max - 1]:
            i_max = i + 1

        tile_ids = [str(x) for x in tiles["tile_id"].values[i:i_max]]
        quality = str(tiles["quality"].values[i])

        if counts[quality] >= max_routes[quality]:
            i = i_max
            continue

        log.info("Overall progress: %i %%", round(i / len(tiles) * 100))
        log.info("Downloading FCD Traces (tiles %s)", tile_ids)
        spark_context = spark.sparkContext

        time_start = time()
        traces = (
            read_traces_fcd(
                tile_ids,
                spark_context,
                fcd_credentials,
                date_start,
                date_end,
                trace_limit,
            )
            .filter(lambda x: x[0] != "0")
            .cache()
        )
        _fcd_extracted_routes = traces.count()
        time_end = time()
        _fcd_extraction_time = time_end - time_start
        log.info(
            "Extracted %i traces in %.2f s", _fcd_extracted_routes, _fcd_extraction_time
        )
        if _fcd_extracted_routes < trace_limit * threads / 2:
            # add two more days
            date_start = datetime.strftime(
                datetime.strptime(date_start, DATE_FORMAT) - timedelta(2), DATE_FORMAT
            )
            log.info("New date_start: %s", date_start)

        fcd_extracted_routes += _fcd_extracted_routes
        fcd_extraction_time += _fcd_extraction_time

        schema_ = T.StructType(
            [
                T.StructField("tile_id", T.StringType(), True),
                T.StructField(
                    "traces",
                    T.ArrayType(
                        T.StructType(
                            [
                                T.StructField("org", T.StringType(), True),
                                T.StructField(
                                    "trace",
                                    T.ArrayType(T.ArrayType(T.FloatType())),
                                    True,
                                ),
                            ]
                        )
                    ),
                ),
            ]
        )

        # Adding MQS and country to the traces
        sdf_traces = (
            (
                spark.createDataFrame(
                    traces.reduceByKey(lambda a, b: a + b), schema=schema_
                )
                .withColumn("run_id", F.lit(run_id))
                .join(m14scope, how="left", on=["tile_id"])
            )
            .repartition("tile_id")
            .cache()
        )

        # Sampling FCD routes
        schema = T.StructType(
            [
                T.StructField("tile_id", T.StringType(), True),
                T.StructField("origin", T.StringType(), True),
                T.StructField("destination", T.StringType(), True),
                T.StructField("org", T.StringType(), True),
                T.StructField("route_id", T.StringType(), True),
                T.StructField("run_id", T.StringType(), True),
                T.StructField("quality", T.StringType(), True),
                T.StructField("country", T.StringType(), True),
            ]
        )
        time_start = time()
        new_routes = sdf_traces.mapInPandas(
            lambda x: stream_route_sampler(x, "OSM-STRICT", endpoint), schema=schema
        ).toPandas()
        time_end = time()
        _processing_time = time_end - time_start
        processing_time += _processing_time
        _processed_routes = len(new_routes)
        processed_routes += _processed_routes
        log.info("Processed %i routes in %.2f s", _processed_routes, _processing_time)

        # Cleanup cached RDD
        traces.unpersist()
        sdf_traces.unpersist()

        df_routes = pandas.concat([df_routes, new_routes])

        tiles_sampled += 1
        counts[quality] = df_routes.loc[df_routes.quality == quality, "quality"].shape[
            0
        ]
        log.info(
            "%i / %i target routes generated for %s so far",
            counts[quality],
            max_routes[quality],
            quality,
        )

        if tiles_sampled > 10000:
            # looks like country/mqs is saturated
            log.warning(
                "Already processed %i tiles for %s. Stopping sampling since it looks saturated",
                tiles_sampled,
                country,
            )
            break
        i = i_max

    log.info("Sampling finished OK. Generated %s routes", len(df_routes))
    log.info(
        "Distribution: %s",
        df_routes[["quality", "route_id"]].groupby("quality").count().to_dict(),
    )

    sampling_metadata = pandas.DataFrame(
        {
            "sample_id": [run_id],
            "country": country,
            "fcd_extraction_time": fcd_extraction_time,
            "fcd_extracted_routes": fcd_extracted_routes,
            "processed_routes": processed_routes,
            "processing_time": processing_time,
        }
    )
    df_routes["sample_id"] = run_id

    # remove routes out of scope
    log.info("Filtering out of scope routes")
    df_routes = df_routes[
        df_routes.apply(
            lambda x: country_data["geom"].contains(shapely.wkt.loads(x.origin))
            and country_data["geom"].contains(shapely.wkt.loads(x.destination)),
            axis=1,
        )
    ]
    log.info("Final routes: %i", len(df_routes))

    return df_routes.reset_index(drop=True), sampling_metadata


def stream_route_sampler(iterator, provider, endpoint):
    """Generate Route Sampling (FCD based)
    This function is called directly from spark using Spark Streaming DataFrame's mapInPandas.
    :param iterator: Iterator of pandas DataFrame with columns
        + tile_id: Morton tile level 14 (str)
        + traces: List of dictionaries with keys `org` (str), `trace` (list of [lat,lon])
        + country: Morton tile country
        + quality: MQS of the Morton Tile
        + run_id: run_id from the sample
    :type iterator: pandas.DataFrame iterator
    :yield: DataFrame with columns
        + tile_id
        + route_id
        + origin
        + destination
        + org
        + run_id
        + quality
        + country

    :rtype: pandas.DataFrame
    """
    streets2avoid = [
        "motorway",
        "trunk",
        "primary",
        "cycleway",
        "service",
        "pedestrian",
        "track",
        "construction",
        "service_other",
        "path",
        "dirt",
    ]

    for traces_df in iterator:

        df_routes = pandas.DataFrame()

        for i in range(len(traces_df)):
            if len(traces_df.traces.values[i]) <= 0:
                continue

            if len(traces_df.traces.values[i][0]["trace"]) <= 0:
                continue

            routes = route_sampler(
                list(traces_df.traces.values[i]), streets2avoid, provider, endpoint
            )

            if len(routes) <= 0:
                continue

            tile_id = [traces_df.tile_id.values[i]] * len(routes)

            route_id = [routes[j]["route_id"] for j in range(len(routes))]
            origin = [routes[j]["origin"] for j in range(len(routes))]
            destination = [routes[j]["destination"] for j in range(len(routes))]
            org = [routes[j]["org"] for j in range(len(routes))]

            country = [traces_df.country.values[i]] * len(routes)
            quality = [traces_df.quality.values[i]] * len(routes)
            run_id = [traces_df.run_id.values[i]] * len(routes)

            df_routes = pandas.concat(
                [
                    pandas.DataFrame(
                        {
                            "tile_id": tile_id,
                            "route_id": route_id,
                            "origin": origin,
                            "destination": destination,
                            "org": org,
                            "run_id": run_id,
                            "quality": quality,
                            "country": country,
                        }
                    ),
                    df_routes,
                ]
            )

        yield df_routes


def read_traces_fcd(
    tile_ids,
    spark_context,
    credentials: dict,
    date_start=datetime.strftime(datetime.now() - timedelta(32), DATE_FORMAT),
    date_end=datetime.strftime(datetime.now() - timedelta(30), DATE_FORMAT),
    trace_limit=1000,
):
    """Read FCD traces

    :param tile_ids: List of 14 level Morton tile ids to retrieve from FCD
    :type tile_ids: typing.List(str)
    :param spark_context: SparkContext
    :type spark_context: SparkContext
    :param credentials: FCD credentials
    :type credentials: dict
    :param date_start: Date start
    :type date_start: str
    :param date_end: Date end
    :type date_end: str
    :param trace_limit: Maximum number (approximate) of total traces to retrieve
    :type trace_limit: float
    :return: Spark RDD with rows (int key (trace_id), list (traces))
    :rtype: pyspark.RDD
    """

    traces = spark_context.emptyRDD()

    trace_rdd = [None] * len(tile_ids)
    for i, tile_id in enumerate(tile_ids):

        zoom = len(tile_id)

        # log.info(f"Reading tiles %s", tile_id)
        trace_rdd[i] = spark_context.newAPIHadoopRDD(
            inputFormatClass="com.tomtom.trace.spark.TraceInputFormat",
            keyClass="org.apache.hadoop.io.Text",
            valueClass="java.lang.String",
            conf={
                "com.tomtom.trace.spark.auth.clientId": credentials["clientId"],
                "com.tomtom.trace.spark.auth.clientSecret": credentials["clientSecret"],
                "com.tomtom.trace.spark.auth.tenantId": credentials["tenantId"],
                "com.tomtom.trace.spark.input": "https://ttediteststoragetiled.blob.core.windows.net/",
                "com.tomtom.trace.spark.filter.date": f"{date_start}:{date_end}",
                "com.tomtom.trace.spark.filter.clipping.mode": "TILED_EXACT",
                "com.tomtom.trace.spark.filter.hours": "22-23",
                "com.tomtom.trace.spark.filter.area": f"M{zoom*2}:{tile_id}",
            },
        )
        count = trace_rdd[i].count()

        frac = min(1, trace_limit / max(count, 1))
        if frac < 1:
            trace_rdd[i] = trace_rdd[i].sample(False, frac).map(read_output)
        else:
            trace_rdd[i] = trace_rdd[i].map(read_output)
        # log.info("Populating traces")
        traces = traces.union(trace_rdd[i])

    return traces


def read_output(cache):
    """Process FCD traces (raw) into list of coordinates and keys

    :param cache: FCD traces. Rows if RDD in format ({}, 'json')
    :type cache: list
    :return: (tile_id, traces)
    :rtype: tuple(int, list)
    """

    cache = json.loads(cache[1])

    traces = []
    morton = "0"
    org = ""

    if "reports" not in cache.keys():
        return (morton, traces)

    if "meta" in cache.keys() and "ORGANISATION" in cache["meta"].keys():
        org = cache["meta"]["ORGANISATION"]

    if len(cache["reports"]) > 1:
        reports = cache["reports"]
        this_trace = [(reports[0]["lon"], reports[0]["lat"])]
        for j in range(1, len(reports)):
            if reports[j]["utc"] - reports[j - 1]["utc"] < 20000:
                if (
                    reports[j]["lat"] != reports[j - 1]["lat"]
                    and reports[j]["lon"] != reports[j - 1]["lon"]
                ):
                    this_trace.append((reports[j]["lon"], reports[j]["lat"]))
                if morton == "0":
                    morton = from_degrees(
                        reports[j]["lon"], reports[j]["lat"], 14
                    ).morton
                    morton = str(hex(morton)).split("x")[1]
                if len(this_trace) > 2000:
                    traces.append({"org": org, "trace": this_trace})
                    if j < len(reports) - 1:
                        this_trace = [(reports[j]["lon"], reports[j]["lat"])]
            else:
                if len(this_trace) > 1:
                    traces.append({"org": org, "trace": this_trace})
                if j < len(reports) - 1:
                    this_trace = [(reports[j]["lon"], reports[j]["lat"])]
        if len(this_trace) > 1:
            traces.append({"org": org, "trace": this_trace})

    return (morton, traces)


def route_sampler(
    multi: typing.List[dict], avoid: typing.List[str], prov="OSM", endpoint=None
) -> typing.List[dict]:
    """Generate a sample of routes from Floating Car Data (FCD)
    :param multi: Processed FCD trace(s)
    :type multi: typing.List[dict]
    :param avoid: Highway classification to avoid.
    :type avoid: typing.List[str]
    :param prov: Provider name, defaults to 'OSM'
    :type prov: str, optional
    :return: A list of dictionaries, one for each sampled route.
    :rtype: typing.List[dict]
    Examples::
        >>> from mdbf3.sampler import route_sampler
        >>> route_sampler(multi=[{'org':'example', 'trace':[[52.07019, 4.50059], [52.07028, 4.50019], [52.0703, 4.50005], [52.07031, 4.49991], [52.07033, 4.4999], [52.07035, 4.4999], [52.07036, 4.49988], [52.07038, 4.49985], [52.07039, 4.49983], [52.07047, 4.49981], [52.07055, 4.49981], [52.07063, 4.49982], [52.07071, 4.49986], [52.07105, 4.50004], [52.07135, 4.50013], [52.07135, 4.50015], [52.07135, 4.50018], [52.07136, 4.50021], [52.07138, 4.50023], [52.0714, 4.50023], [52.07142, 4.50023], [52.07143, 4.50021], [52.07145, 4.50019], [52.07145, 4.50015], [52.07145, 4.50012], [52.07144, 4.50009], [52.07143, 4.50007], [52.07151, 4.49991], [52.07174, 4.49874], [52.07205, 4.49737], [52.07207, 4.49731], [52.07208, 4.49728], [52.0721, 4.49725], [52.07212, 4.49722], [52.07235, 4.49704], [52.07264, 4.4968], [52.07269, 4.49674], [52.07272, 4.49666], [52.07299, 4.49559], [52.07305, 4.49536], [52.07311, 4.49514], [52.07337, 4.49429], [52.07342, 4.49418], [52.07348, 4.49411], [52.0737, 4.49391], [52.07377, 4.49386]]}], avoid = ['motorway','trunk', 'primary', 'cycleway', 'service', 'pedestrian', 'track', 'construction', 'service_other', 'path', 'dirt'], provider='OSM')
        [{'origin': [52.07033, 4.4999], 'destination': [52.07377, 4.49386], 'org': 'example', 'geometry': <shapely.geometry.linestring.LineString object at 0x7fe7cd4055b0>, 'route_id': '328110e3-810f-4673-adbf-7eb9cd12c634'}]
    """
    prov = Provider.from_properties(name=prov, endpoint=endpoint)  # to parameters
    routes = []

    if len(multi) > 10000:
        logging.info(
            "Too many traces for this tile. This may cause issues with arules."
            "Sampling 10000 traces"
        )
        multi = random.sample(multi, k=10000)

    for _ in range(len(multi)):

        if len(routes) >= 10:  # 10 routes per tile max.
            break

        trace_index = random.randint(0, len(multi) - 1)
        trace = multi[trace_index]["trace"]

        if len(trace) < 10:
            continue

        org = multi[trace_index]["org"]

        start_index = random.randint(0, len(trace) - 1)
        end_index = random.randint(start_index, len(trace) - 1)

        start = trace[start_index]
        end = trace[end_index]

        if (
            shapely.geometry.Point(start).distance(shapely.geometry.Point(end)) < 0.005
        ):  # .01
            continue

        if len(avoid) > 0 and not compliance_check(
            [float(start[0]), float(start[1])],
            [float(end[0]), float(end[1])],
            avoid,
            prov.endpoint,
        ):
            continue

        # Getting route
        route = prov.getRoute(
            [float(start[0]), float(start[1])], [float(end[0]), float(end[1])]
        )
        if route.geometry is None:
            continue

        # 3rd check: route points
        if len(route.geometry.coords) < 10:  # 20
            continue

        # 4th check:
        if (route.length < 100) or (route.length > 2000) or (route.time < 30):
            continue

        # Overlap check
        overlaps = False
        for each_route in routes:
            if overlap_cond(each_route["geometry"], route.geometry):
                overlaps = True
                break
        if overlaps:
            continue

        routes.append(
            {
                "origin": f"POINT ({start[0]} {start[1]})",
                "destination": f"POINT ({end[0]} {end[1]})",
                "org": org,
                "geometry": route.geometry,
                "route_id": str(uuid.uuid4()),
            }
        )

    return routes


def overlap_cond(route_a, route_b, min_diff=0.9):
    """Check the overlpaing condition between route_a and route_b"""
    lengths = {"route_a": len(route_a.coords), "route_b": len(route_b.coords)}
    geometries = {"route_a": route_a, "route_b": route_b}

    shortest = min(lengths, key=lengths.get)
    largest = max(lengths, key=lengths.get)
    if shortest == largest:
        shortest = "route_a"
        largest = "route_b"

    if geometries[largest].contains(geometries[shortest]) or geometries[
        shortest
    ].contains(geometries[largest]):
        return True

    # The difference between both routes is enough
    diff = geometries[largest].difference(geometries[shortest])
    if isinstance(diff, shapely.geometry.LineString):
        total_diff = len(diff.coords)
    elif isinstance(diff, shapely.geometry.MultiLineString):
        total_diff = sum([len(_.coords) for _ in diff.geoms])
    else:
        total_diff = 0.0

    if (total_diff / len(geometries[largest].coords)) < min_diff:
        return True

    return False


def compliance_check(
    origin,
    destination,
    avoid=None,
    endpoint="http://tt-valhalla-api.westeurope.azurecontainer.io:8002/",
):
    if avoid is None:
        avoid = [
            "motorway",
            "trunk",
            "primary",
            "secondary",
            "cycleway",
            "service",
            "pedestrian",
            "track",
            "construction",
            "service_other",
        ]

    payload = {
        "verbose": True,
        "locations": [
            {"lat": str(origin[1]), "lon": str(origin[0])},
            {"lat": str(destination[1]), "lon": str(destination[0])},
        ],
        "costing": ["auto", "pedestrian", "bicycle"],
    }
    url = endpoint + "locate"
    valid_route = True
    try:
        request = requests.get(url, json=payload, timeout=600)

        content = json.loads(request.content)

        for _, item in enumerate(content):
            if valid_route:
                for edge in item["edges"]:
                    if (
                        edge["edge"]["classification"]["classification"] in avoid
                        or edge["edge"]["classification"]["use"] in avoid
                        or edge["edge"]["classification"]["surface"] in avoid
                        or edge["edge"]["destination_only"]
                    ):
                        valid_route = False
                        break
    except requests.RequestException:
        valid_route = False

    return valid_route
