""" Sampling HDR pipeline"""
import json
import logging
import math
import time
import typing
import uuid
from datetime import datetime, timedelta

import geopandas
import numpy as np
import pandas as pd
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
import requests
import shapely
import shapely.geometry
import sqlalchemy
from pyspark.sql.functions import udf

# Utils imports
from tbt.navutils.navutils.provider import Provider
from tbt.navutils.navutils.route import DistanceOnEarth

log = logging.getLogger(__name__)
DATE_FORMAT = "%Y-%m-%d"
DATE_BACKWARDS_LIMIT = datetime.now() - timedelta(days=30 * 6)


def golden_routes_sampling_entrypoint(
    az_db: dict, hdr_sample_options: dict, fcd_credentials: dict, run_id: str
):
    """
    Sampling function for golden_routes

    :param sample_options: options for the sampling
    :type sample_options: dict
    :param target_mdr_km: Number (approximate) of total km from MDR retrive
    :type target_mdr_km: float
    :param fcd_credentials: FCD credentials
    :type fcd_credentials: dict
    :return: Pandas DataFrame with sampling and sampling metadata
    :rtype: Tuple(pd.DataFrame, dict)
    """
    time_start = time.time()

    if run_id == "auto":
        run_id = str(uuid.uuid4())

    fcd_extraction_time = 0
    fcd_extracted_routes = 0
    processed_routes = 0
    processing_time = 0

    provider = Provider.from_properties(
        name=hdr_sample_options["provider_name"],
        endpoint=hdr_sample_options["provider_endpoint"],
    )

    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark_context = spark.sparkContext

    target_routes = hdr_sample_options["target_routes"]

    sampling_df = pd.DataFrame()

    iteration = 0
    total_routes = 0
    target_routes_it = math.ceil(target_routes * 2)

    fcd_end_date = datetime.strftime(datetime.now() - timedelta(days=30), "%Y-%m-%d")

    while total_routes < target_routes:
        log.info(f"Iteration {iteration}")
        log.info(f"Trying to get {target_routes_it} routes from FCD")
        time_it_start = time.time()
        it_sampling_df, last_fcd_end_date, it_metadata = get_sampling_iteration(
            iteration_options={
                "fcd_end_date": fcd_end_date,
                "country": hdr_sample_options["country"],
                "target_routes": target_routes_it,
                "mdr_perc_threshold": hdr_sample_options["mdr_perc_threshold"],
                "debug": hdr_sample_options["debug"],
            },
            fcd_credentials=fcd_credentials,
            provider=provider,
            spark_context=spark_context,
            az_db=az_db,
        )
        fcd_extracted_routes += it_metadata["fcd_extracted_routes"]
        fcd_extraction_time += it_metadata["fcd_extraction_time"]
        processed_routes += it_metadata["processed_routes"]
        processing_time += it_metadata["processing_time"]

        it_mdr_km = it_sampling_df["total_km_MDR"].sum()
        it_routes = len(it_sampling_df)
        log.info(
            f"{it_mdr_km} MDR kms {it_routes} in routes. {time.time() - time_it_start} secs"
        )

        sampling_df = pd.concat([sampling_df, it_sampling_df])
        total_routes += it_routes

        recall_routes = max(it_routes / target_routes_it, 0.001)
        target_routes_it = math.ceil(
            (target_routes - it_routes) * (1 / recall_routes) * 2
        )
        target_routes_it = min(max(target_routes_it, 1_000), 20_000)
        fcd_end_date = datetime.strftime(
            datetime.fromisoformat(last_fcd_end_date) - timedelta(days=1), DATE_FORMAT
        )

        if datetime.fromisoformat(fcd_end_date) < DATE_BACKWARDS_LIMIT:
            log.info("Six months backwards limit reached. Stopping sampling.")
            log.info(f"Total {len(sampling_df)} routes.")
            break

        if total_routes < target_routes:
            log.info(f"Recall {recall_routes}.")
            log.info(
                f"Next iteration ask for {target_routes_it} routes. Starting from {fcd_end_date}."
            )

        log.info(f"Total {len(sampling_df)} routes.")

        iteration += 1

    target_routes = min(target_routes, len(sampling_df))
    sampling_df = sampling_df.sample(n=target_routes)

    sampling_df["sample_id"] = run_id
    sampling_df["tile_id"] = None
    sampling_df["quality"] = "Q5"
    sampling_df["country"] = hdr_sample_options["country"]
    sampling_df["date_generated"] = datetime.today()

    sampling_extra = sampling_df.copy()

    sampling_extra["percentage_mdr"] = sampling_extra["percentageMDR"]
    sampling_extra["total_km_mdr"] = sampling_extra["total_km_MDR"]
    sampling_extra = sampling_extra[
        [
            "sample_id",
            "route_id",
            "percentage_mdr",
            "total_km",
            "total_km_mdr",
            "provider",
            "date_generated",
        ]
    ]

    sampling_df = sampling_df[
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
    ]

    sampling_metadata = pd.DataFrame(
        {
            "sample_id": [run_id],
            "country": hdr_sample_options["country"],
            "fcd_extraction_time": fcd_extraction_time,
            "fcd_extracted_routes": fcd_extracted_routes,
            "processed_routes": processed_routes,
            "processing_time": processing_time,
            "date_generated": datetime.today(),
            "osm_map_version": None,
            "parameters": json.dumps(hdr_sample_options),
            "api_calls": None,
            "sample_q": f"{hdr_sample_options['sample_q']}",
            "final_routes": len(sampling_df),
        }
    )

    time_end = time.time()
    sampling_metadata["elapsed_time"] = time_end - time_start
    sampling_metadata["metric"] = "HDR"

    return sampling_df, sampling_metadata, sampling_extra


# Util functions

API_KEY_MAPMATCHING = "ZW6EMc883ATyRrv95HBadGvcP4fhTTrC"
NUM_POINTS_INTERPOLATE_MAPMATCHING = 100


def parse_trace(trace: typing.List[str]) -> typing.Tuple[bool, str, str, str]:
    """Parse a FCD trace

    :param trace: fcd_trace
    :type trace: typing.List[str]
    :return: (flag, origin_wkt, destination_wkt, trace_wkt)
    :rtype: typing.Tuple[bool, str, str, str]
    """
    trace = json.loads(trace[1])

    org = ""
    origin, destination = "", ""

    if "meta" not in trace.keys():
        return (False, org, origin, destination)

    if "reports" not in trace.keys():
        return (False, org, origin, destination)

    if len(trace["reports"]) < 2:
        return (False, org, origin, destination)

    org = trace["meta"]["ORGANISATION"]

    # remove 100m (for parkings)
    trace_points = [[p["lon"], p["lat"]] for p in trace["reports"]]
    trace_geom = shapely.geometry.LineString(trace_points)

    doe = DistanceOnEarth()
    trace_total_m = doe.get_4326_geometry_length_in_m(trace_geom)

    if trace_total_m <= 200:
        return (False, org, origin, destination)

    cut_trace = shapely.ops.substring(
        trace_geom, (100 / trace_total_m), -(100 / trace_total_m), normalized=True
    )

    origin = cut_trace.coords[0]

    destination = cut_trace.coords[-1]

    return (True, org, origin, destination)


def country_data_query(country: str, golden_db: dict) -> dict:
    """Function to query the country data

    :param country: Country iso
    :type country: str
    :param golden_db: tbt db credentials
    :type golden_db: dict
    :return: Dictionary with feat_id, geom and country_iso
    :rtype: dict
    """

    connection_string = (
        f'postgresql+psycopg2://{golden_db["user"]}'
        f':{golden_db["password"]}@{golden_db["host"]}:{golden_db["port"]}/{golden_db["database"]}'
    )

    con = sqlalchemy.create_engine(connection_string)

    sql = (
        "SELECT feat_id, geom, iso_a3 as country_iso "
        "FROM golden_routes.country_data "
        f"WHERE iso_a3 = '{country}'"
    )

    return geopandas.read_postgis(sql, con).iloc[0].to_dict()


def fcd_query(
    spark_context: pyspark.SparkContext,
    credentials: typing.Dict[str, str],
    input_url_fcd: str,
    date_start: str,
    date_end: str,
    country_feat_id: str,
) -> pyspark.RDD:
    """Function to make fcd query

    :param spark_context: spark context
    :type spark_context: pyspark.SparkContext
    :param credentials: fcd credentials
    :type credentials: Dict[str, str]
    :param input_url_fcd: url for the fcd archive
    :type input_url_fcd: str
    :param date_start: date to start fcd query
    :type date_start: str
    :param date_end: date to end fcd query
    :type date_end: str
    :param country_feat_id: the uuid for the country (from MNR)
    :type country_feat_id: str
    :return: pyspark RDD with all the traces
    :rtype: pyspark.RDD
    """
    return spark_context.newAPIHadoopRDD(
        inputFormatClass="com.tomtom.trace.spark.TraceInputFormat",
        keyClass="org.apache.hadoop.io.Text",
        valueClass="java.lang.String",
        conf={
            "com.tomtom.trace.spark.auth.clientId": credentials["clientId"],
            "com.tomtom.trace.spark.auth.clientSecret": credentials["clientSecret"],
            "com.tomtom.trace.spark.auth.tenantId": credentials["tenantId"],
            "com.tomtom.trace.spark.input": input_url_fcd,
            "com.tomtom.trace.spark.filter.date": f"{date_start}:{date_end}",
            "com.tomtom.trace.spark.filter.area": f"{country_feat_id}",
            "com.tomtom.trace.spark.filter.hours": "22-23",
        },
    )


def read_traces_fcd(
    credentials: dict,
    spark_context: pyspark.SparkContext,
    country_data: dict,
    fcd_frac: float,
    date_start=datetime.strftime(datetime.now() - timedelta(days=1), DATE_FORMAT),
    date_end=datetime.strftime(datetime.now(), DATE_FORMAT),
    debug: bool = False,
) -> pyspark.RDD:
    """Read FCD traces

    :param credentials: FCD credentials
    :type credentials: dict
    :param spark_context: pyspark.SparkContext
    :type spark_context: pyspark.SparkContext
    :param country_data: country data from database (country_iso, mnr_feat_id, geom)
    :type country_data: dict
    :param fcd_frac: Fraction (0-1) of FCD traces to retrieve (random sample)
    :type fcd_frac: float
    :param date_start: Date start
    :type date_start: str, optional
    :param date_end: Date end
    :type date_end: str, optional
    :param debug: debug flag
    :type debug: bool
    :return: Spark RDD with rows (int key (trace_id), list (traces))
    :rtype: pyspark.RDD
    """

    if debug:
        log.info("FCD using debug options")

    country_iso = country_data["country_iso"]

    frac_multiplier = 2
    if "US" in country_iso:
        frac_multiplier = 0.5

    country_feat_id = country_data["feat_id"]
    log.info(f"Feat_id={country_feat_id} for {country_iso}.")
    start_time = time.time()
    log.info(
        f"Get FCD traces for {country_iso} between {date_start}  and {date_end} from 22 to 23"
    )

    input_url_fcd = "https://ttediteststorage.blob.core.windows.net/fullv2/"
    traces_rdd = fcd_query(
        spark_context=spark_context,
        credentials=credentials,
        input_url_fcd=input_url_fcd,
        date_start=date_start,
        date_end=date_end,
        country_feat_id=country_feat_id,
    )
    log.info("Finish getting FCD trace")
    trace_fraction = min(fcd_frac * frac_multiplier, 1.0)

    log.info(f"Sampling from FCD (fraction={trace_fraction})")

    if debug:
        log.info("Not sampling for debugging")
        traces_rdd = (
            traces_rdd.map(parse_trace).filter(lambda x: x[0]).map(lambda x: x[1:])
        )
    else:
        traces_rdd = (
            traces_rdd.sample(withReplacement=False, fraction=trace_fraction)
            .map(parse_trace)
            .filter(lambda x: x[0])
            .map(lambda x: x[1:])
        )
    log.info(f"Finished sampling. {time.time() - start_time} secs.")

    return traces_rdd


def get_route_wrap(provider: Provider, row) -> typing.Tuple:
    """Wrapping from Provider getRoute function"""
    route = provider.getRoute(row["origin"], row["destination"])
    route_coords = route.geometry.coords[:]
    route_distance = float(route.length)
    if len(route_coords) <= 2 or route_distance == 0:
        return (False, [])
    return True, *row, route.geometry.coords[:], float(route.length), int(route.time)


def get_feats_ids(
    route: typing.List[float], distance: float
) -> typing.List[typing.Dict[str, typing.Union[str, float]]]:
    """Map matching to genesis to get the features ids of a route

    :param route: Coordinates in EPSG:4326 (lon, lat) format of route
    :type route: typing.List[float]
    :param distance: Total distance of the route
    :type distance: float
    :return: Dictionary of features ids with (feature_id, wkt geometry, travel_distance)
    :rtype: typing.List[typing.Dict[str, typing.Union[str, float]]]
    """
    time.sleep(0.05)
    chunks = 1
    if distance > 100_000:
        chunks = distance // 50_000
    route_line = shapely.geometry.LineString(route)
    step_size = route_line.length / (NUM_POINTS_INTERPOLATE_MAPMATCHING - 1)
    global_route = [
        route_line.interpolate(step_size * i).coords[0]
        for i in range(NUM_POINTS_INTERPOLATE_MAPMATCHING)
    ]
    feats = []
    for route in np.array_split(global_route, chunks):
        checkpoints = ";".join([f"{loc[0]:.6f},{loc[1]:.6f}" for loc in route])

        url = (
            f"https://api.tomtom.com/snap-to-roads/1/snap-to-roads?key={API_KEY_MAPMATCHING}"
            f"&points={checkpoints}"
            f"&fields={{route{{type,geometry{{type,coordinates}},"
            f"properties{{id,speedRestrictions{{maximumSpeed{{value,unit}}}},"
            f"traveledDistance{{value,unit}},address{{roadName,roadNumbers}}}}}},"
            f"distances{{total,road,offRoad}}}}&vehicleType=PassengerCar&measurementSystem=metric"
        )

        request = requests.get(url, timeout=600)
        content = json.loads(request.content)

        feats += content["route"]

    return feats


def wrap_get_feats_ids(row: typing.List) -> typing.Tuple:
    """Wrapping get_feat_ids"""
    result = []
    feats = get_feats_ids(row["route"], row["distance"])
    for feat in feats:
        feat_id = feat["properties"]["id"]
        geom = shapely.geometry.LineString(feat["geometry"]["coordinates"]).wkt
        travel_distance = feat["properties"]["traveledDistance"]["value"]
        result.append(
            [
                row["route_id"],
                row["origin"],
                row["destination"],
                row["distance"],
                feat_id,
                travel_distance,
                geom,
            ]
        )
    return result


def is_mdr(geom: str) -> bool:
    """query to get if wkt_geom is in mdr

    :param geom: geom wkt to query
    :type geom: str
    :return: if geom it is in mdr
    :rtype: bool
    """
    time.sleep(0.05)
    url = (
        "http://driven-km-lb-e047cd72a7da7f10.elb.eu-west-1.amazonaws.com:8080"
        "/driven-kilometers-scope-service/in-scope"
    )
    return (
        requests.post(
            url,
            data=geom,
            headers={"accept": "*/*", "Content-Type": "text/plain"},
            timeout=600,
        ).content
        == b"true"
    )


def check_trace_in_country(country_geom, trace: typing.Tuple) -> bool:

    """Check if a trace is contain in a country

    :param country_geom: shapely object with country geom
    :type country_geom: shapely multipolygon
    :param trace: trace parsed from FCD ('org', origin, destination)
    :type trace: typing.Tuple
    :return: True if contry_geom contains the origin and destiantion of the trace
    :rtype: bool
    """
    origin_contain = country_geom.contains(shapely.geometry.Point(trace[1]))
    destination_contain = country_geom.contains(shapely.geometry.Point(trace[2]))

    return origin_contain and destination_contain


def get_sampling_iteration(
    iteration_options: dict,
    fcd_credentials: dict,
    provider: Provider,
    spark_context: pyspark.SparkContext,
    az_db: dict,
):
    """
    Sampling function for golden_routes (iteration)

    :param iteration_options: options for the iteration
    :type iteration_options: dict
    :param target_mdr_km: Number (approximate) of total km from MDR retrive
    :type target_mdr_km: float
    :param fcd_credentials: FCD credentials
    :type fcd_credentials: dict
    :param az_db: tbt database credentials
    :type az_db: dict
    :return: Pandas DataFrame with sampling and last end date of FCD traces as string
    :rtype: Tuple(pd.DataFrame, str)
    """

    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    debug = iteration_options["debug"]
    if debug:
        log.info("Using debug options")
    country = iteration_options["country"]
    target_routes = iteration_options["target_routes"]
    mdr_perc_threshold = iteration_options["mdr_perc_threshold"]

    # Get FCD traces
    fcd_extraction_time = 0
    fcd_extracted_routes = 0
    fcd_traces = []
    log.info("Asking for FCD traces.")
    fcd_time_start = time.time()
    total_routes = 0
    date_end = iteration_options["fcd_end_date"]
    date_start = datetime.strftime(
        datetime.fromisoformat(date_end) - timedelta(days=1), DATE_FORMAT
    )
    route_limit = target_routes

    log.info("Query for country data")
    country_data = country_data_query(country, az_db)

    fcd_frac = 0.0002

    while total_routes < target_routes:
        while_start = time.time()
        log.info("Reading FCD traces.")
        start_time = time.time()
        traces = read_traces_fcd(
            credentials=fcd_credentials,
            country_data=country_data,
            spark_context=spark_context,
            date_start=date_start,
            date_end=date_end,
            debug=iteration_options["debug"],
            fcd_frac=fcd_frac,
        )
        if debug:
            traces_sample = traces.take(route_limit)
        else:
            traces_sample = traces.take(1_000_000)
        fcd_extracted_routes += len(traces_sample)

        log.info(
            f"Finished reading FCD traces. {len(traces_sample)} traces. "
            f"{time.time() - start_time} secs"
        )

        log.info(f"Checking if traces in {country_data['country_iso']}.")
        country_geom = country_data["geom"]
        if shapely.__version__ == "2.0.0":
            shapely.prepare(country_geom)
        country_traces = [
            trace
            for trace in traces_sample
            if check_trace_in_country(country_geom, trace)
        ]
        traces_retrive = len(country_traces)

        log.info(
            f"{len(traces_sample) - traces_retrive} removed."
            f"{traces_retrive} traces after country filter."
        )

        fcd_traces.extend(country_traces)

        total_routes += traces_retrive
        route_limit -= traces_retrive

        fcd_frac = min(
            max((fcd_frac * route_limit) / (traces_retrive + 1), 0.000001), 0.5
        )

        date_start = datetime.strftime(
            datetime.fromisoformat(date_start) - timedelta(days=1), DATE_FORMAT
        )
        date_end = datetime.strftime(
            datetime.fromisoformat(date_end) - timedelta(days=1), DATE_FORMAT
        )

        if datetime.fromisoformat(date_end) < DATE_BACKWARDS_LIMIT:
            log.info("Six months backwards limit reached. Stopping sampling.")
            break

        if total_routes < target_routes:
            log.info(
                f"More routes needed. Actual {total_routes}/{target_routes} routes. "
                f"{time.time() - while_start} secs."
            )
            log.info(
                f"Next fcd iteration trying to get {route_limit} routes (frac = {fcd_frac})"
            )
    fcd_time = time.time() - fcd_time_start
    fcd_extraction_time += fcd_time
    log.info(f"Final {len(fcd_traces)} traces. {fcd_time} secs")

    # Create spark dataframe
    sampling_df = spark.createDataFrame(
        fcd_traces,
        schema=T.StructType(
            [
                T.StructField("org", T.StringType()),
                T.StructField("origin", T.ArrayType(T.DoubleType())),
                T.StructField("destination", T.ArrayType(T.DoubleType())),
            ]
        ),
    ).repartition(16)
    sampling_df = sampling_df.withColumn("route_id", F.expr("uuid()")).cache()
    log.info(f"Created DataFrame for processing: {sampling_df.count()} rows")

    # Routing
    sampling_df = (
        sampling_df.rdd.map(lambda row: get_route_wrap(provider=provider, row=row))
        .filter(lambda x: x[0])
        .map(lambda x: x[1:])
        .toDF(
            schema=T.StructType(
                [
                    T.StructField("org", T.StringType()),
                    T.StructField("origin", T.ArrayType(T.DoubleType())),
                    T.StructField("destination", T.ArrayType(T.DoubleType())),
                    T.StructField("route_id", T.StringType()),
                    T.StructField("route", T.ArrayType(T.ArrayType(T.DoubleType()))),
                    T.StructField("distance", T.DoubleType()),
                    T.StructField("travel_time", T.IntegerType()),
                ]
            )
        )
        .cache()
    )
    start_processing_time = time.time()
    # MapMatching
    sampling_feat_id_df = (
        sampling_df.rdd.flatMap(wrap_get_feats_ids)
        .toDF(
            schema=T.StructType(
                [
                    T.StructField("route_id", T.StringType()),
                    T.StructField("origin", T.ArrayType(T.DoubleType())),
                    T.StructField("destination", T.ArrayType(T.DoubleType())),
                    T.StructField("distance", T.DoubleType()),
                    T.StructField("feat_id", T.StringType()),
                    T.StructField("travel_distance", T.IntegerType()),
                    T.StructField("geom", T.StringType()),
                ]
            )
        )
        .cache()
    )
    # MDR check
    unique_sampling_feat_id_df = sampling_feat_id_df.dropDuplicates(["feat_id", "geom"])

    udf_is_mdr = udf(is_mdr, T.BooleanType())
    unique_sampling_feat_id_df = unique_sampling_feat_id_df.withColumn(
        "is_MDR", udf_is_mdr(F.col("geom"))
    )
    join_sampling_feat_id_df = sampling_feat_id_df.join(
        unique_sampling_feat_id_df.select(["is_MDR", "feat_id", "geom"]),
        on=["feat_id", "geom"],
        how="left",
    )
    join_sampling_feat_id_df = join_sampling_feat_id_df.withColumn(
        "distanceMDR", F.when(F.col("is_MDR"), F.col("travel_distance")).otherwise(0)
    )

    results_df = join_sampling_feat_id_df.groupBy("route_id").agg(
        (F.sum(F.col("distanceMDR")) / F.sum(F.col("travel_distance")) * 100).alias(
            "percentageMDR"
        ),
        F.sum((F.col("travel_distance")) / 1000).alias("total_km"),
        F.sum((F.col("distanceMDR")) / 1000).alias("total_km_MDR"),
    )

    sampling_df = sampling_df.join(results_df, on="route_id", how="left")

    # Results to pandas and threhsold check
    sampling_df = sampling_df.drop("route").toPandas()
    sampling_df = sampling_df[sampling_df["percentageMDR"] >= mdr_perc_threshold * 100]

    sampling_df["origin"] = sampling_df["origin"].apply(
        lambda p: shapely.geometry.Point(p).wkt
    )
    sampling_df["destination"] = sampling_df["destination"].apply(
        lambda p: shapely.geometry.Point(p).wkt
    )

    sampling_df["provider"] = provider.name
    processing_time = time.time() - start_processing_time

    metadata = {
        "fcd_extraction_time": fcd_extraction_time,
        "fcd_extracted_routes": fcd_extracted_routes,
        "processed_routes": len(fcd_traces),
        "processing_time": processing_time,
    }

    return sampling_df, date_end, metadata
