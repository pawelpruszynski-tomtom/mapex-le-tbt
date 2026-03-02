""" functions to split_region pipeline """
import logging
import re

import numpy
import pandas
import pyspark.sql
import pyspark.sql.functions as F
import requests
import shapely.geometry
import shapely.wkt

import tbt.navutils.common.morton as morton

log = logging.getLogger(__name__)


def loopfile(read_data):
    """Read and parse a list of polylines from read_data"""
    polylines = []
    polyline = ""
    countend = 0
    for line in read_data:
        clean_line = re.sub(r"\s+", " ", line.strip())
        if len(clean_line) > 1:
            try:
                x_coord = ""
                y_coord = ""
                coords = clean_line.split(" ")
                if float(coords[0]) > -180:
                    x_coord = coords[0]
                    y_coord = coords[1]
                    polyline += x_coord + " " + y_coord + ","
            except ValueError:
                pass
        if len(clean_line) == 1:
            polyline = "("

        if clean_line == "END":
            countend += 1
            if (countend % 2) == 1:
                polyline = polyline[0 : len(polyline) - 1]
                polyline += ")"
                polylines.append(polyline)
    return polylines


def createwkt(polylines):
    """Create a list of polylines to WKT polygon"""
    polygon = ""
    if len(polylines) > 0:
        polygon = "POLYGON ("
        for polyline in polylines:
            polygon += polyline + ","
        polygon = polygon[0 : len(polygon) - 1]
        polygon += ")"
    return polygon


def convert_poly_to_wkt(options: dict):
    """Download a file of polylines and convert to WKT polygon"""
    poly_url = options["poly_url"]

    log.info("Downloading .poly file and converting to WKT")
    request = requests.get(poly_url, timeout=600)
    read_data = request.content.decode("ascii").split("\n")
    polylines = loopfile(read_data)
    wkt = createwkt(polylines)
    return wkt


def create_area_m14(options: dict, wkt: str) -> pyspark.sql.DataFrame:
    """Function to create a pyspark DataFrame with m14 tiles"""
    log.info("Generating m14 tiles")

    new_region_iso = options["new_region_iso"]

    poly = shapely.wkt.loads(wkt)
    minlon, minlat, maxlon, maxlat = poly.bounds
    grid_resolution = options["grid_resolution"]

    lons = numpy.linspace(minlon, maxlon, grid_resolution)
    lats = numpy.linspace(minlat, maxlat, grid_resolution)

    tiles = []

    for i in range(grid_resolution):
        for j in range(grid_resolution):
            lon = lons[i]
            lat = lats[j]
            if poly.contains(shapely.geometry.Point([lon, lat])):
                morton_object = morton.from_degrees(lon, lat, 14)
                tiles.append(morton_object.morton)
    tiles = list(set(tiles))

    tile_id = [""] * len(tiles)
    min_lon = [0] * len(tiles)
    min_lat = [0] * len(tiles)
    max_lon = [0] * len(tiles)
    max_lat = [0] * len(tiles)

    for i, tile in enumerate(tiles):
        morton_object = morton.from_morton(tile, 14)
        tile_id[i] = hex(tile).split("x")[1]
        min_lon[i] = morton_object.min_x()
        min_lat[i] = morton_object.min_y()
        max_lon[i] = morton_object.min_x()
        max_lat[i] = morton_object.max_y()

    tiles_df = pandas.DataFrame(
        {
            "tile_id": tile_id,
            "min_lon": min_lon,
            "max_lon": max_lon,
            "min_lat": min_lat,
            "max_lat": max_lat,
            "level": 14,
            "country": new_region_iso,
        }
    )

    log.info("Generated %i morton 14 tiles for %s", len(tiles_df), new_region_iso)
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    sdf = spark.createDataFrame(
        tiles_df,
        schema="""
            tile_id string,
            min_lon double,
            max_lon double,
            min_lat double,
            max_lat double,
            level int,
            country string
        """,
    )
    return sdf


def process_m14_data(m14: pyspark.sql.DataFrame, sdf: pandas.DataFrame, options: dict):
    """Function to process the m14 tiles spark DataFrmae"""
    new_region_iso = options["new_region_iso"]
    country = options["country"]
    default_quality = options["default_quality"]

    already_there_tiles = m14.filter(f"country='{new_region_iso}'").count()
    if already_there_tiles > 0:
        raise Exception(f"Area {new_region_iso} is already in m14scope.")

    sdf = sdf.join(
        m14.filter(f"country='{country}'").select("tile_id", "quality"),
        on="tile_id",
        how="left",
    ).withColumn("quality", F.coalesce(F.col("quality"), F.lit(default_quality)))
    return sdf
