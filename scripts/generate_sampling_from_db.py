"""Creates sampling_samples and sampling_metadata parquet files from database route_data."""

import json
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Load environment variables
load_dotenv()

# Database configuration from .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

# Output paths
OUTPUT_SAMPLES_PATH = "data/tbt/sampling/sampling_samples.parquet"
OUTPUT_METADATA_PATH = "data/tbt/sampling/sampling_metadata.parquet"


def fetch_route_data_from_db(sample_id: str) -> list:
    """
    Fetch route_data from database for given sample_id (which equals pipeline_id).

    Args:
        sample_id: UUID of the sample/pipeline

    Returns:
        List of route_data dictionaries
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Query to fetch route_data using sample_id as pipeline_id
        query = f"""
            SELECT route_data 
            FROM {DB_SCHEMA}.routes 
            WHERE pipeline_id = %s
        """

        cursor.execute(query, (sample_id,))
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        if not rows:
            raise ValueError(f"No route data found for sample_id/pipeline_id: {sample_id}")

        # Extract route_data from each row
        route_data_list = [row[0] for row in rows]

        print(f"✅ Fetched {len(route_data_list)} routes from database for sample_id: {sample_id}")
        return route_data_list

    except Exception as e:
        print(f"❌ Database error: {e}")
        raise


def convert_route_data_to_geojson(route_data_list: list, sample_id: str) -> dict:
    """
    Convert route_data list to GeoJSON FeatureCollection format.

    Args:
        route_data_list: List of route_data dictionaries
        sample_id: Sample ID to use for all routes (overrides route_data sample_id)

    Returns:
        GeoJSON FeatureCollection
    """
    features = []

    for route_data in route_data_list:
        # Parse origin and destination coordinates
        # origin format: "POINT(lon lat)"
        origin_str = route_data.get("origin", "")
        if origin_str.startswith("POINT("):
            coords = origin_str.replace("POINT(", "").replace(")", "").split()
            origin_lon, origin_lat = float(coords[0]), float(coords[1])
        else:
            origin_lon, origin_lat = 0.0, 0.0

        # to_coord format: "lat, lon"
        to_coord = route_data.get("to_coord", "0.0, 0.0")
        dest_lat, dest_lon = map(float, to_coord.split(","))

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [origin_lon, origin_lat],
                    [dest_lon, dest_lat]
                ]
            },
            "properties": {
                "sample_id": sample_id,  # Use provided sample_id instead of route_data sample_id
                "tile_id": route_data.get("tile_id"),
                "route_id": route_data.get("route_id"),
                "quality": route_data.get("quality"),
                "country": route_data.get("country"),
                "date_generated": route_data.get("date_generated"),
                "org": route_data.get("org"),
                "name": route_data.get("name"),
                "from_coord": route_data.get("from_coord"),
                "to_coord": route_data.get("to_coord"),
                "length_orbis": route_data.get("length_orbis"),
                "length_genesis": route_data.get("length_genesis")
            }
        }
        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    return geojson


def create_sampling_samples(geojson: dict, spark: SparkSession) -> None:
    """Create sampling_samples parquet file."""
    schema = StructType([
        StructField("sample_id", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("destination", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("quality", StringType(), True),
        StructField("country", StringType(), True),
        StructField("date_generated", StringType(), True),
        StructField("org", StringType(), True),
    ])

    rows = []
    for feature in geojson["features"]:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]
        origin_lon, origin_lat = coords[0]
        dest_lon, dest_lat = coords[-1]
        rows.append((
            props.get("sample_id"),
            props.get("tile_id"),
            f"POINT({origin_lon} {origin_lat})",
            f"POINT({dest_lon} {dest_lat})",
            props.get("route_id"),
            props.get("quality"),
            props.get("country"),
            props.get("date_generated"),
            props.get("org"),
        ))

    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").parquet(OUTPUT_SAMPLES_PATH)

    # Show unique sample_ids for verification
    unique_sample_ids = df.select("sample_id").distinct().collect()
    print(f"✅ sampling_samples.parquet created with {len(rows)} rows → {OUTPUT_SAMPLES_PATH}")
    print(f"   Unique sample_id values: {[row['sample_id'] for row in unique_sample_ids]}")


def create_sampling_metadata(geojson: dict, spark: SparkSession) -> None:
    """Create sampling_metadata parquet file."""
    schema = StructType([
        StructField("sample_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("fcd_extraction_time", FloatType(), True),
        StructField("fcd_extracted_routes", FloatType(), True),
        StructField("processed_routes", FloatType(), True),
        StructField("processing_time", FloatType(), True),
        StructField("date_generated", StringType(), True),
        StructField("osm_map_version", StringType(), True),
        StructField("parameters", StringType(), True),
        StructField("api_calls", StringType(), True),
        StructField("sample_q", StringType(), True),
        StructField("final_routes", IntegerType(), True),
        StructField("elapsed_time", IntegerType(), True),
        StructField("metric", StringType(), True),
    ])

    # Derive unique (sample_id, country, date_generated) combinations
    seen = set()
    rows = []
    for feature in geojson["features"]:
        props = feature["properties"]
        key = (props.get("sample_id"), props.get("country"), props.get("date_generated"))
        if key in seen:
            continue
        seen.add(key)
        sample_id, country, date_generated = key
        rows.append((
            sample_id,
            country,
            None,   # fcd_extraction_time
            None,   # fcd_extracted_routes
            None,   # processed_routes
            0.0,    # processing_time
            date_generated,
            date_generated,
            '{"mode": "all_fail"}',
            None,   # api_calls
            None,   # sample_q
            0,      # final_routes
            None,   # elapsed_time
            "TbT",
        ))

    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").parquet(OUTPUT_METADATA_PATH)

    print(f"✅ sampling_metadata.parquet created with {len(rows)} rows → {OUTPUT_METADATA_PATH}")
    if rows:
        print(f"   Sample IDs in metadata: {[row[0] for row in rows]}")


def main(sample_id: str):
    """Main function to generate sampling data from database.

    Args:
        sample_id: UUID that serves as both sample_id and pipeline_id
    """
    print(f"🔄 Generating sampling data from database")
    print(f"   sample_id (also pipeline_id): {sample_id}")

    # Fetch data from database (sample_id is used as pipeline_id in query)
    route_data_list = fetch_route_data_from_db(sample_id)

    # Convert to GeoJSON format (using sample_id for all routes)
    geojson = convert_route_data_to_geojson(route_data_list, sample_id)

    # Initialize Spark
    spark = SparkSession.builder.appName("generate_sampling_from_db").getOrCreate()

    # Create parquet files
    create_sampling_samples(geojson, spark)
    create_sampling_metadata(geojson, spark)

    spark.stop()
    print("✅ Sampling data generation completed")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_sampling_from_db.py <sample_id>")
        print("\nNote: sample_id is also used as pipeline_id to query the database")
        print("\nExample:")
        print("  python generate_sampling_from_db.py b294bb07-b9d6-4e6f-8100-b909fe6227df")
        sys.exit(1)

    sample_id = sys.argv[1]
    main(sample_id)

    pipeline_id = sys.argv[1]
    sample_id = sys.argv[2]
    main(pipeline_id, sample_id)

