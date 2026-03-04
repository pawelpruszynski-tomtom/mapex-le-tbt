"""Creates sampling_samples parquet in data/tbt/sampling/ from li_input/geojson/Routes2check.geojson."""

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

GEOJSON_PATH = Path(__file__).parents[1] / "li_input" / "geojson" / "Routes2check.geojson"
OUTPUT_PATH = "data/tbt/sampling/sampling_samples.parquet"

spark = SparkSession.builder.appName("generate_sampling_samples").getOrCreate()

schema = StructType(
    [
        StructField("sample_id", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("destination", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("quality", StringType(), True),
        StructField("country", StringType(), True),
        StructField("date_generated", StringType(), True),
        StructField("org", StringType(), True),
    ]
)

with open(GEOJSON_PATH, encoding="utf-8") as fh:
    geojson = json.load(fh)

rows = []
for feature in geojson["features"]:
    props = feature["properties"]
    coords = feature["geometry"]["coordinates"]
    origin_lon, origin_lat = coords[0]
    dest_lon, dest_lat = coords[-1]
    rows.append(
        (
            props.get("sample_id"),
            props.get("tile_id"),
            f"POINT({origin_lon} {origin_lat})",
            f"POINT({dest_lon} {dest_lat})",
            props.get("route_id"),
            props.get("quality"),
            props.get("country"),
            props.get("date_generated"),
            props.get("org"),
        )
    )

df = spark.createDataFrame(rows, schema)
df.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"sampling_samples.parquet created with {len(rows)} rows → {OUTPUT_PATH}")

