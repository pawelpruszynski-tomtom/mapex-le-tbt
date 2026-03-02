"""Creates sampling_metadata parquet in data/tbt/sampling/ from li_input/geojson/Routes2check.geojson."""

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

GEOJSON_PATH = Path(__file__).parents[1] / "li_input" / "geojson" / "Routes2check.geojson"
OUTPUT_PATH = "data/tbt/sampling/sampling_metadata.parquet"

spark = SparkSession.builder.appName("generate_sampling_metadata").getOrCreate()

schema = StructType(
    [
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
    ]
)

with open(GEOJSON_PATH, encoding="utf-8") as fh:
    geojson = json.load(fh)

# Derive unique (sample_id, country, date_generated) combinations from features
seen = set()
rows = []
for feature in geojson["features"]:
    props = feature["properties"]
    key = (props.get("sample_id"), props.get("country"), props.get("date_generated"))
    if key in seen:
        continue
    seen.add(key)
    sample_id, country, date_generated = key
    rows.append(
        (
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
        )
    )

df = spark.createDataFrame(rows, schema)
df.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"sampling_metadata.parquet created with {len(rows)} rows → {OUTPUT_PATH}")

