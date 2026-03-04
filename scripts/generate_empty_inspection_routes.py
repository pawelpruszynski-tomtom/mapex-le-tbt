"""Creates an empty inspection_routes parquet file in data/tbt/inspection/."""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.appName("generate_empty_inspection_routes").getOrCreate()

schema = StructType(
    [
        StructField("route_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("sample_id", StringType(), True),
        StructField("competitor", StringType(), True),
        StructField("provider_route", StringType(), True),
        StructField("provider_route_time", DoubleType(), True),
        StructField("provider_route_length", DoubleType(), True),
        StructField("origin", StringType(), True),
        StructField("destination", StringType(), True),
        StructField("provider", StringType(), True),
        StructField("competitor_route", StringType(), True),
        StructField("competitor_route_length", DoubleType(), True),
        StructField("competitor_route_time", DoubleType(), True),
        StructField("rac_state", StringType(), True),
        StructField("run_id", StringType(), True),
    ]
)

df = spark.createDataFrame([], schema)
df.write.mode("overwrite").parquet("data/tbt/inspection/inspection_routes.parquet")

print("Empty inspection_routes.parquet created in data/tbt/inspection/")

