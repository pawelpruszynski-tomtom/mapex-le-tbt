"""Creates an empty inspection_metadata parquet file in data/tbt/inspection/."""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.appName("generate_empty_inspection_metadata").getOrCreate()

schema = StructType(
    [
        StructField("run_id", StringType(), True),
        StructField("sample_id", StringType(), True),
        StructField("provider", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("mapdate", StringType(), True),
        StructField("product", StringType(), True),
        StructField("country", StringType(), True),
        StructField("mode", StringType(), True),
        StructField("competitor", StringType(), True),
        StructField("mcp_tasks", StringType(), True),
        StructField("completed", BooleanType(), True),
        StructField("inspection_date", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("rac_elapsed_time", FloatType(), True),
        StructField("fcd_elapsed_time", FloatType(), True),
        StructField("total_elapsed_time", FloatType(), True),
        StructField("api_calls", StringType(), True),
        StructField("sanity_fail", BooleanType(), True),
        StructField("sanity_msg", StringType(), True),
    ]
)

df = spark.createDataFrame([], schema)
df.write.mode("overwrite").parquet("data/tbt/inspection/inspection_metadata.parquet")

print("Empty inspection_metadata.parquet created in data/tbt/inspection/")

