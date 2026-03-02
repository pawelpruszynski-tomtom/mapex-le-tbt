"""Creates an empty critical_sections_with_mcp_feedback parquet file in data/tbt/inspection/."""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.appName(
    "generate_empty_critical_sections_with_mcp_feedback"
).getOrCreate()

schema = StructType(
    [
        StructField("run_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("case_id", StringType(), True),
        StructField("mcp_state", StringType(), True),
        StructField("reference_case_id", StringType(), True),
        StructField("error_subtype", StringType(), True),
    ]
)

df = spark.createDataFrame([], schema)
df.write.mode("overwrite").parquet(
    "data/tbt/inspection/critical_sections_with_mcp_feedback.parquet"
)

print(
    "Empty critical_sections_with_mcp_feedback.parquet created in data/tbt/inspection/"
)

