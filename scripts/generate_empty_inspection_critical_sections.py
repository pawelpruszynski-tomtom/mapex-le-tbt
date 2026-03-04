"""Creates an empty inspection_critical_sections parquet file in data/tbt/inspection/."""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.appName(
    "generate_empty_inspection_critical_sections"
).getOrCreate()

schema = StructType(
    [
        StructField("route_id", StringType(), True),
        StructField("case_id", StringType(), True),
        StructField("stretch", StringType(), True),
        StructField("stretch_length", DoubleType(), True),
        StructField("fcd_state", StringType(), True),
        StructField("pra", DoubleType(), True),
        StructField("prb", DoubleType(), True),
        StructField("prab", DoubleType(), True),
        StructField("lift", DoubleType(), True),
        StructField("tot", IntegerType(), True),
        StructField("reference_case_id", StringType(), True),
        StructField("run_id", StringType(), True),
    ]
)

df = spark.createDataFrame([], schema)
df.write.mode("overwrite").parquet(
    "data/tbt/inspection/inspection_critical_sections.parquet"
)

print("Empty inspection_critical_sections.parquet created in data/tbt/inspection/")

