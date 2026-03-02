from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType, LongType
import sys

csv_name = sys.argv[1]
city_name = sys.argv[2]

# Inicjalizacja sesji Spark
spark = (SparkSession.builder
         .appName("Create Empty Parquet")
         .getOrCreate())

# Definiowanie schematu zgodnego z twoimi danymi
# Dostosuj pola do rzeczywistej struktury inspection_routes
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

# Creating dataframe from csv file
df = spark.read.option("header", "false").schema(schema).csv(f"li_input/csv/{csv_name}.csv")

# Zapisywanie do formatu Parquet
df.write.mode("overwrite").parquet(f"data/tbt/sampling/sampling_samples_{city_name}.parquet")

print("Inspection Parquet Parquet file created in directory: li_input/parquet")