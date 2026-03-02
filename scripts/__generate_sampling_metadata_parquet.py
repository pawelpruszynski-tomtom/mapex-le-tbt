from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType, LongType
import sys

country_code = sys.argv[1]
city_name = sys.argv[2]
sample_id = sys.argv[3]

# Inicjalizacja sesji Spark
spark = (SparkSession.builder
         .appName("Create Empty Parquet")
         .getOrCreate())

# Definiowanie schematu zgodnego z twoimi danymi
# Dostosuj pola do rzeczywistej struktury inspection_routes
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

# Creating dataframe from csv file
data = [[
    sample_id,
    country_code,
    None,
    None,
    None,
    0.0,
    '2025-09-30',
    '2025-09-30',
    '{"mode": "all_fail", "product": "0000.00.000", "base_providers": "OSM-STRICT", "run_id_sample": "new", "comment": "N/A", "countries": "POL", "mapdate": "2022-06-21", "trace_limit": "10000", "max_routes": "{\"Q1\":3500, \"Q2\":2800, \"Q3\":700, \"Q4\":0, \"Q5\":0}", "providers": "OSM-STRICT", "map_versions": {"OSM": {"version": "3.1.4", "tileset_last_modified": 1655939140, "tileset_last_modified_date": "2022-06-22 23:05:40"}}}',
    None,
    '2025-Q3',
    0,
    None,
    'TbT'
]]

df = spark.createDataFrame(data, schema)

# Zapisywanie do formatu Parquet
df.write.mode("overwrite").parquet(f"li_input/parquet/sampling_metadata_{city_name}.parquet")

print("Inspection Metadata Parquet file created in directory: li_input/parquet")