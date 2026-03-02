# Databricks notebook source
# MAGIC %md
# MAGIC # Create new FCD features with other features to create a full training dataset. 
# MAGIC
# MAGIC This notebook can be launched as a databricks job. Multiple versions can be launched in parallel. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %pip install shapely geopandas folium matplotlib mapclassify sqlalchemy pymorton

# COMMAND ----------

import fcd_py
from pyspark.sql import SparkSession
import json
from pyspark.sql import functions as F
import sqlalchemy
import geopandas as gpd
import shapely
import shapely.wkt
from shapely.geometry import mapping
from datetime import datetime, timedelta
import copy
from tqdm import tqdm
tqdm.pandas()

# plotting
import seaborn as sns
import matplotlib.pyplot as plt

# morton
import pymorton as pm


# COMMAND ----------

credentials = json.loads(dbutils.secrets.get(scope = "mdbf3", key = "fcd_credentials"))

# COMMAND ----------

# sys.path.append("/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/notebooks/")
%run "/Workspace/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/error_classification/call_FCD_API_new.py"

# COMMAND ----------

# MAGIC %run "/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/notebooks/utils"
# MAGIC

# COMMAND ----------

# MAGIC %run "/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/notebooks/define_table_schema"
# MAGIC

# COMMAND ----------

# Define parameters
start_index = int(dbutils.widgets.get("start_index"))
end_index = int(dbutils.widgets.get("end_index"))
chunk_size = int(dbutils.widgets.get("chunk_size"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Retrieve intermediate table and Feature table
# MAGIC - Intermediate table for stretch and route
# MAGIC - Feature table for all the other features

# COMMAND ----------

# intermediate table
intermediate_data = (
    spark.read.format('delta').load(intermediate_data_path)
    .drop("sdo_response", "sdo_data_start_date", "sdo_data_end_date", "sdo_api_call_date")
)
intermediate_data = intermediate_data.dropDuplicates(["stretch"])

# COMMAND ----------

# feature table
feature_table = (
    spark.read.format('delta').load(feature_table_path)
    .drop("date", "provider", "metric", "country", 
          "pra", "prb", "prab", "tot", "lift", 
          "error_type", "error_label")
)

# COMMAND ----------

# join them 
combined = (
    intermediate_data
    .join(feature_table, on="case_id", how="inner")
)

# COMMAND ----------

df_to_featurize = combined.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sort by Morton/geohash

# COMMAND ----------

# extract start and endpoints and buffer for start and endpoint
df_to_featurize["lat"] = df_to_featurize["stretch"].apply(lambda x: shapely.wkt.loads(x).coords[0][0])
df_to_featurize["lon"] = df_to_featurize["stretch"].apply(lambda x: shapely.wkt.loads(x).coords[0][-1])

# Apply the function to each row in the DataFrame with precision=13 for Morton 13 tiles
df_to_featurize['morton'] = df_to_featurize.apply(lambda row: pm.interleave_latlng(row['lat'], row['lon']), axis=1)

# Sort the DataFrame by the 'morton' column
df_to_featurize.sort_values(by='morton', inplace=True, ignore_index=True)

# sort by stretch so to use the cache of FCD
# df_to_featurize.sort_values(by=["lat", "lon"], axis=0, inplace=True, ignore_index=True, ascending=True)
df_to_featurize.drop(["lat", "lon", "morton"], axis=1, inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Calculate new FCD features 

# COMMAND ----------

# set start and end index of df
end_index = min(end_index, len(df_to_featurize))

print(f"Featurizing df from index {start_index} to {end_index}")
df_to_featurize = df_to_featurize.iloc[start_index:end_index]
df_to_featurize.reset_index(inplace=True, drop=True)

# COMMAND ----------

# Calculate the number of chunks needed
n_chunks = len(df_to_featurize) // chunk_size
if len(df_to_featurize) % chunk_size != 0:
    n_chunks += 1  # Add an extra chunk if there are leftovers

# Loop over the chunks
for i in range(n_chunks):
    print(f"Chunk {i} out of {n_chunks}. Progress: {(i/n_chunks)*100}%")
    
    # Calculate the start and end indices for the current chunk
    start_index_loop = i * chunk_size
    end_index_loop = min((i + 1) * chunk_size, len(df_to_featurize))

    # Get the current chunk of the DataFrame
    chunk = df_to_featurize.iloc[start_index_loop:end_index_loop]
    chunk.reset_index(inplace=True, drop=True)

    fcd_featurizer = FCDFeaturizer(
        chunk,
        trace_retrieval_geometry="bbox_json",
        traces_limit=2000,
        fcd_credentials=credentials,
        spark_context=spark
    )

    fcd_features = fcd_featurizer.featurize()
    fcd_features.rename(columns={
        'pra': 'pra_new',
        'prb': 'prb_new',
        'tot': 'tot_new'}, inplace=True)

    df_chunk_featurized = pd.concat([chunk, fcd_features], axis=1, verify_integrity=True)

    # Convert 'tot' column to integer if it's in float format
    if 'tot' in df_chunk_featurized.columns and df_chunk_featurized['tot'].dtype != 'int64':
        df_chunk_featurized['tot'] = df_chunk_featurized['tot'].astype('int')
    
    # Convert the featurized chunk to a Spark DataFrame and write it to the data lake
    spark_df = spark.createDataFrame(df_chunk_featurized)

    # Write to the data lake, overwrite on the first iteration, append on subsequent iterations
    spark_df.write.mode("append") \
                .format('delta') \
                .option("overwriteSchema", "true") \
                .save("dbfs:/error_classification/OMA-1043/feature_table_w_new_fcd_features_01.delta")

print("SUCCESS")
