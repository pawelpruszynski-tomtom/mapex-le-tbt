# Databricks notebook source
# MAGIC %md
# MAGIC # Check Log Tables
# MAGIC In this notebook, log tables of the EC submodule (and of the deprecated EC API) can be checked. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %run "../utils"
# MAGIC

# COMMAND ----------

# MAGIC %run "../define_table_schema"

# COMMAND ----------

import pandas as pd
import sqlalchemy
import json

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import pyspark.sql.types as T
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# COMMAND ----------


mcp_oauth = json.loads(dbutils.secrets.get(key='mcp_oauth', scope='utils'))
storage_account_name = 'mcpengbronze'
blob_container_name = 'directions-metric'
metric_name = 'tbt'

spark.conf.set("fs.azure.account.auth.type.mcpengbronze.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.mcpengbronze.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.mcpengbronze.dfs.core.windows.net", mcp_oauth['client_id'])
spark.conf.set("fs.azure.account.oauth2.client.secret.mcpengbronze.dfs.core.windows.net",  mcp_oauth['client_secret'])
spark.conf.set("fs.azure.account.oauth2.client.endpoint.mcpengbronze.dfs.core.windows.net",  mcp_oauth['client_endpoint'])

# COMMAND ----------

print(prod_log_table_path)
print(dev_log_table_path)


# COMMAND ----------

# MAGIC %md
# MAGIC # Dev log table

# COMMAND ----------

df = spark.read.format("delta").load(dev_log_table_path)
df.limit(10).display()

# COMMAND ----------

df.sort(col("prediction_date").desc()).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Prod log table

# COMMAND ----------

df = spark.read.format("delta").load(prod_log_table_path)

# COMMAND ----------

# DBTITLE 1,show df with most recent inspections first
df.sort(col("prediction_date").desc()).display()

# COMMAND ----------

# DBTITLE 1,Check if there are rows containing nulls
# Find the columns of the DataFrame
columns = df.columns

# Construct a condition where any of the columns is null
condition = ' OR '.join([f'({col}) IS NULL' for col in columns])

# Filter the DataFrame for rows with null values in any column
df_with_nulls = df.filter(condition)

# Show all rows with null values
df_with_nulls.count()

# COMMAND ----------

# DBTITLE 1,Check schema
spark.read.format("delta").load(prod_log_table_path).printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check log table for specific inspection

# COMMAND ----------

df.printSchema()

# COMMAND ----------

pdf = df[df["run_id"] == "a75c68ba-a935-4e72-bdc7-4680459b8a10"].toPandas()

# COMMAND ----------

print(pdf[pdf["error_label"] == "no_error"].shape[0])
(pdf[pdf["error_label"] == "potential_error"].shape[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### (OPTIONAL): Overwrite schema of log table

# COMMAND ----------

# DBTITLE 1,Define schema for overwriting schema
schema=T.StructType(
            [
                # metadata
                T.StructField("run_id", T.StringType()),
                T.StructField("country", T.StringType()),
                T.StructField("provider", T.StringType()),
                T.StructField("route_id", T.StringType()),
                T.StructField("case_id", T.StringType()),
                T.StructField("route", T.StringType()),
                T.StructField("stretch", T.StringType()),
                # General features
                T.StructField("is_tbt", T.IntegerType()),
                T.StructField("is_hdr", T.IntegerType()),
                T.StructField("drive_left_side", T.IntegerType()),
                T.StructField("drive_right_side", T.IntegerType()),
                # Geometrical features
                T.StructField("route_length", T.FloatType()),
                T.StructField("stretch_length", T.FloatType()),
                T.StructField("stretch_start_position_in_route", T.FloatType()),
                T.StructField("stretch_end_position_in_route", T.FloatType()),
                T.StructField("stretch_starts_at_the_route_start", T.FloatType()),
                T.StructField("stretch_ends_at_the_route_end", T.FloatType()),
                T.StructField("heading_stretch", T.FloatType()),
                T.StructField("left_turns", T.FloatType()),
                T.StructField("straight_turns", T.FloatType()),
                T.StructField("right_turns", T.FloatType()),
                T.StructField("total_right_angle", T.FloatType()),
                T.StructField("max_right_angle", T.FloatType()),
                T.StructField("total_left_angle", T.FloatType()),
                T.StructField("max_left_angle", T.FloatType()),
                T.StructField("absolute_angle", T.FloatType()),
                T.StructField("has_left_turns", T.FloatType()),
                T.StructField("has_right_turns", T.FloatType()),
                T.StructField("min_curvature", T.FloatType()),
                T.StructField("max_curvature", T.FloatType()),
                T.StructField("mean_curvature", T.FloatType()),
                T.StructField("distance_start_end_stretch", T.FloatType()),
                T.StructField("route_coverage", T.FloatType()),
                T.StructField("stretch_covers_route", T.FloatType()),
                T.StructField("tortuosity", T.FloatType()),
                T.StructField("sinuosity", T.FloatType()),
                T.StructField("density", T.FloatType()),
                # FCD features
                T.StructField("pra", T.FloatType()),
                T.StructField("prb", T.FloatType()),
                T.StructField("prab", T.FloatType()),
                T.StructField("lift", T.FloatType()),
                T.StructField("tot", T.IntegerType()),
                # SDO Features
                T.StructField("CONSTRUCTION_AHEAD_TT", T.IntegerType()),
                T.StructField("MANDATORY_STRAIGHT_ONLY", T.IntegerType()),
                T.StructField("MANDATORY_STRAIGHT_OR_LEFT", T.IntegerType()),
                T.StructField("MANDATORY_TURN_RESTRICTION", T.IntegerType()),
                T.StructField("MANDATORY_TURN_LEFT_ONLY", T.IntegerType()),
                T.StructField("MANDATORY_TURN_LEFT_OR_RIGHT", T.IntegerType()),
                T.StructField("MANDATORY_LEFT_OR_STRAIGHT_OR_RIGHT", T.IntegerType()),
                T.StructField("MANDATORY_TURN_RIGHT_ONLY", T.IntegerType()),
                T.StructField("MANDATORY_STRAIGHT_OR_RIGHT", T.IntegerType()),
                T.StructField("NO_ENTRY", T.IntegerType()),
                T.StructField("NO_MOTOR_VEHICLE", T.IntegerType()),
                T.StructField("NO_CAR_OR_BIKE", T.IntegerType()),
                T.StructField("NO_LEFT_OR_RIGHT_TT", T.IntegerType()),
                T.StructField("NO_LEFT_TURN", T.IntegerType()),
                T.StructField("NO_RIGHT_TURN", T.IntegerType()),
                T.StructField("NO_VEHICLE", T.IntegerType()),
                T.StructField("NO_STRAIGHT_OR_LEFT_TT", T.IntegerType()),
                T.StructField("NO_STRAIGHT_OR_RIGHT_TT", T.IntegerType()),
                T.StructField("NO_STRAIGHT_TT", T.IntegerType()),
                T.StructField("NO_TURN_TT", T.IntegerType()),
                T.StructField("NO_U_OR_LEFT_TURN", T.IntegerType()),
                T.StructField("NO_U_TURN", T.IntegerType()),
                T.StructField("ONEWAY_TRAFFIC_TO_STRAIGHT", T.IntegerType()),
                T.StructField("sdo_api_response", T.StringType()),
                # error label and probability 
                T.StructField("probability", T.FloatType()),
                T.StructField("error_label", T.StringType()),
                # Model metadata
                T.StructField("ml_model_options", T.StringType()),
                T.StructField("prediction_date", T.StringType()),
                T.StructField("sample_metric", T.StringType()),
                T.StructField("model_run_id", T.StringType()),
            ])

# COMMAND ----------

# Using selectExpr allows casting multiple columns in a single operation
# Cast the DataFrame columns to the new schema
df_casted = df
for field in schema.fields:
    df_casted = df_casted.withColumn(field.name, col(field.name).cast(field.dataType))

# COMMAND ----------

df_casted.printSchema()

# COMMAND ----------

# DBTITLE 1,CAUTION: Overwrite schema
# (
#     df_casted
#     .write
#     .format("delta")
#     .partitionBy("is_tbt", "error_label", "country")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .save(log_table_path)
# )

# COMMAND ----------

spark.read.format("delta").load(log_table_path).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### (OPTIONAL): Rename delta table and have a new one created
# MAGIC

# COMMAND ----------

# DBTITLE 1,CAUTION: 
# # Define the paths to the existing and new Delta tables
# old_table_path = log_table_path
# new_table_path = "abfss://directions-metric@mcpengbronze.dfs.core.windows.net/tbt/dev/ec_model_submodule/ec_model_logs_prerelease.delta"

# # Read the old Delta table
# df = spark.read.format("delta").load(old_table_path)

# # Write the DataFrame as a new Delta table with the new name (path)
# df.write.format("delta").mode("overwrite").save(new_table_path)


# COMMAND ----------

spark.read.format("delta").load(new_table_path).display()
