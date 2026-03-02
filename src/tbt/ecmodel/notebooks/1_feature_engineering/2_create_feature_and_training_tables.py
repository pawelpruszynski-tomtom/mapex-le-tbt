# Databricks notebook source
# MAGIC %md
# MAGIC # Create Feature table and Train Table
# MAGIC In this notebook, Train tables are constructed with different characteristics. 
# MAGIC While the feature table contains also metadata for cases, the train table only contains features and the target variable (error/no error)
# MAGIC
# MAGIC - Train table: contains all cases and only relevant features, stored in datalake and as HIVE table
# MAGIC - Train table deduplicated: train table deduplicated on country+stretch_length, trying to mitigate that critical sections are reused and thus might influence performance metric calculations. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import general libraries, functions and variables

# COMMAND ----------

# MAGIC %run "../utils"

# COMMAND ----------

# MAGIC %run "../define_table_schema"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import notebook-specific libraries

# COMMAND ----------

from pyspark.sql.functions import col,  min as min_, max as max_
from datetime import timedelta, datetime
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC # Check out the feature table

# COMMAND ----------

feature_table = spark.read.format('delta').load(feature_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Deduplicate feature table 
# MAGIC As in subsequent inspections we are reusing critical sections, it might happen that the same error is included in the training data multiple times. Thus, we need to deduplicate into individual errors
# MAGIC
# MAGIC Two approaches: 
# MAGIC - As a first deduplication step, we deduplicate on country + route_length + stretch_length
# MAGIC - As a more strict approach, we deduplicate only on country + stretch_length

# COMMAND ----------

# Assuming df is your DataFrame
original_count = feature_table.count()
feature_table_deduplicated_1 = feature_table.dropDuplicates(["country", "route_length", "stretch_length"])
feature_table_deduplicated_2 = feature_table.dropDuplicates(["country", "stretch_length"])
deduplicated_count_1 = feature_table_deduplicated_1.count()
deduplicated_count_2 = feature_table_deduplicated_2.count()

print(f"Number of rows removed during deduplication 1: {original_count - deduplicated_count_1}")
print(f"Number of rows removed during deduplication 2: {original_count - deduplicated_count_2}")


# COMMAND ----------

print(feature_table_deduplicated_path)
print(feature_table_deduplicated_country_stretchlength_path)

# COMMAND ----------

# # save deduplicated df 1
# (
#   feature_table_deduplicated_1
#   .write
#   .format('delta')
#   .mode('overwrite')
#   .option("overwriteSchema", "True")
#   .save(feature_table_deduplicated_path)
# )

# COMMAND ----------

# # save deduplicated 2
# (
#   feature_table_deduplicated_2
#   .write
#   .format('delta')
#   .mode('overwrite')
#   .option("overwriteSchema", "True")
#   .save(feature_table_deduplicated_country_stretchlength_path)
# )

# COMMAND ----------

# MAGIC %md
# MAGIC # Start process: create feature table and training table

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists error_classification;

# COMMAND ----------

print(feature_table_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- potentially drop feature_table
# MAGIC DROP TABLE if exists error_classification.feature_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE table if not exists error_classification.feature_table using delta LOCATION 'wasbs://devcontainer@adlsmapsanalyticsmdbf.blob.core.windows.net/error_classification/training/feature_table_deduplicated.delta';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check out data

# COMMAND ----------

spark.read.format('delta').load(feature_table_deduplicated_path).describe().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM error_classification.feature_table
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT metric, provider, 
# MAGIC     count(*) as n, 
# MAGIC     sum(cast(has_error as int)) as n_errors, round(sum(cast(has_error as int))/ count(*), 3) as error_rate, 
# MAGIC     count_if(error_type = 'no source') as n_nosource,
# MAGIC     count(distinct(country)) as n_countries
# MAGIC FROM error_classification.feature_table
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from error_classification.feature_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   error_type, count(*) as n
# MAGIC FROM error_classification.feature_table
# MAGIC GROUP BY 1
# MAGIC ORDER BY n desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   provider, 
# MAGIC   count(*) as n, 
# MAGIC   sum(cast(has_error as int)) as n_errors, 
# MAGIC   round(sum(cast(has_error as int))/ count(*), 3) as error_rate, 
# MAGIC   count_if(error_type = 'WDTRF') as n_WDTRF,
# MAGIC   count_if(error_type = 'MMAN') as n_MMAN,
# MAGIC   count_if(error_type = 'MBP') as n_MBP,
# MAGIC   count_if(error_type = 'SGEO') as n_SGEO,
# MAGIC   count_if(error_type = 'WRC') as n_WRC,
# MAGIC   count_if(error_type = 'WGEO') as n_WGEO,
# MAGIC   count_if(error_type = 'MGSC') as n_MGSC,
# MAGIC   count_if(error_type = 'IMPLICIT') as n_IMPLICIT,
# MAGIC   count_if(error_type = 'no source') as n_nosource
# MAGIC FROM error_classification.feature_table
# MAGIC WHERE country = 'USA'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC # Create train table
# MAGIC We now select only relevant features and target variable and create the table "train_table", stored in local dbfs, required for AUTOML experiments

# COMMAND ----------

# key identifier(s)
id_cols = ['case_id']
# attribute columns are used to describe cases/ breackdown analysis but generally not as features 
attrib_cols = ['country', 'provider', 'error_type', "date", "metric"]

target_column = 'has_error'

selected_features = [
    # general features
    'drive_right_side',
    'is_tbt',
    # geometric features
    'route_length',
    'stretch_length',
    'stretch_start_position_in_route',
    'stretch_end_position_in_route',
    'stretch_starts_at_the_route_start',
    'stretch_ends_at_the_route_end',
    'heading_stretch',
    'left_turns',
    'straight_turns',
    'right_turns',
    'total_right_angle',
    'max_right_angle',
    'total_left_angle',
    'max_left_angle',
    'absolute_angle',
    'has_left_turns',
    'has_right_turns',
    'min_curvature',
    'max_curvature',
    'mean_curvature',
    'distance_start_end_stretch',
    'route_coverage',
    'stretch_covers_route',
    'tortuosity',
    'sinuosity',
    'density',
    # FCD FEATURES #
    'pra',
    'prb',
    'prab',
    'lift',
    'tot',
    # SDO FEATURES #
    'CONSTRUCTION_AHEAD_TT',
    'MANDATORY_STRAIGHT_ONLY',
    'MANDATORY_STRAIGHT_OR_LEFT',
    'MANDATORY_TURN_RESTRICTION',
    'MANDATORY_TURN_LEFT_ONLY',
    'MANDATORY_TURN_LEFT_OR_RIGHT',
    'MANDATORY_LEFT_OR_STRAIGHT_OR_RIGHT',
    'MANDATORY_TURN_RIGHT_ONLY',
    'MANDATORY_STRAIGHT_OR_RIGHT',
    'NO_ENTRY',
    'NO_MOTOR_VEHICLE',
    'NO_CAR_OR_BIKE',
    'NO_LEFT_OR_RIGHT_TT',
    'NO_LEFT_TURN',
    'NO_RIGHT_TURN',
    'NO_VEHICLE',
    'NO_STRAIGHT_OR_LEFT_TT',
    'NO_STRAIGHT_OR_RIGHT_TT',
    'NO_STRAIGHT_TT',
    'NO_TURN_TT',
    'NO_U_OR_LEFT_TURN',
    'NO_U_TURN',
    'ONEWAY_TRAFFIC_TO_STRAIGHT'
]

training_data_cols = selected_features + [target_column]
test_data_cols = id_cols + attrib_cols + selected_features + [target_column]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessing

# COMMAND ----------

dict_fillna = {
'CONSTRUCTION_AHEAD_TT':-1,
'MANDATORY_STRAIGHT_ONLY':-1,
'MANDATORY_STRAIGHT_OR_LEFT':-1,
'MANDATORY_TURN_RESTRICTION':-1,
'MANDATORY_TURN_LEFT_ONLY':-1,
'MANDATORY_TURN_LEFT_OR_RIGHT':-1,
'MANDATORY_LEFT_OR_STRAIGHT_OR_RIGHT':-1,
'MANDATORY_TURN_RIGHT_ONLY':-1,
'MANDATORY_STRAIGHT_OR_RIGHT':-1,
'NO_ENTRY':-1,
'NO_MOTOR_VEHICLE':-1,
'NO_CAR_OR_BIKE':-1,
'NO_LEFT_OR_RIGHT_TT':-1,
'NO_LEFT_TURN':-1,
'NO_RIGHT_TURN':-1,
'NO_VEHICLE':-1,
'NO_STRAIGHT_OR_LEFT_TT':-1,
'NO_STRAIGHT_OR_RIGHT_TT':-1,
'NO_STRAIGHT_TT':-1,
'NO_TURN_TT':-1,
'NO_U_OR_LEFT_TURN':-1,
'NO_U_TURN':-1,
'ONEWAY_TRAFFIC_TO_STRAIGHT':-1,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the Schema

# COMMAND ----------

(
spark
.read
.format('delta')
.load(feature_table_deduplicated_path)
).printSchema()

# COMMAND ----------

feature_table = (
    spark
    .read
    .format('delta')
    .load(feature_table_deduplicated_path)
    .fillna(dict_fillna)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create train and test table
# MAGIC From the feature table, containing all the cases, create a test table (sampled from the last 2 months only), and keep the rest as training table. 

# COMMAND ----------

# Separate the last two months data

# Find the date 2 months ago from the latest date in your data
three_months_ago = feature_table.select(max_(col("date"))).head()[0] - pd.DateOffset(months=3)

# Separate the last two months data
last_quarter = feature_table.filter(col("date") >= three_months_ago)

# Sample 10000 random rows from the last two months data
test_table = (
    last_quarter
    .sample(False, 5000/last_quarter.count()) 
)

# The rest of the data becomes your training set
training_table = (
    feature_table
    .subtract(test_table)
)

# COMMAND ----------

# Get the number of rows, min, and max of the date column for the train and test DataFrames
feature_table_rows = feature_table.count()

train_rows = training_table.count()
train_min_date = training_table.select(min_(col("date"))).first()[0]
train_max_date = training_table.select(max_(col("date"))).first()[0]

test_rows = test_table.count()
test_min_date = test_table.select(min_(col("date"))).first()[0]
test_max_date = test_table.select(max_(col("date"))).first()[0]

print(f"Feature DataFrame: rows={feature_table_rows}")
print(f"Train DataFrame: rows={train_rows}, min_date={train_min_date}, max_date={train_max_date}")
print(f"Test DataFrame: rows={test_rows}, min_date={test_min_date}, max_date={test_max_date}")


# COMMAND ----------

# drop columns not needed anymore
test_table = test_table.select(test_data_cols)
training_table = training_table.select(training_data_cols)

# COMMAND ----------

training_table.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Save training table to dbfs

# COMMAND ----------

training_table_path

# COMMAND ----------

(
  training_table
  .write
  .format('delta')
  .mode('overwrite')
  .option("overwriteSchema", "True")
  .save(training_table_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Also save it in dbfs, as required to run AutoML

# COMMAND ----------

(
    training_table
    .write
    .format('delta')
    .mode('overwrite')
    .option("overwriteSchema", "True")
    .save("dbfs:/error_classification/training_table.delta")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save training table to HIVE metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE if exists error_classification.training_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE table error_classification.training_table using delta LOCATION 'dbfs:/error_classification/training_table.delta';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from error_classification.training_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from error_classification.training_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select mean(has_error::int) as mean, sum(has_error::int) number_of_errors, count(has_error) total from error_classification.training_table

# COMMAND ----------

# MAGIC %md
# MAGIC # Save test table to dbfs

# COMMAND ----------

test_table_path

# COMMAND ----------

(
  test_table
  .write
  .format('delta')
  .mode('overwrite')
  .option("overwriteSchema", "True")
  .save(test_table_path)
)

# COMMAND ----------


