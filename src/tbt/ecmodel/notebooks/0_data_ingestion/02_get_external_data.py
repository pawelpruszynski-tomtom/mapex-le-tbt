# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook to get external data
# MAGIC Based on the case_ids stored in the intermediate internal table, retrieve FCD and SDO data. 
# MAGIC
# MAGIC Check first if they are already included in the external_data_table. 
# MAGIC
# MAGIC For FCD: 
# MAGIC   - Optional to include: check if data is available in database
# MAGIC   - if not, call FCD API
# MAGIC
# MAGIC For SDO: 
# MAGIC   - Optional to include: check if data is available in datalake table
# MAGIC   - if not, call SDO API
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Parameters to run the notebook
# MAGIC - Debug: if True it won't save the results
# MAGIC - Check only new cases: if True, it will only call the api for the case_ids that have no sdo data yet

# COMMAND ----------

DEBUG = False
REVIEW_STATUS = True

# SDO Parameters
TIMEWINDOW=4 # around the error date (in weeks)

# COMMAND ----------

# MAGIC %md
# MAGIC # Import general libraries, functions and variables

# COMMAND ----------

# MAGIC %run "../utils"

# COMMAND ----------

# MAGIC %run "../define_table_schema"

# COMMAND ----------

# MAGIC %run "../call_FCD_API"

# COMMAND ----------

# MAGIC %run "../call_SDO_API"

# COMMAND ----------

# load credentials for FCD API
fcd_credentials = json.loads(dbutils.secrets.get(scope = "mdbf3", key = "fcd_credentials"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Import notebook-specific libraries

# COMMAND ----------

import shapely.wkt
import shapely.geometry

import requests
import time
from tqdm import tqdm
import logging

from datetime import datetime, timedelta

# COMMAND ----------

# define log for FCD function
log = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data

# COMMAND ----------

# internal data
internal_data = spark.read.format('parquet').load(internal_data_path)

internal_data_count = internal_data.count()
print(f"Internal data containes {internal_data_count} rows")

# try to load external data
try:
    external_data = spark.read.format('delta').load(external_data_path)
    print(f'External data loaded successfully. It contains {external_data.count()} rows')
    EXTERNAL_DATA_EXISTS = True
except:
    print(f'The file external_data does not exist.')
    EXTERNAL_DATA_EXISTS = False

# COMMAND ----------

# MAGIC %md
# MAGIC # Find case_ids for which external data is missing and must be retrieved

# COMMAND ----------

if EXTERNAL_DATA_EXISTS:
    missing_case_ids = (
        internal_data
        .join(external_data, on='case_id', how='leftanti')
    )
else:
    missing_case_ids = internal_data

# COMMAND ----------

missing_case_ids.persist()
missing_case_ids.display()
print(missing_case_ids.count())
missing_case_ids.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC # Get FCD data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check if FCD data is already available in database
# MAGIC Retrieve available data from BEFORE the model was put into production

# COMMAND ----------

ics = read_tbt_datalake("inspection_critical_sections.delta")

fcd_available_in_db = (
    missing_case_ids.select("route_id", "case_id", "stretch_length", "country", "provider")
    .join(
        ics.select("route_id", "case_id", "pra", "prb", "prab", "lift", "tot")
        .filter(ics.prab != -1),
        on=["case_id", "route_id"],
        how="inner")
)

# COMMAND ----------

fcd_available_in_db.persist()
fcd_available_in_db.display()
print(fcd_available_in_db.count())
fcd_available_in_db.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if FCD is available from datalake AFTER model was put into production
# MAGIC

# COMMAND ----------

cswmd = spark.read.format('delta').load("dbfs:/mnt/tbt/delta-tables/critical_sections_with_model_predictions.delta")

# COMMAND ----------

fcd_available_in_cswmd = (
    missing_case_ids.select("route_id", "case_id", "stretch_length", "country", "provider")
    .join(
        cswmd.select("route_id", "case_id", "pra", "prb", "prab", "lift", "tot")
        .filter(cswmd.prab != -1),
        on=["case_id", "route_id"],
        how="inner")
)

# COMMAND ----------

fcd_available_in_cswmd.persist()
fcd_available_in_cswmd.display()
print(fcd_available_in_cswmd.count())
fcd_available_in_cswmd.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine all available FCD features

# COMMAND ----------

fcd_available = (
    fcd_available_in_db
    .union(fcd_available_in_cswmd)
    .dropDuplicates()
)

# COMMAND ----------

fcd_available.persist()
fcd_available.display()
print(fcd_available.count())
fcd_available.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIONAL: Get the remaining case_ids not available in database, in order to retrieve them via FCD API

# COMMAND ----------

# # get case_ids that are in missing case_ids, but not available in the database
# missing_case_ids_after_fcd_check = (
#     F.broadcast(missing_case_ids)
#     .join(fcd_available_in_db, on=["case_id", "route_id"], how='leftanti')
# )

# # check how many rows need to be retrieved
# missing_case_ids_after_fcd_check_count = missing_case_ids_after_fcd_check.count()
# print(f"For {internal_data_count - missing_case_ids_after_fcd_check_count} case_ids FCD data are available in the database")
# print(f"For {missing_case_ids_after_fcd_check_count} case_ids FCD data is missing and must be retrieved")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieve remaining cases from FCD API

# COMMAND ----------

# # call FCD API 
# fcd_data_from_API = evaluate_with_RFC(missing_case_ids_after_fcd_check, fcd_credentials)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine FCD data from DB and from API call

# COMMAND ----------

# fcd_data_from_API_spark = spark.createDataFrame(fcd_data_from_API)
# FCD_combined = fcd_data_from_API_spark.union(fcd_available_in_db)

# # check if for all case_ids that missed FCD data, we now added FCD data
# FCD_check = missing_case_ids.join(FCD_combined, on=["case_id", "route_id"], how='leftanti')
# FCD_check_count = FCD_check.count()

# if FCD_check_count == 0:
#     print("All case_ids in combined_df match with the original dataframe.")
# else:
#     raise Exception(f"There are {FCD_check_count} case_ids in missing_case_ids for which no FCD data was retrieved.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save combined FCD data

# COMMAND ----------

FCD_combined = fcd_available

# COMMAND ----------

# Apply the schema to the DataFrame
for field in FCD_intermediate_data_schema:
    column_name = field.name
    FCD_combined = FCD_combined.withColumn(column_name, F.col(column_name).cast(field.dataType))

print(FCD_intermediate_data_path)

# COMMAND ----------

FCD_intermediate_data_path

# COMMAND ----------

# Save the data
(
    FCD_combined
    .repartition("country", "provider")
    .write
    .mode("append")
    .format('delta')
    # .option("overwriteSchema", "true")
    .partitionBy(["country", "provider"])
    .save(FCD_intermediate_data_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # SDO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for which case_ids SDO data is already available in the database, and which are missing

# COMMAND ----------

available_SDO_data = spark.read.format('delta').load(f'{BLOB_URL}/tbt_errors/1_raw_data_ext_SDO.delta')

# COMMAND ----------

# check if it is empty responses only
available_SDO_data.persist()
available_SDO_data.display()
print(available_SDO_data.count())
available_SDO_data.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add new cases immediately

# COMMAND ----------

# append new case_ids to intermediate SDO data immediately
for field in SDO_intermediate_data_schema:
    column_name = field.name
    available_SDO_data = available_SDO_data.withColumn(column_name, F.col(column_name).cast(field.dataType))

# COMMAND ----------

(
    available_SDO_data
    .write
    .mode("append")
    .format('delta')
    .partitionBy(["country", "provider"])
    .save(SDO_intermediate_data_path)
)

# COMMAND ----------

# get case_ids that are are still missing, to retrieve them in next step from API
missing_case_ids_after_sdo_check = (
    F.broadcast(missing_case_ids)
    .join(available_SDO_data, on='case_id', how='leftanti')
)

# COMMAND ----------

# extract case_ids to list
sdo_missing_case_ids_list = [row["case_id"] for row in missing_case_ids_after_sdo_check.collect()]

# COMMAND ----------

print(f"For {internal_data_count - len(sdo_missing_case_ids_list)} case_ids SDO data are available in the datalake")
print(f"For {len(sdo_missing_case_ids_list)} cases SDO data is missing and must be retrieved")

# COMMAND ----------

SDO_intermediate_data_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call SDO API to retrieve SDO data

# COMMAND ----------

# API calls planning
batch_size = 500
batch_plan = [sdo_missing_case_ids_list[i * batch_size:(i + 1) * batch_size] for i in range((len(sdo_missing_case_ids_list) + batch_size - 1) // batch_size)]

for batch_iteration, batch_ids in enumerate(tqdm(batch_plan)):
    if batch_iteration > 0:
        time.sleep(20)
        
    # Get the batch's data and extract SDO features
    batch = missing_case_ids.filter(F.col('case_id').isin(batch_ids)).rdd.map(wrapper_sdo).toDF(schema=SDO_intermediate_data_schema)

    # every 20 batches, monitor if API still returns correct output
    if batch_iteration % 5 == 0 or batch_iteration == 0 or batch_iteration == 1:
        batch.limit(10).show(truncate=150)

    # Save the data
    (
        batch
        .write
        .mode("append")
        .format('delta')
        .partitionBy(["country", "provider"])
        .save(SDO_intermediate_data_path)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check SDO responses

# COMMAND ----------

SDO_intermediate_data_path

# COMMAND ----------

sdo = spark.read.format('delta').load(SDO_intermediate_data_path)
sdo.persist()

print(f"Intermediate SDO data has {sdo.count()} rows")
distinct_responses = sdo.select("sdo_response").distinct()
distinct_responses.display()
print(f"There are {distinct_responses.count()} distinct API responses")
print(f"There are {sdo.dropDuplicates(['case_id']).count()} distinct case_ids")

sdo.unpersist()
