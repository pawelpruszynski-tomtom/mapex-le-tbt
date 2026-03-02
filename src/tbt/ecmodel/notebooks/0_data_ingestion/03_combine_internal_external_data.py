# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook to combine internal and external data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Parameters to run the notebook
# MAGIC - Review_status: if intermediate data should be evaluated or not

# COMMAND ----------

REVIEW_STATUS = True

# COMMAND ----------

# MAGIC %md
# MAGIC # Import general libraries, functions and variables

# COMMAND ----------

# MAGIC %run "../utils"

# COMMAND ----------

# MAGIC %run "../define_table_schema"

# COMMAND ----------

# MAGIC %md
# MAGIC # Import notebook-specific libraries

# COMMAND ----------

from pyspark.sql.functions import col, isnan, when, count, min, max, current_date, isnull

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data

# COMMAND ----------

internal_data = spark.read.format('parquet').load(internal_data_path)

FCD_data = spark.read.format('delta').load(FCD_intermediate_data_path)
SDO_data = spark.read.format('delta').load(SDO_intermediate_data_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Combine Data

# COMMAND ----------

intermediate_data = (
    internal_data
    .join(
          FCD_data
          .select("route_id", "case_id", "pra", "prb", "prab", "lift", "tot"),
          on=['case_id', "route_id"], 
          how='left'
          )
    .join(
          SDO_data
          .select("case_id", "sdo_response", "sdo_data_start_date", "sdo_data_end_date", "sdo_api_call_date"),
          on='case_id',
          how='left')
)


# COMMAND ----------

intermediate_data = intermediate_data.dropDuplicates(["case_id"])

# COMMAND ----------

# drop route_id as it's not needed any more
intermediate_data = intermediate_data.drop("route_id")

# drop stretch_length as it will be computed in the featurization step
intermediate_data = intermediate_data.drop("stretch_length")

# COMMAND ----------

# drop rows containing NAs
intermediate_data = intermediate_data.dropna()

# COMMAND ----------

if REVIEW_STATUS: 
    intermediate_data.persist()

    intermediate_data.display()

    # check how many rows
    print(f"Intermediate data contains {intermediate_data.count()} rows")

    # check for NaNs
    print("NaN values per column: ")
    (
        intermediate_data
        .select([count(when(col(c).isNull(), c)).alias(c) for c in intermediate_data.columns])
        .show()
    )

    # value counts per column
    # intermediate_data.groupBy("provider").count().show()

    # intermediate_data.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC # Export Data

# COMMAND ----------

# Apply the schema to the DataFrame
for field in intermediate_data_schema:
    column_name = field.name
    intermediate_data = intermediate_data.withColumn(column_name, col(column_name).cast(field.dataType))

# COMMAND ----------

intermediate_data_path

# COMMAND ----------

# Save the data
(
    intermediate_data
    .repartition("metric", "error_label", "country")
    .write
    .mode("overwrite")
    .format('delta')
    .partitionBy(["metric", "error_label", "country"])
    .save(intermediate_data_path)
)

# COMMAND ----------

# # store intermediate data
# append_new_data_only(intermediate_data, intermediate_data_path)
