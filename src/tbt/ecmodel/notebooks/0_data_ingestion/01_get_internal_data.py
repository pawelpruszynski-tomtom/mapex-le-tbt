# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook to get internal data
# MAGIC In this notebook, internal data needed to compute training features for model training are retrieved. 

# COMMAND ----------

# MAGIC %md
# MAGIC # User Parameters
# MAGIC - Debug: if True, results won't be saved
# MAGIC - Check only new cases: if True, feature table will only be updated with new cases not already in feature table

# COMMAND ----------

DEBUG = False 
CHECK_ONLY_NEW_CASES = False 
REVIEW_STATUS = True # if data should be evaluated before and after writing to datalake 
SAVE_MODE = "overwrite" # append or overwrite

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

# add imports if needed
from pyspark.sql.functions import col, isnan, when, count, min, max, current_date, isnull

# COMMAND ----------

# MAGIC %md
# MAGIC # Process start
# MAGIC - Retrieve raw data from tbt datalake
# MAGIC - Adjust the query based on conditions set in the flags at the start of the notebook, and in the settings notebook
# MAGIC - Join tables 
# MAGIC - Evaluate the intermediate internal data 
# MAGIC - Write to intermediate data lake storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve data from datalake

# COMMAND ----------

selh = read_tbt_datalake("scheduled_error_logs_history.delta")
ics = read_tbt_datalake("inspection_critical_sections.delta")
im = read_tbt_datalake("inspection_metadata.delta")
sm = read_tbt_datalake("sampling_metadata.delta")
ir = read_tbt_datalake("inspection_routes.delta")


# check out number of rows per table
print(selh.count())
print(ics.count())
print(im.count())
print(sm.count())
print(ir.count())


# create temporary views to be able to query the tables with SQL
selh.createOrReplaceTempView("selh")
ics.createOrReplaceTempView("ics")
im.createOrReplaceTempView("im")
sm.createOrReplaceTempView("sm")
ir.createOrReplaceTempView("ir")

# COMMAND ----------

if CHECK_ONLY_NEW_CASES:
    existing_internal_data = spark.read.format('delta').load(internal_data_path)
    existing_case_ids = str(existing_internal_data.rdd.map(lambda x: x['case_id']).collect())[1:-1]
    query_cond = f"and selh.case_id not in ({existing_case_ids})"
else:
    query_cond = ""

print(query_cond)

# COMMAND ----------

# check if / adjust this query to get the correct data 
internal_data = spark.sql(f"""
select 
    selh.run_id,
    selh.route_id,
    selh.case_id,
    selh.review_date as date,
    selh.country, 
    selh.provider,
    selh.stretch as stretch, 
    ics.stretch_length,
    selh.provider_route as route,
    case
        when sm.metric in ('HDR_v2', 'HDR')
            then 'HDR'
        else 'TbT'
        end metric,
    case
        when selh.error_type in ('discard', null)
            then 'no_error'
        else
            'error'
        end error_label,
    case
        when selh.error_type is null
            then 'discard'
        else
            selh.error_type
        end error_type
from selh
left join ics
on selh.case_id = ics.case_id 
left join im 
on selh.run_id = im.run_id 
left join sm
on im.sample_id = sm.sample_id
-- do not include cases without error_type
where selh.error_type is not null
-- do not include no source as error type
and selh.error_type != "no source"
-- potentially do not include cases that are already included in the internal 
{query_cond}
""").dropDuplicates(["case_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop rows with null values

# COMMAND ----------

# Drop rows with null values in the columns
internal_data = internal_data.na.drop(subset=["date", "country", "provider", "stretch_length"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add the route to the cases where it is missing
# MAGIC For where it is missing, join the route from the table inspection_routes based on the run_id and route_id keys. 

# COMMAND ----------

# store original columns
original_columns = internal_data.columns

# add an additional column with all the routes
internal_data = (
    internal_data
    .join(ir.select("run_id", "route_id", "provider_route"),
          on = ["run_id", "route_id"],
          how = "left")
    # Fill the 'route' column with 'provider_route' where 'route' is null
    .withColumn('route', 
                 when(isnull(col('route')), col('provider_route')).otherwise(col('route'))
                 )
    # Select the original columns again
    .select(original_columns)
    # delete the run_id column as it's not needed any mor
    .drop("run_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Inspection and Checks

# COMMAND ----------

if REVIEW_STATUS: 

    internal_data.persist()

    internal_data.display()

    # check how many rows
    print(f"Internal data contains {internal_data.count()} rows")

    # check for NaNs
    print("NaN values per column: ")
    (
        internal_data
        .select([count(when(col(c).isNull(), c)).alias(c) for c in internal_data.columns])
        .show()
    )

    # value counts per column
    internal_data.groupBy("metric").count().show()
    internal_data.groupBy("error_label").count().show()
    internal_data.groupBy("error_type").count().show()
    internal_data.groupBy("provider").count().show()

    # start and end date
    min_date = internal_data.agg(min("date")).first()[0]
    max_date = internal_data.agg(max("date")).first()[0]

    print(f"Minimum date: {min_date}")
    print(f"Maximum date: {max_date}")

    rows_with_future_date_count = internal_data.filter(internal_data["date"] > current_date()).count()    
    print(f"There are {rows_with_future_date_count} rows that have an inspection date that lies in the future. ")

    internal_data.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save internal data in intermediate data lake storage

# COMMAND ----------

internal_data.printSchema()

# COMMAND ----------

# Apply the schema to the DataFrame
for field in internal_data_schema:
    column_name = field.name
    internal_data = internal_data.withColumn(column_name, col(column_name).cast(field.dataType))

# COMMAND ----------

# check file paths for saving the raw data
print(internal_data_path)

# COMMAND ----------

# Save the data
(
    internal_data
    .repartition("metric", "error_label", "country")
    .write
    .mode("overwrite")
    .format('parquet')
    .option("overwriteSchema", "true")
    .partitionBy(["metric", "error_label", "country"])
    .save(internal_data_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check currently stored data

# COMMAND ----------

if REVIEW_STATUS:
    check1 = spark.read.format('parquet').load(internal_data_path)
    print("Total evaluated cases stored:", check1.count())
    check1.limit(5).display()
