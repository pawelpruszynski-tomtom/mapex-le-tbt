# Databricks notebook source
# MAGIC %md
# MAGIC # Launch an Artificial Inspection
# MAGIC Documentation on the Artificial Inspection process is provided on [Confluence](https://confluence.tomtomgroup.com/display/MANA/New+EC+process)
# MAGIC
# MAGIC
# MAGIC ## Tables that are produced/updated in this notebook
# MAGIC - intermediate data
# MAGIC   - All cases of that were discarded by the EC process for the last n Orbis releases.
# MAGIC   - written to devcontainer@adlsmapsanalyticsmdbf
# MAGIC - sample data
# MAGIC   - a sample of specified size taken from the intermediate data
# MAGIC   - written to devcontainer@adlsmapsanalyticsmdbf
# MAGIC
# MAGIC
# MAGIC From tbt database:
# MAGIC - inspection_metadata
# MAGIC   - updated in tbt PostgresDB (dev or prod)
# MAGIC   - added or updated in devcontainer@adlsmapsanalyticsmdbf
# MAGIC - error_logs
# MAGIC   - updated in tbt PostgresDB (dev or prod)
# MAGIC   - added or updated in devcontainer@adlsmapsanalyticsmdbf
# MAGIC - scheduled_metrics
# MAGIC   - updated in tbt PostgresDB (dev or prod)
# MAGIC   - added or updated in devcontainer@adlsmapsanalyticsmdbf
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # User Inputs

# COMMAND ----------

# optionally restart kernel: 
dbutils.library.restartPython()

# COMMAND ----------

# specify if you want to write to "dev" or "prod" tbt database
# only select prod after testing on dev DB that everything works
ENVIRONMENT = "dev"

# Specify which versions should be considered for the artificial inspection
RELEASES = ["OV_23390.000", "OV_23370.001_no_navigability", "OV_23360.000"]
releases_str = str(RELEASES)[1:-1]

# Specify the maximum amount of tasks to be sent to MCP
max_mcp_tasks = 400

# As artificial inspections are done on a quarterly basis --> set current quarter (2023-Q3)
# If more than one inspection is done per quarter, specify running integer (2023-Q3-1, 2023-Q3-2, ...)
quarter = "2023-Q3"

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup and data import

# COMMAND ----------

import uuid
import copy
from pyspark.sql.functions import col, countDistinct, lit

# COMMAND ----------

# MAGIC %run "./00_utils"

# COMMAND ----------

# define settings for dev/prod environment
if ENVIRONMENT == "dev":
    db_key = "db_credentials_tbt_dev"
elif ENVIRONMENT == "prod":
    db_key = "db_credentials_tbt"
else:
    raise ValueError("Invalid environment specified")

# COMMAND ----------

# connect to tbt dev or prod database
tbt_db = json.loads(dbutils.secrets.get(scope="utils", key=db_key))
tbt_engine = sqlalchemy.create_engine(
        f'postgresql+psycopg2://{tbt_db["user"]}:{tbt_db["password"]}'
        f'@{tbt_db["host"]}:{tbt_db["port"]}/{tbt_db["database"]}'
    ).execution_options(autocommit=True)

print(tbt_engine)

# COMMAND ----------

# Read datalake
ics = read_tbt_datalake("inspection_critical_sections.delta")
im = read_tbt_datalake("inspection_metadata.delta")
ir = read_tbt_datalake("inspection_routes.delta")

# create temporary views to be able to query the tables with SQL
ics.createOrReplaceTempView("ics")
im.createOrReplaceTempView("im")
ir.createOrReplaceTempView("ir")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sample Generation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select all cases for the last three OV releases that were discarded by the EC model.

# COMMAND ----------

# select artificial inspection sample 
df = spark.sql(f"""
select
    ics.run_id,
    ics.case_id,
    ics.stretch,
    ir.provider_route,
    ir.competitor_route,
    im.country,
    im.provider,
    im.product,
    im.competitor,
    im.inspection_date,
    ics.route_id
from ics 
left join im 
on ics.run_id = im.run_id
left join ir
on ics.run_id = ir.run_id and ics.route_id = ir.route_id
where im.product in ('OV_23390.000', 'OV_23370.001_no_navigability', 'OV_23360.000')
and ics.pra = -1
and ics.fcd_state = 'no error'
""")

# create TempView
df.createOrReplaceTempView("df")

# COMMAND ----------

print(f"Number of cases: {df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add recency column, ranking cases with the exact same stretch by recency of inspection

# COMMAND ----------

df = spark.sql(f"""
    select 
        t.run_id,
        t.case_id,
        t.stretch,
        t.provider_route,
        t.competitor_route,
        t.country,
        t.provider,
        t.product,
        t.competitor,
        t.inspection_date,
        t.route_id,
        ROW_NUMBER() OVER (PARTITION BY t.stretch ORDER BY t.inspection_date DESC) AS recency
        from df
        as t
""")

# create TempView again
df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write intermediate data to storage
# MAGIC This is to keep track of which cases were used for the artificial inspection. It contains the recency column, to keep track of which cases were selected to go to MCP. (only cases with recency == 1, see next step)

# COMMAND ----------

intermediate_cases_path = f'{BLOB_URL}/error_classification/artificial_inspections/{quarter}/intermediate_cases_with_recency.delta'
print(intermediate_cases_path)

# COMMAND ----------

# write to intermedate storage
if ENVIRONMENT == "prod":
    (
        df.withColumn("inserted_to_DB", lit(f"{ENVIRONMENT}_{current_date}"))
        .write
        .mode("append")
        .format('delta')
        .save(intermediate_cases_path)
    )

# COMMAND ----------

spark.read.format('delta').load(intermediate_cases_path).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select most recent critical section per unique stretch only. 
# MAGIC Deduplication by taking the critical section (i.e. geometry) from the most recent inspection only. 

# COMMAND ----------

df = spark.sql(f"""
    select *
    from df
    where recency = 1
""")

# COMMAND ----------

# convert spark df to pandas df
pandas_df = df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take a sample only
# MAGIC

# COMMAND ----------

number_unique_cases = pandas_df.shape[0]
print(f"Number of cases: {number_unique_cases}")

df_sample = pandas_df.sample(n = max_mcp_tasks, axis = 0, ignore_index = True)
sample_size = df_sample.shape[0]

print(f"Number of cases in df sample: {sample_size}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity checks

# COMMAND ----------

# Check for no duplicated stretches
if sample_size != df_sample["stretch"].nunique():
    raise ValueError('Data has duplicate stretches')

# Check and drop column recency and inspection_date
if df_sample["recency"].unique()[0] == 1:
    df_sample.drop(columns = ["recency", "inspection_date"], inplace = True)
else:
    raise ValueError('Column Recency has values other than 1')

# COMMAND ----------

# check sample manually as well
display(df_sample)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write sample to intermediate storage

# COMMAND ----------

sample_path = f'{BLOB_URL}/error_classification/artificial_inspections/{quarter}/sample_sent_to_mcp.delta'
print(sample_path)

# COMMAND ----------

# write to intermedate storage
if ENVIRONMENT == "prod":
    # add row also to datalake table to keep track what was inserted when
    df_sample_copy = df_sample.copy()
    df_sample_copy["inserted_to_DB"] = f"{ENVIRONMENT}_{current_date}"

    (
        spark.createDataFrame(df_sample_copy)
        .write
        .mode("append")
        .format('delta')
        .option("mergeSchema", "true")
        .save(sample_path)
    )

# COMMAND ----------

spark.read.format('delta').load(sample_path).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Export to DB

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table inspection_metadata
# MAGIC Insert new row for the artificial inspection in table inspection_metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define run_id and row data

# COMMAND ----------

# create run_id
if 'run_id' not in locals() and 'run_id' not in globals():
    run_id = str(uuid.uuid4())
    print(run_id)
else:
    print("run_id already exists")

# fill other cols
im_data = {
    "run_id": run_id,
    "sample_id": None,
    "reference_run_id": None,
    "provider": "TT",
    "endpoint": None,
    "mapdate": None,
    "product": None,
    "country": None,
    "mode": None,
    "competitor": None,
    "mcp_tasks": sample_size,
    "rac_elapsed_time": None,
    "fcd_elapsed_time": None,
    "total_elapsed_time": None,
    "api_calls": None,
    "mcp_start_date": None,
    "mcp_end_date": None,
    "mcp_elapsed_time": None,
    "comment": "Artificial Inspection",
    "completed": False,
    "inspection_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "mcp_review": True,
    "sanity_fail": None,
    "sanity_msg": None
}

# Create a DataFrame with specified data types
im_df = pd.DataFrame([im_data])
im_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check data types and column names

# COMMAND ----------

# read original inspection_metadata table from database
im_check = pd.read_sql_query(f"""
    select * from inspection_metadata
    limit 1
    ;
""", con=tbt_engine.raw_connection())

# cast the datatypes to the new table
dtype_mapping = im_check.dtypes.to_dict()
im_df = im_df.astype(dtype_mapping)

# check if the two dfs are equal
check_dataframe_equality(im_df, im_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to DB and datalake

# COMMAND ----------

im_schema = T.StructType(
    [
        T.StructField("run_id", T.StringType()),
        T.StructField("sample_id", T.StringType()),
        T.StructField("reference_run_id", T.StringType()),
        T.StructField("provider", T.StringType()),
        T.StructField("endpoint", T.StringType()),
        T.StructField("mapdate", T.DateType()),
        T.StructField("product", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("mode", T.StringType()),
        T.StructField("competitor", T.StringType()),
        T.StructField("mcp_tasks", T.StringType()),
        T.StructField("rac_elapsed_time", T.FloatType()),
        T.StructField("fcd_elapsed_time", T.FloatType()),
        T.StructField("total_elapsed_time", T.FloatType()),
        T.StructField("api_calls", T.StringType()), 
        T.StructField("mcp_start_date", T.DateType()),
        T.StructField("mcp_end_date", T.DateType()),
        T.StructField("mcp_elapsed_time", T.FloatType()),
        T.StructField("comment", T.StringType()),
        T.StructField("completed", T.StringType()),
        T.StructField("inspection_date", T.DateType()),
        T.StructField("mcp_review", T.BooleanType()),
        T.StructField("sanity_fail", T.BooleanType()),
        T.StructField("sanity_msg", T.StringType()),
        T.StructField("inserted_to_DB", T.StringType()),
    ]
)

# COMMAND ----------

# path for inspection_metadata table in datalake 
im_df_path = f'{BLOB_URL}/error_classification/artificial_inspections/inspection_metadata.delta'
print(im_df_path)

# add row also to datalake table to keep track what was inserted when
im_df_copy = im_df.copy()
im_df_copy["inserted_to_DB"] = f"{ENVIRONMENT}_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"
im_df_copy

# COMMAND ----------

# check db connection again (check if writing to dev (dev) or prod (tbt)
print(tbt_engine)

# COMMAND ----------

# Insertion
im_df.to_sql("inspection_metadata", con=tbt_engine, if_exists='append', index=False, schema="public")

# write to datalake
(
    spark.createDataFrame(im_df_copy, schema = im_schema)
    .write
    .mode("append")
    .format('delta')
    .save(im_df_path)
)

# COMMAND ----------

# check if it was correctly inserted
check_df = pd.read_sql_query(f"""
    select * from inspection_metadata
    where run_id = '{run_id}'
    ;
""", con=tbt_engine.raw_connection())

check_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table error_logs
# MAGIC Add sample to error_logs table in database

# COMMAND ----------

# replace original run_id with newly created run_id
df_sample["run_id"] = run_id

# add columns error_type, evaluated_by, review_date, comment
df_sample.insert(5, "error_type", None, True)
df_sample.insert(6, "evaluated_by", None, True)
df_sample.insert(7, "review_date", None, True)
df_sample.insert(8, "comment", None, True)
df_sample.insert(14, "error_subtype", None, True)

df_sample.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check data types and column names

# COMMAND ----------

error_logs_df = pd.read_sql_query(f"""
    select 
    run_id::uuid, 
    case_id::uuid,
    st_astext(stretch) as stretch,
    st_astext(provider_route) as provider_route,
    st_astext(competitor_route) as competitor_route,
    error_type, 
    evaluated_by, 
    review_date::timestamp, 
    comment, 
    country, 
    provider, 
    product, 
    competitor,
    route_id::uuid, 
    error_subtype
    from error_logs
    limit 1
    ;
""", con=tbt_engine.raw_connection())

# cast the datatypes to the new table
dtype_mapping = error_logs_df.dtypes.to_dict()
df_sample = df_sample.astype(dtype_mapping)

check_dataframe_equality(df_sample, error_logs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to DB and datalake

# COMMAND ----------

el_schema=T.StructType(
    [
        T.StructField("run_id", T.StringType()),
        T.StructField("case_id", T.StringType()),
        T.StructField("stretch", T.StringType()),
        T.StructField("provider_route", T.StringType()),
        T.StructField("competitor_route", T.StringType()),
        T.StructField("error_type", T.StringType()),
        T.StructField("evaluated_by", T.StringType()),
        T.StructField("review_date", T.DateType()),
        T.StructField("comment", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("provider", T.StringType()),
        T.StructField("product", T.StringType()),
        T.StructField("competitor", T.StringType()),
        T.StructField("error_subtype", T.StringType()),
        T.StructField("inserted_to_DB", T.StringType()),
    ]
)

# COMMAND ----------

# path for inspection_metadata table in datalake 
el_df_path = f'{BLOB_URL}/error_classification/artificial_inspections/error_logs.delta'
print(el_df_path)

# add row also to datalake table to keep track what was inserted when
df_sample_copy = df_sample.copy()
df_sample_copy["inserted_to_DB"] = f"{ENVIRONMENT}_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"
df_sample_copy.head()

# COMMAND ----------

# check db connection again (check if writing to dev (dev) or prod (tbt)
print(tbt_engine)

# COMMAND ----------

# Insertion to DB
df_sample.to_sql(
    name = "error_logs", 
    con = tbt_engine, 
    if_exists='append', 
    index=False, 
    schema="public"
    )

# write to datalake
(
    spark.createDataFrame(df_sample_copy, schema=el_schema)
    .write
    .mode("append")
    .format('delta')
    .option("mergeSchema", "true")
    .save(el_df_path)
)

# COMMAND ----------

# check if it was correctly inserted
check_df = pd.read_sql_query(f"""
    select 
    run_id::uuid, case_id::uuid,
    st_astext(stretch) as stretch,
    st_astext(provider_route) as provider_route,
    st_astext(competitor_route) as competitor_route,
    error_type, evaluated_by, 
    review_date::timestamp, 
    comment, country, provider, product, competitor,
    route_id::uuid, 
    error_subtype
    from error_logs
    where run_id = '{run_id}'
    ;
""", con=tbt_engine.raw_connection())

check_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table scheduled_metrics
# MAGIC Add row to scheduled_metrics table to avoid it being added after MCP is finished

# COMMAND ----------

nr_routes = df_sample["provider_route"].nunique()

sm_data = {
    "run_id": run_id,
    "routes": nr_routes,
    "errors": None,
    "provider_km": None,
    "provider_hours": None,
    "eph": None,
    "metrics_per_error_type": None,
    "metrics_per_mqs": None,
    "publish_value_stream": False,
    "publish_harold": False,
    "failed_routes": None,
    "competitor_hours": None,
    "competitor_km": None,
    "eph_lower": None,
    "eph_upper": None,
    "rac_state": None,
    "failed_routes_lower": None,
    "failed_routes_upper": None,
    "invalid_routes": None,
    "invalid_routes_info": None
}

# Create a DataFrame with specified data types
sm_df = pd.DataFrame([sm_data])
sm_df

# COMMAND ----------

sm_check_df = pd.read_sql_query(f"""
    select * from scheduled_metrics
    limit 1
    ;
""", con=tbt_engine.raw_connection())

# cast the datatypes to the new table
# dtype_mapping = sm_check_df.dtypes.to_dict()
# sm_df = sm_df.astype(dtype_mapping)

check_dataframe_equality(sm_df, sm_check_df)

# COMMAND ----------

sm_schema = T.StructType(
    [
        T.StructField("run_id", T.StringType()),
        T.StructField("routes", T.IntegerType()),
        T.StructField("errors", T.IntegerType()),
        T.StructField("provider_km", T.FloatType()),
        T.StructField("provider_hours", T.FloatType()),
        T.StructField("eph", T.FloatType()),
        T.StructField("metrics_per_error_type", T.StringType()),
        T.StructField("metrics_per_mqs", T.StringType()),
        T.StructField("publish_value_stream", T.BooleanType()),
        T.StructField("publish_harold", T.BooleanType()),
        T.StructField("failed_routes", T.FloatType()),
        T.StructField("competitor_hours", T.FloatType()),
        T.StructField("competitor_km", T.FloatType()),
        T.StructField("eph_lower", T.FloatType()),
        T.StructField("eph_upper", T.FloatType()),
        T.StructField("rac_state", T.StringType()),
        T.StructField("failed_routes_lower", T.FloatType()),
        T.StructField("failed_routes_upper", T.FloatType()),
        T.StructField("invalid_routes", T.BooleanType()),
        T.StructField("invalid_routes_info", T.StringType()),
        T.StructField("inserted_to_DB", T.StringType()),
    ]
)

# COMMAND ----------

# path for inspection_metadata table in datalake 
sm_df_path = f'{BLOB_URL}/error_classification/artificial_inspections/scheduled_metrics.delta'
print(sm_df_path)

# add row also to datalake table to keep track what was inserted when
sm_df_copy = sm_df.copy()
sm_df_copy["inserted_to_DB"] = f"{ENVIRONMENT}_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"
sm_df_copy

# COMMAND ----------

# check db connection again (check if writing to dev (dev) or prod (tbt)
print(tbt_engine)

# COMMAND ----------

# Insertion
sm_df.to_sql(name = "scheduled_metrics", 
con = tbt_engine, 
if_exists='append', 
index=False, 
schema="public")

# write to datalake
(
    spark.createDataFrame(sm_df_copy, schema=sm_schema)
    .write
    .mode("append")
    .format('delta')
    .option("mergeSchema", "true")
    .save(sm_df_path)
)

# COMMAND ----------

# check if it was correctly inserted
check_df = pd.read_sql_query(f"""
    select * from scheduled_metrics
    where run_id = '{run_id}'
    ;
""", con=tbt_engine.raw_connection())

check_df

# COMMAND ----------

run_id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Follow-up steps
# MAGIC - Once done, please contact @Wendy Els & MCP and inform them about the artificial inspection, and specify how to prioritize this artificial inspection. 
# MAGIC - Also, make sure to transfer the annotated data from table error_logs once MCP is done. 
