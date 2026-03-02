# Databricks notebook source
# MAGIC %md
# MAGIC # Create new FCD features with other features to create a full test dataset. 
# MAGIC
# MAGIC **Steps**:
# MAGIC
# MAGIC - Get sample from current training datatable
# MAGIC   - Should be rather most recent cases from good quality countries
# MAGIC - Featurize it with new feature FCD features
# MAGIC   - Should take about 12h to featurize: about 5000 cases. 
# MAGIC - Combine new FCD features with the sample
# MAGIC - save new dataset
# MAGIC
# MAGIC Locations of the saved files are available from a table on this Confluence Page: https://confluence.tomtomgroup.com/display/MANA/Docs+-+EC+Model+Training 

# COMMAND ----------

# MAGIC %pip install --extra-index-url https://svc-ar-maps-analytics-editor:<secret>@artifactory.tomtomgroup.com/artifactory/api/pypi/maps-fcd-pypi-release/simple fcd_py==5.0.615
# MAGIC %pip install shapely geopandas folium matplotlib mapclassify sqlalchemy

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

# COMMAND ----------

credentials = json.loads(dbutils.secrets.get(scope = "mdbf3", key = "fcd_credentials"))

# COMMAND ----------

# sys.path.append("/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/notebooks/")
%run "/Workspace/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/notebooks/define_table_schema"
%run "/Workspace/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/error_classification/call_FCD_API_new.py"


# COMMAND ----------

# MAGIC %run "/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/notebooks/utils"

# COMMAND ----------

# how many stretches we want
limit = 5000

selh = read_tbt_datalake("scheduled_error_logs_history.delta")
selh.createOrReplaceTempView("selh")
sm = read_tbt_datalake("sampling_metadata.delta")
sm.createOrReplaceTempView("sm")
im = read_tbt_datalake("inspection_metadata.delta")
im.createOrReplaceTempView("im")

df = spark.sql(f"""
select 
    selh.case_id,
    selh.country, 
    selh.stretch as stretch, 
    selh.review_date as review_date,
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
        end error_type,
    case
        when sm.metric in ('HDR_v2', 'HDR')
            then 'HDR'
        else 'TbT'
        end metric
from selh
left join im 
on selh.run_id = im.run_id 
left join sm
on im.sample_id = sm.sample_id
-- for now, get only from countries where we have good FCD data (TOP 30)
where selh.country in ('USA', 'FRA', 'ITA', 'MLT', 'GBR', 'DEU', 'ESP', 'CAN', 'BEL', 'LUX', 'NLD', 'POL', 'AUT', 'PRT', 'SWE', 'DNK', 'HUN', 'CZE', 'GRC', 'ROU', 'SVK', 'IRL', 'SVN', 'FIN', 'BGR', 'LTU', 'EST', 'HRV', 'LVA', 'CYP')

""")

# count rows
rows = df.count()

sample_fraction = limit / rows

df = df.sample(sample_fraction, seed = 1234345).toPandas()

print(f"Rows: {df.shape[0]}")
df.head()

# COMMAND ----------

df.country.hist()

# COMMAND ----------

df.error_label.value_counts(normalize=True)

# COMMAND ----------

fcd_featurizer = FCDFeaturizer(
  df, 
  trace_retrieval_geometry = "bbox_json",
  traces_limit = 2000,
  fcd_credentials=credentials, 
  spark_context=spark
  )

# COMMAND ----------

fcd_features = fcd_featurizer.featurize(show_progress=True)

# COMMAND ----------

df = pd.concat([df, fcd_features], axis = 1)


# COMMAND ----------

# # create training table with old fcd features
# (
#     spark.createDataFrame(df)
#     .write
#     .mode("append")
#     .format('delta')
#     .option("overwriteSchema", "true")
#     .save("dbfs:/error_classification/OMA-1043/test_table_new_fcd_features.delta")
# )

# COMMAND ----------

print("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Featurized table

# COMMAND ----------

new_fcd_features = spark.read.format("delta").load("dbfs:/error_classification/OMA-1043/test_table_new_fcd_features.delta")
new_fcd_features = new_fcd_features.toPandas()

# COMMAND ----------

new_fcd_features["metric"].value_counts(normalize=True)

# COMMAND ----------

# Histograms for all features
new_fcd_features.hist(bins=20, figsize=(15, 10))
plt.show()

# COMMAND ----------

new_fcd_features

# COMMAND ----------

sns.lmplot(x="pra_to_b_contained", y="pra_to_b", hue="error_label", data=new_fcd_features)

# COMMAND ----------

new_fcd_features["error_label"].value_counts(normalize=True)

# COMMAND ----------

new_fcd_features[new_fcd_features["pra_to_b_contained"] > 0.5]["error_label"].count()

# COMMAND ----------

new_fcd_features[new_fcd_features["pra_to_b_contained"] > 0.2]["error_label"].value_counts(normalize = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine with feature table 

# COMMAND ----------

new_fcd_features = spark.read.format("delta").load("dbfs:/error_classification/OMA-1043/test_table_new_fcd_features.delta")

# COMMAND ----------

# requires connection set to adlsmapsanalyticsmdbf/devcontainer
storage_account_name = 'adlsmapsanalyticsmdbf'
storage_account_access_key = dbutils.secrets.get(scope = "mdbf3", key = "storage_account_access_key")
blob_container_name = 'devcontainer'
BLOB_URL = f'wasbs://{blob_container_name}@{storage_account_name}.blob.core.windows.net'
spark.conf.set(f'fs.azure.account.key.{storage_account_name}.blob.core.windows.net', storage_account_access_key)

feature_table = spark.read.table("hive_metastore.error_classification.feature_table")

# COMMAND ----------

# plot date histogram

# Convert the Spark DataFrame to a Pandas DataFrame
df = feature_table[["date"]].toPandas()

# Ensure that the 'date' column is in datetime format
df['date'] = pd.to_datetime(df['date'])
df['date'].hist(bins=20, figsize=(15, 10))
plt.show()

# COMMAND ----------

df_new = (
    new_fcd_features
    .join(feature_table.drop("pra", "prb", "prab", "tot", "lift", "error_type", "error_label"), on="case_id", how="left")
).toPandas()

# COMMAND ----------

df_new.head(2)

# COMMAND ----------

# filter out na columns
df_new = df_new[~df_new.isna().any(axis=1)]
case_id_list = list(df_new["case_id"])
df_old = feature_table.filter(feature_table["case_id"].isin(case_id_list)).toPandas()

# COMMAND ----------

print(df_new.shape[0])
print(df_old.shape[0])

# COMMAND ----------

df_new.head(3)

# COMMAND ----------

df_old.head(3)

# COMMAND ----------

training_table_old = df_old.drop(["case_id", "date", "provider", "metric", "country", "error_type", "error_label"], axis=1)
training_table_old.head(3)

# COMMAND ----------

training_table_new = df_new.drop(["stretch", "case_id", "date", "review_date", "provider", "metric", "country", "error_type", "error_label"], axis=1)
training_table_new.head(3)

# COMMAND ----------

# # create training table
# (
#     spark.createDataFrame(training_table_new)
#     .write
#     .mode("overwrite")
#     .format('delta')
#     .option("overwriteSchema", "true")
#     .save("dbfs:/error_classification/OMA-1043/training_table_new_fcd_features.delta")
# )

# COMMAND ----------

# # create training table with old fcd features
# (
#     spark.createDataFrame(training_table_old)
#     .write
#     .mode("append")
#     .format('delta')
#     .option("overwriteSchema", "true")
#     .save("dbfs:/error_classification/OMA-1043/training_table_old_fcd_features.delta")
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a combined training dataset

# COMMAND ----------

df_combined = df_new.merge(df_old[["case_id", "pra", "prb", "prab", "tot", "lift"]], on=["case_id"])


# COMMAND ----------

df_combined

# COMMAND ----------

# create training table with old fcd features
(
    spark.createDataFrame(df_combined)
    .write
    .mode("append")
    .format('delta')
    .option("overwriteSchema", "true")
    .save("dbfs:/error_classification/OMA-1043/training_table_combined_fcd_features.delta")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check if the error label is the same for old and new df

# COMMAND ----------

merged = df_new[["case_id", "has_error"]].merge(df_old[["case_id", "has_error"]], on= "case_id")
boolean = merged["has_error_x"] == merged["has_error_y"]
boolean.unique()
