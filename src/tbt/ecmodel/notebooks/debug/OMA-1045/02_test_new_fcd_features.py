# Databricks notebook source
# MAGIC %md
# MAGIC # Test new FCDFeaturizer Class and features
# MAGIC The module is available from this file: ``github-maps-analytics-error-classification/error_classification/call_FCD_API_new.py``

# COMMAND ----------

# MAGIC %pip install --extra-index-url https://svc-ar-maps-analytics-editor:AP5GYCYPFsETQzbsgnE8a6cjhNEcSvTaNTUvzNmkDHQTRt9GhcqKa3zAe9j2@artifactory.tomtomgroup.com/artifactory/api/pypi/maps-fcd-pypi-release/simple fcd_py==5.0.615
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

# MAGIC %run "/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/notebooks/utils"

# COMMAND ----------

# FCD credentials
credentials = json.loads(dbutils.secrets.get(scope = "mdbf3", key = "fcd_credentials"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define FCDFeaturizer module

# COMMAND ----------

# Optional 
# import sys
# sys.path.append('/Workspace/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/error_classification')

# import FCDFeaturizer module
%run "/Workspace/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/error_classification/call_FCD_API_new.py"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load some data for testing 

# COMMAND ----------

# how many stretches we want
limit = 100

selh = read_tbt_datalake("scheduled_error_logs_history.delta")
selh.createOrReplaceTempView("selh")

df = spark.sql(f"""
select 
    selh.case_id,
    selh.country, 
    selh.stretch as stretch, 
    -- selh.review_date as date,
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
-- for now, get only from countries where we have good FCD data
where selh.country in ('NLD', 'BEL', 'USA', 'GBR', 'DEU', 'FRA', 'ESP')
""")

# count rows
rows = df.count()

sample_fraction = limit / rows


df = df.sample(sample_fraction, seed = 1234345).toPandas()

print(f"Rows: {df.shape[0]}")
df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # Tests, Checks and Analyses

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simple tests that it is working

# COMMAND ----------

fcd_featurizer = FCDFeaturizer(
  df, 
  trace_retrieval_geometry = "bbox_json",
  traces_limit = 2000,
  fcd_credentials=credentials, 
  spark_context=spark
  )

# COMMAND ----------

index = 7
fcd_featurizer.visualize(index, visualize_all_traces=True)

# COMMAND ----------

fcd_features = fcd_featurizer.featurize(show_progress=True)

# COMMAND ----------

fcd_features

# COMMAND ----------

# check cases where pra_to_b_contained is > 0.95
fcd_features[fcd_features["pra_to_b_contained"] > 0.95]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the distribution of the features 

# COMMAND ----------

# Histograms for all features
fcd_features.hist(bins=20, figsize=(15, 10))
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test if a larger number of traces makes a large difference. 

# COMMAND ----------

df_sample = df.iloc[0:20,:]
fcd_featurizer_1000 = FCDFeaturizer(df_sample, fcd_credentials=credentials, traces_limit=1000, spark_context=spark)
fcd_featurizer_2000 = FCDFeaturizer(df_sample, fcd_credentials=credentials, traces_limit=2000, spark_context=spark)
fcd_featurizer_5000 = FCDFeaturizer(df_sample, fcd_credentials=credentials, traces_limit=5000, spark_context=spark)

fcd_features_1000 = fcd_featurizer_1000.featurize(show_progress=True)
fcd_features_2000 = fcd_featurizer_2000.featurize(show_progress=True)
fcd_features_5000 = fcd_featurizer_5000.featurize(show_progress=True)

# COMMAND ----------

fcd_features_1000.columns

# COMMAND ----------

# List of features to plot
features_to_plot = fcd_features_1000.columns

# Create a 4x4 grid of subplots
fig, axs = plt.subplots(4, 4, figsize=(15, 15))  # Adjust the figsize as needed

# Flatten the array of axes for easy iteration
axs = axs.flatten()

# Plot each feature in its own subplot
for i, feature in enumerate(features_to_plot):
    axs[i].plot(fcd_features_1000.index, fcd_features_1000[feature], label='1000 traces')
    axs[i].plot(fcd_features_2000.index, fcd_features_2000[feature], label='2000 traces')
    axs[i].plot(fcd_features_5000.index, fcd_features_5000[feature], label='5000 traces')

    # Set title and labels
    axs[i].set_title(feature)
    axs[i].set_xlabel('Index')
    axs[i].set_ylabel('Value')

    # Add a legend if desired
    axs[i].legend()

# Adjust the layout so that subplots do not overlap
plt.tight_layout()

# Show the plot
plt.show()

# COMMAND ----------

diff_df = fcd_features_5000 - fcd_features_2000
diff_df = diff_df.drop(["tot_contained", "tot"], axis=1)
plt.figure(figsize=(15,15))
sns.heatmap(diff_df, annot=True)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test old vs new FCD features 
# MAGIC - Check latency difference
# MAGIC - Check difference in features

# COMMAND ----------

# MAGIC %md
# MAGIC ### New features

# COMMAND ----------

ics_limit = read_tbt_datalake("inspection_critical_sections.delta").select("case_id", "stretch", "fcd_state").limit(10).toPandas()
fcd_featurizer = FCDFeaturizer(ics_limit, fcd_credentials=credentials, spark_context=spark)
fcd_features_new = fcd_featurizer.featurize(show_progress=True)
fcd_features_new = fcd_features_new.add_suffix('_new')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Old features

# COMMAND ----------

# get old fcd featurization functions
%run "/Workspace/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/error_classification/call_FCD_API.py"

# COMMAND ----------

ics_limit["stretch_length"] = 100
old_fcd_features = evaluate_with_RFC(ics_limit, fcd_credentials=credentials)


# COMMAND ----------

fcd_features_old = old_fcd_features.add_suffix('_old')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine and compare
# MAGIC

# COMMAND ----------

fcd_features_all = pd.concat([fcd_features_old, fcd_features_new], axis = 1)
selected_features = fcd_features_all[["pra_old", "prb_old", "prab_old", "lift_old", "pra_new", "prb_new", "pra_and_b_new", "pra_to_b_new"]]

# COMMAND ----------

corr_matrix = selected_features.corr()

sn.heatmap(corr_matrix, annot=True)
plt.show()

# COMMAND ----------

selected_features[["pra_old", "pra_new"]].plot()

# COMMAND ----------

selected_features[["prb_old", "prb_new"]].plot()

# COMMAND ----------

selected_features[["prab_old", "pra_to_b_new"]].plot()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Featurize some training data with new FCD features

# COMMAND ----------

fcd_featurizer = FCDFeaturizer(test_data, fcd_credentials=credentials, spark_context=spark)
fcd_features = fcd_featurizer.featurize(show_progress=True)
fcd_features

# COMMAND ----------

# add features to df
df_with_features = pd.concat([test_data, fcd_features], axis = 1)

# COMMAND ----------

# add numerical has_error column
df_with_features["has_error"] = df_with_features["error_label"].apply(lambda x: 1 if x == "error" else 0)

# COMMAND ----------

# Histograms for all features
df_with_features.hist(bins=15, figsize=(15, 10))
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test model loading

# COMMAND ----------

import mlflow
logged_model = 'runs:/b3739211e69e40de8fdfd06b10af89e3/model'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)

