# Databricks notebook source
# MAGIC %md
# MAGIC # Create new FCD features with other features to create a full training dataset. 
# MAGIC
# MAGIC **Steps**:
# MAGIC
# MAGIC - Get sample from current intermediate datatable
# MAGIC   - Should be rather most recent cases from good quality countries
# MAGIC - Featurize it with new feature FCD features
# MAGIC - save it periodically 
# MAGIC
# MAGIC Locations of the saved files are available from a table on this Confluence Page: https://confluence.tomtomgroup.com/display/MANA/Docs+-+EC+Model+Training 

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %pip install --extra-index-url https://svc-ar-maps-analytics-editor:AP5GYCYPFsETQzbsgnE8a6cjhNEcSvTaNTUvzNmkDHQTRt9GhcqKa3zAe9j2@artifactory.tomtomgroup.com/artifactory/api/pypi/maps-fcd-pypi-release/simple fcd_py==5.0.615
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
print(gpd.__version__)
print(shapely.__version__)

# COMMAND ----------

credentials = json.loads(dbutils.secrets.get(scope = "mdbf3", key = "fcd_credentials"))

# COMMAND ----------

# MAGIC %run "/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/notebooks/utils"
# MAGIC

# COMMAND ----------

# sys.path.append("/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/notebooks/")
%run "/Workspace/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/error_classification/call_FCD_API_new.py"

# COMMAND ----------

# MAGIC %run "/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/notebooks/define_table_schema"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Tables

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

# MAGIC %md
# MAGIC # Create Training and Testing Table 

# COMMAND ----------

# MAGIC %md
# MAGIC # Evaluate new FCD features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load featurized table

# COMMAND ----------

# Load featurized table
new_fcd_features = spark.read.format("delta").load("dbfs:/error_classification/OMA-1043/feature_table_w_new_fcd_features_01.delta")
new_fcd_features = new_fcd_features.toPandas()

# COMMAND ----------

new_fcd_features.shape[0]

# COMMAND ----------

new_fcd_features.tail(10)

# COMMAND ----------

gdf = new_fcd_features.copy()
gdf["stretch"] = gpd.GeoSeries.from_wkt(new_fcd_features["stretch"])
gdf = gpd.GeoDataFrame(gdf, geometry="stretch", crs=4326)

# COMMAND ----------

gdf.iloc[:][["stretch", "tot_new", "tot_contained"]].explore()

# COMMAND ----------

new_fcd_features.drop_duplicates("case_id")

# COMMAND ----------

new_fcd_features["metric"].value_counts(normalize=True)

# COMMAND ----------

# Histograms for all features
new_fcd_features[["tot_new", "pra_new", "prb_new", "pra_to_b", "prb_to_a", "tot_contained", "pra_to_b_contained", "prb_to_a_contained", "traffic_direction_contained", "traffic_direction"]].hist(bins=20, figsize=(15, 10))
plt.show()

# COMMAND ----------

sns.lmplot(x="pra_to_b_contained", y="pra_to_b", hue="error_label", data=new_fcd_features)
plt.show()

# COMMAND ----------

new_fcd_features[new_fcd_features["tot_contained"] > 0].shape[0]

# COMMAND ----------

new_fcd_features["error_label"].value_counts(normalize=True)

# COMMAND ----------

new_fcd_features[new_fcd_features["pra_to_b_contained"] > 0.9]["error_label"].value_counts(normalize = True)

# COMMAND ----------

new_fcd_features[new_fcd_features["traffic_direction_contained"] == 0]["error_label"].value_counts(normalize = True)

# COMMAND ----------

new_fcd_features.query('country == "GBR" and traffic_direction_contained > 0')["error_label"].value_counts(normalize = True)

# COMMAND ----------

new_fcd_features["country"].hist(figsize = (30,10))

# COMMAND ----------

new_fcd_features["date"].hist(bins=50,figsize = (30,10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for TbT vs HDR 

# COMMAND ----------

print(new_fcd_features.query('metric=="TbT"')["has_error"].value_counts(normalize=True))
print(new_fcd_features.query('metric=="HDR"')["has_error"].value_counts(normalize=True))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Repeat evaluation with preprocessing of new FCD features
# MAGIC To separate cases where no FCD data is available from cases where features are indeed 0, preprocessing is applied. 

# COMMAND ----------

# Check Feature distribution after optional preprocessing of new FCD features
def fcd_feature_preprocessing(row):
    new_fcd_features = [
        "pra_new", "prb_new", "pra_not_b", "prb_not_a", "pra_and_b", "pra_to_b", 
        "prb_to_a", "pra_to_b_contained", "prb_to_a_contained", "pra_to_b_not_contained",
        "prb_to_a_not_contained", "traffic_direction", "traffic_direction_contained"
    ]
    new_fcd_features_contained = [
        "pra_to_b_contained", "prb_to_a_contained", "pra_to_b_not_contained",
        "prb_to_a_not_contained", "traffic_direction_contained"
    ]

    if row['tot_new'] == 0:
        for column in new_fcd_features:
            row[column] = -2.0

    if row['tot_contained'] < 6:
        for column in new_fcd_features_contained:
            row[column] = -2.0

    return row

new_fcd_features_preprocessed = new_fcd_features.apply(fcd_feature_preprocessing, axis=1)

new_fcd_features_preprocessed[["pra_new", "prb_new", "pra_to_b", "prb_to_a", "tot_contained", "pra_to_b_contained", "prb_to_a_contained", "traffic_direction_contained", "traffic_direction", "lift"]].hist(bins=20, figsize=(15, 10))
plt.show()

# COMMAND ----------

sns.lmplot(x="pra_to_b_contained", y="pra_to_b", hue="error_label", data=new_fcd_features_preprocessed)
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

def plot_thresholds_vs_proportion(df, threshold_column, label_column, thresholds):
    proportions = []
    labels = ["total", '= -2', '= 0']

    total = df[label_column].value_counts(normalize=True).get(1, 0)
    equal_minus_2 = df[df[threshold_column] == -2][label_column].value_counts(normalize=True).get(1, 0)
    equal_0 = df[df[threshold_column] == 0][label_column].value_counts(normalize=True).get(1, 0)

    proportions.append(total)
    proportions.append(equal_minus_2)
    proportions.append(equal_0)

    for threshold in thresholds:
        # Apply the threshold
        filtered_df = df[df[threshold_column] > threshold]
        # Calculate normalized value counts for the label_column
        value_counts = filtered_df[label_column].value_counts(normalize=True)
        # Get the proportion of the 1 label, or 0 if it doesn't exist
        proportion_1 = value_counts.get(1, 0)
        proportions.append(proportion_1)
        labels.append(f'>{threshold}')

    # Plotting
    plt.figure(figsize=(12, 6))
    bars = plt.bar(labels, proportions, color='skyblue')
    plt.title(f'Proportion of Error by Thresholds for feature {threshold_column}')
    plt.xlabel(f'Thresholds for feature {threshold_column}')
    plt.ylabel('Proportion of Error')

    # Add the bar labels
    plt.bar_label(bars, fmt='%.2f')

    plt.xticks(rotation=45)
    plt.tight_layout() # Adjusts the plot to ensure everything fits without overlapping
    plt.show()

thresholds = [-2, -0.1, 0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.999]

# COMMAND ----------

plot_thresholds_vs_proportion(new_fcd_features_preprocessed, 'pra_to_b_contained', 'error_label', thresholds)

# COMMAND ----------

plot_thresholds_vs_proportion(new_fcd_features, 'pra_to_b_contained', 'error_label', thresholds)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate new FCD features by comparing it to ad hoc featurization

# COMMAND ----------

df = new_fcd_features.iloc[200:210]

# COMMAND ----------

fcd_featurizer = FCDFeaturizer(
        df,
        trace_retrieval_geometry="bbox_json",
        traces_limit=1000,
        fcd_credentials=credentials,
        spark_context=spark
    )

# COMMAND ----------

fcd_features = fcd_featurizer.featurize(show_progress=True)

# COMMAND ----------

fcd_features.reset_index(inplace=True, drop=True)
fcd_features

# COMMAND ----------

idx = 5

# COMMAND ----------

a = fcd_featurizer.gdf.iloc[idx, :].loc["stretch"]
print(a)

# COMMAND ----------

fcd_featurizer.gdf.iloc[[idx],:]

# COMMAND ----------

df.iloc[[idx],:]

# COMMAND ----------

assert df.iloc[idx,:]["case_id"] == fcd_featurizer.gdf.iloc[idx,:]["case_id"]

# COMMAND ----------

fcd_featurizer.visualize(idx, visualize_all_traces=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # TODO: Transfer the new features to feature table and balanced dataset 
# MAGIC - Load new Fcd Feature table and balanced table
# MAGIC - Add new FCD to balanced table
# MAGIC     - Join the new FCD features to the balanced table, drop na rows
# MAGIC     - Check if the full balanced table is featurized with fcd
# MAGIC - Replace feature table with new feature table incl. new FCD features
# MAGIC     - antijoin the balanced table on the whole fcd feature table
# MAGIC - Write both tables to their according location
# MAGIC     - Feature table to hive.error_classification.feature_table
# MAGIC     - balanced table to hive.error_classification.balanced_table

# COMMAND ----------

# Load featurized table
new_fcd_features = spark.read.format("delta").load("dbfs:/error_classification/OMA-1043/feature_table_w_new_fcd_features_01.delta")
new_fcd_features = new_fcd_features.toPandas()

# COMMAND ----------

balanced_data = (
    spark.read.format('delta').load(balanced_data_path)
).toPandas()

# COMMAND ----------

balanced_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join the new FCD features to feature table and balanced data

# COMMAND ----------

balanced_data_case_ids = list(balanced_data["case_id"])

# COMMAND ----------

print(new_fcd_features[new_fcd_features["case_id"].isin(balanced_data_case_ids)].value_counts("metric", normalize=False))
print(new_fcd_features[new_fcd_features["case_id"].isin(balanced_data_case_ids)].value_counts("metric", normalize=True))
print(new_fcd_features[new_fcd_features["case_id"].isin(balanced_data_case_ids)].value_counts("has_error", normalize=True))

# COMMAND ----------

feature_table = new_fcd_features[~new_fcd_features["case_id"].isin(balanced_data_case_ids)]
test_table = new_fcd_features[new_fcd_features["case_id"].isin(balanced_data_case_ids)]

# COMMAND ----------

feature_table

# COMMAND ----------

test_table

# COMMAND ----------

# (
#     spark.createDataFrame(feature_table)
#     .write
#     .format('delta')
#     .mode('overwrite')
#     .option("overwriteSchema", "True")
#     .save("dbfs:/error_classification/feature_table.delta")
# )

# COMMAND ----------

# %sql

# DROP TABLE if exists error_classification.feature_table;
# CREATE table error_classification.feature_table using delta LOCATION 'dbfs:/error_classification/feature_table.delta';

# COMMAND ----------

# (
#     spark.createDataFrame(test_table)
#     .write
#     .format('delta')
#     .mode('overwrite')
#     .option("overwriteSchema", "True")
#     .save("dbfs:/error_classification/test_table.delta")
# )

# COMMAND ----------

# %sql

# DROP TABLE if exists error_classification.test_table;

# CREATE table error_classification.test_table using delta LOCATION 'dbfs:/error_classification/test_table.delta';

# COMMAND ----------


