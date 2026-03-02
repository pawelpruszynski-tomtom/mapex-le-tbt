# Databricks notebook source
# MAGIC %md
# MAGIC # Feature analysis
# MAGIC In this notebook, features are analyzed

# COMMAND ----------

# MAGIC %run "../utils"

# COMMAND ----------

# MAGIC %run "../define_table_schema"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import notebook-specific libraries

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col, isnan, when, count, min, max, current_date, isnull
import plotly.express as px

# COMMAND ----------

# read input data
# choose old or new one, depending on which you want to visualize
feature_table_verion = "NEW"

if feature_table_verion == "OLD":
    # OLD feature table (used for training in 06/2023)
    feature_table = spark.read.format('delta').load('wasbs://devcontainer@adlsmapsanalyticsmdbf.blob.core.windows.net/tbt_errors/2_features_data.delta')
    
elif feature_table_verion == "NEW":
    # NEW feature table (used for training in 11/2023)
    feature_table = spark.read.format('delta').load(feature_table_deduplicated_path)

# COMMAND ----------

# load both feature tables to compare them
feature_table_old_spark = spark.read.format('delta').load('wasbs://devcontainer@adlsmapsanalyticsmdbf.blob.core.windows.net/tbt_errors/2_features_data.delta')
feature_table_new_spark = spark.read.format('delta').load(feature_table_deduplicated_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get general statistics 
# MAGIC

# COMMAND ----------

# potential filter
feature_table = feature_table.filter(col("is_tbt") == 0)

# COMMAND ----------

feature_table.persist()

print("Data:")
feature_table.display()

# check how many rows
total_rows = feature_table.count()
print(f"Internal data contains {total_rows} rows")


# check for NaNs
print("NaN values per column: ")
(
    feature_table
    .select([count(when(col(c).isNull(), c)).alias(c) for c in feature_table.columns])
    .display()
)

# value counts per column and calculate relative rates
metric_counts = feature_table.groupBy("is_tbt").count()
metric_rates = metric_counts.withColumn("rate", col("count") / total_rows)
metric_rates.show()

error_label_counts = feature_table.groupBy("has_error").count()
error_label_rates = error_label_counts.withColumn("rate", col("count") / total_rows)
error_label_rates.show()

feature_table.groupBy("error_type").count().show()
feature_table.groupBy("provider").count().show()

# calculate error rate per metric (HDR/TbT)
tbt_counts = feature_table.groupBy("is_tbt").count().alias('total_counts')
error_counts_by_tbt = feature_table.filter("has_error = 1").groupBy("is_tbt").count().alias('error_counts')
error_percentage_by_tbt = tbt_counts.join(error_counts_by_tbt, "is_tbt", 'left_outer') \
                                    .select(tbt_counts["is_tbt"], 
                                            tbt_counts["count"].alias("total_count"), 
                                            F.coalesce(error_counts_by_tbt["count"], F.lit(0)).alias("error_count"),
                                            (F.coalesce(error_counts_by_tbt["count"], F.lit(0)) / tbt_counts["count"] * 100).alias("error_percentage"))
print("Error rate per metric (HDR/TbT):")
error_percentage_by_tbt.show()

# missing FCD data
condition_FCD_missing = (
    (col("pra") <= 0) &
    (col("prb") <= 0) &
    (col("prab") <= 0) &
    (col("lift") <= 0) &
    (col("tot") <= 0)
)
condition_stretch_too_long = (col("lift") == -1)

fcd_data_missing = feature_table.filter(condition_FCD_missing).count()
fcd_long_stretch = feature_table.filter(condition_stretch_too_long).count()
print(f"FCD data is missing or 0 for {fcd_data_missing} cases, that is in {fcd_data_missing/total_rows}%")
print(f"FCD data is missing because the stretch is too long for {fcd_long_stretch} cases, that is in {fcd_long_stretch/total_rows}%")

# start and end date
min_date = feature_table.agg(min("date")).first()[0]
max_date = feature_table.agg(max("date")).first()[0]

print(f"Minimum date: {min_date}")
print(f"Maximum date: {max_date}")

rows_with_future_date_count = feature_table.filter(feature_table["date"] > current_date()).count()    
print(f"There are {rows_with_future_date_count} rows that have an inspection date that lies in the future. ")


# describe datatable
print("Describe data:")
feature_table.describe().display()

# describe null value ratios
null_value_ratios = feature_table.select([(count(when(col(c).isNull(), c)) / total_rows).alias(c) for c in feature_table.columns])
print("Null value ratios per column: ")
null_value_ratios.display()

feature_table.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare old and new feature tables

# COMMAND ----------

# Statistical summary comparison
common_features = list(set(feature_table_old_spark.columns) & set(feature_table_new_spark.columns))
feature_table_old = feature_table_old_spark.select(common_features).sample(0.5, seed=1).toPandas()
feature_table_new = feature_table_new_spark.select(common_features).sample(0.25, seed=1).toPandas()
print(feature_table_old.shape[0])
print(feature_table_new.shape[0])

# COMMAND ----------

# Statistical summary comparison
stats_feature_table_old = feature_table_old.describe().transpose()
stats_feature_table_new = feature_table_new.describe().transpose()

# COMMAND ----------

# Calculate the difference in means
stats_diff = pd.DataFrame()
stats_diff['old_mean'] = stats_feature_table_old['mean']
stats_diff['new_mean'] = stats_feature_table_new['mean']
stats_diff['mean_diff'] = stats_feature_table_new['mean'] - stats_feature_table_old['mean']
stats_diff['mean_rel_change'] = (stats_feature_table_new['mean'] - stats_feature_table_old['mean']) / stats_feature_table_old['mean'].replace(0, np.nan) * 100

stats_diff['old_median'] = stats_feature_table_old['50%']
stats_diff['new_median'] = stats_feature_table_new['50%']
stats_diff['median_diff'] = stats_feature_table_new['50%'] - stats_feature_table_old['50%']
stats_diff['median_rel_change'] = (stats_feature_table_new['50%'] - stats_feature_table_old['50%']) / stats_feature_table_old['50%'].replace(0, np.nan) * 100

# COMMAND ----------

pd.options.display.float_format = '{:.5f}'.format
stats_diff.sort_values(by="mean_rel_change", ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize relative changes

# COMMAND ----------

# Visualize the relative changes
stats_diff[['mean_rel_change', 'std_rel_change', '50%_rel_change']].plot(kind='bar', figsize=(10, 5))
plt.title('Relative Changes in Statistical Summary')
plt.ylabel('Relative Change')
plt.xlabel('Features')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize absolute changes

# COMMAND ----------

curvature_features = ["min_curvature", "max_curvature", "mean_curvature", "route_length", "stretch_length"]

sdo_features = [
    "CONSTRUCTION_AHEAD_TT",
    "MANDATORY_STRAIGHT_ONLY",
    "MANDATORY_STRAIGHT_OR_LEFT",
    "MANDATORY_TURN_RESTRICTION",
    "MANDATORY_TURN_LEFT_ONLY",
    "MANDATORY_TURN_LEFT_OR_RIGHT",
    "MANDATORY_LEFT_OR_STRAIGHT_OR_RIGHT",
    "MANDATORY_TURN_RIGHT_ONLY",
    "MANDATORY_STRAIGHT_OR_RIGHT",
    "NO_ENTRY",
    "NO_MOTOR_VEHICLE",
    "NO_CAR_OR_BIKE",
    "NO_LEFT_OR_RIGHT_TT",
    "NO_LEFT_TURN",
    "NO_RIGHT_TURN",
    "NO_VEHICLE",
    "NO_STRAIGHT_OR_LEFT_TT",
    "NO_STRAIGHT_OR_RIGHT_TT",
    "NO_STRAIGHT_TT",
    "NO_TURN_TT",
    "NO_U_OR_LEFT_TURN",
    "NO_U_TURN",
    "ONEWAY_TRAFFIC_TO_STRAIGHT"
]

fcd_features = ["pra", "prb", "prab", "lift"]

geo_features = set(stats_diff.index) - (set(curvature_features) | set(sdo_features) | set(fcd_features))

stats_diff_curvature = stats_diff.filter(items = curvature_features, axis=0)
stats_diff_sdo = stats_diff.filter(items = sdo_features, axis=0)
stats_diff_fcd = stats_diff.filter(items = fcd_features, axis=0)
stats_diff_geo = stats_diff.filter(items = geo_features, axis=0)

# COMMAND ----------

# Visualize the differences for traffic signs
for df in [stats_diff_curvature, stats_diff_sdo, stats_diff_fcd, stats_diff_geo]: 
    df[["mean_diff", "median_diff", "std_diff"]].plot(kind='bar', figsize=(10, 5))
    plt.title('Differences in Statistical Summary')
    plt.ylabel('Difference')
    plt.xlabel('Features')
    plt.show()


# COMMAND ----------

# Combined boxplots for the numerical columns
numerical_columns = feature_table_old.select_dtypes(include=np.number).columns.tolist()
for col in numerical_columns:
    plt.figure(figsize=(10,5))
    plt.title(f'Boxplot for {col}')
    sns.boxplot(data=[feature_table_old[col], feature_table_new[col]], orient='h')
    plt.yticks(np.arange(2), ('feature_table_old', 'feature_table_new'))
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the distribution of the date 

# COMMAND ----------

dates = feature_table.select("date").toPandas()

# COMMAND ----------

dates["date"].hist(rwidth=0.9, bins=40)

# Set grid
plt.grid(axis='y', alpha=0.75)

# Set xlabel, ylabel, and title
plt.xlabel('Date')
plt.ylabel('Frequency')
plt.title('Distribution of Dates')

# Show the plot
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check if there are duplicated stretches or routes

# COMMAND ----------

# check if there are rows that have the same stretch_length and route_length
(
    feature_table
    .select("route_length", "stretch_length")
    .groupBy("route_length", "stretch_length")
    .count()
    .where("`count` > 1")
    .sort("count", ascending=False)
    .display()
)

# COMMAND ----------

df = feature_table.toPandas()

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Create a DataFrame of duplicate rows
duplicate_rows_df = df[df.duplicated(subset=["stretch_length", "route_length"], keep=False)]

# Group by date and count duplicates
dup_counts = duplicate_rows_df.groupby('date').size()


# Group by date and count duplicates
dup_counts = duplicate_rows_df.groupby('date').size()

# Calculating and printing total number of duplicates
total_duplicates = dup_counts.sum()
print(f"Total number of duplicates: {total_duplicates}")

# Plot
fig, ax = plt.subplots(figsize=(12, 6))
ax.bar(dup_counts.index, dup_counts.values)
plt.ylabel('count')

# Format x-axis
ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.gcf().autofmt_xdate() # Rotation
plt.title("Number of Duplicates by date")

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Investigate variables with differences for error/no_error class

# COMMAND ----------

# take sample of feature table and convert it to pandas
feature_table_pdf = feature_table.sample(withReplacement=False, fraction=0.3).toPandas()

# COMMAND ----------

feature_table_pdf.head(5)

# COMMAND ----------

feature_table_pdf.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize feature differences for categories "error" and "no_error"
# MAGIC Histograms and Boxplots are plotted, showing potential differences in the distribution of the features. 

# COMMAND ----------

if feature_table_verion == "OLD":
    features_not_visualized = ["tbt_version", "route", "stretch", 'case_id', "metric", 'date', 'country', 'provider', "error_type", "error_label", "has_error", 'sdo_api_response', 'traffic_signs',
       'model_metadata', 'prediction', 'probability', 'original_run_id',
       'proba_threshold']
    feature_table_pdf["error_label"] = np.where(feature_table_pdf["has_error"] == 1, "error", "no_error")
elif feature_table_verion == "NEW":
    features_not_visualized = ['case_id', "metric", 'date', 'country', 'provider', "error_type", "error_label", "has_error"]

# List of features to visualize
features_to_visualize = [item for item in feature_table_pdf.columns if item not in features_not_visualized]

# Loop through features and create histograms
for i, feature in enumerate(features_to_visualize):
    if feature in ["route_length", "stretch_length", "straight_turns", "total_right_angle", "max_right_angle", "total_left_angle", 
                   "max_left_angle", "absolute_angle", "min_curvature", "max_curvature", "mean_curvature", "distance_start_end_stretch"]:
        # remove outliers outside IQR * 1.5
        Q1 = feature_table_pdf[feature].quantile(0.10)
        Q3 = feature_table_pdf[feature].quantile(0.90)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        temp_df = feature_table_pdf[(feature_table_pdf[feature] > lower_bound) & (feature_table_pdf[feature] <= upper_bound)]
    else:
        temp_df = feature_table_pdf

    # Plot histogram and boxplot
    print(f"Feature: {feature}")

    plt.figure(figsize=(18, 6))

    plt.subplot(1, 2, 1)
    sns.histplot(data=temp_df, x=feature, hue="error_label", hue_order=["error", "no_error"], 
                 stat="proportion", common_norm=False, alpha=0.7, bins=100)

    plt.subplot(1, 2, 2)
    sns.boxplot(data=temp_df, x=feature, y="error_label", order=["error", "no_error"])

    plt.tight_layout()
    plt.show()


