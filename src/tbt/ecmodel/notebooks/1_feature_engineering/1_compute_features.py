# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook to compute features from intermedate table
# MAGIC This notebook makes use of the featurization module and computes the feature table, based on the intermediate storage table. 
# MAGIC
# MAGIC The intermedate storage table contains:
# MAGIC - case_id & 
# MAGIC - FCD features
# MAGIC - SDO response
# MAGIC - Stretch & Route
# MAGIC
# MAGIC Thus we can compute all general, geometric, and SDO features.  

# COMMAND ----------

# MAGIC %md
# MAGIC # User Parameters

# COMMAND ----------

REVIEW_STATUS = True # if data should be evaluated before and after writing to datalake 

# COMMAND ----------

# MAGIC %md
# MAGIC # Import general libraries, functions and variables

# COMMAND ----------

# MAGIC %run "../utils"

# COMMAND ----------

# MAGIC %run "../define_table_schema"

# COMMAND ----------

# for this import to work, the two import lines from src have to be commented out in the featurization script
from api.src.featurization import Featurizer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import notebook-specific libraries

# COMMAND ----------

# # import pyproj
# import shapely.wkt
# import shapely.geometry
# import scipy.interpolate
from tqdm import tqdm
from pyspark.sql.functions import col, isnan, when, count, min, max, current_date, isnull

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data

# COMMAND ----------

feature_table_path

# COMMAND ----------

# internal data
intermediate_data = spark.read.format('delta').load(intermediate_data_path)

intermediate_data_count = intermediate_data.count()
print(f"Internal data containes {intermediate_data_count} rows")

# try to load feature table
try:
    feature_table = spark.read.format('delta').load(feature_table_path)
    print(f'Feature table loaded successfully. It contains {feature_table.count()} rows')
    FEATURE_TABLE_EXISTS = True
except:
    print(f'The file feature_table does not exist.')
    FEATURE_TABLE_EXISTS = False

# COMMAND ----------

# MAGIC %md
# MAGIC # Check which cases are not yet featurized

# COMMAND ----------

if FEATURE_TABLE_EXISTS:
    missing_case_ids = (
        intermediate_data
        .join(feature_table, on='case_id', how='leftanti')
    )
else:
    missing_case_ids = intermediate_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Define featurization functions
# MAGIC

# COMMAND ----------

# convert to pandas
missing_case_ids_pdf = missing_case_ids.to_pandas_on_spark()

# COMMAND ----------

missing_case_ids_pdf

# COMMAND ----------

# rename the sdo_response column
missing_case_ids_pdf.rename(columns={'sdo_response':'sdo_api_response'}, inplace=True)

# COMMAND ----------

import warnings
warnings.filterwarnings('ignore')

feature_data_list = []

# Iterate over the rows
for index, row in tqdm(missing_case_ids_pdf.iterrows(), total = missing_case_ids_pdf.shape[0]):
    # convert one row of the data to dictionary
    row_dict = row.to_dict()

    # initialize Featurizer
    featurizer = Featurizer(row_dict)

    # calculate general features
    general_features = featurizer.calculate_general_features()

    # calculate geometric features
    geometric_features = featurizer.calculate_geometric_features()

    # calculate SDO features
    sdo_features = featurizer.calculate_sdo_features(row_dict)

    # retrieve already calculated fcd features
    fcd_features = {
        "pra": row_dict["pra"],
        "prb": row_dict["prb"],
        "prab": row_dict["prab"],
        "lift": row_dict["lift"],
        "tot": row_dict["tot"]
    }
    
    metadata = {
        "case_id": row_dict["case_id"],
        "date": row_dict["date"],
        "provider": row_dict["provider"],
        "metric": row_dict["metric"],
        "country": row_dict["country"]
    }

    targets = {
            "error_type": row_dict["error_type"],
            "error_label": row_dict["error_label"],
            "has_error": 0 if row_dict["error_label"] == "no_error" else 1
        }

    combined_data = {
        # add metadata
        **metadata,
        # add features
        **general_features, 
        **geometric_features, 
        **fcd_features, 
        **sdo_features, 
        # add targets
        **targets
        }

    # add combined data to output list
    feature_data_list.append(combined_data)

# COMMAND ----------

# convert to pandas df
feature_data = pd.DataFrame(feature_data_list)

# COMMAND ----------

feature_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Check featurized data

# COMMAND ----------

feature_data.display()

# Check how many rows
print(f"Feature data contains {len(feature_data)} rows")

# Check for NaNs
print("NaN values per column: ")
print(feature_data.isnull().sum().to_string())

print("----- Value counts per column -----")
print(feature_data['metric'].value_counts())
print("-----------------------------------")
print(feature_data['error_label'].value_counts())
print("-----------------------------------")
print(feature_data['error_type'].value_counts())

# COMMAND ----------

# Drop rows with NA values
feature_data = feature_data.dropna()

# COMMAND ----------

# MAGIC %md
# MAGIC # Save feature data

# COMMAND ----------

# convert pandas to spark
feature_table = spark.createDataFrame(feature_data)

# COMMAND ----------

# Apply the schema to the DataFrame
for field in feature_table_schema:
    column_name = field.name
    feature_table = feature_table.withColumn(column_name, col(column_name).cast(field.dataType))

# COMMAND ----------

# Save the data
(
    feature_table
    .repartition("metric", "error_label", "country")
    .write
    .mode("append")
    .format('delta')
    .partitionBy(["metric", "error_label", "country"])
    .save(feature_table_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Check if data was exported properly

# COMMAND ----------

if REVIEW_STATUS:
    check = spark.read.format('delta').load(feature_table_path)

    print("Total cases of external data:", check.count())
    check.limit(50).display()
