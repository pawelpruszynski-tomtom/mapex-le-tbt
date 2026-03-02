# Databricks notebook source
# MAGIC %md
# MAGIC # MLflow tests
# MAGIC Test model loading from MLflow Model registry

# COMMAND ----------

# MAGIC %pip install xgboost==1.7.6

# COMMAND ----------

# MAGIC %pip install mlflow==2.5.0

# COMMAND ----------

import mlflow
import json
mlflow.__version__

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

# Model name and stage
model_name = 'ec_combined_prod'
model_stage = 'staging'

logged_model = f'models:/{model_name}/{model_stage}'

# Load model as a PyFuncModel.
loaded_model = mlflow.sklearn.load_model(logged_model)

# Initialize MLflow client
client = MlflowClient()

# Fetch the latest version of the model in the specified stage
model_version_details = client.get_latest_versions(model_name, stages=[model_stage])

# Assuming you want the latest version in the production stage
latest_model_version = model_version_details[-1]

# Get the source run ID from the latest model version
source_run_id = latest_model_version.run_id

# create model uri
logged_model_uri = f"runs:/{source_run_id}/model"

model_info = mlflow.models.get_model_info(logged_model_uri)

inputs = model_info._signature_dict["inputs"]
inputs = json.loads(inputs)

col_names = []
for i in inputs:
    col_names.append(i["name"])
col_names

# COMMAND ----------

geopandas.__version__

# COMMAND ----------

from pprint import pprint
float(mlflow.get_run(source_run_id).data.tags["custom_threshold"])

# COMMAND ----------


# default threshold 
mlflow.get_run(source_run_id).data.metrics["ot_98_threshold"]

# COMMAND ----------

# Now you can use the run ID to further investigate the run or its artifacts
# For example, you can load the model from the run
logged_model_uri = f"runs:/{source_run_id}/model"

# Load model as a PyFuncModel
loaded_model = mlflow.sklearn.load_model(logged_model_uri)

# COMMAND ----------

logged_model.get_params()

# COMMAND ----------



# COMMAND ----------

model_uri = client.get_model_version_download_uri('ec_tbt_prod','production')
model_info = mlflow.models.get_model_info(model_uri)
model_info._signature_dict
