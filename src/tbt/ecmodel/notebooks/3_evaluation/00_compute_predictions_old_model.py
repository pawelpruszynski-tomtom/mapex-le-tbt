# Databricks notebook source
# MAGIC %md
# MAGIC # Compute predictions for old model
# MAGIC This notebook is needed because the old model has different dependencies, so computing predictions in the same notebook for old and new model is not possible. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import general libraries, functions and variables

# COMMAND ----------

# MAGIC %run "../utils"

# COMMAND ----------

# MAGIC %run "../define_table_schema"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import notebook-specific libraries

# COMMAND ----------

# # install dependencies
# %pip install sqlalchemy
# %pip install mlflow
# %pip install cloudpickle==1.6.0
# %pip install psutil==5.8.0
# %pip install scikit-learn==0.24.1
# %pip install xgboost==1.4.2

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load test data and models

# COMMAND ----------

# load test data
test_table = spark.read.format('delta').load(test_table_path)

# load artificial inspection data
quarter = "2023-Q3"
featurized_artificial_inspection_df = (
    spark.read.format('delta')
    .load(f'{BLOB_URL}/error_classification/artificial_inspections/{quarter}/featurized_artificial_inspection.delta')
    .dropDuplicates(["case_id"])
)

# load model 1 (typically current production model)
run_id_1 = "82395780afab4ec6bc3ee4c5736ae87f"
logged_model_1 = f'runs:/{run_id_1}/model'
loaded_model_1 = mlflow.sklearn.load_model(logged_model_1)


# # Predict on a Spark DataFrame.
# test_table_with_preds = test_table.withColumn('predictions_1', loaded_model(struct(*map(col, test_table.columns))))

# COMMAND ----------

# run this if you want to work on the artificial inspection data
test_table = featurized_artificial_inspection_df

# COMMAND ----------

def get_input_cols(loaded_model): 
    model_input_schema = loaded_model.metadata.get_input_schema().to_dict()
    model_input_cols = [col["name"] for col in model_input_schema]
    return model_input_cols

# input_cols_model_1 = get_input_cols(loaded_model_1)

# COMMAND ----------

probability_threshold = 0.032

model_features = loaded_model_1[0].transformers[0][2]

probabilities = loaded_model_1.predict_proba(test_table.toPandas().loc[:, model_features])

probabilities = probabilities[:, 1]

predictions = [1 if prob >= probability_threshold else 0 for prob in probabilities]

# COMMAND ----------

pd.Series(probabilities)

# COMMAND ----------

test_table_pandas = test_table.toPandas()
test_table_pandas["probability_1"] = pd.Series(probabilities)
test_table_pandas["prediction_1"] = predictions
test_table_with_preds = spark.createDataFrame(test_table_pandas)

# COMMAND ----------

test_table_with_preds.display()

# COMMAND ----------

(
  test_table_with_preds
  .write
  .format('delta')
  .mode('overwrite')
  .option("overwriteSchema", "True")
  .save(f'{BLOB_URL}/error_classification/artificial_inspections/{quarter}/artificial_inspections_w_preds1.delta')
)
