# Databricks notebook source
# MAGIC %md
# MAGIC # Research
# MAGIC In this notebook it is investigated if we can load a ML model on a cluster running a different DB runtime that what the model was trained on. 
# MAGIC Ticket: https://jira.tomtomgroup.com/browse/OMA-853 

# COMMAND ----------

!pip install mlflow

# COMMAND ----------

import mlflow
mlflow.set_tracking_uri('databricks')

# COMMAND ----------

logged_model = 'runs:/69d14861e2be4482b1e059e9f15955eb/model'

# COMMAND ----------

# load with pyfunc
loaded_model = mlflow.pyfunc.load_model(logged_model)

# COMMAND ----------

# load with spark 
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')
