# Databricks notebook source
# MAGIC %md
# MAGIC # Monitor discard rate
# MAGIC This notebook is used to monitor the discard rate of the model in production over time
# MAGIC The discard rate is how many cases are discarded by the model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import general libraries, functions and variables

# COMMAND ----------

# MAGIC %run "../utils"

# COMMAND ----------

# MAGIC %run "../define_table_schema"

# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.sql.functions import when, col, sum

import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read in data

# COMMAND ----------

ics = read_tbt_datalake("inspection_critical_sections.delta")
im = read_tbt_datalake("inspection_metadata.delta")

# create temporary views to be able to query the tables with SQL
ics.createOrReplaceTempView("ics")
im.createOrReplaceTempView("im")

# COMMAND ----------

df = spark.sql(f"""
select 
    -- ics.run_id,
    -- ics.route_id,
    -- ics.case_id,
    ics.fcd_state,
    im.inspection_date as date
    
from ics
left join im
on ics.run_id = im.run_id
where ics.prab = -1
-- and im.date > '2023-01-01'
""")

# COMMAND ----------

df = df.withColumn('is_potential_error', when(col("fcd_state") == "potential_error", 1).otherwise(0))
df = df.withColumn('is_no_error', when(col("fcd_state") == "no error", 1).otherwise(0))

# COMMAND ----------

pdf = df.toPandas()

# COMMAND ----------

# compute average discard rate since the model put in production
total_discards = pdf["is_no_error"].sum()
total_cases = pdf.shape[0]
discard_rate = total_discards / total_cases
discard_rate

# COMMAND ----------

# Set 'date' as the index of the DataFrame
pdf.set_index('date', inplace=True)

# COMMAND ----------

pdf

# COMMAND ----------

# Group by date and sum 'is_potential_error'
daily_sum_potential_errors = pdf.groupby(pdf.index)['is_potential_error'].sum()
daily_sum_no_error_errors = pdf.groupby(pdf.index)['is_no_error'].sum()
discard_rate = daily_sum_no_error_errors / (daily_sum_no_error_errors + daily_sum_potential_errors)

# COMMAND ----------

plt.figure(figsize=(10,6))  # Set the figure size
plt.plot(discard_rate, marker='o', linewidth=2, color='blue')  # Set marker, linewidth and color
plt.title('Discard Rate Over Time', fontsize=20)  # Add a title
plt.xlabel('Time', fontsize=15)  # Add x-label
plt.ylabel('Discard Rate', fontsize=15)  # Add y-label
plt.grid(True)  # Add a grid
plt.xticks(fontsize=12, rotation=45)  # Adjust x-ticks
plt.yticks(fontsize=12)  # Adjust y-ticks
plt.tight_layout()  # Adjust layout
plt.show()
