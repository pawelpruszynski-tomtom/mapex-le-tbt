# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluate artificial inspection
# MAGIC In this notebook, the current ML model in production is evaluated based on an artificial inspection sent to MCP and their feedback. 
# MAGIC
# MAGIC Documentation on the Artificial Inspection process is provided on [Confluence](https://confluence.tomtomgroup.com/display/MANA/New+EC+process)

# COMMAND ----------

# MAGIC %md
# MAGIC # User inputs

# COMMAND ----------

# specify run_id of artificial inspection you want to evaluate
run_id = "4c419c75-4a55-4285-8114-07af58efd466" # "d673f560-d8d3-45c3-9712-e68f099fc89f"

quarter = "2023-Q3"

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup and data import

# COMMAND ----------

# MAGIC %run "../utils"

# COMMAND ----------

# connect to tbt database (prod)
tbt_db = json.loads(dbutils.secrets.get(scope="utils", key="db_credentials_tbt"))

tbt_engine = sqlalchemy.create_engine(
        f'postgresql+psycopg2://{tbt_db["user"]}:{tbt_db["password"]}'
        f'@{tbt_db["host"]}:{tbt_db["port"]}/{tbt_db["database"]}'
    ).execution_options(autocommit=True)

print(tbt_engine)

# COMMAND ----------

# import critical sections data
sql_template = f"""
select * from error_logs el
where el.run_id = '{run_id}'
;
"""

el = pd.read_sql_query(
    sql_template, 
    con=tbt_engine.raw_connection()
)

# COMMAND ----------

# TODO: import table with prediction scores from log table (once log table is implemented)


# COMMAND ----------

# MAGIC %md
# MAGIC # Run performance analysis

# COMMAND ----------

el

# COMMAND ----------

# All of the cases were discards --> FNR easy to calculate
FN = el[el["error_type"] != "discard"]
FNR = FN.shape[0] / el.shape[0]
FNR

# COMMAND ----------

# MAGIC %md
# MAGIC # Temp: Test log table

# COMMAND ----------

import json
mcp_oauth = json.loads(dbutils.secrets.get(key='mcp_oauth', scope='utils'))
storage_account_name = 'mcpengbronze'
blob_container_name = 'directions-metric'

metric_name = 'tbt'
MCP_BLOB_URL = f'abfss://{blob_container_name}@{storage_account_name}.dfs.core.windows.net/{metric_name}'

spark.conf.set("fs.azure.account.auth.type.mcpengbronze.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.mcpengbronze.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.mcpengbronze.dfs.core.windows.net", mcp_oauth['client_id'])
spark.conf.set("fs.azure.account.oauth2.client.secret.mcpengbronze.dfs.core.windows.net",  mcp_oauth['client_secret'])
spark.conf.set("fs.azure.account.oauth2.client.endpoint.mcpengbronze.dfs.core.windows.net",  mcp_oauth['client_endpoint'])

spark.read.format('delta').load('abfss://directions-metric@mcpengbronze.dfs.core.windows.net/ec_model_api/prod/logs.delta').display()
