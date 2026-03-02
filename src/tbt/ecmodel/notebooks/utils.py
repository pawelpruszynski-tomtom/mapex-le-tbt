# Databricks notebook source
"""
This notebook is used to define functions used in multiple notebooks
Functions can easily be imported to other notebooks by running `%run "./utils.ipynb"`
CAUTION: this will overwrite namespaces. 
"""

# import standard libraries used throughout the training pipeline
import pandas as pd
import numpy as np
import json
import sqlalchemy

import os
from datetime import datetime

import pyspark.sql.functions as F
import pyspark.sql.types as T


# set default connection to devcontainer datalake - acts as intermediate storage
storage_account_name = 'adlsmapsanalyticsmdbf'
storage_account_access_key = ''
blob_container_name = 'devcontainer'
BLOB_URL = f'wasbs://{blob_container_name}@{storage_account_name}.blob.core.windows.net'
spark.conf.set(f'fs.azure.account.key.{storage_account_name}.blob.core.windows.net', storage_account_access_key)


# function to read raw data from mcpeng / directions-metric data lake
def read_tbt_datalake(table_name):
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

    output_file = spark.read.format('delta').load(f'{MCP_BLOB_URL}/delta-tables/{table_name}')
    return output_file



def check_dataframe_equality(df1, df2):
    column_names_df1 = df1.columns
    column_names_df2 = df2.columns

    max_columns = max(len(column_names_df1), len(column_names_df2))

    # Create lists with NaNs for missing columns
    column_names_df1 = list(column_names_df1) + [None] * (max_columns - len(column_names_df1))
    column_names_df2 = list(column_names_df2) + [None] * (max_columns - len(column_names_df2))

    if column_names_df1 != column_names_df2: 
        # Create a DataFrame with two columns
        column_names_df = pd.DataFrame({'DataFrame 1 Column Names': column_names_df1,
                                        'DataFrame 2 Column Names': column_names_df2})
        display(column_names_df)
        raise ValueError("Dataframes have different number of columns")
    else: 
        print("Column names are the same")

    dtype_df1 = df1.dtypes.astype(str)
    dtype_df2 = df2.dtypes.astype(str)

    max_columns = max(len(dtype_df1), len(dtype_df2))

    # Create lists with NaNs for missing columns
    dtype_df1 = list(dtype_df1) + [None] * (max_columns - len(dtype_df1))
    dtype_df2 = list(dtype_df2) + [None] * (max_columns - len(dtype_df2))

    if dtype_df1 != dtype_df2: 
        # Create a DataFrame with two columns
        column_types_df = pd.DataFrame({'Column names': column_names_df1,
                                        'DataFrame 1 Column types': dtype_df1,
                                        'DataFrame 2 Column types': dtype_df2})
        display(column_types_df)
        raise ValueError("Dataframes have different column types")
    else: 
        print("Column types are the same")


def append_new_data_only(df, destination_path):
    try:
        existing_df = spark.read.format('delta').load(destination_path)
        print("Existing data successfully loaded")
    except:
        # Define schema for the DataFrame
        schema = T.StructType([
            T.StructField('case_id', T.StringType()),
        ])

        existing_df = spark.createDataFrame([], schema)
        print("Existing data could not be found")
    
    df.persist()
    df_count = df.count()

    # get new cases only
    new_cases_only = (
        F.broadcast(df)
        .join(existing_df, on='case_id', how='leftanti')
    )

    new_cases_only.persist()
    new_cases_only_count = new_cases_only.count()
    print(f"Original df contains {df_count} rows. New cases only df contains {new_cases_only_count} rows. \nThus, {df_count - new_cases_only_count} case_ids were already included in the destination df and thus will not be appended there.")

    # Save the data
    (
        new_cases_only
        .write
        .mode("append")
        .format('delta')
        .save(destination_path)
    )
    print("Saving successful")
    


# get current date as string
current_date = datetime.now().strftime("%Y-%m-%d")
current_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("Success: All utils functions and variables imported")
