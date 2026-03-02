Data infrastructure
====================

By default, the TbT metric stores data in PostgreSQL (for MCP operations, visualization and dashboards) and in the MCP data lake (for historical, temporary data, and automatic validation). 

MCP Data Lake
--------------

There are two qualities of data stored in the MCP Data Lake (bronze layer)

1. Temporary output data from the kedro pipelines. 
    + Stored in `abfss://directions-metric@mcpengbronze.dfs.core.windows.net/tbt/tmp/{run_id}/data` and follows the same structure as the local `data/` folder.
    + Very useful to debug or restore past inspections.
    + It should be deleted from time to time to save costs.
    + Data is saved in spark parquet format.

2. Historical and live data for the TbT metric.
    + Stored in `abfss://directions-metric@mcpengbronze.dfs.core.windows.net/tbt/delta-tables/` as delta tables with the corresponding table name.

The credentials to access the MCP data lake are stored as Databricks secrets, and they can be loaded from any python cell running 
from the databricks workspace `MDBF <https://adb-8671683240571497.17.azuredatabricks.net/#ml/dashboard>`_. 
For example, the following cell access the `inspection_metadata` table stored in the MCP data lake.

.. code-block:: python

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

    spark.read.format('delta').load(f'{MCP_BLOB_URL}/delta-tables/inspection_metadata.delta').display()

PostgreSQL database
-------------------

The PostgreSQL database keeps the most recent historical data that is also stored in the MCP Data Lake (with some exceptions). 
The are some reasons for keeping a PostgreSQL "copy" of the data. 

1. Simpler access. Reports and dashboards can point to the PostgreSQL database and their connection to a conventional database is simpler than using object storages.
2. Geospatial queries. We can exploit the geospatial indexing with the postgis extension and make fast queries to the data, e.g. retrieving routes that are contained in a specific polygon.
3. QGIS access. MCP uses QGIS to edit the reviews, so it is easier to have a PostgreSQL table that MCP can edit (`error_logs`).

.. important::

    An exception we refer above is table `error_logs`, where column `error_type` is only available in PostgreSQL because it is a temporary table where MCP 
    updates their review. Once the manual inspection finishes, the information from `error_logs` is written into tables 
    `critical_sections_with_mcp_feedback` and `scheduled_error_logs_history` (that are both in PostgreSQL and the Data Lake) **and deleted from `error_logs`** in the PostgreSQL database. 
    The other exeption is table `scheduled_bootstrap` that contains the bootstrap resamplings and due to its size we prefer not to duplicate it in PostgreSQL and it is only available in the data lake.

The credentials to access the PostgreSQL database can be found in the 
`credentials.yml <https://adlsmapsanalyticsmdbf.blob.core.windows.net/kedro/tbt/credentials.yml?sp=r&st=2022-11-22T11:54:51Z&se=2023-11-01T19:54:51Z&spr=https&sv=2021-06-08&sr=b&sig=PuGaZhgew4LMpVFiEHNQBum35oz3lJZNV2fn0Ls21n8%3D>`_ 
file.
The Azure Database for PostgreSQL flexible server resource can be managed from the `Azure Portal <https://portal.azure.com/#@TomTomInternational.onmicrosoft.com/resource/subscriptions/4e4d81ed-6892-4815-abc9-595deab6b8d5/resourceGroups/MDBF/providers/Microsoft.DBforPostgreSQL/flexibleServers/directions-tbt/overview>`_.


Tables
-------

sampling_samples
^^^^^^^^^^^^^^^^^^

Contains the origins and destinations from TbT sampling processes.

.. image:: gfx/tbt_sampling_samples.png

sampling_metadata
^^^^^^^^^^^^^^^^^^^

Contains the metadata asociated with the sampling generation process. 

.. image:: gfx/tbt_sampling_metadata.png

inspection_metadata
^^^^^^^^^^^^^^^^^^^^^

Contains the metadata associated with each inspection. Includes the job input parameters, times, summary of the automatic part, and MCP related columns.

.. image:: gfx/tbt_inspection_metadata.png

The following columns are filled in by MCP once the manual inspection finishes.

+ completed: bool 
+ comment: text
+ mcp_start_date: date
+ mcp_end_date: date

.. important:: 

    MCP related columns are only available in the PostgreSQL copy of the data. The rest of the columns are available in both PostgreSQL and the MCP Data Lake.

inspection_routes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Keeps a record of the routes that were computed in an inspection (`run_id`). Competitor routes are reused from this table in future inspections.

.. image:: gfx/tbt_inspection_routes.png

inspection_critical_sections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Contains the geometries with RAC potential errors asociated with a route and whether FCD confirms that it is an error. 
Critical sections that were not previously evaluated will be shared with MCP.

.. image:: gfx/tbt_inspection_critical_sections.png

error_logs
^^^^^^^^^^^^

Contains the critical sections that MCP should evaluate manually. It includes only the latest (ongoing) inspections. Once MCP finishes, the information is moved to `scheduled_error_logs_history`.

.. image:: gfx/tbt_error_logs.png

The following columns are filled in by MCP

+ error_type: text. Possible values are discard, WDTRF, MMAN, SGEO, MBP, WGEO, MGSC, IMPLICIT, WRC.
+ review_date: date
+ evaluated_by: text
+ comment: text

scheduled_error_logs_history
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Historical data evaluated by MCP. Same schema as `error_logs`.

critical_sections_with_mcp_feedback
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Potential errors that have already been evaluated by MCP and their correct type. It is used by the metric as a truth data source. Some cases are not actually reviewed by MCP, but they are duplicated of a case that MCP did review.

.. image:: gfx/tbt_critical_sections_with_mcp_feedback.png

scheduled_metrics
^^^^^^^^^^^^^^^^^^^

Computed metrics, after manual validation.

.. image:: gfx/tbt_scheduled_metrics.png


scheduled_bootstrap
^^^^^^^^^^^^^^^^^^^^^

Stores the bootstrap resampling results that were used to compute the confidence intervals of the metric.

.. image:: gfx/tbt_scheduled_bootstrap.png


aggregation_ww
^^^^^^^^^^

This is a table has all worldwide aggregations. It basically stores the results from the aggregation_ww pipeline.

It is only available in postgres.

.. image:: gfx/aggregation_ww.png


aggregation_weights
^^^^^^^^^^

This is a table contains all the country weights and it is used by aggregation_ww pipeline.

It is only available in postgres.

.. image:: gfx/aggregation_weights.png

M14Scope
^^^^^^^^^^

This is a table inherited from the legacy sampling that is still in use by TbT sampling process. It contains the relation 
of Morton tiles at zoom 14 that are part of each country and their MQS (Q1, Q2, ... Q5).

It is located at `abfss://directions-metric@mcpengbronze.dfs.core.windows.net/TbT/adlsmapsanalyticsmdbf_backup/gold/M14Scope`.

.. image:: gfx/tbt_m14scope.png


Code snippets
--------------

Retrieve `scheduled_metrics` and `scheduled_bootstrap` from the Data Lake.

.. code:: python

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

    spark.read.format('delta').load(f'{MCP_BLOB_URL}/delta-tables/scheduled_metrics.delta').display()
    spark.read.format('delta').load(f'{MCP_BLOB_URL}/delta-tables/scheduled_bootstrap.delta').display()

Retrieve `scheduled_metrics` from Postgres.

.. code:: python

    import pandas
    import sqlalchemy
    import json

    AZ_DB = json.loads(dbutils.secrets.get(scope = "utils", key = "db_credentials_tbt"))
    engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{AZ_DB["user"]}:{AZ_DB["password"]}@{AZ_DB["host"]}:{AZ_DB["port"]}/{AZ_DB["database"]}')

    pandas.read_sql_query('select * from scheduled_metrics', engine)

