Development guide
-----------------

.. _github: https://github.com/tomtom-internal/github-maps-analytics-tbt

Clone the source `github`_ repository in your local folder.

.. code-block:: sh
   
   git clone git@github.com:tomtom-internal/github-maps-analytics-tbt.git

Open the cloned root folder `github-maps-analytics-tbt` in Visual Studio Code (vscode) and then reopen in container. Make sure to have the following 
vscode extensions installed and also `docker` and `docker-compose` available.

+ ms-azuretools.vscode-docker
+ ms-vscode-remote.remote-containers

For security, credentials are not included in the repo, but on separate configuration files. To run the pipelines in this project, 
you must copy the following files to the project.

+ `credentials.yml <https://adlsmapsanalyticsmdbf.blob.core.windows.net/kedro/tbt/credentials.yml?sp=r&st=2022-11-22T11:54:51Z&se=2023-11-01T19:54:51Z&spr=https&sv=2021-06-08&sr=b&sig=PuGaZhgew4LMpVFiEHNQBum35oz3lJZNV2fn0Ls21n8%3D>`_ should be placed in `conf/local/credentials.yml` and `conf/dev/credentials.yml`
+ `secrets.yml <https://adlsmapsanalyticsmdbf.blob.core.windows.net/kedro/tbt/secrets.yml?sp=r&st=2022-12-05T14:28:22Z&se=2023-12-01T22:28:22Z&sv=2021-06-08&sr=b&sig=XqhgOH%2BAwNCzyOmRGqAVwJ2c6EnJRNntmjL0Q4oKHIs%3D>`_ should be placed in `conf/base/parameters/secrets.yml`


That's it! You should now be able to debug TbT and add new functionalities to the metric 🚀

Project structure
^^^^^^^^^^^^^^^^^^

We use `kedro <https://kedro.readthedocs.io/en/stable/index.html>`_ as the data pipeline framework for the TbT metric. 
Therefore, the folder tree of the TbT project follows kedro's structure, with some minor additions.

.. code-block:: zsh

   .
   ├── .devcontainer                # vscode configuration for docker-compose
   ├── .github                      
   │   └── workflows                # github actions
   ├── .vscode                      # vscode configuration for running kedro tasks and pipeliens
   ├── conf                         # Kedro configuration
   │   ├── base                     # Default kedro parameters and configuration (production)
   │   └── local                    # Kedro parameters, configuration and credentials (local - overwrites production options)
   ├── data                         # Kedro data folder ({MCP_BLOB_URL}/tmp/{run_id}/data/ in production)
   ├── docs
   │   └── source                   # Documentation .rst files to build with `kedro build-docs`
   ├── environment                  # docker image to run with docker-compose
   │   └── jupyter
   ├── logs                         # Kedro logs (when running locally)
   ├── notebooks                    # Jupyter notebooks
   │   ├── databricks               # Production notebooks to run from databricks jobs
   │   └── dev                      # Development jupyter notebooks to test locally
   ├── src                          # Project source code
   │   ├── tbt                      # TbT python library
   │   │   ├── pipelines            # Kedro pipelines for inspection, model train, export, scheduled tasks...
   │   │   ├── utils                # Common code (mostly comming from TbT) to use in several pipelines
   │   │   ├── pipeline_registry.py # Kedro pipeline definitions
   │   │   └── settings.py          # Kedro project settings
   │   └── tests                    # Unit tests
   ├── .gitignore                   # Global .gitignore file
   ├── docker-compose               # docker-compose yml (overiden by .devcontainer)
   ├── pyproject.toml               # Kedro .toml file (do not modify)
   ├── README.md                    # Github md documentation
   └── setup.cfg                    # Kedro .cfg file (do not modify)


Databricks notebook
^^^^^^^^^^^^^^^^^^^^

The notebook `notebooks/databricks/tbt-metric.py` runs the production pipeline of TbT in databricks. 

.. important::
   
   The production notebook `notebooks/databricks/tbt-metric.py` should not be edited in general. If we need to modify something, it 
   should be done in Databricks and then the changes committed and pushed from the Databricks UI.


There is a bash script `/dbfs/FileStore/tbt_metric/init_script.sh` that runs in the driver when the 
`Directions-TbT-job <https://adb-8671683240571497.17.azuredatabricks.net/?o=8671683240571497#job/537853824293819>`_ is triggered. 
That script should not be edited in general, unless the github token expires, and it can be overwritten with the following code running
from any cell in any notebook.

.. code-block:: python

   GITHUB_TOKEN = "ghp_******"
   GITHUB_USER = "JuanCarlosLaria-TomTom"
   init_script = f"""#!/bin/sh

   rm -r /root/projects/tbt
   git clone --single-branch --recurse-submodules --branch master  https://{GITHUB_USER}:{GITHUB_TOKEN}@github.com/tomtom-internal/github-maps-analytics-tbt.git /root/projects/tbt
   
   pip install  /root/projects/tbt/src

   """

   dbutils.notebook.shell.shell.system(f'echo "{init_script}" > /dbfs/FileStore/tbt_metric/init_script.sh')
