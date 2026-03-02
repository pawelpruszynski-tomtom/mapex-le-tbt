# Databricks notebook source
# MAGIC %md
# MAGIC # Error Classification Model training
# MAGIC Required runtime version: **13.3.x-cpu-ml-scala2.12**
# MAGIC
# MAGIC Technical documentation provided here: https://confluence.tomtomgroup.com/display/MANA/%5BDocumentation%5D+EC+Model+Training 
# MAGIC
# MAGIC This notebook is used to train an Error classification model for TbT/HDR. 
# MAGIC It is designed in a way that the only user input required can be set in the section -- USER INPUT --. Then, the full notebook can be run to add one model with the required specifications to MLflow. 
# MAGIC
# MAGIC
# MAGIC Steps of the notebook:
# MAGIC - **Custom user input:** Define options for model training
# MAGIC - Load master table data from delta table and sanity checks
# MAGIC - Select relevant variables for model training and correct pre-processing steps for different variable type
# MAGIC - Train model and log model, metrics, plots, tables and results to the correct MLflow experiment.
# MAGIC - TODO: launch differerent experiments with different parameters (hyper-parameters tuning)
# MAGIC
# MAGIC The outcome of this notebook is one trained model logged to MLflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Libraries

# COMMAND ----------

# MAGIC %pip install sqlalchemy==1.4.39
# MAGIC %pip install mlflow==2.5.0
# MAGIC %pip install cloudpickle==2.0.0
# MAGIC %pip install configparser==5.2.0
# MAGIC %pip install pandas==1.4.4
# MAGIC %pip install psutil==5.9.0
# MAGIC %pip install scikit-learn==1.1.1
# MAGIC %pip install typing-extensions==4.10.0
# MAGIC %pip install xgboost==1.4.2
# MAGIC %pip install shap==0.39.0
# MAGIC %pip install numba==0.55.0
# MAGIC %pip install matplotlib==3.4.2

# COMMAND ----------

!pwd

# COMMAND ----------

# general
import os
import gc
import uuid
import shutil
import numpy as np
import pandas as pd
import copy
import logging
from datetime import datetime, timedelta
import pytz
import tempfile

# visualization
import matplotlib.pyplot as plt
from matplotlib.figure import Figure

# MLflow
import mlflow
from mlflow.tracking import MlflowClient

# sklearn pipelines and pre-processing
import sklearn
from sklearn import set_config
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler, FunctionTransformer
from sklearn.model_selection import train_test_split

# sklearn calibration
from sklearn.isotonic import IsotonicRegression
from sklearn.calibration import calibration_curve, CalibratedClassifierCV

# sklearn metrics
from sklearn.metrics import (
    accuracy_score, recall_score, f1_score, roc_auc_score, 
    precision_score, plot_precision_recall_curve, average_precision_score,
    confusion_matrix, ConfusionMatrixDisplay)

# models
from xgboost import XGBClassifier
# from sklearn.ensemble import RandomForestClassifier
# import lightgbm
# from lightgbm import LGBMClassifier
# import catboost 
# from catboost import CatBoostClassifier

# explainability
import shap
from shap import KernelExplainer, TreeExplainer, summary_plot

print(f"MLflow version: {mlflow.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC # -- USER INPUT --: Set options for model training

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow setup
# MAGIC
# MAGIC **Fixed parameters**
# MAGIC - mlflow_tracking_uri: `[databricks]`
# MAGIC   - this should not be changed, as we always want to log to databricks
# MAGIC - mlflow_experiment_name `[/Shared/tbt_error_classification/ml_experiments/tbt_binary_classification]`:
# MAGIC   - this points to the experiment where all EC models are logged
# MAGIC   - In the future, potentially use:
# MAGIC     - `[/Shared/tbt_error_classification/ml_experiments/tbt_binary_classification]` for TbT models
# MAGIC     - `[/Shared/tbt_error_classification/ml_experiments/hdr_binary_classification]` for HDR models
# MAGIC
# MAGIC **Changeable Parameters:** 
# MAGIC - environment: `[dev, prod, test]`
# MAGIC   - Added as a tag to MLflow.
# MAGIC - model_version: 
# MAGIC   - Same as release tags for repos: 
# MAGIC     - e.g. ``1.2352.0``: model version 1, year 23, week 51, trial run 0. 
# MAGIC   - Added as tag to MLflow

# COMMAND ----------

mlflow_tracking_uri = "databricks"
mlflow_experiment_name = '/Shared/tbt_error_classification/ml_experiments/tbt_binary_classification'
environment = 'dev'
model_version = dbutils.widgets.get("model_version")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training data
# MAGIC Define the path of the training master table. 

# COMMAND ----------

training_master_table = dbutils.widgets.get("training_master_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Training data Subset
# MAGIC A model can also be trained on a data subset only. 
# MAGIC Possible configurations are: 
# MAGIC
# MAGIC - Metric: `[all, TbT, HDR]`
# MAGIC - FCD: `[all, FCD_available]`

# COMMAND ----------

data_subset_metric = dbutils.widgets.get("data_subset_metric")
data_subset_FCD = dbutils.widgets.get("data_subset_FCD")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Training Features
# MAGIC Define the feature categories that should be used. 
# MAGIC
# MAGIC Choose from:
# MAGIC `general,geometric,fcd_old,fcd_new,sdo`
# MAGIC
# MAGIC They must be specified in a string like above. 

# COMMAND ----------

training_features_string = dbutils.widgets.get("training_features")
training_features = training_features_string.split(',')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train on subsample
# MAGIC Define if the model should be trained on a subsample of the full data before training on the full data. This might be useful for testing purposes, as it accelerates model training. 

# COMMAND ----------

train_on_subsample = True if dbutils.widgets.get("train_on_subsample") == "True" else False

if train_on_subsample:
    training_sample_frac = 0.1
else:
    training_sample_frac = 1.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessing
# MAGIC 1. Define if you want to preprocess new FCD features so that rows with the following conditions: 
# MAGIC - tot_new == 0: all features except tot_contained are set to -2
# MAGIC - if tot_contained == 0: contained features are set to -2
# MAGIC
# MAGIC 2. Define if you want to rescale all feature columns to be centered around zero with unit variance.
# MAGIC

# COMMAND ----------

fcd_feature_preprocessing = True if dbutils.widgets.get("fcd_feature_preprocessing") == "True" else False
standardize_numerical = True if dbutils.widgets.get("standardize_numerical") == "True" else False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train - Validation - Test Split
# MAGIC We want to split the input data into training, validation and test data, stratified by y
# MAGIC
# MAGIC Typically, we want: 
# MAGIC
# MAGIC - Train (70% of the dataset used to train the model)
# MAGIC - Validation (20% of the dataset used to tune the hyperparameters of the model)
# MAGIC - Test (10% of the dataset used to report the true performance of the model on an unseen dataset)
# MAGIC
# MAGIC Here we define a flexible splitting strategy, so we can do 2 (train-val) or 3 splits (train-val-test)
# MAGIC
# MAGIC Typical train-val-test split:
# MAGIC
# MAGIC     split_weights = {
# MAGIC         'train':0.7,
# MAGIC         'val':0.2,
# MAGIC         'test':0.1}
# MAGIC
# MAGIC
# MAGIC Typical train-val split: 
# MAGIC
# MAGIC     split_weights = {
# MAGIC         'train':0.8,
# MAGIC         'val':0.2,
# MAGIC         'test':0.0}

# COMMAND ----------

split_weights = {
    'train':0.7,
    'val':0.2,
    'test':0.1
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Costs
# MAGIC Our Objective function allows to allocate costs to missed real errors, and gains on saved errors. 
# MAGIC
# MAGIC The baseline costs are:
# MAGIC
# MAGIC     saved_error_cost = 2
# MAGIC     missed_error_cost = 10
# MAGIC
# MAGIC However, you can experiment with different costs.
# MAGIC
# MAGIC Currently the threshold is not optimized based on costs yet, but the total benefit is only included as a column in the CSV logged to MLflow

# COMMAND ----------

saved_error_cost = 2
missed_error_cost = 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model & Model Parameters
# MAGIC Define Model and its parameters. 
# MAGIC
# MAGIC In theory, the architecture of the model is flexible enough to train multiple model types with one run (e.g. XGBoost, logistic regression, naive bayes, etc.), given that other options specified above do not change. However, if parameters are specified, it gets a bit messy. Currently, only XGBClassifier works. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Parameters
# MAGIC There are four options to define model parameters ``[none, custom, load, gridsearch]``:
# MAGIC - ``none`` Define no parameters at all, the model will be called with default settings
# MAGIC     - Caution: This might lead to the output of predict_proba being outside the range [0,1]
# MAGIC - ``custom`` Define custom parameters in a dictionary
# MAGIC - ``load`` Load parameters from a previous model
# MAGIC - ``gridsearch`` (TODO) Find best parameters via hyperparameter optimization Gridsearch

# COMMAND ----------


model_params_option = dbutils.widgets.get("model_params_option")



if model_params_option == "none":
    model_params = None

elif model_params_option == "custom":
    # define custom parameters
    model_params = {}
    model_params['objective'] = "binary:logistic"
    model_params['learning_rate'] = 0.3
    model_params['max_depth'] = 5
    model_params['min_child_weight'] = 1.2
    model_params['gamma'] = 0.01
    model_params['max_delta_step'] = 1 

elif model_params_option == "load":
    # set _id to run_id of the model that you want to load
    _id = '82395780afab4ec6bc3ee4c5736ae87f' # 5f3606a43bc5458390d039771a90fb6a
    model_loc = f"runs:/{_id}/model"

    loaded_model = mlflow.sklearn.load_model(model_loc)
    model_params = loaded_model.steps[-1][1].get_params()
    model_params['class_weight'] = None


# COMMAND ----------

# MAGIC %md
# MAGIC ### Model
# MAGIC Define what model type(s) you want to train
# MAGIC
# MAGIC Currently, only XGBClassifier works. 
# MAGIC
# MAGIC TODO: This needs adjustment if more than one model is trained, as different models need different parameters. 

# COMMAND ----------

models = {
    "model0":{
        "model":XGBClassifier(**model_params)
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calibration
# MAGIC Define if model should be calibrated or not
# MAGIC If True, the classifier will be wrapped in sklearn's CalibratedClassifierCV class. 
# MAGIC
# MAGIC See docs: https://scikit-learn.org/stable/modules/generated/sklearn.calibration.CalibratedClassifierCV.html

# COMMAND ----------

add_calibration = True if dbutils.widgets.get("add_calibration") == "True" else False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature importance and explanability
# MAGIC
# MAGIC SHAP is a game-theoretic approach to explain machine learning models, providing a summary plot
# MAGIC of the relationship between features and model output. Features are ranked in descending order of
# MAGIC importance, and impact/color describe the correlation between the feature and the target variable.
# MAGIC - Generating SHAP feature importance is a very memory intensive operation, so to ensure that AutoML can run trials without
# MAGIC   running out of memory, we disable SHAP by default.<br />
# MAGIC   You can set the flag defined below to `shap_enabled = True` and re-run this notebook to see the SHAP plots.
# MAGIC - To reduce the computational overhead of each trial, a single example is sampled from the validation set to explain.<br />
# MAGIC   For more thorough results, increase the sample size of explanations, or provide your own examples to explain.
# MAGIC - SHAP cannot explain models using data with nulls; if your dataset has any, both the background data and
# MAGIC   examples to explain will be imputed using the mode (most frequent values). This affects the computed
# MAGIC   SHAP values, as the imputed samples may not match the actual data distribution.
# MAGIC
# MAGIC For more information on how to read Shapley values, see the [SHAP documentation](https://shap.readthedocs.io/en/latest/example_notebooks/overviews/An%20introduction%20to%20explainable%20AI%20with%20Shapley%20values.html).

# COMMAND ----------

shap_enabled = True if dbutils.widgets.get("shap_enabled") == "True" else False
shap_sample_size = int(dbutils.widgets.get("shap_sample_size"))

# COMMAND ----------

# MAGIC %md
# MAGIC # -- END USER INPUT --

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %run "../utils"

# COMMAND ----------

# MAGIC %run "../define_table_schema"

# COMMAND ----------

# configure sklearn so that estimators will be displayed as a diagram
sklearn.set_config(display="diagram")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging
# MAGIC The log is added to MLflow as an artifact. 

# COMMAND ----------

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

class Logging():
    """
    Helper class to beautify logs and make them more useful.
    Checkpoints are the steps in the notebook
    Comments are used to log comments and values of variables at different steps, and with dub=True,
    you just need to log the variable name and it will be logged as:
    "variable_name": value
    """
    def __init__(self, timezone = 'Europe/Madrid'):
        self.timezone = timezone
        self.tz = pytz.timezone(self.timezone)
        self.timestamps = []
        self.checkpoints = []
        self.comments = []
        
    def _get_timestamp(self):
        timestamp = '[{} ({})]'.format(datetime.now(tz = self.tz).strftime('%Y-%m-%d %H:%M'), self.timezone)
        return timestamp
        
    def checkpoint(self, checkpoint, end = '\n'):
        self.timestamps += [self._get_timestamp()]
        self.checkpoints += [checkpoint.lower()]
        self.comments += [[]]
        print('{} {}'.format(self.timestamps[-1], self.checkpoints[-1]), end = end)
        
    # INTERESTING
    def comment(self, comments, dub = False, timestamp = False, end = '\n'):
        if type(comments) == str:
            comments = [comments]
        for comment in comments:
            if dub:
                comment = '{}: {}'.format(comment, eval(comment))
            if timestamp:
                comment = '{} {}'.format(self._get_timestamp(), comment) if timestamp else comment
            self.comments[-1] += [comment]
            print(comment, end = end)
        
    # INTERESTING
    def history(self, print_output = True):
        output = 'LOG HISTORY\n'
        for count_checkpoint, (timestamp, checkpoint, comments) in enumerate(zip(self.timestamps, self.checkpoints, self.comments)):
            last_checkpoint = count_checkpoint == len(self.checkpoints) - 1
            connector_checkpoint = '└->' if last_checkpoint else '|->'
            output += '{} {} -> {}\n'.format(connector_checkpoint, timestamp, checkpoint)
            for i, comment in enumerate(comments):
                leftmost_char = ' ' if last_checkpoint else '|'
                empty_spaces = ' ' * (len(timestamp) + 5)
                last_comment = i == len(comments) - 1
                connector_comment = '└->' if last_comment else '|->'
                output += '{} {} {} {}\n'.format(leftmost_char, empty_spaces, connector_comment, comment)
        print(output) if print_output else None
        return output

# COMMAND ----------

log = Logging()
log.checkpoint('Training Notebook - start')

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow

# COMMAND ----------

# connect to mlflow experiment
mlflow.set_tracking_uri(mlflow_tracking_uri) 
mlflow.set_experiment(experiment_name = mlflow_experiment_name)

experiment_id = mlflow.get_experiment_by_name(mlflow_experiment_name).experiment_id
artifact_location = mlflow.get_experiment_by_name(mlflow_experiment_name).artifact_location
log.comment(['experiment_id', 'mlflow_experiment_name', 'artifact_location'], dub = True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data

# COMMAND ----------

# random state for random processes
random_state=23452345

# COMMAND ----------

# General features
general_features = [
    'drive_right_side',
    'is_tbt',
]

# Geometric features
geometric_features = [
    'route_length',
    'stretch_length',
    'stretch_start_position_in_route',
    'stretch_end_position_in_route',
    'stretch_starts_at_the_route_start',
    'stretch_ends_at_the_route_end',
    'heading_stretch',
    'left_turns',
    'straight_turns',
    'right_turns',
    'total_right_angle',
    'max_right_angle',
    'total_left_angle',
    'max_left_angle',
    'absolute_angle',
    'has_left_turns',
    'has_right_turns',
    'min_curvature',
    'max_curvature',
    'mean_curvature',
    'distance_start_end_stretch',
    'route_coverage',
    'stretch_covers_route',
    'tortuosity',
    'sinuosity',
    'density',
]

# FCD features (OLD)
fcd_features_old = [
    'pra',
    'prb',
    'prab',
    'lift',
    'tot',
]

# FCD features (NEW)
fcd_features_new = [
    "tot_new",
    "pra_new",
    "prb_new",
    "pra_not_b",
    "prb_not_a",
    "pra_and_b",
    "pra_to_b", 
    "prb_to_a", 
    "tot_contained",
    "pra_to_b_contained", 
    "prb_to_a_contained", 
    "pra_to_b_not_contained",
    "prb_to_a_not_contained",
    "traffic_direction",
    "traffic_direction_contained",
    "ab_intersect",
]

# SDO features
sdo_features = [
    'CONSTRUCTION_AHEAD_TT',
    'MANDATORY_STRAIGHT_ONLY',
    'MANDATORY_STRAIGHT_OR_LEFT',
    'MANDATORY_TURN_RESTRICTION',
    'MANDATORY_TURN_LEFT_ONLY',
    'MANDATORY_TURN_LEFT_OR_RIGHT',
    'MANDATORY_LEFT_OR_STRAIGHT_OR_RIGHT',
    'MANDATORY_TURN_RIGHT_ONLY',
    'MANDATORY_STRAIGHT_OR_RIGHT',
    'NO_ENTRY',
    'NO_MOTOR_VEHICLE',
    'NO_CAR_OR_BIKE',
    'NO_LEFT_OR_RIGHT_TT',
    'NO_LEFT_TURN',
    'NO_RIGHT_TURN',
    'NO_VEHICLE',
    'NO_STRAIGHT_OR_LEFT_TT',
    'NO_STRAIGHT_OR_RIGHT_TT',
    'NO_STRAIGHT_TT',
    'NO_TURN_TT',
    'NO_U_OR_LEFT_TURN',
    'NO_U_TURN',
    'ONEWAY_TRAFFIC_TO_STRAIGHT',
]

feature_cols = []
# Check the parameters and concatenate the feature lists accordingly
if 'general' in training_features:
    feature_cols += general_features
if 'geometric' in training_features:
    feature_cols += geometric_features
if 'fcd_old' in training_features:
    feature_cols += fcd_features_old
if 'fcd_new' in training_features:
    feature_cols += fcd_features_new
if 'sdo' in training_features:
    feature_cols += sdo_features


# define explicitly which features are categorical and numerical
numerical_features = feature_cols
categorical_features = []

# target column for binary classification
target_col = "has_error"

# when loading data from spark/delta table, read only these columns
input_columns = feature_cols + [target_col]

# COMMAND ----------

n_features = len(feature_cols)
n_categorical_features = len(categorical_features)
n_numeric_features = n_features - n_categorical_features

log.comment(['n_features', 'n_numeric_features', 'n_categorical_features'], dub = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

environment = dbutils.widgets.get("environment")
if environment == 'dev':
    storage_account_name_dev = 'stdastdirectionsmadev'
    blob_container_name_dev = 'devcontainer'
    client_id_dev = os.getenv('AZURE_STORAGE_CLIENT_ID')
    client_secret_dev = os.getenv('AZURE_STORAGE_CLIENT_SECRET')
    client_tenant_dev = os.getenv('AZURE_STORAGE_TENANT_ID')
    
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name_dev}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name_dev}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name_dev}.dfs.core.windows.net", f"{client_id_dev}")
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name_dev}.dfs.core.windows.net", f"{client_secret_dev}")
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name_dev}.dfs.core.windows.net", f"https://login.microsoftonline.com/{client_tenant_dev}/oauth2/token")

elif environment == 'prod':
    storage_account_name_prod = 'stdastdirectionsmaprod'
    blob_container_name_prod = 'devcontainer'
    client_id_pro = os.getenv('AZURE_STORAGE_CLIENT_ID')
    client_secret_pro = os.getenv('AZURE_STORAGE_CLIENT_SECRET')
    client_tenant_pro = os.getenv('AZURE_STORAGE_TENANT_ID')

    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name_prod}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name_prod}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name_prod}.dfs.core.windows.net", f"{client_id_pro}")
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name_prod}.dfs.core.windows.net", f"{client_secret_pro}")
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name_prod}.dfs.core.windows.net", f"https://login.microsoftonline.com/{client_tenant_pro}/oauth2/token")

else:
    raise ValueError("Environment provided unknown.")

try:
    # for hive metastore tables
    master_table = spark.read.table(training_master_table)
except Exception:
    # for custom delta tables
    master_table = spark.read.format("delta").load(training_master_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check data

# COMMAND ----------

# check if the required input_columns are all existing in the training master table, otherwise throw an error
assert all([c in master_table.columns for c in input_columns]), "Missing columns:{}".format([c for c in input_columns if c not in master_table.columns])

# {c.name:c.dataType for c in master_table.schema}

# COMMAND ----------

# select only the required columns and move to pandas
# e.g. some columns in spark df (e.g. traffic_signs:map) cannot be loaded into pandas directly
df_loaded = master_table.select(input_columns).toPandas()
df_loaded.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove rows with inf values from df_loaded and raise error if they exceed 1% of the data
# MAGIC

# COMMAND ----------

# Remove rows with inf values from df_loaded and raise error if they exceed 1% of the data
original_count = len(df_loaded)
df_loaded = df_loaded.replace([np.inf, -np.inf], np.nan)
removed_count = df_loaded.isna().any(axis=1).sum()
if removed_count / original_count > 0.01:
    raise ValueError("Removed data with inf values exceeds 1% of the total data.")
df_loaded = df_loaded.dropna()
print(f"Removed {removed_count} rows ({(removed_count / original_count) * 100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FCD Feature engineering (optional)

# COMMAND ----------

if fcd_feature_preprocessing: 

    def fcd_feature_preprocessing(row):
        new_fcd_features = [
            "pra_new", "prb_new", "pra_not_b", "prb_not_a", "pra_and_b", "pra_to_b", 
            "prb_to_a", "pra_to_b_contained", "prb_to_a_contained", "pra_to_b_not_contained",
            "prb_to_a_not_contained", "traffic_direction", "traffic_direction_contained"
        ]
        new_fcd_features_contained = [
            "pra_to_b_contained", "prb_to_a_contained", "pra_to_b_not_contained",
            "prb_to_a_not_contained", "traffic_direction_contained"
        ]

        if row['tot_new'] == 0:
            for column in new_fcd_features:
                row[column] = -2.0

        if row['tot_contained'] < 6:
            for column in new_fcd_features_contained:
                row[column] = -2.0

        return row

    df_loaded = df_loaded.apply(fcd_feature_preprocessing, axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use subset of training data only
# MAGIC

# COMMAND ----------

# potentially filter for metric subset
if data_subset_metric == "TbT":
    df_loaded = df_loaded.loc[df_loaded["is_tbt"] == 1]
elif data_subset_metric == "HDR": 
    df_loaded = df_loaded.loc[df_loaded["is_tbt"] == 0]

# potentially filter for FCD subset
if data_subset_FCD == "FCD_available":
    df_loaded = df_loaded[(df_loaded['tot'] > 0)]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data sanity checks
# MAGIC - target column distribution
# MAGIC - empty variables/ 0 variance 
# MAGIC - (monovariate) outliers - TBD

# COMMAND ----------

target_col_distribution = pd.concat([
    df_loaded[target_col].value_counts(dropna=False), 
    df_loaded[target_col].value_counts(normalize=True, dropna=False)
    ], axis=1) 
target_col_distribution

# COMMAND ----------

df_loaded.describe()

# COMMAND ----------

feature_vars = df_loaded[feature_cols].var()
feature_nunique = df_loaded[feature_cols].nunique()

feature_nans = df_loaded[feature_cols].isna().sum()

df_feature_stats = pd.DataFrame({
    'unique_values':feature_nunique, 
    'variance':feature_vars, 
    'nulls':feature_nans,
    'nulls_fraction':feature_nans/df_loaded[feature_cols].count() })
    
df_feature_stats

# COMMAND ----------

# identify columns that should be discarded
zero_variance_cols = list(df_feature_stats.index[df_feature_stats['variance']==0])
log.comment(['zero_variance_cols'], dub=True)

high_nans_fraction_cols = list(df_feature_stats.index[df_feature_stats['nulls_fraction']>0.90])
log.comment(['high_nans_fraction_cols'], dub=True)

columns_to_exclude = list(set(zero_variance_cols + high_nans_fraction_cols))

log.comment(['columns_to_exclude'], dub=True)

# prepare lists for feature columns
if len(columns_to_exclude):
    feature_cols = [c for c in feature_cols if c not in columns_to_exclude]
    numerical_features = [c for c in numerical_features if c not in columns_to_exclude]
    categorical_features = [c for c in categorical_features if c not in columns_to_exclude]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data characteristics to log to MLflow
# MAGIC TODO

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessessing

# COMMAND ----------

# initiate list for potential data transformers/preprocessors
transformers = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Impute missing values (NaNs)
# MAGIC Potential strategies: 
# MAGIC - impute with mean value, median value or most frequent value
# MAGIC - impute with 0
# MAGIC - impute with negative value (e.g. -1), if difference btw 0 and null is meaningful (e.g. traffic signs)
# MAGIC
# MAGIC For now we use: 
# MAGIC - Traffic signs are imputed -1
# MAGIC - Other numerical values are imputed with mean

# COMMAND ----------

# impute NaNs for traffic sign
dict_fillna = {
'CONSTRUCTION_AHEAD_TT':-1,
'MANDATORY_STRAIGHT_ONLY':-1,
'MANDATORY_STRAIGHT_OR_LEFT':-1,
'MANDATORY_TURN_RESTRICTION':-1,
'MANDATORY_TURN_LEFT_ONLY':-1,
'MANDATORY_TURN_LEFT_OR_RIGHT':-1,
'MANDATORY_LEFT_OR_STRAIGHT_OR_RIGHT':-1,
'MANDATORY_TURN_RIGHT_ONLY':-1,
'MANDATORY_STRAIGHT_OR_RIGHT':-1,
'NO_ENTRY':-1,
'NO_MOTOR_VEHICLE':-1,
'NO_CAR_OR_BIKE':-1,
'NO_LEFT_OR_RIGHT_TT':-1,
'NO_LEFT_TURN':-1,
'NO_RIGHT_TURN':-1,
'NO_VEHICLE':-1,
'NO_STRAIGHT_OR_LEFT_TT':-1,
'NO_STRAIGHT_OR_RIGHT_TT':-1,
'NO_STRAIGHT_TT':-1,
'NO_TURN_TT':-1,
'NO_U_OR_LEFT_TURN':-1,
'NO_U_TURN':-1,
'ONEWAY_TRAFFIC_TO_STRAIGHT':-1,
}

df_loaded = df_loaded.fillna(dict_fillna)

# impute other numerical
numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors="coerce"))),
    ("imputer", SimpleImputer(strategy="mean"))
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature standardization

# COMMAND ----------

if standardize_numerical:
    standardizer = StandardScaler()
else:
    standardizer = None

numerical_pipeline.steps.append(("standardizer", standardizer))

# add numerical pipeline to the transformers
transformers.append(("numerical", numerical_pipeline, numerical_features))

# COMMAND ----------

# combine all pre-processing steps for different column types:
preprocessor = ColumnTransformer(transformers, remainder="passthrough", sparse_threshold=0)

# COMMAND ----------

# MAGIC %md
# MAGIC # Train - Validation - Test Split
# MAGIC Split the input data into training, validation and test data, stratified by y
# MAGIC
# MAGIC Here we define a flexible splitting strategy, so we can do 2 (train-val) or 3 splits (train-val-test)

# COMMAND ----------

# initialize convenience containers for indices for splits, Xs and ys
idxs = {}
xs = {}
ys = {}
y_means = {}

# split data in 'train', 'val', 'test'
if split_weights.get('val', 0) and split_weights.get('test', 0):
    df_alias =  ['train', 'val', 'test']

    # split train and test=(test+valid) from entire data set
    x_train, x_test, y_train, y_test = train_test_split(df_loaded[feature_cols], df_loaded[target_col], test_size=1-split_weights['train'], random_state=random_state, stratify=df_loaded[target_col]) 

    # split test into validation and test
    x_val, x_test, y_val, y_test = train_test_split(x_test, y_test, test_size=split_weights['test']/(split_weights['test'] + split_weights['val']), stratify=y_test, random_state=random_state) 

# split only train and val
elif (
    (split_weights.get('val', 0) and split_weights.get('test', 0) == 0) 
    or (split_weights.get('test', 0) and split_weights.get('val', 0) == 0)
):
    df_alias =  ['train', 'val']
    x_train, x_val, y_train, y_val = train_test_split(df_loaded[feature_cols], df_loaded[target_col], test_size=split_weights['val'], stratify=df_loaded[target_col], random_state=random_state)


# COMMAND ----------

for alias in df_alias:
    # assign to xs, ys, idxs containers
    xs[alias] = eval('x_'+alias)
    idxs[alias]=xs[alias].index
    ys[alias] = eval('y_'+alias)
    y_means[alias] = ys[alias].mean()
    
    # check that indices are indeed the same for xs and ys
    assert(ys[alias].index.all() == idxs[alias].all())

    print(f"{alias}: n:{len(xs[alias])}, pct: {round(len(xs[alias])/df_loaded.shape[0],3)}, y_sum: {ys[alias].sum()}, y_mean: {round(y_means[alias], 4)}")



# COMMAND ----------

# Garbage collector
while gc.collect()>0: pass

# COMMAND ----------

# MAGIC %md
# MAGIC # Start MLflow run
# MAGIC
# MAGIC mlflow.autolog automatically calls the autolog function for the specific library/model used (e.g. ``mlflow.sklearn.autolog()``, or ``mlflow.xgboost.autolog()``)
# MAGIC
# MAGIC - MLflow autolog docs: https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.autolog 
# MAGIC - MLflow sklearn docs: https://mlflow.org/docs/latest/python_api/mlflow.sklearn.html 
# MAGIC - MLflow xgboost docs: https://mlflow.org/docs/latest/python_api/mlflow.xgboost.html

# COMMAND ----------

# Enable automatic logging with specific configurations.
mlflow.sklearn.autolog(
    log_input_examples=True, # Log the first few input examples
    silent=True,             # Do not throw exceptions on autologging failure
    exclusive=False,         # Log all information MLflow can track
    log_models=True,         # Log the serialized model after training
    disable=False            # Enable the autologging functionality
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions for Model training

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calibration function

# COMMAND ----------

def calibration_plot(ys, probas):
    """
    Create calibration plots for given predictions and true labels.

    Parameters:
    - ys (dict): A dictionary mapping dataset aliases (e.g., 'train', 'val', 'test') to the true labels.
    - probas (dict): A dictionary mapping dataset aliases to predicted probabilities for the positive class.

    Returns:
    - matplotlib.figure.Figure: The figure object containing the calibration plots.

    The function plots two rows of subplots. The top row shows histograms of predicted probabilities
    and the bottom row shows calibration curves (reliability diagrams). Each column corresponds to
    a different dataset alias (e.g., 'train', 'val', 'test').

    A calibration curve shows the relationship between the predicted probabilities and the observed
    frequencies. Perfect calibration is indicated by a diagonal line from (0,0) to (1,1). The closer
    the red line (calibration curve) is to the diagonal, the better calibrated the predictions are.
    """

    # Create a subplot grid of size 2xN where N is the number of alias keys in probas
    fig, axs = plt.subplots(2, len(probas), figsize=(9, 6))

    # Loop through the dataset aliases and their corresponding predictions
    for i, alias in enumerate(probas.keys()):
        # Compute calibration curve for the current alias
        cal_y, cal_x = calibration_curve(ys[alias], probas[alias], n_bins=20, strategy='quantile')
        
        # Plot histogram of predicted probabilities for the current alias
        axs[0, i].hist(probas[alias], bins=100, density=True)
        
        # Plot the diagonal line (perfect calibration) and calibration curve
        axs[1, i].plot([0, 1], [0, 1], color='grey')
        axs[1, i].plot(cal_x, cal_y, color='red')
        
        # Set labels and title
        axs[1, i].set_xlabel('Score')
        axs[0, i].title.set_text(alias)

    # Set the x-axis limits for the histograms and calibration curves
    xlim = 0, max([np.quantile(probas[alias], .999) for alias in probas.keys()])
    [(axs[0, i].set_xlim(xlim), axs[1, i].set_xlim(xlim), axs[1, i].set_ylim(xlim)) for i in range(len(probas.keys()))]

    # Set the y-axis limits for the histograms
    ylim = 0, max([axs[0, i].get_ylim()[1] for i in range(len(probas.keys()))])
    [axs[0, i].set_ylim(ylim) for i in range(len(probas.keys()))]

    # Set the y-axis labels for the first column of subplots
    axs[0, 0].set_ylabel('Density')
    axs[1, 0].set_ylabel('Observed frequency')

    # Return the figure containing the calibration plots
    return fig

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objective function
# MAGIC
# MAGIC Custom model evaluation metrics:
# MAGIC
# MAGIC The objective function for this model is maximizing Specificity constrained to at minimum positive class recall (>= 0.95 or 0.98-0.99%).
# MAGIC
# MAGIC **Recall (aka Sensitivity):** is the ratio of true positive predictions to the actual positives (aka actual errors). It is a measure of a classifier's ability to identify true errors. A recall of 0.98, for instance, indicates that 98% of the actual errors cases were correctly identified by the model.
# MAGIC
# MAGIC $$
# MAGIC \text{Recall} = \frac{\text{True Positives}}{\text{True Positives} + \text{False Negatives}} 
# MAGIC $$
# MAGIC
# MAGIC **Negative Recall (aka Specificity):** Specificity is the ratio of true negative predictions to the actual negatives. It is a measure of a classifier's ability to correctly identify negative cases, aka true no_errors. The higher the specificity, the more true no_errors can be discarded, saving MCP tasks. 
# MAGIC
# MAGIC $$
# MAGIC \text{Specificity} = \frac{\text{True Negatives}}{\text{True Negatives} + \text{False Positives}}
# MAGIC $$
# MAGIC
# MAGIC **On one hand, we are aiming for minimum 98% recall, meaning that we would only miss 2% of the actual errors. On the other hand, we want to have as high a negative recall as possible, cause that's the tasks that we are saving from manual validation.**
# MAGIC
# MAGIC ### Objective Function Documentation
# MAGIC
# MAGIC The objective function of our model is designed to maximize the specificity metric while ensuring that the recall for the positive class remains above a predefined threshold (e.g., >= 0.95 or 0.98-0.99%). This means we are focusing on correctly identifying as many of the true negative cases as possible without significantly compromising our ability to detect true positive cases.
# MAGIC
# MAGIC The `iterate_threshold` function listed below evaluates a series of potential threshold values for classification. For each threshold, it computes a suite of metrics, allowing us to inspect the trade-offs between different evaluation criteria and select a threshold that best meets our model’s objectives.
# MAGIC
# MAGIC To apply this objective function, we call the `find_optimal_thresh` function with the dataframe generated by `iterate_threshold`. Specify the target metric as 'recall_score' and the target value as the minimum recall you want to enforce (e.g., 0.95). The function will return the optimal threshold and associated metrics that satisfy the constraints of the objective function.
# MAGIC

# COMMAND ----------

def roc_auc_score_func(y_true, y_score):
    try:
        return sklearn.metrics.roc_auc_score(y_true=y_true, y_score=y_score)
    except:
        return 0

def iterate_threshold(y_true, y_probs, steps=1000, saved_error_cost=2, missed_error_cost=10):
    """
    Function that iterates over 'steps' many probability cutoff thresholds [0,1] 
    and calculate metrics for each of the thresholds. 
    Returns pd.DataFrame with different metrics (columns) and thresholds (rows)
    """
    metrics = []

    for thresh in np.linspace(0, 1, steps):
        y_pred = np.copy(y_probs)
        y_pred[y_pred>thresh] = 1
        y_pred[y_pred<thresh] = 0

        y_true = y_true.astype(bool)
        y_pred = y_pred.astype(bool)

        confusion_matrix_ = sklearn.metrics.confusion_matrix(y_true=y_true, y_pred=y_pred)
        true_negative=confusion_matrix_[0][0]
        false_negative=confusion_matrix_[1][0]
        true_positive=confusion_matrix_[1][1]
        false_positive=confusion_matrix_[0][1]

        metrics.append({
            "threshold":thresh,
            "accuracy_score":sklearn.metrics.accuracy_score(y_true=y_true, y_pred=y_pred),
            "f1_score":sklearn.metrics.f1_score(y_true=y_true, y_pred=y_pred),
            "precision_score":sklearn.metrics.precision_score(y_true=y_true, y_pred=y_pred),
            "recall_score":sklearn.metrics.recall_score(y_true=y_true, y_pred=y_pred),
            "negative_precision":true_negative/(true_negative+false_negative),
            "negative_recall":true_negative/(true_negative+false_positive),
            "roc_auc_score":roc_auc_score_func(y_true=y_true, y_score=y_probs),
            "true_negative":true_negative,
            "false_negative":false_negative,
            "true_positive":true_positive,
            "false_positive":false_positive,
            "confusion_matrix":confusion_matrix_,
            "economic_saving":(true_negative + false_negative) * saved_error_cost - (false_negative * missed_error_cost),
        })

    return pd.DataFrame(metrics)

# objective function
def find_optimal_thresh(threshold_iterations, target_metric='recall_score', target_value=.98):
    """
    Find the threshold that best meets the specified target metric value.

    Parameters:
    - threshold_iterations (pandas.DataFrame): A DataFrame with various performance metrics across different thresholds.
    - target_metric (str): The name of the column in `threshold_iterations` that we want to optimize.
    - target_value (float): The target value for the `target_metric`.

    Returns:
    - dict: A dictionary with the optimal threshold and corresponding metrics.
    
    The function works by calculating the absolute difference between the `target_metric` and the `target_value` for each threshold, then selecting the threshold that minimizes this difference. 
    It ensures that the selected threshold brings the performance metric as close as possible to the target value. 
    """
    
    # select threshold for which the target_metric is closest to target_value
    # TODO: This is not 100% correct as it might select a threshold for which recall score is under the target value. Revise with other approach
    optimal_threshold = (
        threshold_iterations.drop('confusion_matrix', axis=1)
        .iloc[(threshold_iterations[target_metric] - target_value).abs().argsort()[:1]]
    )

    # rename columns
    optimal_threshold.columns = [f'ot_{str(target_value)[2:]}_' + col_name for col_name in optimal_threshold.columns]

    return optimal_threshold.to_dict(orient='records')[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance functions

# COMMAND ----------

def log_feature_importance(model, feature_cols):
    # If the model is a pipeline, the classifier is the last element
    classifier = model[-1]

    # If the classifier is a CalibratedClassifierCV, we need to get the original classifier
    if isinstance(classifier, CalibratedClassifierCV):
        # Usually, the base_estimator is fitted with the best parameters, so we use it.
        # In the case of multiple estimators (from CV), you might want to get importances from all of them
        # or ensure that the CalibratedClassifierCV is fitted with `cv="prefit"`
        classifier = classifier.base_estimator

    # Ensure that the classifier has feature importances
    if hasattr(classifier, 'feature_importances_'):
        fimpo_df = pd.DataFrame({
            'column_name': feature_cols,
            'importance': classifier.feature_importances_
        })
        fimpo_df.sort_values(by='importance', ascending=False, inplace=True)

        # Save the feature importances to a CSV file
        fimpo_filename = 'feature_importance.csv'
        fimpo_df.to_csv(fimpo_filename, index=False)
        mlflow.log_artifact(fimpo_filename, 'fimpo/')
    else:
        print("The classifier does not have a feature_importances_ attribute.")


def log_shap(model, x_train, x_val, n_min = 200):
    # Disable MLflow autologging
    mlflow.autolog(disable=True)
    mlflow.sklearn.autolog(disable=True)

    # Sample background data for SHAP Explainer
    train_sample = x_train.sample(n=min(n_min, x_train.shape[0]), random_state=407872114)
    example = x_val.sample(n=min(n_min, x_val.shape[0]), random_state=407872114)

    # Use Kernel SHAP to explain feature importance on the sampled rows from the validation set.
    predict = lambda x: model.predict(pd.DataFrame(x, columns=x_train.columns))
    explainer = KernelExplainer(predict, train_sample, link="identity")
    shap_values = explainer.shap_values(example, l1_reg=False, nsamples=500)

    # Generate SHAP summary bar plot
    plt.figure(figsize=(15, 10))
    shap.summary_plot(shap_values, train_sample, plot_type="bar", show=False, plot_size=[15, 10])
    with tempfile.TemporaryDirectory() as dirpath:
        filepath = os.path.join(dirpath, "shap_barplot.png")
        plt.savefig(filepath)
        mlflow.log_artifact(filepath, 'fimpo/')
    plt.close()

    # Generate SHAP beeswarm plot
    plt.figure(figsize=(15, 10))
    shap.summary_plot(shap_values, example, class_names=model.classes_, show=False, plot_size=[15, 10])
    with tempfile.TemporaryDirectory() as dirpath:
        filepath = os.path.join(dirpath, "shap_beeswarm_plot.png")
        plt.savefig(filepath)
        mlflow.log_artifact(filepath, 'fimpo/')
    plt.close()

    # save shap feature importance as table
    shap_sum = np.abs(shap_values).mean(axis=0)
    importance_df = pd.DataFrame({
        'column_name': x_val.columns.tolist(),
        'shap_importance': shap_sum.tolist()
    }).sort_values('shap_importance', ascending=False)

    # Save the DataFrame as a CSV and log it to MLflow
    with tempfile.TemporaryDirectory() as tempdir:
        filepath_val = os.path.join(tempdir, "shap_importance.csv")
        importance_df.to_csv(filepath_val)
        mlflow.log_artifact(filepath_val, 'fimpo/')
        

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model training function
# MAGIC The run_trial function is designed to train a machine learning model, evaluate its performance across different thresholds, and log the results using MLflow. 

# COMMAND ----------

def run_trial(
    model, 
    training_sample_frac=1, 
    add_tags=None, 
    add_params=None, 
    target_metric='recall_score', 
    steps=1000, 
    verbose=False):
    """
    Train a machine learning model and log performance metrics using MLflow.

    Parameters:
    - model: The machine learning pipeline/model to be trained and evaluated.
    - training_sample_frac (float): Fraction of training data to use for training the model. Default is 1 (use all data).
    - add_tags (dict): Additional tags to be logged with MLflow.
    - add_params (dict): Additional parameters to be logged with MLflow.
    - target_metric (str): The metric to target for optimizing the threshold. Default is 'recall_score'.
    - steps (int): Number of steps to iterate over when computing thresholds. Default is 100.
    - verbose (bool): Whether to print additional output during training and evaluation. Default is False.

    Returns:
    - dict: A dictionary containing MLflow run ID, the trained model, tags, and metrics.
    
    The function begins by subsampling the training data based on `training_sample_frac`, then fits the provided `model` to the training data. 
    It then computes various performance metrics for training, validation, and test sets, including average precision scores and optimal thresholds for different recall values (e.g., 0.95, 0.98, 0.99). 
    Finally, it logs these metrics along with other information using MLflow and returns the results.
    """

    # Optionally take subsample only
    if 0 <= training_sample_frac < 1:
        x_train = xs['train'].sample(frac=training_sample_frac) 
        y_train= ys['train'].loc[x_train.index]
    else:
        x_train = xs['train']
        y_train= ys['train']

    x_val = xs["val"]
    y_val = ys["val"]

    # Add tags and parameters to MLflow
    tags = {
            'model_version': model_version, 
            'model_type': type(model["classifier"]).__name__
        }
    
    model_params = model.steps[-1][1].get_params()
    
    if verbose:
        model_params.pop('verbose')
    
    params = {
            **model_params,
            'n_columns': len(x_train.columns),
            'n_rows': len(x_train),
        }
    
    if add_tags:
        tags = {
            **tags,
            **add_tags,
            'training_sample_frac':training_sample_frac,
        }

    if add_params:
        params = {
            **params,
            **add_params
        }

    
    with mlflow.start_run() as run:
        mlflow_id = run.to_dictionary()['info']['run_id']
        tags = {
            **tags,
            **{"run_id": mlflow_id}
        }
        
        # fit the model with training data and autolog metrics for train and val dataset
        model.fit(
            X=x_train, 
            y=y_train, 
            # classifier__eval_set=[(x_val, y_val)] 
        )

        # compute custom metrics and optimal thresholds
        threshold_iterations_val = iterate_threshold(y_true=ys['val'], y_probs=model.predict_proba(xs['val'])[:, 1], steps=steps, saved_error_cost=2, missed_error_cost=10)
        threshold_iterations_test = iterate_threshold(y_true=ys['test'], y_probs=model.predict_proba(xs['test'])[:, 1], steps=steps, saved_error_cost=2, missed_error_cost=10)
       
        optimal_threshold_r95 = find_optimal_thresh(threshold_iterations_val, target_metric='recall_score', target_value=.95)
        optimal_threshold_r98 = find_optimal_thresh(threshold_iterations_val, target_metric='recall_score', target_value=.98)
        optimal_threshold_r99 = find_optimal_thresh(threshold_iterations_val, target_metric='recall_score', target_value=.99)
        metrics = {**optimal_threshold_r95, **optimal_threshold_r98, **optimal_threshold_r99}

        result = {
            'mlflow_id': mlflow_id,
            'model': model,
            **tags,
            # 'status': ho.STATUS_OK,  # this is needed if we want to use hiperopt
            **optimal_threshold_r95,
            **optimal_threshold_r98,
            **optimal_threshold_r99,
            **metrics,
        }
        
        # set tags
        mlflow.set_tags(tags = tags)

        # log metrics
        # TODO
        mlflow.log_metrics(metrics = metrics)

        # log confusion matrices
        # train

        # val
        val_conf_mat = threshold_iterations_val.loc[threshold_iterations_val["threshold"] == optimal_threshold_r98["ot_98_threshold"]]["confusion_matrix"].iloc[0]
        val_conf_mat = val_conf_mat.astype('float') / val_conf_mat.sum(axis=1)[:, np.newaxis]
        cm = ConfusionMatrixDisplay(confusion_matrix=val_conf_mat)
        cm.plot(cmap='Blues')
        with tempfile.TemporaryDirectory() as dirpath:
            filepath = os.path.join(dirpath, "val_conf_mat.png")
            plt.savefig(filepath)
            mlflow.log_artifact(filepath, 'conf_mat/')
            

        # test
        test_conf_mat = threshold_iterations_test.loc[threshold_iterations_test["threshold"] == optimal_threshold_r98["ot_98_threshold"]]["confusion_matrix"].iloc[0]
        test_conf_mat = test_conf_mat.astype('float') / test_conf_mat.sum(axis=1)[:, np.newaxis]
        cm = ConfusionMatrixDisplay(confusion_matrix=test_conf_mat)
        cm.plot(cmap='Blues')
        with tempfile.TemporaryDirectory() as dirpath:
            filepath = os.path.join(dirpath, "test_conf_mat.png")
            plt.savefig(filepath)
            mlflow.log_artifact(filepath, 'conf_mat/')
        

        ## Log CSV to MLflow
        ## Write csv from stats dataframe
        with tempfile.TemporaryDirectory() as tempdir:

            # Specify the file paths within the temporary directory
            filepath_val = os.path.join(tempdir, "threshold_iterations_val.csv")
            filepath_test = os.path.join(tempdir, "threshold_iterations_test.csv")

            # Write the DataFrames to the CSV files
            threshold_iterations_val.to_csv(filepath_val)
            threshold_iterations_test.to_csv(filepath_test)
    
            # Log the CSV files as artifacts
            mlflow.log_artifact(filepath_val)
            mlflow.log_artifact(filepath_test)
            
        # log calibration plot
        probas = {}
        for alias in xs.keys():
            probas[alias] = model.predict_proba(xs[alias])[:, 1]

        result["probas"] = probas

        calibration_plot_fig = calibration_plot(ys, probas)
        
        with tempfile.TemporaryDirectory() as dirpath:
            filepath = os.path.join(dirpath, "calibration_plot.png")
            plt.savefig(filepath)
            mlflow.log_artifact(filepath)

        # log XGBoost feature importance
        log_feature_importance(model=model, feature_cols=feature_cols)
        
        # log SHAP feature importance
        if shap_enabled:
            log_shap(model, x_train, x_val, n_min = shap_sample_size)
        
    return result, threshold_iterations_test, optimal_threshold_r98

# COMMAND ----------

# MAGIC %md
# MAGIC # Training Run

# COMMAND ----------

# loop over all models and train
for model_name, model_dict in models.items():

    classifier = model_dict['model']
    
    # optionally add calibration
    if add_calibration:
        classifier = CalibratedClassifierCV(classifier, method='isotonic', cv=5)

    # create pipeline with preprocessors 
    model_pipeline = Pipeline([
                        ("preprocessor", preprocessor),
                        ("standardizer", standardizer),
                        ("classifier", classifier),
                    ])

    model_dict['pipeline'] = model_pipeline
    model_dict['results'], threshold_iterations_test, optimal_threshold_r98= run_trial(
            model = model_pipeline,
            training_sample_frac = training_sample_frac,
            add_tags={"data_subset_metric": data_subset_metric,
                      "data_subset_FCD": data_subset_FCD,
                      "feature_sets_used": training_features_string, 
                      "training_sample_frac": training_sample_frac,
                      "calibrated": str(add_calibration),
                      "comment": dbutils.widgets.get("comment"),
                      },
            steps=1000,
            verbose=False
        )
    
    print(model_name, "done")
    print("-"*50)

# COMMAND ----------


