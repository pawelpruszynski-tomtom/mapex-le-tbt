# Databricks notebook source
# MAGIC %md
# MAGIC # Comparison of models 
# MAGIC - Load newly trained model
# MAGIC - featurize remaining cases
# MAGIC - save dataset 
# MAGIC

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

# MAGIC %pip install shapely geopandas folium matplotlib mapclassify sqlalchemy pymorton

# COMMAND ----------

# mlflow and spark
import mlflow
from pyspark.sql.functions import struct, col
from pyspark.sql import functions as F

# sklearn
from sklearn.metrics import r2_score, confusion_matrix, roc_auc_score, roc_curve, auc

# plotting
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
from statsmodels.api import qqplot_2samples

import fcd_py
from pyspark.sql import SparkSession
import json
from pyspark.sql import functions as F
import sqlalchemy
import geopandas as gpd
import shapely
import shapely.wkt
from shapely.geometry import mapping
from datetime import datetime, timedelta
import copy
from tqdm import tqdm
tqdm.pandas()

# plotting
import seaborn as sns
import matplotlib.pyplot as plt

# morton
import pymorton as pm


# COMMAND ----------

balanced_data = spark.read.format('delta').load(balanced_data_path).toPandas()


# COMMAND ----------

credentials = json.loads(dbutils.secrets.get(scope = "mdbf3", key = "fcd_credentials"))

# COMMAND ----------

# sys.path.append("/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/notebooks/")
%run "/Workspace/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/error_classification/call_FCD_API_new.py"

# COMMAND ----------



    fcd_featurizer = FCDFeaturizer(
        chunk,
        trace_retrieval_geometry="bbox_json",
        traces_limit=2000,
        fcd_credentials=credentials,
        spark_context=spark
    )

    fcd_features = fcd_featurizer.featurize()
    fcd_features.rename(columns={
        'pra': 'pra_new',
        'prb': 'prb_new',
        'tot': 'tot_new'}, inplace=True)

    df_chunk_featurized = pd.concat([chunk, fcd_features], axis=1, verify_integrity=True)

    # Convert 'tot' column to integer if it's in float format
    if 'tot' in df_chunk_featurized.columns and df_chunk_featurized['tot'].dtype != 'int64':
        df_chunk_featurized['tot'] = df_chunk_featurized['tot'].astype('int')
    
    # Convert the featurized chunk to a Spark DataFrame and write it to the data lake
    spark_df = spark.createDataFrame(df_chunk_featurized)

    # Write to the data lake, overwrite on the first iteration, append on subsequent iterations
    spark_df.write.mode("append") \
                .format('delta') \
                .option("overwriteSchema", "true") \
                .save("dbfs:/error_classification/OMA-1043/feature_table_w_new_fcd_features_01.delta")

print("SUCCESS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## artificial_inspection

# COMMAND ----------

# Load artificial inspection data
# specify run_id of artificial inspection you want to evaluate
run_id = "4c419c75-4a55-4285-8114-07af58efd466"
quarter = "2023-Q3"

# connect to tbt database (prod)
tbt_db = json.loads(dbutils.secrets.get(scope="utils", key="db_credentials_tbt"))
tbt_engine = sqlalchemy.create_engine(
        f'postgresql+psycopg2://{tbt_db["user"]}:{tbt_db["password"]}'
        f'@{tbt_db["host"]}:{tbt_db["port"]}/{tbt_db["database"]}'
    ).execution_options(autocommit=True)
print(tbt_engine)

# import critical sections data
sql_template = f"""
SELECT 
    el.run_id::text,
    el.case_id::text,
    el.route_id::text,
    el.country,
    el.provider,
    case
        when sm.metric in ('HDR_v2', 'HDR')
            then 'HDR'
        else 'TbT'
        end metric,
    st_astext(el.stretch) as stretch, 
    st_astext(el.provider_route) as route, 
    el.product,
    el.competitor,
    el.error_type,
    el.error_subtype,
    CASE
        WHEN el.error_type IN ('discard', NULL)
            THEN 'no_error'
        ELSE
            'error'
    END AS error_label
FROM error_logs AS el
left join inspection_critical_sections ics
on el.case_id = ics.case_id 
left join inspection_metadata im 
on ics.run_id = im.run_id 
left join sampling_metadata sm
on im.sample_id = sm.sample_id
WHERE el.run_id = '{run_id}'
"""

artificial_inspection = pd.read_sql_query(
    sql_template, 
    con=tbt_engine.raw_connection()
)

# create spark df
artificial_inspection = spark.createDataFrame(artificial_inspection).dropDuplicates(["case_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTION 1: Featurize Artificial inspection data
# MAGIC After featurization, predictions must still be obtained from model 1 (see 00_compute_predictions_old_model)

# COMMAND ----------

# function to featurize a dataframe of cases

import requests
from tqdm import tqdm
def featurize_df(df, api_url = "http://maps-analytics-error-classification.dadev.maps.az.tt3.com/featurize/"):
    """
    Function to featurize a dataframe of cases
    :param df: pd.DataFrame with columns case_id, country, stretch, route, metric
    :param api_url: url of the featurization API
    :return: pd.DataFrame (featurized) 
    """
    # select relevant columns only
    api_columns = ["case_id", "country", "stretch", "route", "metric"]
    df = df.loc[:, api_columns]
    
    # add logging column (default to 0)
    df["logging"] = 0

    # initiate temporary list to store output
    output_list = []
    
    # iterate over rows, get prediction for each row, and add it to the dataframe
    for index, row in tqdm(df.iterrows()):
        payload = row.to_dict()
        r = requests.post(
            api_url,
            data=json.dumps(payload),
        )
        if r.status_code != 200:
            # Raise error
            raise Exception("Unexpected response from API")
        features = json.loads(r.content)

        # append features to list
        output_list.append(features)

    # convert list to dataframe
    output_df = pd.DataFrame(output_list)

    # add it to input dataframe
    output_df = pd.concat([df, output_df], axis=1)

    # return dataframe
    return output_df


# COMMAND ----------

# read featurized artifial inspection
featurized_artificial_inspections_path = f'{BLOB_URL}/error_classification/artificial_inspections/{quarter}/featurized_artificial_inspection.delta'

# COMMAND ----------

# DBTITLE 1,Run this cell only if you want to featurize the dataframe. 
# df = artificial_inspection.toPandas()
# # featurize df
# featurized_df = featurize_df(df)

# # write to datalake
# (
#     spark.createDataFrame(featurized_df)
#     .write
#     .mode("append")
#     .format('delta')
#     .save(featurized_artificial_inspections_path)
# )
# print("done")

# COMMAND ----------

if artificial_inspection_data_option == "option_1":
    featurized_artificial_inspection_df = spark.read.format('delta').load(featurized_artificial_inspections_path).dropDuplicates(["case_id"])

    print(featurized_artificial_inspection_df.count())
    featurized_artificial_inspection_df.display()

    # add the features to the original dataframe
    artificial_inspection = (
        artificial_inspection
        .join(
            featurized_artificial_inspection_df.drop("country", "provider", "metric", "logging"),
            on=["case_id"],
            how="left")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTION 2: Load artificial inspection data with predictions of old model

# COMMAND ----------

if artificial_inspection_data_option == "option_2":

    # path to featurized artificial inspections with predictions of model 1
    artificial_inspection_w_preds1 = spark.read.format('delta').load(f'{BLOB_URL}/error_classification/artificial_inspections/{quarter}/artificial_inspections_w_preds1.delta')

    # add ground truth labels
    artificial_inspection_w_preds1 = (
        artificial_inspection_w_preds1
        .join(
            artificial_inspection.select("case_id", "error_label"),
            on=["case_id"],
            how="left")
    )

    # add has_error column
    from pyspark.sql.functions import when, col
    artificial_inspection_w_preds1 = artificial_inspection_w_preds1.withColumn("has_error", when(col("error_label") == "error", 1).otherwise(0))

    artificial_inspection = artificial_inspection_w_preds1.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTION 3: Load features and predictions from critical_sections_with_model_predictions table
# MAGIC This has the upside that features, predictions and probabilities are what the model actually predicted at the time.
# MAGIC
# MAGIC However, currently it seems that for most cases of the artificial inspection, no data was logged in table critical_sections_with_model_predictions. We have to make this work, as otherwise artificial inspection is not really meaningful. 

# COMMAND ----------

if artificial_inspection_data_option == "option_3":

    cswmd = spark.read.format('delta').load("dbfs:/mnt/tbt/delta-tables/critical_sections_with_model_predictions.delta")
    ics = read_tbt_datalake("inspection_critical_sections.delta")

    artificial_inspection_featurized = (
        artificial_inspection
        .join(
            ics.select("case_id", "reference_case_id"),
            on=["case_id"],
            how="left"
        )
        .withColumn(
            'case_id',
            F.when(F.col('reference_case_id').isNull(), F.col("case_id"))
            .otherwise(F.col("reference_case_id"))
        )
        .join(
            cswmd.drop(*["country", "provider", "run_id", "route_id", "route", "stretch", "sdo_api_response", "traffic_signs", "model_metadata"]),
            on=["case_id"],
            how="left"
        )
        .withColumnRenamed("prediction", "prediction_1")
        .withColumnRenamed("probability", "probability_1")
        .withColumn(
            'is_tbt',
            F.when(F.col('metric').isin('HDR_v2', 'HDR'), 0)
            .otherwise(1)
        )
    )

    artificial_inspection = artificial_inspection_featurized.toPandas()

    # add column has_error
    artificial_inspection["has_error"] = np.where(artificial_inspection['error_label'] == 'no_error', 0, 1)
    artificial_inspection["prediction_1"] = np.where(artificial_inspection['prediction_1'] == 'no error', 0, 1)

    artificial_inspection = artificial_inspection.dropna(subset=["probability_1"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## discards_pdf

# COMMAND ----------

cswmd = spark.read.format('delta').load("dbfs:/mnt/tbt/delta-tables/critical_sections_with_model_predictions.delta")
sm = read_tbt_datalake("sampling_metadata.delta")
im = read_tbt_datalake("inspection_metadata.delta")

# COMMAND ----------

discards = (
    cswmd
    .filter(cswmd.prediction == "no error")
    .withColumnRenamed("prediction", "prediction_1")
    .withColumnRenamed("probability", "probability_1")
    # add sample_id
    .join(im.select("run_id", "sample_id"),
          on = ["run_id"],
          how = "left") 
    # add metric
    .join(sm.select("sample_id", "metric"),
          on = ["sample_id"],
          how = "left")
    # add is_tbt
    .withColumn('is_tbt', F.when(F.col('metric').isin(['HDR_v2', 'HDR']), 0).otherwise(1))
)

discards_pdf = discards.toPandas()

# COMMAND ----------

# add column for prediction of old model
discards_pdf["prediction_1"] = [0 for i in range(len(discards_pdf))]

# COMMAND ----------

# MAGIC %md
# MAGIC ## combined_data
# MAGIC Caution: Currently discard data does not have a column has_error

# COMMAND ----------

# combine the two
col_list = list(set(discards_pdf.columns) & set(test_table.columns))
selected_test_data = test_table[col_list]
selected_discards = discards_pdf.sample(5000, random_state=1)

# add column has_error
selected_discards["has_error"] = [0 for i in range(len(selected_discards))]
selected_discards = selected_discards[col_list]
combined_data = pd.concat([test_table, selected_discards], ignore_index=True)

error_ratio = combined_data[combined_data["has_error"] == 1].shape[0] / combined_data.shape[0]
print(f"Error ratio: {error_ratio}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## balanced_data
# MAGIC Here we are aiming to have a discard rate of 13%, and MCP labels available for the whole sample
# MAGIC As the 400 artificial inspections should represent 13% of all cases, the following is true: 
# MAGIC
# MAGIC Given:
# MAGIC $$ n_{\text{discard}} = 400 = 0.13 \cdot n_{\text{total}} $$
# MAGIC
# MAGIC Thus
# MAGIC $$ n_{\text{total}} = \frac{400}{0.13} = 3076 $$
# MAGIC
# MAGIC Thus
# MAGIC $$ n_{\text{non-discard}} = n_{\text{total}} - n_{\text{discard}} = 3076 - 400 = 2676$$
# MAGIC

# COMMAND ----------

# check if it already exists, if not create it
try:
    balanced_data = spark.read.format('delta').load(balanced_data_path).toPandas()
    print("Read balanced data from datalake")
except:
    # combine test_data & artificial_inspection
    col_list = list(set(artificial_inspection.columns) & set(test_table.columns))
    selected_test_data = test_table[col_list].sample(2676, random_state=1)
    selected_artificial_inspection = artificial_inspection[col_list]
    balanced_data = pd.concat([selected_test_data, selected_artificial_inspection], ignore_index=True)
    # write to datalake
    (
      spark.createDataFrame(balanced_data)
      .write
      .format('delta')
      .mode('overwrite')
      .save(balanced_data_path)
    )
    print("Written balanced data to datalake")

# statistics on selected test dataset
print(f"test_data true positives {selected_test_data.query('has_error == 1 & prediction_1 == 1').shape[0]}")
print(f"test_data false positives {selected_test_data.query('has_error == 0 & prediction_1 == 1').shape[0]}")

# get some statistics on balanced dataset
error_ratio = balanced_data[balanced_data["has_error"] == 1].shape[0] / balanced_data.shape[0]
print(f"Error ratio: {error_ratio}")
print(f"Number of rows: {balanced_data.shape[0]}")
balanced_data.value_counts("metric", normalize=True)

# check missing FCD data
fcd_data_missing = balanced_data[
    (balanced_data['pra'] == 0) &
    (balanced_data['prb'] == 0) &
    (balanced_data['prab'] == 0) &
    (balanced_data['lift'] == 0) &
    (balanced_data['tot'] == 0)
].shape[0]

fcd_long_stretch = balanced_data[(balanced_data["lift"] == -1)].shape[0]
print(f"FCD data is missing or 0 for {fcd_data_missing} cases, that is in {fcd_data_missing/balanced_data.shape[0]}%")
print(f"FCD data is missing because the stretch is too long for {fcd_long_stretch} cases, that is in {fcd_long_stretch/balanced_data.shape[0]}%")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load input feature names
# MAGIC Different models have different input feature names, and also, depending on the model pipeline, the model feature names must be accessed differently. Below are options, you have to test which one works. 

# COMMAND ----------

input_columns_load_option = 2

if input_columns_load_option == 0:
    # print schema
    for field in loaded_model_1.metadata.get_input_schema():
        print(field)
    # get schema to list
    model_input_schema = loaded_model_1.metadata.get_input_schema().to_dict()
    input_cols_model_2 = [col["name"] for col in model_input_schema]

elif input_columns_load_option == 1:
    input_cols_model_2 = list(loaded_model_1.steps[0][1].feature_names_in_)

# this should work for model wrapped in sklearn's CalibratedClassifierCV class
elif input_columns_load_option == 2:
    input_cols_model_2 = list(loaded_model_1.feature_names_in_)


# COMMAND ----------

# MAGIC %md
# MAGIC # Add predictions of new model

# COMMAND ----------

probability_threshold = 0.028

# obtain predictions for all dataframes for all loaded models
dataframe_list = [test_table, artificial_inspection, discards_pdf, combined_data, balanced_data]
model_list = [loaded_model_2, loaded_model_3, loaded_model_4]

for dataframe in dataframe_list:
    for i, model in enumerate(model_list): 
        probabilities = model.predict_proba(balanced_data.loc[:, input_cols_model_2])
        probabilities = probabilities[:, 1]
        predictions = [1 if prob >= probability_threshold else 0 for prob in probabilities]
        prob_col_name = f"probability_{i+2}" #i+2 because probability_1 is the probability of the old model
        pred_col_name = f"prediction_{i+2}"
        balanced_data[prob_col_name] = pd.Series(probabilities)
        balanced_data[pred_col_name] = predictions


# COMMAND ----------

# MAGIC %md
# MAGIC # Define custom function for evaluation plots

# COMMAND ----------

# compute custom metrics
def compute_recall_and_negative_recall(y_true, y_pred):
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

    recall = tp / (tp + fn)
    negative_recall = tn / (tn + fp)
    discard_rate = (tn + fn) / len(y_true)

    print(f'Recall: {recall}')
    print(f'Negative Recall: {negative_recall}')
    print(f'Discard Rate: {discard_rate}')


def calculate_and_plot_confusion_matrix(ground_truth, predictions, model_title):
    """
    Calculate the confusion matrix and plot it for the given predictions against the ground truth.

    :param ground_truth: the ground truth labels
    :param predictions: the predicted labels
    :param model_title: the title for the model to be displayed in the plot
    """
    cm = confusion_matrix(ground_truth, predictions)
    plt.figure(figsize=(5,5))
    sns.heatmap(cm, annot=True, fmt=".0f", linewidths=.5, square=True, cmap='Blues')
    plt.ylabel('Actual label')
    plt.xlabel('Predicted label')
    plt.title(f'Confusion Matrix for {model_title}')
    plt.show()



def calculate_and_plot_roc(df, ground_truth_column, prediction_column_1, prediction_column_2):
    """
    Calculate and print the AUC, calculate the ROC curve, and then plot the ROC curve 
    for two models based on the given DataFrame.

    :param df: the DataFrame that contains the data
    :param ground_truth_column: the column name for the ground truth
    :param prediction_column_1: the column name for predictions from the first model
    :param prediction_column_2: the column name for predictions from the second model
    """
    # Calculate and print the AUC for both models
    auc_1 = roc_auc_score(df[ground_truth_column], df[prediction_column_1])
    auc_2 = roc_auc_score(df[ground_truth_column], df[prediction_column_2])

    print(f'AUC for Model 1: {auc_1}')
    print(f'AUC for Model 2: {auc_2}')

    # Calculate ROC curve for both models
    fpr1, tpr1, _ = roc_curve(df[ground_truth_column], df[prediction_column_1])
    fpr2, tpr2, _ = roc_curve(df[ground_truth_column], df[prediction_column_2])

    # Plot the ROC curve
    plt.figure()
    lw = 2
    plt.plot(fpr1, tpr1, color='darkorange', lw=lw, label='ROC curve 1 (area = %0.2f)' % auc_1)
    plt.plot(fpr2, tpr2, color='blue', lw=lw, label='ROC curve 2 (area = %0.2f)' % auc_2)
    plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver operating characteristic')
    plt.legend(loc="lower right")
    plt.show()


def plot_probability_comparison(df, prob_col1, prob_col2, truth_col):
    """
    Create a scatter plot to compare the predicted probabilities of two models.

    :param df: DataFrame containing the model probabilities and ground truth
    :param prob_col1: column name for probabilities from the first model
    :param prob_col2: column name for probabilities from the second model
    :param truth_col: column name for the ground truth labels
    """
    plt.figure(figsize=(10,10))
    plt.scatter(df[prob_col1], df[prob_col2], c=df[truth_col], alpha=0.5, cmap='viridis')
    plt.xlabel(f'Predicted Probability from Old Model ({prob_col1})')
    plt.ylabel(f'Predicted Probability from New Model ({prob_col2})')
    plt.title('Comparison of Predicted Probabilities')
    plt.plot([0, 1], [0, 1], color='red', linestyle='--')  # Adjusted to plot y=x line
    plt.colorbar(label='Ground Truth (has_error)')
    plt.grid(True)
    plt.show()


def qq_plot_comparison(df, proba_col1, proba_col2):
    """
    Create a QQ plot to compare two sets of probabilities from different models.

    :param df: DataFrame containing the model probabilities
    :param proba_col1: column name for probabilities from the first model
    :param proba_col2: column name for probabilities from the second model
    """
    prob1 = df[proba_col1].sort_values()
    prob2 = df[proba_col2].sort_values()

    # Create QQ plot
    plt.figure(figsize=(5,5))
    qqplot_2samples(prob1, prob2, line ='45')
    plt.xlabel(f'Quantiles of Probabilities from Old Model (1)')
    plt.ylabel(f'Quantiles of Probabilities from New Model (2)')
    plt.title('QQ Plot - Model 1 vs Model 2')
    plt.show()



def plot_fnr_vs_discard_rate(df, cutoff_probability=0.032):
    thresholds = np.linspace(0, 1, 200)
    rates = {
        'threshold': [],
        'fnr_1': [],
        'discard_rate_1': [],
        'fnr_2': [],
        'discard_rate_2': []
    }

    for thresh in thresholds:
        # Predict labels based on thresholds
        df['pred_1'] = df['probability_1'] >= thresh
        df['pred_2'] = df['probability_2'] >= thresh

        # Calculate False Negatives and True Positives
        df['fn_1'] = (~df['pred_1']) & df['has_error']
        df['tp_1'] = df['pred_1'] & df['has_error']
        df['fp_1'] = df['pred_1'] & (~df['has_error'])
        df['tn_1'] = (~df['pred_1']) & (~df['has_error'])

        df['fn_2'] = (~df['pred_2']) & df['has_error']
        df['tp_2'] = df['pred_2'] & df['has_error']
        df['fp_2'] = df['pred_2'] & (~df['has_error'])
        df['tn_2'] = (~df['pred_2']) & (~df['has_error'])

        # Calculate rates
        fnr_1 = df['fn_1'].sum() / (df['fn_1'].sum() + df['tp_1'].sum())
        discard_rate_1 = (df['tp_1'].sum() + df['fp_1'].sum()) / len(df)

        fnr_2 = df['fn_2'].sum() / (df['fn_2'].sum() + df['tp_2'].sum())
        discard_rate_2 = (df['tp_2'].sum() + df['fp_2'].sum()) / len(df)

        # Store rates
        rates['threshold'].append(thresh)
        rates['fnr_1'].append(fnr_1)
        rates['discard_rate_1'].append(discard_rate_1)
        rates['fnr_2'].append(fnr_2)
        rates['discard_rate_2'].append(discard_rate_2)

    # Creating plotly figure
    fig = go.Figure()

    # Adding FNR and Discard Rate traces for Probability 1
    fig.add_trace(go.Scatter(x=rates['threshold'], y=rates['fnr_1'],
                             mode='lines', name='FNR Probability 1'))
    fig.add_trace(go.Scatter(x=rates['threshold'], y=rates['discard_rate_1'],
                             mode='lines', name='Discard Rate Probability 1', line=dict(dash='dash')))
    
    # Adding FNR and Discard Rate traces for Probability 2
    fig.add_trace(go.Scatter(x=rates['threshold'], y=rates['fnr_2'],
                             mode='lines', name='FNR Probability 2'))
    fig.add_trace(go.Scatter(x=rates['threshold'], y=rates['discard_rate_2'],
                             mode='lines', name='Discard Rate Probability 2', line=dict(dash='dash')))
    

    # Add a vertical line
    fig.add_shape(
        type='line',
        x0=cutoff_probability,
        x1=cutoff_probability,
        y0=0,
        y1=1,
        line=dict(color='Red')
    )
    # Update layout
    fig.update_layout(title='False Negative Rate vs Discard Rate',
                      xaxis_title='Threshold',
                      yaxis_title='Rate',
                      legend_title='Legend')

    return fig


def calculate_recall_specificity_and_plotly(df, cutoff_probability=0.032):
    thresholds = np.linspace(0, 1, 200)
    rates = {
        'threshold': [],
        'recall_1': [],
        'specificity_1': [],
        'recall_2': [],
        'specificity_2': []
    }

    for thresh in thresholds:
        # Predict labels based on thresholds
        df['pred_1'] = df['probability_1'] >= thresh
        df['pred_2'] = df['probability_2'] >= thresh

        # Calculate True Positives, False Negatives, True Negatives, and False Positives
        tp_1 = ((df['pred_1']) & (df['has_error'])).sum()
        fn_1 = ((~df['pred_1']) & (df['has_error'])).sum()
        tn_1 = ((~df['pred_1']) & (~df['has_error'])).sum()
        fp_1 = ((df['pred_1']) & (~df['has_error'])).sum()

        tp_2 = ((df['pred_2']) & (df['has_error'])).sum()
        fn_2 = ((~df['pred_2']) & (df['has_error'])).sum()
        tn_2 = ((~df['pred_2']) & (~df['has_error'])).sum()
        fp_2 = ((df['pred_2']) & (~df['has_error'])).sum()

        # Calculate recall (true positive rate) and specificity (true negative rate)
        recall_1 = tp_1 / (tp_1 + fn_1) if (tp_1 + fn_1) > 0 else 0
        specificity_1 = tn_1 / (tn_1 + fp_1) if (tn_1 + fp_1) > 0 else 0

        recall_2 = tp_2 / (tp_2 + fn_2) if (tp_2 + fn_2) > 0 else 0
        specificity_2 = tn_2 / (tn_2 + fp_2) if (tn_2 + fp_2) > 0 else 0

        # Store rates
        rates['threshold'].append(thresh)
        rates['recall_1'].append(recall_1)
        rates['specificity_1'].append(specificity_1)
        rates['recall_2'].append(recall_2)
        rates['specificity_2'].append(specificity_2)

    # Creating plotly figure
    fig = go.Figure()

    # Adding Recall and Specificity traces for Probability 1
    fig.add_trace(go.Scatter(x=rates['threshold'], y=rates['recall_1'],
                             mode='lines', name='Recall Old Model (1)'))
    fig.add_trace(go.Scatter(x=rates['threshold'], y=rates['specificity_1'],
                             mode='lines', name='Specificity Old Model (1)', line=dict(dash='dash')))
    
    # Adding Recall and Specificity traces for Probability 2
    fig.add_trace(go.Scatter(x=rates['threshold'], y=rates['recall_2'],
                             mode='lines', name='Recall New Model (2)'))
    fig.add_trace(go.Scatter(x=rates['threshold'], y=rates['specificity_2'],
                             mode='lines', name='Specificity New Model (2)', line=dict(dash='dash')))
    
    # Add a vertical line
    fig.add_shape(
        type='line',
        x0=cutoff_probability,
        x1=cutoff_probability,
        y0=0,
        y1=1,
        line=dict(color='Red')
    )
    
    # Update layout
    fig.update_layout(title='Recall vs Specificity',
                      xaxis_title='Threshold',
                      yaxis_title='Rate',
                      legend_title='Legend')

    return fig



def calculate_recall_specificity_threshold_plot(df):
    thresholds = np.linspace(0, 1, 10000)
    rates = {
        'threshold': [],
        'recall_1': [],
        'specificity_1': [],
        'recall_2': [],
        'specificity_2': []
    }

    for thresh in thresholds:
        # Predict labels based on thresholds
        df['pred_1'] = df['probability_1'] >= thresh
        df['pred_2'] = df['probability_2'] >= thresh

        # Calculate True Positives, False Negatives, True Negatives, and False Positives
        tp_1 = ((df['pred_1']) & (df['has_error'])).sum()
        fn_1 = ((~df['pred_1']) & (df['has_error'])).sum()
        tn_1 = ((~df['pred_1']) & (~df['has_error'])).sum()
        fp_1 = ((df['pred_1']) & (~df['has_error'])).sum()

        tp_2 = ((df['pred_2']) & (df['has_error'])).sum()
        fn_2 = ((~df['pred_2']) & (df['has_error'])).sum()
        tn_2 = ((~df['pred_2']) & (~df['has_error'])).sum()
        fp_2 = ((df['pred_2']) & (~df['has_error'])).sum()

        # Calculate recall (true positive rate) and specificity (true negative rate)
        recall_1 = tp_1 / (tp_1 + fn_1) if (tp_1 + fn_1) > 0 else 0
        specificity_1 = tn_1 / (tn_1 + fp_1) if (tn_1 + fp_1) > 0 else 0

        recall_2 = tp_2 / (tp_2 + fn_2) if (tp_2 + fn_2) > 0 else 0
        specificity_2 = tn_2 / (tn_2 + fp_2) if (tn_2 + fp_2) > 0 else 0

        # Store rates
        rates['threshold'].append(thresh)
        rates['recall_1'].append(recall_1)
        rates['specificity_1'].append(specificity_1)
        rates['recall_2'].append(recall_2)
        rates['specificity_2'].append(specificity_2)


    # Find the optimal threshold for recall > 0.98 for model 1
    for i, recall in enumerate(rates['recall_1']):
        if recall < 0.98:
            optimal_threshold_1 = rates['threshold'][i-1]
            optimal_recall_1 = rates['recall_1'][i-1]
            optimal_specificity_1 = rates['specificity_1'][i-1]

            break

    # Find the optimal threshold for recall > 0.98 for model 2
    for i, recall in enumerate(rates['recall_2']):
        if recall < 0.98:
            optimal_threshold_2 = rates['threshold'][i-1]
            optimal_recall_2 = rates['recall_2'][i-1]
            optimal_specificity_2 = rates['specificity_2'][i-1]
            break


    # Creating plotly figure
    fig = go.Figure()

    # Create hover text for both models
    hover_text_1 = ['Threshold: {:.5f}'.format(thresh) for thresh in rates['threshold']]
    hover_text_2 = ['Threshold: {:.5f}'.format(thresh) for thresh in rates['threshold']]


    # Adding Recall and Specificity traces for Probability 1
    fig.add_trace(go.Scatter(x=rates['recall_1'], y=rates['specificity_1'],
                             mode='lines', name='Old Model (1)',
                             text=hover_text_1, hoverinfo='text+x+y'))

    # Adding Recall and Specificity traces for Probability 2
    fig.add_trace(go.Scatter(x=rates['recall_2'], y=rates['specificity_2'],
                             mode='lines', name='New Model (2)',
                             line=dict(dash='dash'),
                             text=hover_text_2, hoverinfo='text+x+y'))


    # Add dashed vertical line at the optimal threshold
    fig.add_shape(type="line", 
                    x0=optimal_recall_1, y0=0, 
                    x1=optimal_recall_1, y1=1,
                    line=dict(dash="dash", width=2, color="red"))

    print(f"Optimal Threshold Old Model (1): {optimal_threshold_1}")
    print(f"Optimal Threshold New Model (2): {optimal_threshold_2}")
    print(f"Recall Old Model (1): {optimal_recall_1}")
    print(f"Recall New Model (2): {optimal_recall_2}")
    print(f"Specificity Old Model (1): {optimal_specificity_1}")
    print(f"Specificity New Model (2): {optimal_specificity_2}")
    
    # Update layout
    fig.update_layout(title='Recall vs Specificity',
                      xaxis_title='Recall',
                      yaxis_title='Specificity',
                      legend_title='Legend')

    return fig


def calculate_recall_specificity_threshold_variable_plot(df):
    thresholds = np.linspace(0, 0.1, 2000)
    
    # Identify columns that contain probabilities
    probability_columns = [col for col in df.columns if col.startswith('probability_')]
    
    rates = {'threshold': []}
    optimal_vals = {}
    for col in probability_columns:
        rates[f'recall_{col}'] = []
        rates[f'specificity_{col}'] = []
        optimal_vals[col] = {'threshold': None, 'recall': None, 'specificity': None}

    for thresh in thresholds:
        rates['threshold'].append(thresh)
        for prob_col in probability_columns:
            pred_col = f'pred_{prob_col}'
            # Predict labels based on thresholds
            df[pred_col] = df[prob_col] >= thresh

            # Calculate True Positives, False Negatives, True Negatives, and False Positives
            tp = ((df[pred_col]) & (df['has_error'])).sum()
            fn = ((~df[pred_col]) & (df['has_error'])).sum()
            tn = ((~df[pred_col]) & (~df['has_error'])).sum()
            fp = ((df[pred_col]) & (~df['has_error'])).sum()

            # Calculate recall (true positive rate) and specificity (true negative rate)
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            specificity = tn / (tn + fp) if (tn + fp) > 0 else 0

            # Store rates
            rates[f'recall_{prob_col}'].append(recall)
            rates[f'specificity_{prob_col}'].append(specificity)

            # Check if this is the first instance of recall dropping below 0.98
            if optimal_vals[prob_col]['recall'] is None and recall < 0.98:
                optimal_vals[prob_col]['threshold'] = rates['threshold'][-2]  # previous threshold
                optimal_vals[prob_col]['recall'] = rates[f'recall_{prob_col}'][-2]  # previous recall
                optimal_vals[prob_col]['specificity'] = rates[f'specificity_{prob_col}'][-2]  # previous specificity

    # Creating plotly figure
    fig = go.Figure()

    for i, prob_col in enumerate(probability_columns):
        recall_col = f'recall_{prob_col}'
        specificity_col = f'specificity_{prob_col}'

        # Adding Recall and Specificity traces for each model
        if prob_col == "probability_1":
             model_name = "Old Model (1)"
        elif prob_col == "probability_2":
             model_name = "New Model (2)"
        elif prob_col == "probability_3":
             model_name = "New Model TbT only (3)"
        elif prob_col == "probability_4":
             model_name = "New Model HDR only (4)"
        else:
            model_name = prob_col

        # Print optimal values
        print(f"Optimal Threshold {model_name}: {optimal_vals[prob_col]['threshold']}")
        print(f"Recall {model_name}: {optimal_vals[prob_col]['recall']}")
        print(f"Specificity {model_name}: {optimal_vals[prob_col]['specificity']}")

        # Create hover text
        hover_text = [f'Threshold: {thresh:.5f}' for thresh in rates['threshold']]

        fig.add_trace(go.Scatter(x=rates[recall_col], y=rates[specificity_col],
                                 mode='lines', name=model_name,
                                 text=hover_text, hoverinfo='text+x+y'))

        # Add dashed vertical line at the optimal threshold for each model
        fig.add_shape(type="line", 
                      x0=optimal_vals[prob_col]['recall'], y0=0, 
                      x1=optimal_vals[prob_col]['recall'], y1=1,
                      line=dict(dash="dash", color="red", width=2))

    # Update layout
    fig.update_layout(title='Recall vs Specificity',
                      xaxis_title='Recall',
                      yaxis_title='Specificity',
                      legend_title='Legend')

    return fig


# COMMAND ----------

# MAGIC %md
# MAGIC # Basic performance metrics
# MAGIC - Confusion matrices
# MAGIC - ROC curve
# MAGIC - Probability plots

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select data you want to evaluate on
# MAGIC Potentially, you can also add a filter (e.g. by metric, provider, country)

# COMMAND ----------

# df = artificial_inspection
# df = discards_pdf
# df = combined_data
df = balanced_data
# df = test_table

# df with FCD data available only
# df = df[(df['tot'] > 0)]

# df with HDR or TbT only
# df = pd.DataFrame(df.query("is_tbt == 0"))

# orbis only
# df = df[df['product'].str.startswith('O')]

# orbis only and only tbt/hdr
# df = df[(df['product'].str.startswith('O')) & (df['is_tbt'] == 1)]

# COMMAND ----------

calculate_recall_specificity_threshold_variable_plot(df)

# COMMAND ----------

print(df.value_counts("metric"))
print(df.shape[0])
df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recall, negative recall, and discard rate

# COMMAND ----------

# prediction_1
print("For prediction_1")
compute_recall_and_negative_recall(df['has_error'], df['prediction_1'])

# prediction_2
print("For prediction_2")
compute_recall_and_negative_recall(df['has_error'], df['prediction_2'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confusion matrices

# COMMAND ----------

ground_truth = df['has_error']
predictions_1 = df['prediction_1']  # Predictions from Model 1
predictions_2 = df['prediction_2']  # Predictions from Model 2

calculate_and_plot_confusion_matrix(ground_truth, predictions_1, 'Old Model (1)')
calculate_and_plot_confusion_matrix(ground_truth, predictions_2, 'New Model (2)')

# COMMAND ----------

# confusion matrix to see where old and new model agree/disagree
cm1 = confusion_matrix(df["prediction_1"], df["prediction_2"])
plt.figure(figsize=(5,5))
sns.heatmap(cm1, annot=True, fmt=".0f", linewidths=.5, square=True, cmap='Blues')
plt.ylabel('Prediction Old Model (1)')
plt.xlabel('Prediction New Model (2)')
plt.title('Confusion Matrix for old and new model predictions')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ROC curve

# COMMAND ----------

calculate_and_plot_roc(df, 'has_error', 'probability_1', 'probability_2')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare predicted probabilities

# COMMAND ----------

plot_probability_comparison(df, 'probability_1', 'probability_2', 'has_error')

# COMMAND ----------

qq_plot_comparison(df, 'probability_1', 'probability_2')

# COMMAND ----------

plot_fnr_vs_discard_rate(df)

# COMMAND ----------

calculate_recall_specificity_and_plotly(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recall-Specificity tradeoff plot

# COMMAND ----------

calculate_recall_specificity_threshold_plot(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Values for excel template to have two-sided confusion matrix

# COMMAND ----------

def print_table(df):
    # Define the conditions for each cell of the table
    conditions = [
        # No actual error (has_error == 0)
        ("No error, both got it right", (df["prediction_1"] == 0) & (df["prediction_2"] == 0) & (df["has_error"] == 0)),
        ("No error, old got it wrong, new got it right", (df["prediction_1"] == 1) & (df["prediction_2"] == 0) & (df["has_error"] == 0)),
        ("No error, old got it right, new got it wrong", (df["prediction_1"] == 0) & (df["prediction_2"] == 1) & (df["has_error"] == 0)),
        ("No error, both got it wrong", (df["prediction_1"] == 1) & (df["prediction_2"] == 1) & (df["has_error"] == 0)),

        # Actual error (has_error == 1)
        ("Error, both got it wrong", (df["prediction_1"] == 0) & (df["prediction_2"] == 0) & (df["has_error"] == 1)),
        ("Error, old got it wrong, new got it right", (df["prediction_1"] == 0) & (df["prediction_2"] == 1) & (df["has_error"] == 1)),
        ("Error, old got it right, new got it wrong", (df["prediction_1"] == 1) & (df["prediction_2"] == 0) & (df["has_error"] == 1)),
        ("Error, both got it right", (df["prediction_1"] == 1) & (df["prediction_2"] == 1) & (df["has_error"] == 1)),
    ]
    
    # Calculate the values for each condition to fill the table
    results = {desc: df[cond].shape[0] for desc, cond in conditions}

    # Print out the formatted table
    print("\t\tOld (baseline) model")
    print("\t\tNo error\t\tPotential error")
    print(f"New model\tNo\tYes\tNo\tYes")
    print(f"No error\t{results['No error, both got it right']}\t{results['Error, both got it wrong']}\t{results['No error, old got it wrong, new got it right']}\t{results['Error, old got it right, new got it wrong']}")
    print(f"Potential error\t{results['No error, old got it right, new got it wrong']}\t{results['Error, old got it wrong, new got it right']}\t{results['No error, both got it wrong']}\t{results['Error, both got it right']}")
    print("\n")

    # Print the descriptions with case counts
    for description in conditions:
        print(f"{description[0]}: {results[description[0]]} cases")



# Call the function with the dataframe
print_table(df)


# COMMAND ----------

def calculate_probabilities(df):
    # Number of actual errors and no errors
    actual_errors = df[df["has_error"] == 1].shape[0]
    actual_no_errors = df[df["has_error"] == 0].shape[0]

    # True Positives and True Negatives for each model
    tp_new = df[(df["prediction_2"] == 1) & (df["has_error"] == 1)].shape[0]
    tn_new = df[(df["prediction_2"] == 0) & (df["has_error"] == 0)].shape[0]
    tp_old = df[(df["prediction_1"] == 1) & (df["has_error"] == 1)].shape[0]
    tn_old = df[(df["prediction_1"] == 0) & (df["has_error"] == 0)].shape[0]

    # Conditional probabilities for the new model
    p_new_error_given_error = tp_new / actual_errors
    p_new_no_error_given_no_error = tn_new / actual_no_errors

    # Conditional probabilities for the old (baseline) model
    p_old_error_given_error = tp_old / actual_errors
    p_old_no_error_given_no_error = tn_old / actual_no_errors

    # Conditional probabilities given the other model's prediction
    p_new_no_error_given_old_no_error = df[(df["prediction_1"] == 0) & (df["prediction_2"] == 0)].shape[0] / df[df["prediction_1"] == 0].shape[0]
    p_new_error_given_old_error = df[(df["prediction_1"] == 1) & (df["prediction_2"] == 1)].shape[0] / df[df["prediction_1"] == 1].shape[0]
    
    # Print the results
    print(f"P(New model predicts error | error) = {p_new_error_given_error:.3f} (TPR New model)")
    print(f"P(New model predicts no error | no error) = {p_new_no_error_given_no_error:.3f} (TNR New model)")
    print(f"P(baseline predicts error | error) = {p_old_error_given_error:.3f} (TPR baseline)")
    print(f"P(baseline predicts no error | no error) = {p_old_no_error_given_no_error:.3f} (TNR baseline)")
    print()
    print(f"P(New model predicts no error | baseline predicts no error) = {p_new_no_error_given_old_no_error:.3f}")
    print(f"P(New model predicts error | baseline predicts error) = {p_new_error_given_old_error:.3f}")

# Call the function with the dataframe
calculate_probabilities(df)


# COMMAND ----------

# MAGIC %md
# MAGIC # Investigate False Negatives
# MAGIC

# COMMAND ----------

print("All False Negatives")
df[((df["prediction_1"] == 0) | (df["prediction_2"] == 0)) & (df["has_error"] == 1)]

# COMMAND ----------

print("Error, both got it wrong")
df[(df["prediction_1"] == 0) & (df["prediction_2"] == 0) & (df["has_error"] == 1)]

# COMMAND ----------

print("Error, old got it right, new got it wrong")
df[(df["prediction_1"] == 1) & (df["prediction_2"] == 0) & (df["has_error"] == 1)]

# COMMAND ----------

print("Error, old got it wrong, new got it right")
df[(df["prediction_1"] == 0) & (df["prediction_2"] == 1) & (df["has_error"] == 1)]

# COMMAND ----------

# MAGIC %md
# MAGIC # Test calibrated model
# MAGIC If probas are calibrated, I expect the sum of probas of a test set is very similar to total amount of true positive (real errors) in the same dataset.
# MAGIC That's also how I would like to use probas, to estimate number of real errors in a new inspection.

# COMMAND ----------

# test for the whole test dataset
print(f"The dataset has {df.shape[0]} rows")
print(f"The sum of the probabilities 1 is {df['probability_1'].sum()}")
print(f"The sum of the probabilities 2 is {df['probability_2'].sum()}")

print(f"The total number of errors is {df['has_error'].sum()}")
print(f"The dataset has error rate of {df['has_error'].sum() / df.shape[0]}")


# COMMAND ----------

# MAGIC %md
# MAGIC Test for parts of the df only and see if it is consistent

# COMMAND ----------

# Shuffle the DataFrame
df = df.sample(frac=1, random_state=42)

# Split the DataFrame into n random samples
dfs = np.array_split(df, 20)

errors = []
sums = []

# Apply the provided code on each sample
for i, sample in enumerate(dfs):
    print(f"Sample {i+1}:")
    sum_probability = sample['probability_2'].sum()
    error_count = sample['has_error'].sum()
    print(f"The dataset has {sample.shape[0]} rows")
    print(f"The sum of the probabilities is {sum_probability}")
    print(f"The total number of errors is {error_count}")
    print("\n")  # Print a newline for readability
    errors.append(error_count)
    sums.append(sum_probability)

# Create a bar plot
x = range(1, len(dfs) + 1)
plt.bar(x, errors, width=0.4, label='Number of Errors')
plt.bar(x, sums, width=0.4, label='Sum of Probabilities', align='edge')

plt.xlabel('Sample Number')
plt.ylabel('Value')
plt.title('Comparison of Number of Errors and Sum of Probabilities')
plt.xticks(x)
plt.legend(loc="lower right")

plt.show()


# Create a scatter plot
plt.scatter(errors, sums)

plt.xlabel('Number of Errors')
plt.ylabel('Sum of Probabilities')
plt.title('Scatter plot of Number of Errors and Sum of Probabilities')
plt.grid(True)

plt.show()


# COMMAND ----------

# Separate the DataFrame into two: one with errors and one without
errors_df = df[df['has_error'] == 1]
no_errors_df = df[df['has_error'] == 0]

# Determine the sample size for each group
sample_size = len(df) // 20

errors = []
sums = []

for i in range(10):
    # Adjust the ratio of errors for each sample
    error_ratio = (i + 1) / 10.0
    no_error_ratio = 1.0 - error_ratio

    # Create a new sample with a different error ratio
    sample_df = pd.concat([errors_df.sample(int(error_ratio * sample_size), replace=True), 
                           no_errors_df.sample(int(no_error_ratio * sample_size), replace=True)])

    # Calculate the sum of probabilities and the number of true errors
    sample_sum_prob = sample_df['probability_2'].sum()
    sample_true_errors = sample_df['has_error'].sum()

    # Print the results
    print(f"Sample {i+1} (error ratio: {error_ratio:.1f}):")
    print(f"The sum of the probabilities is {sample_sum_prob}")
    print(f"The total number of errors is {sample_true_errors}")
    print("\n")

    errors.append(sample_true_errors)
    sums.append(sample_sum_prob)

# Create a bar plot
x = range(1, 11)
plt.bar(x, errors, width=0.4, label='Number of Errors')
plt.bar(x, sums, width=0.4, label='Sum of Probabilities', align='edge')
plt.xlabel('Sample Number')
plt.ylabel('Value')
plt.title('Comparison of Number of Errors and Sum of Probabilities')
plt.xticks(x)
plt.legend()
plt.show()

# Fit a linear regression model
coef = np.polyfit(errors, sums, 1)
poly1d_fn = np.poly1d(coef) 

print(f"The linear regression coefficient is: {poly1d_fn}")

# Compute R^2
r2 = r2_score(sums, poly1d_fn(errors))

# Create a scatter plot
plt.scatter(errors, sums)
plt.plot(errors, poly1d_fn(errors), color='red')  # add regression line
plt.text(min(errors), max(sums), f'$R^2 = {r2:.2f}$')  # add R^2 to the plot
plt.xlabel('Number of Errors')
plt.ylabel('Sum of Probabilities')
plt.title('Scatter plot of Number of Errors and Sum of Probabilities')
plt.grid(True)
plt.show()

