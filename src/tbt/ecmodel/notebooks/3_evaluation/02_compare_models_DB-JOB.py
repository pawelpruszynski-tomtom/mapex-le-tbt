# Databricks notebook source
# MAGIC %md
# MAGIC # Comparison of models 
# MAGIC This notebook allows to compare two models logged in MLflow by doing predictions on a test dataset (not seen by both models!), and then comparing their performances.

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup and Options

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

# mlflow and spark
import mlflow
from pyspark.sql.functions import struct, col
from pyspark.sql import functions as F
import xgboost
import pandas as pd
import numpy as np

# sklearn
from sklearn.metrics import r2_score, confusion_matrix, roc_auc_score, roc_curve, auc

# plotting
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
from statsmodels.api import qqplot_2samples

# COMMAND ----------

# MAGIC %md
# MAGIC #--- USER INPUT ---

# COMMAND ----------

run_id_champion = dbutils.widgets.get("run_id_champion")
run_id_challenger = dbutils.widgets.get("run_id_challenger")

# COMMAND ----------

probability_threshold_champion = float(dbutils.widgets.get("proba_threshold_champion"))
probability_threshold_challenger = float(dbutils.widgets.get("proba_threshold_challenger"))

# COMMAND ----------

comparison_dataset = dbutils.widgets.get("comparison_dataset")

# COMMAND ----------

metric_subset = dbutils.widgets.get("metric_subset")

# COMMAND ----------

# MAGIC %md
# MAGIC # --- END USER INPUT

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load models and data

# COMMAND ----------

# # DEBUG ###################
# run_id_champion = "413fb4f00aaf48d28bcec0958386e304"
# run_id_challenger = "b6e81e336e7049d2902db4bd8a62e44b"
# probability_threshold_champion = 0.04
# probability_threshold_challenger = 0.04
# comparison_dataset = "dbfs:/error_classification/test_table.delta"
# metric_subset = "TbT"
# ###########################

# COMMAND ----------

logged_model_1 = f'runs:/{run_id_champion}/model'
logged_model_2 = f'runs:/{run_id_challenger}/model'

loaded_model_1 = mlflow.sklearn.load_model(logged_model_1)
loaded_model_2 = mlflow.sklearn.load_model(logged_model_2)

# COMMAND ----------

df = spark.read.format('delta').load(comparison_dataset).toPandas()

# df with HDR or TbT only
if metric_subset in ("TbT", "HDR"):
    df = df[df["metric"] == metric_subset].reset_index()

# COMMAND ----------

# MAGIC %md
# MAGIC # Add predictions

# COMMAND ----------

# obtain predictions for all dataframes for all loaded models
model_list = [loaded_model_1, loaded_model_2]
probalility_threshold_list = [probability_threshold_champion, probability_threshold_challenger]

for i, model in enumerate(model_list):
    print(i) 
    probabilities = model.predict_proba(df.loc[:, model.feature_names_in_])
    probabilities = probabilities[:, 1]
    predictions = [1 if prob >= probalility_threshold_list[i] else 0 for prob in probabilities]
    prob_col_name = f"probability_{i+1}" 
    pred_col_name = f"prediction_{i+1}"
    df[prob_col_name] = pd.Series(probabilities)
    df[pred_col_name] = predictions


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

