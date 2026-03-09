import time
import pandas as pd
import json
import logging

# Import custom modules
from .featurization import Featurizer
from .prediction import MLModel
from tbt.utils.console_print import conditional_print, conditional_print_warning

log = logging.getLogger(__name__)

class ErrorClassification:
    """Class that performs the end-2-end error classification

    :param pdf: dataframe with input data
    :type pdf: pd.DataFrame with columns "run_id", "route_id", "case_id", "country", "provider", "route", "stretch"
    :param fcd_credentials: dictionary with credentials for FCD
    :type fcd_credentials: dict
    :param ml_model_options: dictionary with options for the ML model
    :type ml_model_options: dict
    :param sample_metric: sample metric to be used for prediction, must be either "TbT" or "HDR"
    :type sample_metric: str
    """

    def __init__(self, pdf: pd.DataFrame, fcd_credentials: dict, ml_model_options: dict, sample_metric: str, spark):
        self.pdf = pdf
        self.fcd_credentials = fcd_credentials
        self.sample_metric = sample_metric
        self.spark = spark

        # select correct ml_model_options dict based on sample_metric
        if self.sample_metric == "TbT":
            self.ml_model_options = ml_model_options["tbt"]
            log.info("TbT inspection detected. Using TbT model.")
            conditional_print("TbT inspection detected. Using TbT model.")
        elif self.sample_metric == "HDR":
            self.ml_model_options = ml_model_options["hdr"]
            log.info("HDR inspection detected. Using HDR model.")
            conditional_print("HDR inspection detected. Using HDR model.")
        else:
            log.warning(f"The given sample_metric is not valid, defaulting to combined model. Please supply a valid sample_metric (Must be either 'TbT' or 'HDR', supplied sample_metric={self.sample_metric})")
            conditional_print_warning(f"The given sample_metric is not valid, defaulting to combined model. Please supply a valid sample_metric (Must be either 'TbT' or 'HDR', supplied sample_metric={self.sample_metric})")
            self.ml_model_options = ml_model_options["combined"]

        # Initialize MLModel
        self.ml_model = MLModel(self.ml_model_options)     


    def featurize(self) -> pd.DataFrame:
        """Function performing feature engineering.

        :return: featurized dataframe
        :rtype: pd.DataFrame
        """

        # initialize Featurizer
        data_featurization = Featurizer(self.pdf, self.fcd_credentials, self.sample_metric, spark=self.spark)

        # Perform data processing and feature engineering
        featurized_df = data_featurization.featurize()

        return featurized_df


    def predict_on_df(
            self,
            featurized_df: pd.DataFrame,
            ) -> pd.DataFrame:
        """Function that performs prediction.

        :param featurized_df: dataframe with featurized data
        :type featurized_df: pd.DataFrame
        :return: dataframe with predictions
        :rtype: pd.DataFrame
        """

        # Perform prediction
        df_with_predictions = self.ml_model.predict(featurized_df)

        return df_with_predictions
    
    def create_log_table(
            self, 
            featurized_df: pd.DataFrame, 
            predictions: pd.DataFrame
            ) -> pd.DataFrame:
        """Function that creates a log table with features, predictions, model metadata.

        :param featurized_df: dataframe with featurized data
        :type featurized_df: pd.DataFrame
        :param predictions: dataframe with predictions
        :type predictions: pd.DataFrame
        :return: dataframe with log table
        :rtype: pd.DataFrame
        """

        # combine pdf, featurized_df and predictions to create log table
        log_df = pd.concat([
            self.pdf,
            featurized_df,
            predictions
            ], axis=1)
        
        # add prediction date
        log_df["prediction_date"] = time.strftime("%Y-%m-%d")

        # add sample_metric
        log_df["sample_metric"] = self.sample_metric

        ml_model_options = self.ml_model.get_model_options()
        log_df["ml_model_options"] = json.dumps(ml_model_options)
        log_df["model_run_id"] = str(ml_model_options["model_run_id"])

        # cast all nan to -1 
        log_df = log_df.fillna(-1)

        # cast to correct datatypes 
        dtype_mapping = {
            'run_id': 'string',
            'country': 'string',
            'provider': 'string',
            'route_id': 'string',
            'case_id': 'string',
            'route': 'string',
            'stretch': 'string',
            'sample_metric': 'string',
            'is_tbt': 'int',
            'is_hdr': 'int',
            'drive_left_side': 'int',
            'drive_right_side': 'int',
            'route_length': 'float',
            'stretch_length': 'float',
            'stretch_start_position_in_route': 'float',
            'stretch_end_position_in_route': 'float',
            'stretch_starts_at_the_route_start': 'float',
            'stretch_ends_at_the_route_end': 'float',
            'heading_stretch': 'float',
            'left_turns': 'float',
            'straight_turns': 'float',
            'right_turns': 'float',
            'total_right_angle': 'float',
            'max_right_angle': 'float',
            'total_left_angle': 'float',
            'max_left_angle': 'float',
            'absolute_angle': 'float',
            'has_left_turns': 'float',
            'has_right_turns': 'float',
            'min_curvature': 'float',
            'max_curvature': 'float',
            'mean_curvature': 'float',
            'distance_start_end_stretch': 'float',
            'route_coverage': 'float',
            'stretch_covers_route': 'float',
            'tortuosity': 'float',
            'sinuosity': 'float',
            'density': 'float',
            # old fcd features
            'pra': 'float',
            'prb': 'float',
            'prab': 'float',
            'lift': 'float',
            'tot': 'int',
            # new fcd features
            'tot_new': 'int',
            'pra_new': 'float',
            'prb_new': 'float',
            'pra_not_b': 'float',
            'prb_not_a': 'float',
            'pra_and_b': 'float',
            'pra_to_b': 'float',
            'prb_to_a': 'float',
            'tot_contained': 'int',
            'pra_to_b_contained': 'float',
            'prb_to_a_contained': 'float',
            'pra_to_b_not_contained': 'float',
            'prb_to_a_not_contained': 'float',
            'traffic_direction': 'float',
            'traffic_direction_contained': 'float',
            'ab_intersect': 'int',
            # SDO features
            'CONSTRUCTION_AHEAD_TT': 'int',
            'MANDATORY_STRAIGHT_ONLY': 'int',
            'MANDATORY_STRAIGHT_OR_LEFT': 'int',
            'MANDATORY_TURN_RESTRICTION': 'int',
            'MANDATORY_TURN_LEFT_ONLY': 'int',
            'MANDATORY_TURN_LEFT_OR_RIGHT': 'int',
            'MANDATORY_LEFT_OR_STRAIGHT_OR_RIGHT': 'int',
            'MANDATORY_TURN_RIGHT_ONLY': 'int',
            'MANDATORY_STRAIGHT_OR_RIGHT': 'int',
            'NO_ENTRY': 'int',
            'NO_MOTOR_VEHICLE': 'int',
            'NO_CAR_OR_BIKE': 'int',
            'NO_LEFT_OR_RIGHT_TT': 'int',
            'NO_LEFT_TURN': 'int',
            'NO_RIGHT_TURN': 'int',
            'NO_VEHICLE': 'int',
            'NO_STRAIGHT_OR_LEFT_TT': 'int',
            'NO_STRAIGHT_OR_RIGHT_TT': 'int',
            'NO_STRAIGHT_TT': 'int',
            'NO_TURN_TT': 'int',
            'NO_U_OR_LEFT_TURN': 'int',
            'NO_U_TURN': 'int',
            'ONEWAY_TRAFFIC_TO_STRAIGHT': 'int',
            # Metadata
            'sdo_api_response': 'string',
            'probability': 'float',
            'error_label': 'string',
            'ml_model_options': 'string',
            'prediction_date': 'string',
            'model_run_id': 'string'
        }

        log_df = log_df.astype(dtype_mapping)

        # # DEBUG ONLY: print all columns and their datatypes
        # for col in log_df.columns:
        #     log.info(f"Datatype of column {col}: {log_df[col].dtype}, example value: {log_df[col][0]}")

        return log_df
        

    def run(self):
        """Wrapper function that runs the error classification pipeline.

        :return: dataframe with
        :rtype: pd.Series
        """

        # log start time and nr of critical sections
        start_time = time.time()
        nr_critical_section = self.pdf.shape[0]

        # featurizing
        log.info("Featurizing...")
        conditional_print("Featurizing...")
        featurized_df = self.featurize()
        fcd_elapsed_time = time.time() - start_time
        minutes, seconds = divmod(fcd_elapsed_time, 60)
        log.info(f"Finished featurization of {nr_critical_section} critical sections in {int(minutes)} minutes and {seconds:.2f} seconds.")
        conditional_print(f"Finished featurization of {nr_critical_section} critical sections in {int(minutes)} minutes and {seconds:.2f} seconds.")

        # predict
        log.info("Predicting...")
        conditional_print("Predicting...")
        predictions = self.predict_on_df(featurized_df)

        # create log table
        ec_model_log_table = self.create_log_table(featurized_df, predictions)
        log.info(f"Finished predicting and logging {nr_critical_section} critical sections with model: model_run_id={ec_model_log_table['model_run_id'][0]}")
        conditional_print(f"Finished predicting and logging {nr_critical_section} critical sections with model: model_run_id={ec_model_log_table['model_run_id'][0]}")

        # extract only predictions to return
        predictions = pd.Series(predictions["error_label"])

        # log elapsed time
        elapsed_time = time.time() - start_time
        minutes, seconds = divmod(elapsed_time, 60)
        time_per_critical_section = elapsed_time / nr_critical_section
        
        ec_model_log_table["time_per_critical_section"] = time_per_critical_section

        log.info(f"Finished e2e error classification of {nr_critical_section} critical sections in {int(minutes)} minutes and {seconds:.2f} seconds.")
        conditional_print(f"Finished e2e error classification of {nr_critical_section} critical sections in {int(minutes)} minutes and {seconds:.2f} seconds.")
        log.info(f"Time per critical section: {time_per_critical_section:.2f} seconds.")
        conditional_print(f"Time per critical section: {time_per_critical_section:.2f} seconds.")

        return predictions, ec_model_log_table
        return predictions, ec_model_log_table

