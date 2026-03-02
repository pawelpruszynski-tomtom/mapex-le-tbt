import pandas as pd
import numpy as np
import mlflow
from mlflow.tracking import MlflowClient
import json


class MLModel:
    def __init__(self, ml_model_options):
        """Constructor for MLModel class

        :param model_options: dictionary with model options
        :type model_options: dictionary
        :return: prediction and probabilities
        :rtype: numpy array
        """
        model_name = ml_model_options["model_name"]
        model_stage = ml_model_options["model_stage"]

        #### Get model feature names from MLflow model registry ####
        logged_model_uri = f'models:/{model_name}/{model_stage}'
        client = MlflowClient()

        # get all versions of the model in the production stage
        model_version_details = client.get_latest_versions(model_name, stages=[model_stage])
                
        # latest version in the production stage
        latest_model_version = model_version_details[-1]
        
        # Get the source run ID from the latest model version
        source_run_id = latest_model_version.run_id

        # create model uri
        logged_model_uri = f"runs:/{source_run_id}/model"

        model_info = mlflow.models.get_model_info(logged_model_uri)

        inputs = model_info._signature_dict["inputs"]
        inputs = json.loads(inputs)

        model_features = []
        for input in inputs:
            model_features.append(input["name"])
 
        ##### Get threshold from MLflow model registry ####
        try:
            probability_threshold = float(mlflow.get_run(source_run_id).data.tags["custom_threshold"])
            print(f"Using probability threshold from custom tag: {probability_threshold}")
        except:
            probability_threshold = float(mlflow.get_run(source_run_id).data.metrics["ot_98_threshold"])    
            print(f"Using optimal probability threshold ot_98_threshold from metrics: {probability_threshold}")

        self.model_features = model_features
        self.probability_threshold = probability_threshold
        self.source_run_id = source_run_id
        self.logged_model = logged_model_uri
        

    def predict(self, feature_data):
        """Function to predict the probability of an error

        :param data: pandas dataframe with the features
        :type data: pandas dataframe
        :return: predictions and probabilities
        :rtype: numpy array
        """

        # load model
        # Try/catch in case the logged model doesn't exist
        try:
            loaded_model = mlflow.sklearn.load_model(self.logged_model)
            
        except Exception as e:
            raise Exception(
                "The logged model doesn't exist or is not compatible with cluster/libraries versions"
            ) from e
                
        # Filter features
        feature_data_filtered = feature_data.loc[:, self.model_features]

        # Compute probabilities and predictions
        probabilities = loaded_model.predict_proba(feature_data_filtered)

        # extract probabilities for being an error 
        probabilities = pd.Series(probabilities[:, 1])
        
        predictions = pd.Series(np.where(probabilities > self.probability_threshold, "potential_error", "no error"))

        # combine probabilities and predictions from dictionary to dataframe
        prediction_df = pd.concat([probabilities, predictions], axis=1)

        # set names of columns
        prediction_df.columns = ["probability", "error_label"]

        return prediction_df
    
    def get_model_options(self):
        """Function to get model options

        :return: model options
        :rtype: dictionary
        """
        model_options = {
            "model_features": self.model_features,
            "probability_threshold": self.probability_threshold,
            "model_run_id": self.source_run_id
        }

        return model_options

