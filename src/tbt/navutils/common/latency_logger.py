import json
import typing
from typing import Optional
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import HttpResponseError
from dataclasses import dataclass
from enum import Enum


@dataclass
class LoggerConfig:
    connection_string: str
    container_name: str


@dataclass
class LogData:
    run_parameters: typing.Dict[str, str]
    run_id: str
    metric: str


class LogType(str, Enum):
    START = "start"
    END = "end"
    ERROR = "error"


def send_log_to_blob(logger_config: LoggerConfig, log_data: LogData, log_type: LogType, error: Optional[str] = None) -> None:
    """
    Sends the log data to an Azure Blob Storage.

    :param logger_config: Contains connection string and container name for blob storage.
    :type logger_config: LoggerConfig
    :param log_data: Contains the run parameters, run ID, and metric.
    :type log_data: LogData
    :param log_type: The type of log being created (e.g., 'start', 'end', 'error').
    :type log_type: LogType
    :param error: Error message, if any, defaults to None.
    :type error: Optional[str]
    :raises ValueError: If required parameters are missing or invalid.
    :return: None
    :rtype: None
    """
    # Prepare the log data to be uploaded
    data = {
        "Date": datetime.now().isoformat(),
        "Provider": log_data.run_parameters['provider'].strip(),
        "Product": log_data.run_parameters['product'].strip(),
        "Metric": log_data.metric,
        "Country": log_data.run_parameters.get('country', None) and log_data.run_parameters['country'].strip(),
        "RunID": log_data.run_id,
        "LogType": log_type.value,
        "Error": error
    }

    # Define the blob name based on the log type and other parameters
    blob_name = f"{log_data.metric}/{log_data.run_parameters['product'].strip()}/{log_data.run_id}/{log_type.value}.json"
    
    # Upload the JSON data to the blob
    try:
        BlobServiceClient.from_connection_string(logger_config.connection_string) \
            .get_container_client(logger_config.container_name) \
            .get_blob_client(blob_name) \
            .upload_blob(json.dumps(data), overwrite=True)
    except Exception as e:
        raise ValueError(f"Failed to upload log to blob: {e}") from e
    

def send_config_to_blob(logger_config: LoggerConfig, config: typing.Dict, metric: str, product: str) -> None:
    """
    Sends the config data to an Azure Blob Storage.

    :param logger_config: Contains connection string and container name for blob storage.
    :type logger_config: LoggerConfig
    :param config: Contains the config.json used in the launcher 
    :type config: Dict
    :param metric: The metric the inspection is being run for
    :type metric: str
    :param product: The product being used in the inspection
    :type product: str
    :raises ValueError: If required parameters are missing or invalid.
    :return: None
    :rtype: None
    """

    # Define the blob name based on the log type and other parameters
    blob_name = f"launcher_config_file/{metric}/{product}/config.json"
    
    # Create the BlobServiceClient
    blob_client = BlobServiceClient.from_connection_string(logger_config.connection_string) \
        .get_container_client(logger_config.container_name) \
        .get_blob_client(blob_name)

    # Check if the blob already exists
    try:
        blob_client.get_blob_properties()
        print(f"Blob {blob_name} already exists. Skipping upload.")
        return
    except HttpResponseError as e:
        if e.status_code == 404:  # Blob does not exist
            pass  # Proceed with the upload
        else:
            raise ValueError(f"Failed to check blob existence: {e}") from e

    # Upload the JSON data to the blob
    try:
        blob_client.upload_blob(json.dumps(config), overwrite=False)
    except Exception as e:
        raise ValueError(f"Failed to upload log to blob: {e}") from e