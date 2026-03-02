""" Pandas operations for metric computation"""
from typing import Dict, Any, Optional, List, Union

import pandas as pd
import json
import numpy as np
from uuid import UUID
from datetime import datetime
import ast
import uuid
import logging

from ..adas import  geometry_utils
from ..common import decorators



class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            # For NumPy arrays, convert to a list of elements
            return obj.tolist()
        # For other types, use the default encoder behavior
        return super().default(obj)


def check_column_exist(*, dataframe: pd.DataFrame, column: str) -> None:
    """Check if a pandas dataframe has a column

    :param dataframe: Pandas dataframe
    :type dataframe: pd.DataFrame
    :param column: Column to check
    :type column: str
    :raises ValueError: If column doesn't exists
    """
    if column not in dataframe.columns:
        raise ValueError(
            f"Column {column} not in route_df columns "
            f"(route_df.columns = {dataframe.columns.values}"
        )


def get_sum_column_deduplicated(
    *,
    routes_df: pd.DataFrame,
    column: str,
    filter_query: Optional[str] = None,
    deduplication_column: str = "route_id",
) -> float:
    """Function to sum a column routewide on a pandas dataframe

    :param routes_df: A pandas dataframe with `route_id` to identify the route and `column` to sum.
    :type routes_df: pd.DataFrame
    :param column: Column from `routes_df` to sum values
    :type column: str
    :param filter_query: Query to filter `routes_df` dataframe, default to None
    :type filter_query: str
    :param deduplication_column: Column from `routes_df` to deduplicate it
    :type deduplication_column: str
    :return: The sum of the values from `column` on `routes_df` that complies `filter_query`
    :rtype: float
    """
    check_column_exist(dataframe=routes_df, column=column)
    check_column_exist(dataframe=routes_df, column=deduplication_column)
    if filter_query is not None:
        routes_df = routes_df.query(expr=filter_query)
    return float(routes_df.drop_duplicates(subset=deduplication_column)[column].sum())


def get_column_value_counts(
    *, routes_df: pd.DataFrame, column: str, filter_query: Optional[str] = None
) -> Dict[Any, int]:
    """Return the number of each value in a column in a dict

    :param routes_df: A pandas dataframe with `column` to count values on.
    :type routes_df: pd.DataFrame
    :param column: The coloumn of the values to count
    :type column: str
    :param filter_query: A query to filter de dataframe, defaults to None
    :type filter_query: Optional[str], optional
    :return: A dict where the key is the value and the value is the count
    :rtype: Dict[Any, int]
    """
    check_column_exist(dataframe=routes_df, column=column)
    if filter_query is not None:
        routes_df = routes_df.query(expr=filter_query)
    return routes_df[column].value_counts().to_dict()


def get_number_errors(
    *,
    routes_df: pd.DataFrame,
    error_column: str = "mcp_state",
    non_error_values: Optional[List[str]] = None,
    filter_query: Optional[str] = None,
) -> int:
    """
    :param routes_df: Pandas dataframe with at least a column `route_id` and `error_column`
    with kind of error.
    :type routes_df: pd.DataFrame
    :param error_column: column from `routes_df` that logs the error, defaults to "mcp_sate"
    :type error_column: str, optional
    :param filter_query: Query to filter `routes_df` dataframe
    :type filter_query: str
    :param non_error_values: values not to consider error, defaults to ["discard"]
    :type non_error_value; Optional[List[str]]
    :return: The total number of errors that complies the `filter_query`
    :rtype: int
    """
    if not non_error_values:
        non_error_values = ["discard"]

    check_column_exist(dataframe=routes_df, column=error_column)

    if filter_query is not None:
        routes_df = routes_df.query(expr=filter_query)

    return int(
        routes_df.loc[
            (~routes_df[error_column].isin(non_error_values))
            & ~routes_df[error_column].isna()
        ].shape[0]
    )


def get_number_routes_deduplicated(
    *,
    routes_df: pd.DataFrame,
    filter_query: Optional[str] = None,
    deduplication_column: str = "route_id",
) -> int:
    """Return the number of routes that complies `filter_query`

    :param routes_df: A pandas dataframe with at least a column `route_id`
    :type routes_df: pd.DataFrame
    :param filter_query: Query to filter `routes_df` dataframe
    :type filter_query: str
    :param deduplication_column: Column from `routes_df` to deduplicate it
    :type deduplication_column: str
    :return: Number of routes from `routes_df` that complies with `filter_query`
    :rtype: int
    """

    check_column_exist(dataframe=routes_df, column=deduplication_column)
    if filter_query is not None:
        routes_df = routes_df.query(expr=filter_query)

    return int(routes_df.drop_duplicates(deduplication_column).shape[0])


def join_queries(query: Optional[str], o_query: Optional[str]) -> Optional[str]:
    """Join pandas queries

    :param query: First query
    :type query: Optional[str]
    :param o_query: Second query
    :type o_query: Optional[str]
    :return: Queries joined by & if both are no None.
    :rtype: Optional[str]
    """
    if query is None and o_query is None:
        return None

    if query is None:
        return o_query

    if o_query is None:
        return query

    return f"({query}) and ({o_query})"


def aggregate_json_style(row, cols):
    def is_valid_type(value):
        is_float = isinstance(value, (float, np.float_)) and not np.isnan(value)
        is_int = isinstance(value, (int, np.int_, np.uint8, np.uint16, np.uint32, np.uint64))
        is_str = isinstance(value, str)
        is_list = isinstance(value, list)
        is_df = isinstance(value, pd.DataFrame)

        return is_float or is_int or is_str or is_list or is_df


    values = {}
    for col in cols:
        if col in row.index and row[col] is not None:
            if is_valid_type(row[col]):
                values[col] = row[col]
    values_sorted = dict(sorted(values.items()))
    return values_sorted


def marshall_to_db(df: pd.DataFrame, id_columns: list, tag_col_name:str='tags')->pd.DataFrame:
    """Generate a new column with all the columns in df except id_columns marshalled in json format.
    Compatible with postgresql jsonb

    :param df: Input dataframe
    :type df: pd.DataFrame
    :param id_columns: Columns to avoid in the json combination
    :type id_columns: list
    :param tag_col_name: name for the combined column, defaults to 'tags'
    :type tag_col_name: str, optional
    :return: Input dataframe with only ID columns and the combined json column
    :rtype: pd.DataFrame
    """

    cols_to_aggregate = list(set(df.columns) - set(id_columns))
    df[cols_to_aggregate] = df[cols_to_aggregate].replace({np.nan: None})
    df[tag_col_name] = df.apply(aggregate_json_style, args=(cols_to_aggregate,), axis=1)
    # Drop the original columns
    df = df.drop(columns=cols_to_aggregate, errors="ignore")
    df = df[id_columns+[tag_col_name]]

    return df

def marshall_to_db_wrap(features):
    drop_cols = ['org', 'trace_date', 'original_length','confidence', 'map_match_id']
    # Drop columns in features from drop_cols list if exists in the dataframe
    features = features.drop(columns=[col for col in drop_cols if col in features.columns], errors='ignore')
    id_columns = [col for col in ['map_info_id','original_trace_id', 'feature_id', 
                                  'feature_id_uuid', 'geom', 'weight', 
                                  'weight_direction'] if col in features.columns]
    features = marshall_to_db(features, id_columns, tag_col_name='tags')
    features['tags'] = features['tags'].apply(lambda x: json.dumps(x, cls=CustomJSONEncoder))
    return features

def marshall_data(features, sample_metadata, map_matching_time, params):
    map_info_id = str(uuid.uuid4())
    logging.info(f"Saving map matched info with uuid: {map_info_id}")
    features['map_info_id'] = map_info_id

    id_columns = ['map_info_id', 'original_trace_id', 'feature_id', 'geom', 'weight', 'weight_direction']
    features = features.drop_duplicates(subset=['original_trace_id', 'feature_id','weight_direction'])
    features = marshall_to_db(features, id_columns, tag_col_name='tags')
    features['tags'] = features['tags'].apply(lambda x: json.dumps(x, cls=CustomJSONEncoder))

    # Add all sanity checks needed
    sanity_checks = dict()
    sanity_checks['commit_version'] = params['commit_version']
    sanity_checks['tlos_utils_commit'] = params['tlos_utils_commit']
    sanity_checks['km_mapmatched'] = round(features['geom'].map(geometry_utils.ensure_geom).map(geometry_utils.get_length).sum()/1000, 3)

    features_metadata = {
        'map_info_id': map_info_id,
        'sample_id': sample_metadata['sample_id'],
        'provider': params['provider'],
        'provider_version': params['provider_version'],
        'date_generated': str(datetime.now()),
        'elapsed_time': int(float(map_matching_time)),
        'country': sample_metadata['country'],
        'region': sample_metadata['region'],
        'sanity_checks': json.dumps(sanity_checks)
    }

    return features_metadata, features


def unmarshall_column(df: pd.DataFrame, tag_col_name:str='tags')->pd.DataFrame:
    """Regenerate a dataframe structure from encoded json column

    :param df: Input dataframe
    :type df: pd.DataFrame
    :param tag_col_name: name for the combined column, defaults to 'tags'
    :type tag_col_name: str, optional
    :return: Input dataframe with the json column expanded
    :rtype: pd.DataFrame
    """
    if isinstance(df[tag_col_name].dropna().iloc[0], str):
        # Fill na to avoid float errors:
        df[tag_col_name] = df[tag_col_name].fillna('{}')
        try: # Try first as json if fails try as dict
            df[tag_col_name] = df[tag_col_name].map(json.loads)
        except json.decoder.JSONDecodeError:
            df[tag_col_name] = df[tag_col_name].map(ast.literal_eval)
    else:
        # Fill na in case of dict
        df[tag_col_name] = df[tag_col_name].fillna({})
    df = pd.concat([df,pd.json_normalize(df[tag_col_name])], axis=1)
    # Drop the encoded column
    df = df.drop(columns=tag_col_name, errors="ignore")
    return df


def update_tags(tags, new_tags):
    if not isinstance(tags, dict):
        tags = json.loads(tags)
    try:
        if not isinstance(new_tags, dict):
            new_tags = json.loads(new_tags)
        tags.update(new_tags)
    except Exception:
        return json.dumps(tags)

    return json.dumps(tags)


def json_serializable_types(obj):
    """Make sure that the object is serializable via json.dumps.
    It transforms UUIDs and Datetimes into string format

    :param obj: Input object
    :type obj: _type_
    :return: Output object (serializable)
    :rtype: _type_

    Example of use:
    inspection_meta = {key: pandas_ops.json_serializable_types(value) for key, value in inspection_meta.items()}
    """
    if isinstance(obj, dict):
        # Recursively convert types in dictionary values
        return {key: json_serializable_types(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple, np.ndarray)):
        # Recursively convert types in list or tuple elements
        return [json_serializable_types(item) for item in obj]
    elif isinstance(obj, UUID):
        # Convert UUID to string
        return str(obj)
    elif np.issubdtype(type(obj), np.number):
        # If it is, cast it to a Python int or float
        if np.issubdtype(type(obj), np.integer):
            return int(obj)
        else:
            return float(obj)
    elif isinstance(obj, datetime):
        # Convert datetime to ISO format
        return obj.isoformat()
    else:
        return obj

class NpEncoder(json.JSONEncoder):
    # Example of use:
    # df_conf_intervals["meta"] = json.dumps({k: v for k, v in res_no_resample.items() if k in km_keys}, cls=NpEncoder)
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

def join_columns(row: pd.Series, columns: List[str], default_value: Any = np.nan, separator: str = "_") -> Union[str, Any]:
    """
    Function to join specified columns with a specified separator if all are not null.

    :param pandas.Series row: A row of the DataFrame.
    :param List[str] columns: List of column names to join.
    :param Any default_value: Default value to return if any column is null. Default is np.nan.
    :param str separator: Separator to join column values. Default is underscore.
    :return: The joined string if all columns are not null, default_value otherwise.
    :rtype: Union[str, Any]
    """
    # Check if any specified column is null

    if all(pd.notnull(row[col]) for col in columns):
        # Join the column values with the separator
        return separator.join(str(row[col]) for col in columns)
    return default_value
