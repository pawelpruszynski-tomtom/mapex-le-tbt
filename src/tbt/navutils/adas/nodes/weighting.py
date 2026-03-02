import pandas as pd
import json
import logging
from ...adas.geometry_utils import is_same_directed, ensure_geom
import uuid
import hashlib

def generate_reproducible_uuids(provider, provider_id, weight_direction):
    # Concatenate the two strings provider and id
    combined_string = str(provider) + str(provider_id) + str(weight_direction)
    # Create a SHA-256 hash object
    sha256 = hashlib.sha256()
    # Update the hash object with the concatenated string
    sha256.update(combined_string.encode())
    # Get the hexadecimal digest of the hash as a string
    combined_uuid = sha256.hexdigest()
    # Create a UUID from the hexadecimal digest
    return str(uuid.UUID(combined_uuid[:32]))


def groupby_get_weight_direction(features:pd.DataFrame)->pd.DataFrame:
    """I receive a dataframe with a "geom" column. I get the first row as reference.
    Then I check for the rest of the rows if the geometry is in the same direction as the reference or not.
    The first value for the weight_direction column is always True.
    The following rows send the reference geometry and the current geometry to the function is_same_directed.
    If they are same directed we put True if not False.

    :param features: _description_
    :type features: pd.DataFrame
    :return: _description_
    :rtype: pd.DataFrame
    """
    features['first_coordinate'] = features['geom'].map(ensure_geom).apply(lambda line: line.coords[0])
    reference = features.loc[features['first_coordinate'] == features['first_coordinate'].min()]['geom'].iloc[0]
    weight_directions = []
    for i in range(0, features.shape[0]):
        weight_directions.append(is_same_directed(reference, features.iloc[i]['geom']))
    features['weight_direction'] = weight_directions
    features.drop(columns=['first_coordinate'], inplace=True)
    return features

def weight_duplicates(features:pd.DataFrame, params:dict)->pd.DataFrame:
    """Weight duplicates approach using the number of times a feature is visited by a trace
    This function adds a column "weight" and a column weight_direction to the features dataframe.

    :param features: Map matched features
    :type features: pd.DataFrame
    :return: Map matched features without duplicates and generating a column for the weight and a column for the weight direction
    :rtype: pd.DataFrame
    """
    original_features = len(features)
    # Sort the DataFrame by original_trace_id
    features.sort_values(by='original_trace_id', inplace=True)
    # I get the features, I group them by feature_id and I apply the function groupby_get_weight_direction to each group.
    # With this I will end with a new column "weight_direction" with True or False values
    features = features.groupby(['feature_id']).apply(groupby_get_weight_direction).reset_index(drop=True)
    features.sort_values(by='original_trace_id', inplace=True)
    # Now we group by ['feature_id', 'weight_direction'] and we count the number of times a feature is visited by a trace in the same direction
    features['weight'] = features.groupby(['feature_id', 'weight_direction'])['feature_id'].transform('count')
    # I group by feature_id and weight_direction and I get the original_trace_id as a list
    features_grouped = features.groupby(['feature_id', 'weight_direction']).agg(
        original_traces_list = ("original_trace_id", lambda x: json.dumps(list(x.astype(str))))
    )
    features = features.merge(
        features_grouped,
        on = ['feature_id', 'weight_direction'],
        how = 'left'
    )  # Didn't work with transform nor apply
    # Once I get the weight I drop duplicates and keep only the first one
    features = features.drop_duplicates(subset=['feature_id', 'weight_direction'], keep='first') # features.shape[0] must be equal to features['weight'].sum()

    final_cost = features['weight'].sum()
    if final_cost!=original_features:
        logging.warning(f"[WEIGHTING] weight_duplicates: original_features={original_features} != final_cost={final_cost}")

    # Generate the unique uuid for the features
    features['feature_id_uuid'] = features.apply(
        lambda x: generate_reproducible_uuids(params['provider'], x['feature_id'], x['weight_direction']),
        axis=1
    )
    features['feature_id_uuid'] = features['feature_id_uuid'].astype(str)

    return features
