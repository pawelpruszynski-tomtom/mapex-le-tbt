import pandas as pd
import sqlalchemy
import typing
import uuid

from ..map_matchers import ValhallaMapMatcher

from .. import geometry_utils, kedro_utils

### DEPRECATED ONLY USED IN OLD LC AND OLD SL


def recover_sample_metadata(params):
    credentials = kedro_utils.get_credentials()
    engine = sqlalchemy.create_engine(credentials["db-credentials-speed-limits"]["con"])

    features_query_metadata = f"""
    select * from new_speed_limits.sampling_metadata 
    where sample_id = '{params['sample_id']}'
    """
    sampling_metadata = pd.read_sql_query(features_query_metadata, engine)
    sampling_metadata["sample_id"] = sampling_metadata["sample_id"].astype(str)
    sampling_metadata["date_generated"] = sampling_metadata["date_generated"].astype(
        str
    )
    sampling_metadata = sampling_metadata.iloc[0].to_dict()

    return sampling_metadata

def recover_sample(params):
    credentials = kedro_utils.get_credentials()
    engine = sqlalchemy.create_engine(credentials["db-credentials-speed-limits"]["con"])

    features_query = f"""
    select * from new_speed_limits.samples 
    where sample_id = '{params['sample_id']}'
    """
    features = pd.read_sql_query(features_query, engine)

    features["geom"] = features["geom"].apply(geometry_utils.ensure_geom)
    features["length"] = features["geom"].map(geometry_utils.get_length) / 1000

    features = features.sample(frac=1)
    features["cumsum_length"] = features["length"].cumsum()

    features = features[
        features["cumsum_length"]
        < int(params["provider_mapmatched_db"]["sample_limit_km"])
    ]
    features.drop(columns=["cumsum_length"], inplace=True)

    sampling_metadata = recover_sample_metadata(params)

    return features, sampling_metadata


def map_matching_info_provider(
    features: pd.DataFrame, params: typing.Dict
) -> pd.DataFrame:
    """Executes the mapmatching with Provider (orbis,L@S,...) if the id is not in a DDBB, else recover it

    Args:
        features (pd.DataFrame): features info
        parameters (typing.Dict): Kedro parameters

    Returns:
        pd.DataFrame: features mapmatched with orbis
    """
    credentials = kedro_utils.get_credentials()
    sql_map_match_metadata = f"""SELECT * from {params['provider_mapmatched_db']['schema']}.map_matched_metadata 
                                where sample_id = '{params['sample_id']}'
                                and product = '{params['provider_product']}'
                                and map_version = '{params['provider_version']}'"""

    engine = sqlalchemy.create_engine(
        credentials[params["provider_mapmatched_db"]["db_con"]]["con"]
    )
    try:
        map_matched_metadata = pd.read_sql_query(sql=sql_map_match_metadata, con=engine)
    except sqlalchemy.exc.ProgrammingError:  # If table does not exist
        map_matched_metadata = pd.DataFrame()
    if len(map_matched_metadata) > 1:
        raise Exception(
            f"More than 1 metadata for the sample {params['sample_id']} and map version {params['Orbis_version']}"
        )

    if len(map_matched_metadata) == 0:
        map_matched_metadata = {}
        mapmatch_id = str(uuid.uuid4())

        valhalla_renaming = {
            "speed": "speed_limit_with_default_value",
            "speed_limit": "speed_limit",
            "lane_count": "lane_counts",
            "id": "edge_id",
            "traversability": "traversability",
            "road_class": "road_class",
            "way_id": "id",
        }
        extra_attributes = ["edge.use", "edge.unpaved"]

        map_match_engine = ValhallaMapMatcher(
            url=params["provider_valhalla_url"], valhalla_renaming=valhalla_renaming
        )
        features_orbis = map_match_engine.map_match_df(
            features, extra_attributes=extra_attributes
        )
        features_orbis["geom"] = features_orbis["geom"].apply(
            geometry_utils.ensure_geom
        )
        features_orbis["length"] = (
            features_orbis["geom"].map(geometry_utils.get_length) / 1000
        )  # KM
        features_orbis["map_match_id"] = mapmatch_id
        # Generate unique id to join both tables
        features_orbis["match_info_id"] = [
            str(uuid.uuid4()) for _ in features_orbis["geom"]
        ]

        # MAP MATCHED + MAP MATCHED METADATA
        map_matched = features_orbis[
            ["map_match_id", "match_info_id", "id", "geom", "trace_id"]
        ]
        map_matched["geom"] = map_matched["geom"].astype(str)

        map_matched_metadata["sample_id"] = params["sample_id"]
        map_matched_metadata["map_match_id"] = mapmatch_id
        map_matched_metadata["product"] = params["provider_product"]
        map_matched_metadata["map_version"] = params["provider_version"]

        map_matched_metadata["map_matched_length_km"] = round(
            features_orbis["length"].sum(), 2
        )

        mapinfo_id = str(uuid.uuid4())
        features_orbis["map_info_id"] = mapinfo_id
        # MAP INFO + MAP INFO METADATA
        columns = [
                "map_info_id",
                "match_info_id",
                "id",
                "length",
                "lane_counts",
                "speed_limit",
                "speed_limit_with_default_value",
                "road_class",
                "use",
                "unpaved",
            ]
        # Fix in case the column is not present
        for col in columns:
            if col not in features_orbis.columns:
                features_orbis[col] = None
        map_info = features_orbis[columns]
        map_info_metadata = {}
        map_info_metadata["map_match_id"] = mapmatch_id
        map_info_metadata["map_info_id"] = mapinfo_id
        map_info_metadata["product"] = params["provider_product"]
        map_info_metadata["map_version"] = params["provider_version"]

        # Save metadata
        map_matched_metadata_df = pd.json_normalize(map_matched_metadata)
        map_matched_metadata_df.to_sql(
            name="map_matched_metadata",
            con=engine,
            schema=params["provider_mapmatched_db"]["schema"],
            index=False,
            if_exists="append",
        )
        map_info_metadata_df = pd.json_normalize(map_info_metadata)
        map_info_metadata_df.to_sql(
            name="map_info_metadata",
            con=engine,
            schema=params["provider_mapmatched_db"]["schema"],
            index=False,
            if_exists="append",
        )

        # Save data
        map_matched.to_sql(
            name="map_matched",
            con=engine,
            schema=params["provider_mapmatched_db"]["schema"],
            index=False,
            if_exists="append",
        )
        map_info.to_sql(
            name="map_info",
            con=engine,
            schema=params["provider_mapmatched_db"]["schema"],
            index=False,
            if_exists="append",
        )

    else:
        # Metadata as json:

        map_matched_metadata = map_matched_metadata.iloc[0].to_dict()
        map_matched_metadata["sample_id"] = str(map_matched_metadata["sample_id"])
        map_matched_metadata["map_match_id"] = str(map_matched_metadata["map_match_id"])

        sql_map_info_metadata = f"""SELECT * from {params['provider_mapmatched_db']['schema']}.map_info_metadata 
                                where map_match_id = '{map_matched_metadata['map_match_id']}'
                                and product = '{params['provider_product']}'
                                and map_version = '{params['provider_version']}'"""

        map_info_metadata = pd.read_sql_query(sql=sql_map_info_metadata, con=engine)
        map_info_metadata = map_info_metadata.iloc[0].to_dict()
        map_info_metadata["map_info_id"] = str(map_info_metadata["map_info_id"])
        map_info_metadata["map_match_id"] = str(map_info_metadata["map_match_id"])

        # recover map_matched
        sql_map_info = f"SELECT * from {params['provider_mapmatched_db']['schema']}.map_info where map_info_id = '{map_info_metadata['map_info_id']}'"
        map_info = pd.read_sql_query(sql=sql_map_info, con=engine)
        map_info["map_info_id"] = map_info["map_info_id"].astype(str)

        # recover map_info
        sql_map_match = f"SELECT * from {params['provider_mapmatched_db']['schema']}.map_matched where map_match_id = '{map_matched_metadata['map_match_id']}'"
        map_matched = pd.read_sql_query(sql=sql_map_match, con=engine)
        map_matched["geom"] = map_matched["geom"].map(geometry_utils.ensure_geom)
        map_matched["map_match_id"] = map_matched["map_match_id"].astype(str)

    return map_matched, map_matched_metadata, map_info, map_info_metadata
