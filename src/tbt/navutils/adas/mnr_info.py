import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.geometry
import shapely.wkt
import sqlalchemy
import uuid
import typing
from . import kedro_utils


class MNRMapInfo:
    def __init__(self, country_iso, con: str = None):
        self.mnr_server, self.mnr_schema = select_mnr_server(country_iso, con)

    def get_mnr_con(self):
        return f'postgresql+psycopg2://mnr_ro:mnr_ro@{self.mnr_server}:5432/mnr'


def select_mnr_server(country_iso: str, con: str = None):
    """Select the MNR server and the newest schema where the country information is saved in MNR
    :param country_iso: ISO-A3 code of the country where to look for
    :type country_iso: str
    :param con: connection string to the PostgreSQL database
    :type con: str

    :return: name of the server where the information is saved
    :rtype: str
    :return: name of the most recent schema where the information is saved
    :rtype: str
    """
    # Create connection with PostgreSQL Database
    if con is None:
        con = kedro_utils.get_credentials()["db-credentials-speed-limits"]["con"]

    az_db = sqlalchemy.create_engine(con)

    # SQL Query to get the region of the selected country
    sql = f"SELECT region_wb \
        FROM new_speed_limits.country_borders \
        WHERE iso_a3 = '{country_iso}'"

    # Get the region in MNR for the selected country
    region = pd.read_sql(sql, con=az_db).values[0]

    # Select the MNR schema depending on the region
    if country_iso in ['USA', 'CAN', 'MEX', 'SPM']:
            mnr_server = 'caprod-cpp-pgmnr-005.flatns.net'
    else:
        if region in [
            'Middle East & North Africa', 'Sub-Saharan Africa',
            'Latin America & Caribbean',
            'East Asia & Pacific', 'South Asia'
            ]:
            mnr_server = 'caprod-cpp-pgmnr-002.flatns.net'
        else:
            mnr_server = 'caprod-cpp-pgmnr-001.flatns.net'

    # Once the mnr_server has been selected, specify the schema
    if mnr_server == 'caprod-cpp-pgmnr-005.flatns.net':
        schema = 'nam'
    else:
        con = sqlalchemy.create_engine(f'postgresql+psycopg2://mnr_ro:mnr_ro@{mnr_server}:5432/mnr')

        # SQL Query to get the most recent schemas of MNR database
        sql = "SELECT schema_name \
            FROM information_schema.schemata \
            WHERE schema_owner = 'mnr' \
            ORDER BY schema_name DESC"

        # Get the most recent MNR Database schema for the selected country
        df_schema = pd.read_sql(sql, con)  # Read MNR schema as pandas DataFrame
        schema = df_schema.loc[df_schema['schema_name'].str.contains(country_iso.lower()), 'schema_name'].values[0]  # Get the name of the most recent schema

    return mnr_server, schema


def get_latest_mnr_version(country_iso: str, con: str = None):
    # Get the server where the country's information is stored in MNR
    mnr = MNRMapInfo(country_iso, con)
    schema = mnr.mnr_schema

    # Create connection with MNR
    con = sqlalchemy.create_engine(mnr.get_mnr_con())

    # Retrieve version
    sql = f"select release_version from {schema}.mnr_meta_release"
    df = pd.read_sql(sql, con)
    version_str = df.squeeze()
    return version_str


def get_lane_details_mnr(country_iso: str, mnr_ids: str, con: str = None):
    """_summary_

    Args:
        country_iso (str): _description_
        mnr_ids (str): _description_
        con (str, optional): connection string to the PostgreSQL database. Defaults to None.
    """

    # Get the server where the country's information is stored in MNR
    mnr = MNRMapInfo(country_iso, con)
    schema = mnr.mnr_schema

    # Create connection with MNR
    con = sqlalchemy.create_engine(mnr.get_mnr_con())

    sql = f"""SELECT mnr_netw_route_link.netw_geo_id,
    mnr_netw_route_link.num_of_lanes,
    validity_direction,lane_validity,
    vt_passenger_car ,vt_resident,
    vt_taxi,
    vt_public_bus,
    vt_medium_truck,
    LENGTH(lane_validity) - LENGTH(REPLACE(lane_validity, '1', '')) AS occurrences_of_1,
    LENGTH(lane_validity) - LENGTH(REPLACE(lane_validity, '0', '')) AS occurrences_of_0
    FROM "{schema}".mnr_netw_route_link
    LEFT JOIN "{schema}".mnr_netw2lane_info ON mnr_netw_route_link.netw_geo_id = mnr_netw2lane_info.netw_id
    LEFT JOIN "{schema}".mnr_lane_info ON mnr_netw2lane_info.lane_info_id = mnr_lane_info.lane_info_id
    WHERE mnr_netw_route_link.num_of_lanes IS NOT null AND (mnr_netw2lane_info.lane_info_type = 1 OR mnr_netw2lane_info.lane_info_type IS NULL)
    AND netw_geo_id in {mnr_ids}
    """

    # Run query with GeoPandas
    mnr_answer = pd.read_sql(sql, con)

    # Transform GeoPandas DataFrame into Pandas DataFrame
    df_mnr_answer = pd.DataFrame(mnr_answer)

    return df_mnr_answer


def get_lacon_from_junction_id_list(country_iso: str, junction_id_list: list, con: str = None):
    """Get lane connectivity info from a list of junction ids.
    It is returned in mnr_lane_connect table format, where each row corresponds to a lane tuple.

    :param country_iso: ISO code of the country where to look for
    :type country_iso: str
    :param junction_id_list: list of junction ids
    :type junction_id_list: list
    :param con: connection string to the PostgreSQL database
    :type con: str
    :return: lane connectivity info in mnr_lane_connect table format
    :rtype: pd:DataFrame
    """

    # Get the server where the country's information is stored in MNR
    mnr = MNRMapInfo(country_iso, con)
    schema = mnr.mnr_schema

    # Create connection with MNR
    con = sqlalchemy.create_engine(mnr.get_mnr_con())

    lane_connect_query = f"""
        SELECT mlc.*, mm.junction_id, mm.geom
            FROM {schema}.mnr_lane_connect mlc
            JOIN {schema}.mnr_maneuver mm
            ON mlc.maneuver_id = mm.feat_id
            WHERE mm.junction_id IN {junction_id_list}
        """
    lane_connect_df = gpd.read_postgis(lane_connect_query, con)
    lane_connect_df = lane_connect_df.to_wkt()
    lane_connect_df = lane_connect_df.rename(columns={"geom": "junction_geom"})
    return lane_connect_df


def get_maneuver_members_from_maneuver_id(country_iso: str, maneuver_id_list: list, con: str = None):
    """Get the maneuver members from a list of maneuver ids.
    Info is saved in the mnr_maneuver_path_idx table in PostgreSQL database.

    :param country_iso: ISO code of the country where to look for
    :type country_iso: str
    :param maneuver_id_list: list of maneuver_id
    :type maneuver_id_list: list
    :param con: connection string to the PostgreSQL database
    :type con: str
    :return: get the maneuver members with their sequence number for every maneuver
    :rtype: pd.DataFrame
    """

    # Get the server where the country's information is stored in MNR
    mnr = MNRMapInfo(country_iso, con)
    schema = mnr.mnr_schema

    # Create connection with MNR
    con = sqlalchemy.create_engine(mnr.get_mnr_con())

    maneuver_members_query = f"""
        SELECT * FROM {schema}.mnr_maneuver_path_idx
            WHERE maneuver_id IN {maneuver_id_list}
        """
    maneuver_members_df = pd.read_sql(maneuver_members_query, con)
    maneuver_members_df["maneuver_seq"] = maneuver_members_df["maneuver_seq"].astype(int)
    maneuver_members_df[["maneuver_id", "netw_id"]] = maneuver_members_df[["maneuver_id", "netw_id"]].astype(str)
    return maneuver_members_df


def get_maneuver_geometry_from_maneuver_id(country_iso: str, maneuver_id_list: list, con: str = None):
    """Get the maneuver geometry from a list of maneuver ids.
    Info is saved in the mnr_maneuver_arrow table in PostgreSQL database.

    :param country_iso: ISO code of the country where to look for
    :type country_iso: str
    :param maneuver_id_list: list of maneuver_id
    :type maneuver_id_list: list
    :return: get the maneuver geometry and direction for every maneuver
    :rtype: pd.DataFrame
    """

    # Get the server where the country's information is stored in MNR
    mnr = MNRMapInfo(country_iso, con)
    schema = mnr.mnr_schema

    # Create connection with MNR
    con = sqlalchemy.create_engine(mnr.get_mnr_con())

    maneuver_members_query = f"""
        SELECT * FROM {schema}.mnr_maneuver_arrow
            WHERE maneuver_id IN {maneuver_id_list}
        """
    maneuver_geom_df = gpd.read_postgis(maneuver_members_query, con)
    maneuver_geom_df = maneuver_geom_df[["maneuver_id", "geom"]]
    maneuver_geom_df["maneuver_id"] = maneuver_geom_df["maneuver_id"].astype(str)
    maneuver_geom_df = maneuver_geom_df.to_wkt()
    maneuver_geom_df = maneuver_geom_df.rename(columns={"geom": "maneuver_geom"})
    return maneuver_geom_df
