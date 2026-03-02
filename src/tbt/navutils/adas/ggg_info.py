from distutils.log import error
import typing
import pandas as pd
import numpy as np
import sqlalchemy
import logging
import re
import geopandas as gpd
from shapely.geometry import LineString, Point
from ..common import decorators
import re
from ..adas.geometry_utils import get_distance_between_points, is_same_directed, create_combined_geom


default_re_pattern = r'^(lanes|maxspeed|divider|highway|routing_class|speed|junction|service|access|surface|oneway|gradient)'



def select_ggg_schema(
    ggg_con: str, country_iso: str
):  # TODO: improve to be able to select a specific schema
    """Select the GGG schema and the newest schema where the country information is saved in GGG
    :param ggg_con: SQAlchemy connection engine string
    :type ggg_con: str
    :param country_iso: ISO-A3 code of the country where to look for
    :type country_iso: str

    :return: name of the most recent schema where the information is saved
    :rtype: str
    """

    ggg_db = sqlalchemy.create_engine(ggg_con)

    # SQL Query to get the most recent schemas of GGG database
    sql = (
        "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name DESC"
    )
    df_schema = pd.read_sql(sql, ggg_db)

    # Drop schemas not belonging to countries
    excluded_schema_list = ['public', 'pg_toast', 'pg_catalog', 'information_schema']
    df_schema = df_schema.loc[~df_schema['schema_name'].isin(excluded_schema_list)].reset_index(drop=True)

    # Filter schema of the country that we are looking for
    schema = df_schema.loc[
        df_schema["schema_name"].str.contains(country_iso.lower()), "schema_name"
    ].values[
        0
    ]  # Get the name of the most recent schema

    return schema

@decorators.timing
def findTagsGGG(ggg_con: str, country_iso: str, osm_way_ids: typing.Tuple, return_nodes:bool=False):
    """
    Find TAGs in GGG database given the list of "osm_way_id" to search for in GGG

    :param ggg_con: SQAlchemy connection engine string
    :type ggg_con: str
    :param country_iso: ISO-A3 code of the country where to look for
    :type country_iso: str
    :param str osm_way_ids: str with the "way_id" to search for in GGG
    :type osm_way_ids: tuple
    :param str return_nodes: True if nodes list is needed in the response. Default false
    :type return_nodes: bool
    :return: DataFrame with TAGs
    :rtype: Pandas.DataFrame()
    """

    # Get the server where the country's information is stored in MNR
    ggg_schema = select_ggg_schema(ggg_con, country_iso)

    osm_way_ids_str = ",".join(["'" + str(osm_id) + "'" for osm_id in osm_way_ids])

    if return_nodes:
        nodes_sql = "posmway.nodes, "
    else:
        nodes_sql = ""

    sql = f"""SELECT posmway.id,{nodes_sql} posmway.tags\
        FROM {ggg_schema}.planet_osm_ways AS posmway \
        WHERE posmway.id IN ({osm_way_ids_str})"""

    ggg_answer = pd.read_sql(sql, ggg_con)
    return ggg_answer


def tag_processor(tag, regex_tags=default_re_pattern):
    parsed = {}
    if len(tag) % 2 != 0:
        logging.warning("unbalanced tags processing way_id")
    for i in range(0, len(tag) - 1, 2):
        matches = re.findall(regex_tags, tag[i])
        if matches:
            parsed[tag[i]] = tag[i + 1]
    return parsed


def get_ggg_tags(
    ggg_con: str, country_iso: str, osm_way_ids: typing.Tuple, return_nodes:bool=False, regex_tags: str=default_re_pattern
) -> pd.DataFrame:
    """
    Get the OSM tags for each "osm_way_id"

    :param ggg_con: SQAlchemy connection engine string
    :type ggg_con: str
    :param country_iso: ISO-A3 code of the country where to look for
    :type country_iso: str
    :param str osm_way_ids: str with the "way_id" to search for in GGG
    :type osm_way_ids: tuple
    :param str return_nodes: True if nodes list is needed in the response. Default false
    :type return_nodes: bool

    :return: DataFrame with tags as columns
    :rtype: Pandas.DataFrame()
    """
    osm_tags = findTagsGGG(
        ggg_con=ggg_con,
        country_iso=country_iso,
        osm_way_ids=osm_way_ids,
        return_nodes=return_nodes
    )
    osm_tags["tags_p"] = osm_tags["tags"].apply(lambda tag: tag_processor(tag, regex_tags))
    osm_tags_df = pd.DataFrame(
        osm_tags["tags_p"].tolist(), index=osm_tags.id
    ).reset_index()
    if return_nodes:
        osm_tags_df['nodes'] = osm_tags['nodes']

    recovered_tags = osm_tags["id"].nunique()
    request_tags = len(osm_way_ids)
    perc_loss = (request_tags - recovered_tags) / request_tags
    if perc_loss > 0.1:
        logging.warning(
            f"Requested {request_tags} way_id tags, get {recovered_tags} way_id tags, loss: {perc_loss}"
        )
    else:
        logging.info(
            f"Requested {request_tags} way_id tags, get {recovered_tags} way_id tags, loss: {perc_loss}"
        )

    return osm_tags_df


@decorators.timing
def get_ggg_geom(conn, country_iso, way_ids):
    """
    Get the 3G geometries for each "osm_way_id"

    :param ggg_con: SQAlchemy connection engine string
    :type ggg_con: str
    :param country_iso: ISO-A3 code of the country where to look for
    :type country_iso: str
    :param str way_ids: str with the "way_id" to search for in GGG
    :type way_ids: tuple

    :return: DataFrame with tags as columns
    :rtype: Pandas.DataFrame()
    """
    osm_way_ids_str = ",".join([str(osm_id) for osm_id in way_ids])

    schema = select_ggg_schema(conn, country_iso)

    df_way_geom = gpd.read_postgis(f"""
    select osm_id, way as geom
    from {schema}.planet_osm_line pow where osm_id IN ({osm_way_ids_str})
    """, conn)

    recovered_tags = df_way_geom["osm_id"].nunique()
    request_tags = len(way_ids)
    perc_loss = (request_tags - recovered_tags) / request_tags
    if perc_loss > 0.1:
        logging.warning(
            f"Requested {request_tags} way_id tags, get {recovered_tags} way_id geometries, loss: {perc_loss}"
        )
    else:
        logging.info(
            f"Requested {request_tags} way_id tags, get {recovered_tags} way_id geometries, loss: {perc_loss}"
        )


    return df_way_geom


@decorators.timing
def get_ggg_node_geom(conn, country_iso, node_ids):
    """
    Get the 3G geometries for each "osm_node_id"

    :param ggg_con: SQAlchemy connection engine string
    :type ggg_con: str
    :param country_iso: ISO-A3 code of the country where to look for
    :type country_iso: str
    :param str node_ids: str with the "node_id" to search for in GGG
    :type way_ids: tuple

    :return: DataFrame with tags as columns
    :rtype: Pandas.DataFrame()
    """
    node_ids = node_ids.dropna()
    node_ids_str = ",".join([str(osm_id) for osm_id in node_ids])

    schema = select_ggg_schema(conn, country_iso)

    df_node_geom = pd.read_sql(f"""
    select *
    from {schema}.planet_osm_nodes pow where id IN ({node_ids_str})
    """, conn)

    df_node_geom["lat"] = df_node_geom["lat"]/10**7
    df_node_geom["lon"] = df_node_geom["lon"]/10**7

    gdf_node_geom = gpd.GeoDataFrame(
        df_node_geom, geometry=gpd.points_from_xy(df_node_geom.lon, df_node_geom.lat), crs="EPSG:4326"
    )

    gdf_node_geom = gdf_node_geom.drop(columns=['lat','lon'])

    recovered_tags = gdf_node_geom["id"].nunique()
    request_tags = len(node_ids)
    perc_loss = (request_tags - recovered_tags) / request_tags
    if perc_loss > 0.1:
        logging.warning(
            f"Requested {request_tags} node_id tags, get {recovered_tags} node_id geometries, loss: {perc_loss}"
        )
    else:
        logging.info(
            f"Requested {request_tags} node_id tags, get {recovered_tags} node_id geometries, loss: {perc_loss}"
        )


    return gdf_node_geom


@decorators.timing
def get_ggg_relations_by_way(conn, country_iso, features, relation_type='destination_sign', filter_tags=None):
    """
    Get the 3G relations for each "way_ids" the relation can be in

    :param conn: SQAlchemy connection engine string
    :type conn: str
    :param country_iso: ISO-A3 code of the country where to look for
    :type country_iso: str
    :param features: DataFrame with the information about the way_ids and original geometries to get only relations in the same direction as sample
    :type features: Pandas.DataFrame()
    :param relation_type: str with the relation type we want to get example: destination_sign for signposts or conectivity for lane conn
    :type relation_type: str
    :param filter_tags: str with a tag to filter some rows for exmple if we look only for ttpropietary relations
    :type filter_tags: str


    :return: DataFrame with tags as columns
    :rtype: Pandas.DataFrame()
    """

    # Aux function to get the last from and the first to and intermediate via if they are a way:
    def extract_ways_in_relation(row):
        last_from = None
        first_to = None
        vias_way = []
        pairs = zip(row[::2], row[1::2])
        for (way, relation) in pairs:
            if 'from' in relation:
                last_from = int(way[1:])
            elif 'to' in relation  and first_to is None:
                first_to = int(way[1:])
            elif 'via' in relation and way.startswith('w'):
                vias_way.append(int(way[1:]))

        return [last_from, *vias_way, first_to]

    # Get all relations for the sample
    set_way_ids = set(tuple(features['feature_id'].unique().astype(int).tolist()))
    schema = select_ggg_schema(conn, country_iso)
    sql = f"""
    select id, parts, members, tags
    from {schema}.planet_osm_rels por
    WHERE '{relation_type}' LIKE ANY(por.tags)
    """
    #AND ARRAY[{', '.join(map(str, set_way_ids))}] && por.parts

    relations = pd.read_sql(sql, conn)
    # filter tags based on input parameters for example ttpropietary
    if filter_tags is not None:
        relations = relations[relations['tags'].apply(lambda x: filter_tags in x)]

    # FILTER RELATIONS TO BE ONLY "FROM" AND "TO"
    # Get way id's for the participants and 'from' relation
    relations['from_way_id'] = relations['members'].apply(lambda x: int(x[0][1:])).astype(int)
    relations['way_id_participants'] = relations['members'].apply(extract_ways_in_relation)
    relations['to_way_id'] = relations['way_id_participants'].map(lambda x: x[-1])

    # Get node geom for the decision point (first node in "via")
    # This is used in the inspection pipeline to generate the central segment
    def extract_via_node_in_relation(row):
        pairs = zip(row[::2], row[1::2])
        for (member_id, relation) in pairs:
            if 'via' in relation and member_id.startswith('n'):
                return int(member_id[1:])

    relations['via_decision_point_node_id'] = relations['members'].apply(extract_via_node_in_relation)
    # Add geometry to the decision point node
    via_decision_point_geom = get_ggg_node_geom(conn, country_iso, relations['via_decision_point_node_id'])
    via_decision_point_geom = via_decision_point_geom.rename(columns={'id':'via_decision_point_node_id', 'geometry': 'via_decision_point_geom'})
    relations = relations.merge(via_decision_point_geom, how='left', on='via_decision_point_node_id')


    # Get only relations for the way_id's I'm traversing (FROM)
    relations = features[['feature_id','geom']].drop_duplicates().merge(relations, how='inner', left_on='feature_id', right_on='from_way_id').drop(columns=['feature_id'])
    relations = relations.rename(columns={'geom':'from_original_geom'})
    # Get only relations for the way_id's I'm traversing (TO)
    relations = features[['feature_id']].drop_duplicates().merge(relations, how='inner', left_on='feature_id', right_on='to_way_id').drop(columns=['feature_id'])

    # ADD GEOMETRIES to all the ways in the relations
    wayids_in_participants = relations.explode('way_id_participants')['way_id_participants'].dropna().unique()
    all_geoms_participants = get_ggg_geom(conn, country_iso, wayids_in_participants)
    #TODO assess if this is needed and clean up accordingly
    # Add "FROM" relation geoms
    all_geoms_participants = all_geoms_participants.rename(columns={'osm_id':'from_way_id',
                                                                    'geom':'from_relation_geom',})
    relations = relations.merge(all_geoms_participants, how='left', on='from_way_id')
    # Add "TO" relation geoms
    all_geoms_participants = all_geoms_participants.rename(columns={'from_way_id':'to_way_id',
                                                                    'from_relation_geom':'to_relation_geom'})
    relations = relations.merge(all_geoms_participants, how='left', on='to_way_id')
    relations = relations.drop_duplicates(subset=['from_way_id','id'])

    # Drop all relations without from_relation_geom geometry
    relations = relations[~relations['from_relation_geom'].isna()]
    # Drop all relations without to_relation_geom geometry
    relations = relations[~relations['to_relation_geom'].isna()]
    # Check if from_way is in the same direction as sample_direction
    relations['from_way_same_directed'] = relations.apply(lambda row: is_same_directed(row['from_original_geom'], row['from_relation_geom']), axis=1)
    # Compute distances From1 (first from coordinate), From2 (last from coordinate), To1 (first to coordinate), To2 (last to coordinate)
    # Need F2T1, F1T2, F2T2, F1T1
    # If from and sample are in the SAME diretion:
    # - F2T1 < F1T2
    # - F2T2 < F1T1
    # If from and sample are in REVERSE diretion:
    # - F2T1 > F1T2
    # - F2T2 > F1T1
    def relation_trough_from(from_way_same_directed,F2T1,F1T2,F2T2,F1T1):
        if from_way_same_directed:
            return F2T1 < F1T2 or F2T2 < F1T1
        else:
            return F2T1 > F1T2 or F2T2 > F1T1

    relations['F2T1'] = relations.apply(lambda row: get_distance_between_points(row['from_relation_geom'].coords[-1], row['to_relation_geom'].coords[0]), axis=1)
    relations['F1T2'] = relations.apply(lambda row: get_distance_between_points(row['from_relation_geom'].coords[0], row['to_relation_geom'].coords[-1]), axis=1)
    relations['F2T2'] = relations.apply(lambda row: get_distance_between_points(row['from_relation_geom'].coords[-1], row['to_relation_geom'].coords[-1]), axis=1)
    relations['F1T1'] = relations.apply(lambda row: get_distance_between_points(row['from_relation_geom'].coords[0], row['to_relation_geom'].coords[0]), axis=1)
    relations['relation_correct_direction'] = relations.apply(lambda row: \
                                                              relation_trough_from(row['from_way_same_directed'],
                                                                                   row['F2T1'], row['F1T2'], row['F2T2'], row['F1T1']), axis=1)
    # Filter only relations in sample direction and drop columns
    relations = relations[relations['relation_correct_direction']==True]
    relations = relations.drop(columns=['F2T1','F1T2','F2T2','F1T1','relation_correct_direction'])

    # Generate a geometry combining the from and the to in the relation
    #TODO algorithm to combine the geometries needs to be overhauled: this just combines the first and last ways
    # It should generate a full relation geometry by taking all the ways in the relation (including the via) and combining them
    # The way ids (including via ones) are saved in the "way_id_participants" field in the correct sequence
    # The challenge is that the geometries are not always in the correct direction
    relations['combined_relation_geometry'] = relations.apply(lambda x: create_combined_geom(x['from_relation_geom'], x['to_relation_geom']), axis=1)


    relations.rename(columns={'id':'relation_id'},inplace=True)
    recovered_ways = relations['from_way_id'].nunique()
    request_rels = len(set_way_ids)
    perc_loss = (request_rels - recovered_ways) / request_rels
    if perc_loss > 0.1:
        logging.warning(
            f"Requested {request_rels} id rels, get {recovered_ways} id rels, loss: {perc_loss}"
        )
    else:
        logging.info(
            f"Requested {request_rels} id rels, get {recovered_ways} id rels, loss: {perc_loss}"
        )

    relations = relations.add_prefix(f"{relation_type}_")
    return relations


def get_bus_only_n(bus: str, hov: str) -> int:
    """Function to process the string lanes to get the Bus only designated

    :param bus: Bus lanes string ex: designated||||
    :type bus: str
    :param hov: Hov lanes string ex: designated|yes|yes|yes
    :type hov: str

    :return: bus_only count
    :rtype: int
    """
    if bus == hov:  # Same number of bus lane as hov lane (shared and same positions)
        return 0

    pattern = "designated"

    if isinstance(bus, str):
        bus_n = len(re.findall(pattern, bus))
        buses = bus.split("|")
    else:
        bus_n = 0

    if isinstance(hov, str):
        hov_n = len(re.findall(pattern, hov))
        hovs = hov.split("|")
    else:
        hov_n = 0

    if bus_n == 0:  # No bus lanes only hov > 0
        return 0

    if hov_n == 0:  # No hov only bus:
        return bus_n

    # Most complicated: buses and hovs: study positions

    if len(buses) != len(hovs):
        logging.warning("Something is wrong processing lanes for buses and hov")
    else:
        onlybus_n = 0
        for bus_l, hov_l in zip(buses, hovs):
            if (bus_l != hov_l) and bus_l == pattern:  # Only bus
                onlybus_n += 1
        return onlybus_n

    return 0


def get_hov_n(hov: str) -> int:
    """Function to process the string lanes to get the hov lanes

    :param hov: Hov lanes string ex: designated|yes|yes|yes
    :type hov: str

    :return: bus_only count
    :rtype: int
    """
    pattern = "designated"
    if isinstance(hov, str):
        hov_n = len(re.findall(pattern, hov))
    else:
        hov_n = 0

    return hov_n


def get_psv_only_n(access: str) -> int:
    pattern = r"(no|psv|bus|taxi)"
    if isinstance(access, str):
        access_n = len(re.findall(pattern, access))
    else:
        access_n = 0
    return access_n


def get_osm_lane_empty_only(osm_tags_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find the number of lanes with no info in OSM tags for each "osm_way_id"

    :param osm_tags_df: Dataframe with all OSM tags for the requested way_ids
    :type osm_tags_df: Pandas.DataFrame()
    :return: DataFrame with ID and Lanes info
    :rtype: Pandas.DataFrame()
    """

    if "lanes" not in osm_tags_df.columns:
        osm_tags_df["lanes"] = None
    if "lanes:forward" not in osm_tags_df.columns:
        osm_tags_df["lanes:forward"] = None
    if "lanes:backward" not in osm_tags_df.columns:
        osm_tags_df["lanes:backward"] = None


    return osm_tags_df[["id", "lanes", "lanes:forward", "lanes:backward"]]


def get_osm_bus_only(osm_tags_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find the number of bus only  lanes in OSM tags for each "osm_way_id"

    :param osm_tags_df: Dataframe with all OSM tags for the requested way_ids
    :type osm_tags_df: Pandas.DataFrame()
    :return: DataFrame with ID and BUS ONLY
    :rtype: Pandas.DataFrame()
    """

    if "hov:lanes" not in osm_tags_df.columns:
        osm_tags_df["hov:lanes"] = 0
    if "bus:lanes" not in osm_tags_df.columns:
        osm_tags_df["bus_only"] = 0
        return osm_tags_df[["id", "bus_only"]]
    osm_tags_df["bus_only"] = osm_tags_df[["bus:lanes", "hov:lanes"]].apply(
        lambda x: get_bus_only_n(x["bus:lanes"], x["hov:lanes"]), axis=1
    )
    osm_tags_df["hov"] = osm_tags_df[["hov:lanes"]].apply(
        lambda x: get_hov_n(x["hov:lanes"]), axis=1
    )
    return osm_tags_df[["id", "bus_only", "hov"]]


def get_osm_psv_only(osm_tags_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find the number of Not public lanes in OSM tags for each "osm_way_id"

    :param osm_tags_df: SQAlchemy connection engine string
    :type osm_tags_df: str

    :return: DataFrame with ID and PSV ONLY
    :rtype: Pandas.DataFrame()
    """

    if "access:lanes" not in osm_tags_df.columns:
        osm_tags_df["psv_only"] = 0
        return osm_tags_df[["id", "psv_only"]]

    osm_tags_df["psv_only"] = osm_tags_df["access:lanes"].map(get_psv_only_n)
    return osm_tags_df[["id", "psv_only"]]


def get_osm_bidirectional(osm_tags_df: pd.DataFrame) -> pd.DataFrame:
    # oneway=reversible, oneway=alternating , conditional
    if "oneway" not in osm_tags_df.columns:
        return pd.DataFrame()

    bidirectional_values = ["reversible", "alternating", "conditional"]
    osm_tags_df = osm_tags_df[osm_tags_df["oneway"].isin(bidirectional_values)][["id"]]
    osm_tags_df["bidirectional"] = True

    return osm_tags_df[["id", "bidirectional"]]


def get_osm_suicide_lane(osm_tags_df: pd.DataFrame) -> pd.DataFrame:
    # Key:lanes:both_ways
    if "lanes:both_ways" not in osm_tags_df.columns:
        return pd.DataFrame()

    osm_tags_df["suicide_lane"] = osm_tags_df["lanes:both_ways"].dropna()
    return osm_tags_df[["id", "suicide_lane"]]


def filter_osm_empty(
    tags_df: pd.DataFrame,
    matched_df: pd.DataFrame,
    matched_id_column: str = "way_id_competitor",
    lanes_column: str = "competitor_lane_counts",
):
    """
    Takes an input matched dataframe and return the lanes where osm have no info as boolean

    :param tags_df: Tags found for specified Way id
    :type tags_df: Pandas.DataFrame()
    :param matched_df: Traces dataframe to substract bus only lanes
    :type matched_df: Pandas.DataFrame()
    :param matched_id_column: String to select the correct column for the way_id
    :type matched_id_column: str
    :param lanes_column: String to select the correct column for the lane count
    :type lanes_column: str

    :return: DataFrame without the default lane counts
    :rtype: Pandas.DataFrame()
    """
    empty_lane_df = get_osm_lane_empty_only(tags_df)
    empty_lane_df = empty_lane_df.rename(
        columns={"lanes": "competitor_empty_lanes",
                 "lanes:forward": "competitor_empty_lanes_forward",
                 "lanes:backward": "competitor_empty_lanes_backward"}
    ).copy()
    matched_df_empty = matched_df.merge(
        empty_lane_df, how="left", left_on=matched_id_column, right_on="id"
    )
    matched_df_empty.drop(columns="id", inplace=True)
    # Put nan in all cases where matches
    def select_correct_lane(competitor, tag, forward, backward):
        if competitor > 1:
            return competitor
        if pd.isnull(tag) and pd.isnull(forward) and pd.isnull(backward): # BUG: FIX THIS ERROR
            return 0
        return competitor

    matched_df_empty[lanes_column] = matched_df_empty.apply(
        lambda x: select_correct_lane(x[lanes_column],
                                      x["competitor_empty_lanes"],
                                      x["competitor_empty_lanes_forward"],
                                      x["competitor_empty_lanes_backward"]),
        axis=1,
    )
    matched_df_empty.drop(columns=["competitor_empty_lanes",
                                   "competitor_empty_lanes_forward",
                                   "competitor_empty_lanes_backward"], inplace=True)
    return matched_df_empty


def add_info_bus_only_lanes(
    tags_df: pd.DataFrame,
    matched_df: pd.DataFrame,
    way_id_col: str = "competitor_way_id",
):
    """
    Takes an input matched dataframe and substracts bus only lanes to competitor lane counts

    :param tags_df: Tags found for specified Way id
    :type tags_df: Pandas.DataFrame()
    :param matched_df: Traces dataframe to substract bus only lanes
    :type matched_df: Pandas.DataFrame()
    :param way_id_col: Column name for the way_id to join
    :type way_id_col: str

    :return: DataFrame with ID and BUS ONLY
    :rtype: Pandas.DataFrame()
    """
    bus_only_df = get_osm_bus_only(tags_df)
    matched_df_bus = matched_df.merge(
        bus_only_df, how="left", left_on=way_id_col, right_on="id"
    )
    matched_df_bus.drop(columns="id", inplace=True)
    # matched_df_bus['competitor_lane_counts_filtered'] = matched_df_bus['competitor_lane_counts']-matched_df_bus['bus_only']
    # if matched_df_bus['competitor_lane_counts'].min()<0:
    #     logging.error("Something goes wrong with bus only lane substraction")
    return matched_df_bus


def add_info_not_accesible_lanes(
    tags_df: pd.DataFrame,
    matched_df: pd.DataFrame,
    way_id_col: str = "competitor_way_id",
):
    """
    Takes an input matched dataframe and substracts bus only lanes and other psv, taxi not accesible to the general public to competitor lane counts

    :param tags_df: Tags found for specified Way id
    :type tags_df: Pandas.DataFrame()
    :param matched_df: Traces dataframe to substract bus only lanes
    :type matched_df: Pandas.DataFrame()
    :param way_id_col: Column name for the way_id to join
    :type way_id_col: str


    :return: DataFrame with ID and PSV ONLY
    :rtype: Pandas.DataFrame()
    """
    psv_only_df = get_osm_psv_only(tags_df)
    matched_df_psv = matched_df.merge(
        psv_only_df, how="left", left_on=way_id_col, right_on="id"
    )
    matched_df_psv.drop(columns="id", inplace=True)
    # Discard all bus lane filtered:
    matched_df_psv["psv_only"] = matched_df_psv["psv_only"].where(
        matched_df_psv["bus_only"] == 0, 0
    )
    # matched_df_psv['competitor_lane_counts_filtered'] = matched_df_psv['competitor_lane_counts_filtered']-matched_df_psv['psv_only']
    # if matched_df_psv['competitor_lane_counts_filtered'].min()<0:
    #     logging.error("Something goes wrong with psv only lane substraction")
    return matched_df_psv


def add_info_bidirectional_lanes(
    tags_df: pd.DataFrame,
    matched_df: pd.DataFrame,
    way_id_col: str = "competitor_way_id",
):
    """
    Takes an input matched dataframe and filter all bidirectional as boolean

    :param tags_df: Tags found for specified Way id
    :type tags_df: Pandas.DataFrame()
    :param matched_df: Traces dataframe to substract bus only lanes
    :type matched_df: Pandas.DataFrame()
    :param way_id_col: Column name for the way_id to join
    :type way_id_col: str


    :return: DataFrame with ID and BIDIRECTIONAL
    :rtype: Pandas.DataFrame()
    """
    bidirectional_df = get_osm_bidirectional(tags_df)
    if len(bidirectional_df) == 0:
        matched_df["bidirectional"] = False
        return matched_df

    matched_df_bidirectional = matched_df.merge(
        bidirectional_df, how="left", left_on=way_id_col, right_on="id"
    )
    matched_df_bidirectional.drop(columns="id", inplace=True)
    matched_df_bidirectional["bidirectional"] = matched_df_bidirectional[
        "bidirectional"
    ].fillna(False)

    return matched_df_bidirectional


def add_info_suicide_lanes(
    tags_df: pd.DataFrame,
    matched_df: pd.DataFrame,
    way_id_col: str = "competitor_way_id",
    lane_counts_col: str = "competitor_lane_counts",
):
    """
    Takes an input matched dataframe and substracts suicide lanes

    :param tags_df: Tags found for specified Way id
    :type tags_df: Pandas.DataFrame()
    :param matched_df: Traces dataframe to substract bus only lanes
    :type matched_df: Pandas.DataFrame()
    :param way_id_col: Column name for the way_id to join
    :type way_id_col: str
    :param lane_counts_col: Column name the lane counts, if OSM as competitor, suicide is not counted in the lane counts
    :type lane_counts_col: str

    :return: DataFrame with ID and SUICIDE_LANE
    :rtype: Pandas.DataFrame()
    """
    suicide_lane_df = get_osm_suicide_lane(tags_df)
    if len(suicide_lane_df) == 0:
        matched_df["suicide_lane"] = 0
        return matched_df

    suicide_lane_df["suicide_lane"] = (
        suicide_lane_df["suicide_lane"]
        .astype(matched_df[way_id_col].dtype, errors="ignore")
        .fillna(0)
    )
    matched_df_suicide_lane = matched_df.merge(
        suicide_lane_df, how="left", left_on=way_id_col, right_on="id"
    )
    matched_df_suicide_lane.drop(columns="id", inplace=True)
    matched_df_suicide_lane["suicide_lane"] = (
        matched_df_suicide_lane["suicide_lane"].astype(int, errors="ignore").fillna(0)
    )

    ## OSM don't count suicide lanes so: add
    matched_df_suicide_lane[lane_counts_col] = (
        matched_df_suicide_lane[lane_counts_col]
        + matched_df_suicide_lane["suicide_lane"]
    )

    return matched_df_suicide_lane
