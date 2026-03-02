import numpy as np
from shapely import wkb, wkt, geometry
from shapely import make_valid
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import shapely
from shapely import wkb, wkt
from shapely import geometry
from shapely import ops
import pyproj
import math
import requests
import pandas as pd
import geopandas as gpd
import logging
import typing
from typing import Union
from shapely.geometry import LineString, Point, MultiLineString, MultiPoint


# Default ellipse for the project
geodesic = pyproj.Geod(ellps="WGS84")


def get_length(geom: geometry.LineString) -> float:
    """Compute the length given a shapely object (sum of the distance of it's coordinates using WGS84 ellipsoid)
    If the geom is a string we cast it to shapely format

    :param geom: Shapely object (must be iterable in coords: LINESTRING)
    :type geom: geometry.LineString
    :return: Length in meters for the provided geometry
    :rtype: float
    """
    geom = ensure_geom(geom) # Make sure the geom is shapely object
    try:
        coords = geom.coords[:]

        if geom.has_z:
            length = sum(
                geodesic.inv(lon1, lat1, lon2, lat2)[2]
                for (lon1, lat1, _), (lon2, lat2, _) in zip(coords[:-1], coords[1:])
            )
        else:
            length = sum(
                geodesic.inv(lon1, lat1, lon2, lat2)[2]
                for (lon1, lat1), (lon2, lat2) in zip(coords[:-1], coords[1:])
            )
    except Exception as ex:
        logging.warning(f"Problem measuring the distance: {ex}")
        length = 0.0
    return length

def get_linestringz_total_time_in_seconds(linestringz: geometry.LineString) -> float:
    """Get the total time in seconds for a linestring with third component UTC timestamp

    :param linestringz: Line with a third component UTC timestamp
    :type linestringz: geometry.LineString
    :return: Total time in seconds for this line
    :rtype: float
    """
    return (linestringz.interpolate(1, normalized=True).coords[0][2] - linestringz.interpolate(0, normalized=True).coords[0][2])/1000

def get_linestringz_avg_speed_in_ms(line:geometry.LineString)->float:
    """Get the avg speed in meters per second for a given linestringz

    :param line: linestring in shapely format with a third component
    :type line: geometry.LineString
    :return: Value for the avg speed in meters per second
    :rtype: float
    """
    total_length = get_length(line)
    total_time = get_linestringz_total_time_in_seconds(line)
    tolerance = 1e-3  # Set a small tolerance value (1e-3 = 1 milisecond)
    if abs(total_time) > tolerance:
        return total_length/total_time
    else:
        logging.warning(f"Total time is less than 0.001 seconds for line {line}. Returning 0 for the avg speed")
        return 0.0

def get_distance_between_points(point1: Union[geometry.Point, tuple], point2: Union[geometry.Point, tuple]) -> float:
    """Compute the distance between two Shapely points using WGS84 ellipsoid

    :param point1: First Shapely Point
    :type point1: shapely.geometry.Point
    :param point2: Second Shapely Point
    :type point2: shapely.geometry.Point
    :return: Distance in meters between the two points
    :rtype: float
    """
    try:
        if isinstance(point1,geometry.Point):
            lon1, lat1 = point1.x, point1.y
        else:
            lon1, lat1 = point1[:2]

        if isinstance(point2,geometry.Point):
            lon2, lat2 = point2.x, point2.y
        else:
            lon2, lat2 = point2[:2]

        _, _, distance = geodesic.inv(lon1, lat1, lon2, lat2)
    except:
        distance = 0
    return distance


def intersect_trace_and_polygon(
        trace: shapely.geometry.LineString,
        polygon:shapely.geometry.Polygon,
        ) -> list:
    """Get the intersection between a trace and a polygon

    :param trace: trace to intersect with the polygon
    :type trace: shapely.geometry.LineString
    :param polygon: polygon to intersect with the trace
    :type polygon: shapely.geometry.Polygon
    :return: List of LineStrings that are the intersection between the trace and the polygon
    :rtype: list
    """
    # Check that polygon is valid, else try to make it valid
    try:
        valid_polygon = make_valid(polygon)
    except TypeError:
        logging.warning("Could not make the polygon valid, attempting to load it as a WKT")
        try:
            valid_polygon = make_valid(wkt.loads(polygon))
            logging.info("Polygon loaded as WKT and validated correctly")
        except Exception as ex:
            logging.warning(f"Invalid polygon provided for intersection: {polygon}. Please provide a valid polygon.")
            raise ex

    # Intersect valid polygon with the trace
    intersect_geom = valid_polygon.intersection(trace)

    # If the intersection is a MultiLineString or a GeometryCollection,
    # convert it to a list of LineStrings
    # If it is a LineString, return it as a list of one LineString.
    # On any other case, return an empty LineString.
    if isinstance(intersect_geom, shapely.geometry.MultiLineString) or isinstance(
        intersect_geom, shapely.geometry.collection.GeometryCollection
    ):
        intersect_geom = [
            intersect
            for intersect in intersect_geom.geoms
            if isinstance(intersect, shapely.geometry.linestring.LineString)
        ]
    elif isinstance(intersect_geom, shapely.geometry.linestring.LineString):
        intersect_geom = [intersect_geom]
    else:
        intersect_geom = [shapely.geometry.linestring.LineString()]
    return intersect_geom


def ensure_geom(geom) -> geometry.LineString:
    """
    Converts possible geometry string values to shapely geometry

    :param geom: original geometry to cast
    :type geom: shapely.geometry or str

    :return: Correct parsed geometry
    :rtype: shapely.geometry
    """
    if isinstance(geom, str):
        if (
            geom.startswith("LINE")
            or geom.startswith("POIN")
            or geom.startswith("MULTI")
            or geom.startswith("POLY")
        ):
            geo = wkt.loads(geom)
        else:
            geo = wkb.loads(geom, hex=True)
        return geo
    return geom


def spherical_heading(
    point: shapely.geometry.Point, headed: shapely.geometry.Point
) -> float:
    """Compute heading in degrees (clockwise from north) between two WGS 84 points."""
    # source: https://www.movable-type.co.uk/scripts/latlong.html#bearing

    delta_lon = math.radians(headed.x - point.x)
    lat_p = math.radians(point.y)
    lat_h = math.radians(headed.y)

    y = math.cos(lat_h) * math.sin(delta_lon)
    x = math.cos(lat_p) * math.sin(lat_h) - math.sin(lat_p) * math.cos(
        lat_h
    ) * math.cos(delta_lon)
    heading = math.atan2(y, x)
    return math.degrees(heading)


def get_las_heading(line: geometry.LineString) -> float:
    """Compute the bearing in degrees for a given linestring

    :param line: Linestring in shapely object
    :type line: geometry.LineString
    :return: heading in degrees
    :rtype: float
    """
    point = shapely.geometry.Point(line.coords[0])
    heading = shapely.geometry.Point(line.coords[-1])
    return spherical_heading(point, heading)


def get_bearing(line: geometry.LineString) -> float:
    (long1, lat1) = line.coords[0]
    (long2, lat2) = line.coords[-1]
    fwd_azimuth, back_azimuth, distance = geodesic.inv(long1, lat1, long2, lat2)
    return fwd_azimuth


def get_angle(line: geometry.LineString) -> float:
    """
    Get the angle of a line based on it's first and last point

    :param line: Tags found for specified Way id
    :type line: geometry.LineString

    :return: Value corresponding with the angle
    :rtype: int
    """
    pt1 = line.coords[0]
    pt2 = line.coords[-1]
    x_diff = pt2[0] - pt1[0]
    y_diff = pt2[1] - pt1[1]
    return (math.degrees(math.atan2(y_diff, x_diff)) - 90) % 360



def get_following_point(line, point, forward_perc=0.1, forward_abs=None):
    if forward_abs is not None:
        forward_dist = forward_abs
        norm = False
    else:
        forward_dist = forward_perc
        norm = True
    distance_norm = line.project(point, normalized=norm)
    following_point = line.interpolate(distance_norm + forward_dist, normalized=norm)
    return following_point


def get_delta_angle(angle_a: int, angle_b: int) -> int:
    """Get the angle difference in degrees between tho angles

    :param angle_a: first angle to compare
    :type angle_a: int
    :param angle_b: second angle to compare
    :type angle_b: int
    :return: distance between angles in degrees
    :rtype: int
    """
    a = angle_a - angle_b
    return (a + 180) % 360 - 180


def flat_multlinestring(all_geoms):
    """Takes al values in a list and flatten it into a unique list of linestrings. It transforms list multilinestrings and linestring to a simple list of linestrings.

    :param all_geoms: List of the following possible values: geometry.Multilinestring or geometry.LineString
    :type all_geoms: typing.List[shapely.geometry]
    :return: Flattened list of linestrings
    :rtype: _type_
    """
    linestrings = []
    for geom in all_geoms:
        if isinstance(geom, geometry.MultiLineString):
            linestrings.extend([intersect for intersect in geom.geoms])
        else:
            linestrings.append(geom)

    return linestrings


def get_geoserver_features(url, typeName, min_lat, min_lon, max_lat, max_lon):
    """Function to get Features from a geoserver.

    :param url: lower value for latitude bbox
    :type url: string
    :param typeName: Type name for the feature requested
    :type typeName: string
    :param min_lat: lower value for latitude bbox
    :type min_lat: float
    :param min_lon: lower value for longitude bbox
    :type min_lon: float
    :param max_lat: top value for latitude bbox
    :type max_lat: float
    :param max_lon: top value for longitude bbox
    :type max_lon: float
    :return: _description_
    :rtype: pd.DataFrame()
    """
    req = f"request=GetFeature&typeName={typeName}&outputFormat=application%2Fjson&BBOX={min_lat},{min_lon},{max_lat},{max_lon}"
    resp = requests.get(url=url + req)
    if resp.status_code != 200:
        logging.warning(f"Something goes wrong with geoserver  {resp.content}")
    else:
        json_data = resp.json()
        return pd.json_normalize(json_data["features"])


def get_point_bearing(
    point: typing.Union[geometry.Point, str],
    line: typing.Union[geometry.LineString, str],
) -> float:
    """Get the correct bearing given the point and the line that contains this point

    :param point: Point to calculate the bearing from
    :type point: geometry.Point
    :param line: Linestring along the way to get the heading in the correct direction
    :type line: geometry.LineString
    :return: The bearing angle (clockwise from north)
    :rtype: float
    """
    if isinstance(point, str):
        point = ensure_geom(point)
    if isinstance(line, str):
        line = ensure_geom(line)
    following_point = get_following_point(line, point, forward_perc=0.1)
    bearing_angle = spherical_heading(point, following_point)
    return bearing_angle


def get_point_bearing_or_heading(
    point: typing.Union[geometry.Point, str],
    line: typing.Union[geometry.LineString, str],
    original_heading: float,
) -> float:
    """Get the correct bearing given the point and the line that contains this point.
    If the line is a point, the original heading is returned

    :param point: Point to calculate the bearing from
    :type point: geometry.Point
    :param line: Linestring along the way to get the heading in the correct direction
    :type line: geometry.LineString
    :param original_heading: Original heading from L@S team
    :type original_heading: float
    :return: The bearing angle (clockwise from north)
    :rtype: float
    """
    if isinstance(point, str):
        point = ensure_geom(point)
    if isinstance(line, str):
        line = ensure_geom(line)
    if isinstance(line, geometry.Point):
        return original_heading

    return get_point_bearing(point, line)


def get_line_direction_angle(
    point: shapely.geometry.Point,
    line,
    forward_abs_list = [0, 0.00001]
) -> shapely.geometry.LineString:
    # Angle is the bearing now
    if point and line:
        first_p = get_following_point(line, point, forward_abs=forward_abs_list[0])
        next_p = get_following_point(line, point, forward_abs=forward_abs_list[1])
        line = shapely.geometry.LineString([first_p, next_p])
        return line

def geom_concater(geoms):
    """Join multiple geometries into 1.
    pd.groupby().agg(geom_concater)


    :param geoms: _description_
    :type geoms: _type_
    :return: _description_
    :rtype: _type_
    """
    geoms = [wkt.loads(geom) for geom in geoms]
    if len(geoms) > 1:
        return shapely.ops.linemerge(geoms)
    else:
        return geoms[0]


parse_binary_geom_udf = udf(ensure_geom, StringType())
ensure_geom_udf = udf(ensure_geom, StringType())


def remove_fully_contained_geometries(gdf:gpd.GeoDataFrame, geom_column:str, id_columns:typing.List[str], length_col:str, buffer_size:float=0.00001)->gpd.GeoDataFrame:
    """Filter the rows in gdf where there is other geometry repeated in the dataframe that contains the geometry in geom_column.
    This function remove the contained element and keep the contaner element.
    It uses the id_columns to remove duplicated based on this columns.

    :param gdf: Original input geodataframe with potential duplicates
    :type gdf: gpd.GeoDataFrame
    :param geom_column: Column to look for the duplications
    :type geom_column: str
    :param id_columns: Columns used to deduplicate the geometries
    :type id_columns: typing.List[str]
    :param length_col: Column used to sort the geometries and keep the largest one
    :type length_col: str
    :param buffer_size: buffer size to use in the intersection, defaults to 0.00001
    :type buffer_size: float, optional
    :return: Filtered duplicates geodataframe
    :rtype: gpd.GeoDataFrame
    """
    # Make two copies of the input GeoDataFrame
    df1 = gdf[[geom_column,*id_columns,length_col]].copy()
    df2 = gdf[[geom_column]].copy()

    # Apply a small buffer to one of the copies
    df2[geom_column] = df2[geom_column].buffer(buffer_size)

    # Convert both copies to GeoDataFrames
    gdf1 = gpd.GeoDataFrame(df1, geometry=geom_column)
    gdf2 = gpd.GeoDataFrame(df2, geometry=geom_column)

    # Perform a spatial join using 'contains' operation
    spatial_join = gpd.sjoin(gdf2, gdf1, how='inner', predicate='contains')
    if len(spatial_join) == 0:
        return gdf
    duplicated_geometries = spatial_join[spatial_join.duplicated(subset=id_columns, keep=False)]
    duplicated_geometries = duplicated_geometries.sort_values(by=length_col, ascending=False)
    # Now from the duplicates drop the largest one
    duplicated_geometries = duplicated_geometries.drop_duplicates(subset=id_columns, keep='last')

    # Filter the resulting DataFrame for fully contained geometries drop all rows that are in spatial_join using id_columns
    merged = pd.merge(gdf, duplicated_geometries[id_columns], on=id_columns, how='left', indicator=True)
    # Filter rows present in gdf but not in duplicated_geometries
    filtered_gdf = merged.loc[merged['_merge'] == 'left_only'].drop(columns='_merge').copy()

    return filtered_gdf



def remove_different_angle_geometries(gdf:gpd.GeoDataFrame, intersection_col:str, competitor_col:str, angle_limit:int=45)->gpd.GeoDataFrame:
    """Function to remove the geometries that are not in the same direction from the provider_col and competitor_col based on the angle

    :param gdf: Dataframe from a intersection that contains the intersection_geom, provider_geom, competitor_geom
    :type gdf: gpd.GeoDataFrame
    :param intersection_col: Name of the column that contains the intersection geometry
    :type intersection_col: str
    :param competitor_col: Name of the column that contains the competitor geometry
    :type competitor_col: str
    :param angle_limit: Maximum angle in degrees that is allowed between provider and competitor geoms for the intersected geometry, defaults to 45:int
    :type angle_limit: _type_, optional
    :return: The intersections dataframe filtered with the geometries that are in the same direction based on the angle
    :rtype: gpd.GeoDataFrame
    """

    # Steps
    # Project the first point of the intersection_geom onto the segment of the competitor_geom.
    # Calculate the next point at ~ 10% of trace for the first point of the intersection and the projected point.
    # Calculate the angle formed by the lines of the first point and the 10% line.
    # If the angle is less than angle_limit I accept the trace, otherwise I discard it.

    gdf['intersection_point_1'] = gdf[intersection_col].map(lambda x: x.coords[0])
    gdf['intersection_projected_on_competitor_1'] = gdf.apply(lambda x: x[competitor_col].interpolate(x[competitor_col].project(Point(x['intersection_point_1']))), axis=1)
    gdf['intersection_point_2'] = gdf.apply(lambda x: get_following_point(x[intersection_col], Point(x['intersection_point_1']), forward_perc=0.1), axis=1)
    gdf['intersection_projected_on_competitor_2'] =  gdf.apply(lambda x: get_following_point(x[competitor_col], Point(x['intersection_projected_on_competitor_1']), forward_perc=0.1), axis=1)
    gdf['intersection_angle'] = gdf.apply(lambda x: get_delta_angle(get_bearing(LineString([x['intersection_point_1'],x['intersection_point_2']])),
                                                                    get_bearing(LineString([x['intersection_projected_on_competitor_1'],x['intersection_projected_on_competitor_2']]))), axis=1)
    filtered_gdf = gdf[gdf['intersection_angle'].abs() < angle_limit]
    # drop generated columns but keep the angle for debugging
    filtered_gdf.drop(columns=['intersection_point_1','intersection_projected_on_competitor_1',
                               'intersection_point_2','intersection_projected_on_competitor_2'
                               ], inplace=True)

    return filtered_gdf

def intersects_gdf(
    df_a, df_b, geom_a="geom", geom_b="geom",
    buffer_distance=0.00003,
    trace_id_col='trace_id',
    min_intersection_length_m=1,
    feature_col='feature_id_uuid',
    remove_fully_contained=True,
    remove_different_angle=True,
    **kwargs
):
    gdf_a = gpd.GeoDataFrame(df_a, geometry=geom_a)
    gdf_b = gpd.GeoDataFrame(df_b, geometry=geom_b)
    gdf_b["buffered_geom"] = gdf_b.geometry.buffer(buffer_distance, cap_style=2)
    gdf_b["buffered_geom_copy"] = gdf_b["buffered_geom"].copy()

    # Set the buffered geometry column as the default geometry column
    gdf_b = gdf_b.set_geometry("buffered_geom")
    joined_df = gpd.sjoin(gdf_a, gdf_b, how="left", predicate="intersects", **kwargs)
    # If there is no join reuturn empty
    if len(joined_df) == 0:
        return joined_df
    # Extract intersecting geometries
    lsuffix = kwargs.get("lsuffix", "")
    if lsuffix != "":
        lsuffix = f"_{lsuffix}"
    if lsuffix == "" and geom_a==geom_b:
        lsuffix = "_left"
    rsuffix = kwargs.get("rsuffix", "")
    if rsuffix != "":
        rsuffix = f"_{rsuffix}"
    if rsuffix == "" and geom_a==geom_b:
        rsuffix = "_left"
    intersecting_geoms = joined_df.dropna(subset=[f"{geom_a}{lsuffix}"])

    # Flatten geometries on the provider side if they are MultiLineStrings
    intersecting_geoms[f"{geom_a}{lsuffix}"] = intersecting_geoms.apply(
        lambda x: x[f"{geom_a}{lsuffix}"] if isinstance(x[f"{geom_a}{lsuffix}"], LineString) else flatten_geometries(geom=x[f"{geom_a}{lsuffix}"]),
        axis=1
    )
    # Flatten geometries on the competitor side if they are MultiLineStrings
    intersecting_geoms[f"{geom_b}{rsuffix}"] = intersecting_geoms.apply(
        lambda x: x[f"{geom_b}{rsuffix}"] if isinstance(x[f"{geom_b}{rsuffix}"], LineString) else flatten_geometries(geom=x[f"{geom_b}{rsuffix}"]),
        axis=1
    )

    # Drop multilinestrings and points
    intersecting_geoms = intersecting_geoms[
        intersecting_geoms[f"{geom_a}{lsuffix}"].map(lambda x: isinstance(x, LineString))
    ]
    intersecting_geoms = intersecting_geoms[
        intersecting_geoms[f"{geom_b}{rsuffix}"].map(lambda x: isinstance(x, LineString))
    ]

    # If no join return empty
    if len(intersecting_geoms) == 0:
        return intersecting_geoms

    # Filter intersections where geom_a and geom_b are same directed
    intersecting_geoms['same_direction'] = intersecting_geoms.apply(lambda row: is_same_directed(row[f"{geom_a}{lsuffix}"], row[f"{geom_b}{rsuffix}"]), axis=1)
    intersecting_geoms = intersecting_geoms[intersecting_geoms['same_direction']==True]
      # If there is no join reuturn empty
    if len(intersecting_geoms) == 0:
        return intersecting_geoms

    # Fill rigth as join is left
    intersecting_geoms["buffered_geom_copy"] = intersecting_geoms[
        "buffered_geom_copy"
    ].fillna(geometry.Polygon())
    # Extract intersected geometries
    intersecting_geoms["intersected_geom"] = intersecting_geoms.apply(
        lambda row: row[f"{geom_a}{lsuffix}"].intersection(row["buffered_geom_copy"]),
        axis=1,
    )
    intersecting_geoms = intersecting_geoms.explode(column="intersected_geom")
    intersecting_geoms = intersecting_geoms.set_geometry("intersected_geom")

    intersecting_geoms["intersection_length_m"] = intersecting_geoms[
        "intersected_geom"
    ].map(get_length)

    # Drop geometries smaller than a min length
    intersecting_geoms = intersecting_geoms[intersecting_geoms["intersection_length_m"] > min_intersection_length_m]

    intersecting_geoms = intersecting_geoms.reset_index()
    # Get the biggest value for this id
    unique_intersecting = intersecting_geoms.loc[
        intersecting_geoms.groupby([f'{feature_col}_provider', f'{feature_col}_competitor'])["intersection_length_m"].idxmax()
    ]
    # Delete from unique intersecting all that is not a linestring
    unique_intersecting = unique_intersecting[unique_intersecting['intersected_geom'].map(lambda x: isinstance(x, LineString) and not x.is_empty)]

    # If there is no join reuturn empty
    if len(unique_intersecting) == 0:
        return unique_intersecting

    # Delete from unique intersecting all that is fully contained in the other
    if remove_fully_contained:
        unique_intersecting = remove_fully_contained_geometries(unique_intersecting, 'intersected_geom',
                                                                [f'{feature_col}_provider', f'{feature_col}_competitor'],
                                                                'intersection_length_m')
        # If there is no join return empty
        if len(unique_intersecting) == 0:
            return unique_intersecting  # Empty geodataframe

    # Delete from unique intersecting all that is not in the same direction
    if remove_different_angle:
        unique_intersecting = remove_different_angle_geometries(unique_intersecting, 'intersected_geom', f"{geom_b}{rsuffix}", angle_limit=45)
        # If there is no join reuturn empty
        if len(unique_intersecting) == 0:
            return unique_intersecting  # Empty geodataframe

    # Check if we lost more than 20% of the trace
    intersecting_len = unique_intersecting["intersected_geom"].map(get_length).sum()
    original_len = gdf_a[geom_a].map(get_length).sum()
    if abs(original_len - intersecting_len) > original_len * 0.2:
        logging.warning(
            f"Lost more than 20% of trace meters in trace_id:{df_a[trace_id_col].values[0]}, original len {original_len}, intersecting len {intersecting_len}"
        )

    if intersecting_len > original_len+original_len*0.005: # 0.5% of the original length extra
        logging.warning(
            f"Weird case, larger intersection than original in trace_id:{df_a[trace_id_col].values[0]}"
        )

    unique_intersecting.drop(columns="index", inplace=True)

    return unique_intersecting

def is_continuous_segments(predecessor:str, parent:str)->bool:
    """Returns true if the linestring "predecessor" is continuous with "parent" linestring.
    They are continuous if the angle between the two segments is lower than 5 degrees
    :param predecessor: Linestring with the predecesor geometry
    :type predecessor: str
    :param parent: Linestring with the parent geometry
    :type parent: str
    :return: True if the segments are continuous in angle or False if not
    :rtype: bool
    """
    # angle_limit = 5
    # predecessor = wkt.loads(predecessor)
    # parent = wkt.loads(parent)
    # predecessor_angle = get_angle(
    #     predecessor.coords[-2], predecessor.coords[-1]
    # )  # Last coordinates
    # parent_angle = get_angle(parent.coords[0], parent.coords[1])  # First coordinates
    # delta_angle = get_delta_angle(predecessor_angle, parent_angle)
    # if abs(delta_angle) < angle_limit:
    #     return True
    # else:
    #     return False
    angle_limit = 5
    predecessor = ensure_geom(predecessor)
    parent = ensure_geom(parent)
    predecessor_angle = get_angle(LineString(predecessor.coords[-2:]))  # Last coordinates
    parent_angle = get_angle(LineString(parent.coords[:2]))  # First coordinates
    delta_angle = get_delta_angle(predecessor_angle, parent_angle)
    if abs(delta_angle) < angle_limit:
        return True
    else:
        return False

def is_same_directed(line_a: Union[str, LineString], line_b: Union[str, LineString]) -> bool:
    """Returns true if the segment linestring "line_a" is in the same direction as the linestring "line_b"
    :param line_a: Linestring for the first segment
    :type line_a: str
    :param line_b: Linestring for the second segment
    :type line_b: str
    :return: True if both linestrings are in the same direction, false if not
    :rtype: bool
    """
    line_a_geom = ensure_geom(line_a)
    line_b_geom = ensure_geom(line_b)

    if line_a_geom is None or line_b_geom is None:
        return False

    origin = geometry.Point(line_a_geom.coords[0])
    destination = geometry.Point(line_a_geom.coords[-1])

    # Compare direction of both line segments
    if line_b_geom.project(origin) == line_b_geom.project(
        destination
    ):  # Contiguous segments
        return is_continuous_segments(line_a, line_b) or is_continuous_segments(line_b, line_a)
    else:  # Non-contiguous segments
        return line_b_geom.project(origin) < line_b_geom.project(destination)



def create_combined_geom(from_relation_geom_str, to_relation_geom_str):
    # Parse the LineString representations into Shapely LineString objects
    from_relation_geom = ensure_geom(from_relation_geom_str)
    to_relation_geom = ensure_geom(to_relation_geom_str)

    # Get the first and last points of 'from' geometry
    from_A, from_B = Point(from_relation_geom.coords[0]), Point(from_relation_geom.coords[-1])

    # Get the first and last points of 'to' geometry
    to_X, to_Y = Point(to_relation_geom.coords[0]), Point(to_relation_geom.coords[-1])

    # Calculate distances from 'from' A and B to 'to' X and Y
    distances = {
        'AX': from_A.distance(to_X),
        'AY': from_A.distance(to_Y),
        'BX': from_B.distance(to_X),
        'BY': from_B.distance(to_Y),
    }

    # Determine the 'from_middle' and 'to_middle' points
    min_distance_key = min(distances, key=distances.get)
    from_middle, to_middle = {
        'AX': (from_A, to_X),
        'AY': (from_A, to_Y),
        'BX': (from_B, to_X),
        'BY': (from_B, to_Y),
    }[min_distance_key]

    # Reorder the coordinates accordingly
    from_ordered = LineString(reversed(from_relation_geom.coords)) if from_middle == from_A else from_relation_geom
    to_ordered = LineString(reversed(to_relation_geom.coords)) if to_middle == to_Y else to_relation_geom

    # Concatenate geometries
    combined_relation_geometry = LineString(list(from_ordered.coords) + list(to_ordered.coords)).wkt

    return combined_relation_geometry


def multigeom_to_singlegeom(
    multi_geom: Union[shapely.MultiLineString, shapely.MultiPoint]
) -> Union[LineString, Point]:
    """Convert a single-element MultiLineString or MultiPoint to a LineString or Point by extracting the element

    :param multi_geom: MultiLineString or MultiPoint to be flattened
    :type multi_geom: Union[MultiLineString, MultiPoint]
    :return: LineString or Point with the coordinates of the flattened geometries
    :rtype: Union[LineString, Point]
    """
    # Return geom as is if it's already a LineString or Point
    if isinstance(multi_geom, shapely.LineString) or isinstance(multi_geom, shapely.Point):
        return multi_geom

    if isinstance(multi_geom, shapely.MultiLineString):
        if multi_geom.is_empty:
            return LineString()
        merge_multi_geom = ops.linemerge(multi_geom.geoms)
        try:
            single_geom = LineString([point for line in multi_geom.geoms for point in line.coords[:]])
        except:
            single_geom = LineString(merge_multi_geom.coords[:])
        if not isinstance(single_geom, LineString):
            raise ValueError(f"Could not convert MultiLineString to LineString: {multi_geom}")
        return single_geom
    elif isinstance(multi_geom, shapely.MultiPoint):
        if multi_geom.is_empty:
            return Point()
        single_geom = Point([(p.x, p.y) for p in multi_geom.geoms])
        if not isinstance(single_geom, Point):
            raise ValueError(f"Could not convert MultiPoint to Point: {multi_geom}")
        return single_geom

def flatten_geometries(geom: Union[shapely.MultiLineString, shapely.MultiPoint]):
    """Flatten a MultiLineString or MultiPoint geometry into a LineString

    Args:
        geom (multi_geom): MultiLineString or MultiPoint to be flattened

    Returns:
        multi_geom: Flattened geometry as a LineString
    """
    if geom is None or geom.is_empty or geom == "" or geom == 'nan':
        return LineString()
    if isinstance(geom, MultiLineString) or isinstance(geom, MultiPoint):
        coords = multigeom_to_singlegeom(geom)
    else:
        coords = geom.coords[:]

    return LineString(coords)


def compute_curvature_radius(geom: LineString) -> float:
    """Compute curvature radius of a LineString geometry

    :param geom: LineString geometry
    :type geom: LineString
    :return: Radius of curvature
    :rtype: float
    """
    # If geom is a string, try to convert it to a LineString
    if isinstance(geom, str):
        try:
            geom = wkt.loads(geom)
        except:
            return 0.0

    # If geom is a MultiPoint, Point or empty, return 0.0
    if isinstance(geom, (MultiPoint, Point)) or geom.is_empty:
        return 0.0

    # Flatten multigeom if necessary
    if isinstance(geom, MultiLineString):
        geom = multigeom_to_singlegeom(geom)

    # Convert list of tuples to list of lists
    geom_list = [list(elem) for elem in geom.coords]

    # Convert to dataframe
    df_geom = pd.DataFrame(geom_list, columns=['x', 'y'])

    # Convert to numpy array
    a = df_geom.to_numpy()

    # Get the middle point of the curve
    middle_point_index = round(len(df_geom)/2)
    x_middle = (a[middle_point_index,0])
    y_middle = (a[middle_point_index,1])

    # Compute the derivatives of the curve
    dx = a[-1, 0] - a[0, 0]
    dy = a[0, 1] - a[-1, 1]
    dl = np.sqrt(dx ** 2 + dy ** 2)
    cos = dx / dl
    sin = dy / dl
    dh = cos * (np.interp(x_middle, a[:, 0], a[:, 1]) - y_middle)

    # Compute the radius of curvature
    radius = abs(dl ** 2 / 8 / dh + dh / 2)  # Convert to absolute value to avoid negative radius

    # Convert curvature to meters
    radius_meters = radius * 1e6

    # # Compute the center of curvature
    # x0 = x_middle - (radius - dh) * sin
    # y0 = y_middle - (radius - dh) * cos

    if radius_meters == float("inf"):
        print("infinite radius")

    return radius_meters

def combine_geometries(geom_a, geom_b) -> LineString:
    # Flatten coordinates of geom_a
    if isinstance(geom_a, MultiLineString) or isinstance(geom_a, MultiPoint):
        coords_geom_a = multigeom_to_singlegeom(geom_a)
    else:
        coords_geom_a = geom_a.coords[:]
    # Flatten coordinates of geom_b
    if isinstance(geom_b, MultiLineString) or isinstance(geom_b, MultiPoint):
        coords_geom_b = multigeom_to_singlegeom(geom_b)
    else:
        coords_geom_b = geom_b.coords[:]

    # Check for connectivity
    if coords_geom_a[-1] == coords_geom_b[0]:  # Last point of geom_a equals first point of geom_b
        combined_coords = coords_geom_a + coords_geom_b[1:]  # Skip the first point of geom_b
    elif coords_geom_b[-1] == coords_geom_a[0]:  # Last point of geom_b equals first point of geom_a
        combined_coords = coords_geom_b + coords_geom_a[1:]  # Skip the first point of geom_a
    else:
        combined_coords = coords_geom_a + coords_geom_b  # No connection, just concatenate

    return LineString(combined_coords)

def combine_geometries_wrap(group):
    # If there's only one geometry, return it directly
    if len(group) < 2:
        return group['intersected_geom'].iloc[0]

    # Combine the geometries in the group
    geom_a = group['intersected_geom'].iloc[0]
    geom_b = group['intersected_geom'].iloc[1]
    combined_geom = combine_geometries(geom_a, geom_b)

    return combined_geom
