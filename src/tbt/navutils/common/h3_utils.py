import h3
from shapely.geometry import mapping
from shapely.geometry import LineString
import typing


def get_h3_indexes_along_line(line:LineString, resolution:int, buffer_edge_ratio:int=2, buffer_cap_style:int=1)->typing.List[str]:
    """Get a list containing all the H3 indexes that intersect with the given line adding a buffer.


    :param line: LineString requested to get the H3 indexes
    :type line: LineString
    :param resolution: H3 zoom level resolution
    :type resolution: int
    :param buffer_edge_ratio: Buffer to add to the line to get closest H3 hexagons when the line is not aligned 
                              with the center of the h3 indices. The buffer size will be buffer_edge_ratio multiplied 
                              by the h3 edge length for the specified resulution, defaults to 2
    :type buffer_edge_ratio: int, optional
    :param buffer_cap_style: Buffer end cap style, defaults to 1, optional if needed to be round or flat
    :type buffer_cap_style: int, optional
    :return: List of the H3 indexes that intersect with the line plus the buffer
    :rtype: typing.List[str]
    """
    # Transform lon lat to lat lon for h3
    line_h3 = LineString([(point[1], point[0]) for point in line.coords])
    # Buffer the LineString
    buffered_line = line_h3.buffer(h3.edge_length(resolution, unit='m')*buffer_edge_ratio*1e-5, 
                                   cap_style=buffer_cap_style) # FIXME: update when h3 implements radians
    # Convert the buffered LineString to GeoJSON format
    buffered_geojson = mapping(buffered_line)
    # Get all H3 indexes within the buffered polygon
    h3_indexes = h3.polyfill(buffered_geojson, resolution)

    return h3_indexes


def get_neighbors(h3_indexes:typing.List[str], proximity:int=1)->typing.List[str]:
    """Get the neighbors of the given H3 indexes and update the list.

    :param h3_indexes: List of the H3 indexes to get the neighbors
    :type h3_indexes: typing.List[str]
    :param proximity: kring level for the neighbors, defaults to 1
    :type proximity: int, optional
    :return: H3 indexes that are neighbors of the given H3 indexes
    :rtype: typing.List[str]
    """
    neighbors = set()
    for h3_index in h3_indexes:
        neighbors.update(h3.k_ring(h3_index, proximity))
    return list(neighbors)
