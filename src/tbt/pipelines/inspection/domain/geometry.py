"""Pure geometry helpers — no Spark or Kedro dependency."""

import numpy as np
import pyproj
import shapely.geometry
import shapely.wkt


def convert_to_linestring(wkt_str: str) -> shapely.geometry.LineString:
    """Parse a WKT string into a Shapely LineString."""
    return shapely.wkt.loads(wkt_str)


def distance(start: np.ndarray, end: np.ndarray) -> float:
    """Geodesic distance (metres) between two lon/lat points."""
    geodesic = pyproj.Geod(ellps="WGS84")
    s_lon, s_lat = start
    e_lon, e_lat = end
    return geodesic.inv(s_lon, s_lat, e_lon, e_lat)[2]


def get_length(geom: shapely.geometry.LineString) -> float:
    """Total geodesic length of a LineString in metres."""
    coords = geom.coords[:]
    return sum(distance(start, end) for start, end in zip(coords[:-1], coords[1:]))

