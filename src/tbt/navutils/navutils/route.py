""" Functions to encapsulate Route object """
from dataclasses import dataclass, field

import numpy as np
import shapely.wkt
from shapely.geometry import LineString

from .enum_types import SectionType

BUFFER = 0.0002
EXCESS_LENGTH = 0.2


@dataclass
class RouteSection:
    """Class to encapsulate sections of route"""

    geometry: LineString = field(repr=False)
    section_type: SectionType
    _geometry_str: str = ""

    def __post_init__(self):
        self._geometry_str = (
            f"Origin: {self.geometry.coords[0]}, Destination: {self.geometry.coords[1]}"
        )


class DistanceOnEarth:

    """
    Set of method to calculate the distance between two (LON, LAT) points
    https://www.movable-type.co.uk/scripts/latlong.html
    """

    @staticmethod
    def _deg2rad(deg: float) -> float:
        """
        Convert deg to rad
        :param deg: degrees
        :return: radians
        """

        return deg * (np.pi / 180.0)

    @classmethod
    def get_4326_geometry_length_in_m(cls, geometry) -> float:
        """Linestring length in meters

        :param geometry: linestring in  in 4326 (lon, lat) projection
        :type geometry: shapely.geometry.LineString
        :return: length in meters
        :rtype: float
        """

        return float(
            np.sum(
                [
                    cls.get_distance_from_lat_lon_in_m(
                        lat1=geometry.coords[i - 1][1],
                        lon1=geometry.coords[i - 1][0],
                        lat2=geometry.coords[i][1],
                        lon2=geometry.coords[i][0],
                    )
                    for i in range(1, len(geometry.coords))
                ]
            )
        )

    @classmethod
    def get_distance_from_lat_lon_in_m(
        cls, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> float:
        """
        Find the distance between two points on Earth
        :param lat1: Initial point latitude
        :param lon1: Initial point longitude
        :param lat2: Final point latitude
        :param lon2: Final point longitude
        :return: Distance between two points (in km)
        """

        # Radius of the earth in m
        earth_radius = 6371000

        # Convert LAT and LON difference to radians
        dlat = cls._deg2rad(deg=lat2 - lat1)
        dlon = cls._deg2rad(deg=lon2 - lon1)

        # Haversine formula: https://en.wikipedia.org/wiki/Haversine_formula
        coef_a = np.sin(dlat / 2) * np.sin(dlat / 2) + np.cos(
            cls._deg2rad(deg=lat1)
        ) * np.cos(cls._deg2rad(deg=lat2)) * np.sin(dlon / 2) * np.sin(dlon / 2)

        # arctan2 is the angle in the plane (in radians) between the positive x-axis
        #  and the ray from (0,0) to the point (x,y)
        coef_c = 2 * np.arctan2(np.sqrt(coef_a), np.sqrt(1 - coef_a))

        # Distance in m
        distance = earth_radius * coef_c

        return distance


class Route:
    """Class Route

    Standard route class

    :param geometry: Route's LineString object
    :type geometry: list of lists/tuples
    :param provider: Provider object.
    :type provider: provider
    :param length: Route's travelled distance in meters
    :type length: float

    Examples::

        >>> from mdbf3.route import Route
        >>> route = Route([(51.95432, 4.41344), (51.95496, 4.41504), (51.9551, 4.4155)], 'OSM', 100)
        >>> route.contains(route)
        True

    """

    def __init__(
        self,
        geometry,
        provider,
        length=0.0,
        time=0.0,
        sections=None,
        flag_violation=False,
    ):
        if sections is None:
            sections = []

        self.length = length
        self.provider = provider
        self.time = time
        self.sections = sections

        if len(geometry) > 1:
            self.geometry = LineString(geometry)
            self.length = np.sum(
                [
                    DistanceOnEarth.get_distance_from_lat_lon_in_m(
                        lat1=self.geometry.coords[i - 1][1],
                        lon1=self.geometry.coords[i - 1][0],
                        lat2=self.geometry.coords[i][1],
                        lon2=self.geometry.coords[i][0],
                    )
                    for i in range(1, len(self.geometry.coords))
                ]
            )
        else:
            self.geometry = shapely.wkt.loads("LINESTRING EMPTY")
            self.length = 0.0

        self.flag_violation = flag_violation

    def subroute(self, index_start, index_end):
        """Get a subroute"""
        return Route(self.geometry.coords[index_start : (index_end + 1)], self.provider)

    def contains(self, route_to_contain):
        """Checkes whether the own route contains the given one

        Args:
            route_to_contain (LineString): Route to compare with

        Returns:
            Boolean: True/False
        """
        return self.geometry.buffer(BUFFER).contains(route_to_contain.geometry)

    def intersects(self, route):
        """Checkes whether the own route intersects the given one

        Args:
            route_to_equal (LineString): Route to compare with

        Returns:
            Boolean: True/False
        """
        return self.geometry.buffer(BUFFER).intersects(route.geometry)

    def equals(self, route_to_equal):
        """Checkes whether the own route is equal to the given one

        Args:
            route_to_equal (LineString): Route to compare with

        Returns:
            Boolean: True/False
        """
        return self.contains(route_to_equal) and route_to_equal.contains(self)

    def __str__(self) -> str:
        sections_string = "\n".join([f"{section}" for section in self.sections])
        return_string = (
            f"Length: {self.length}, provider: {self.provider}, time: {self.time}, flag_violation: {self.flag_violation}\n"
            f"Origin: {self.geometry.coords[0]}, Destination: {self.geometry.coords[1]}\n"
            f"Sections: \n{sections_string}"
        )

        return return_string
