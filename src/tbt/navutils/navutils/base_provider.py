"""Base provider module

This module includes base components to handle routing apis

Examples::

    >>> origin = [-3.854731428516601, 40.30458530109934]
    >>> destination = [-3.8428700546717494, 40.29404064165999]
    >>> prov = Provider.from_properties(name='Genesis')
    >>> route = prov.getRoute(origin, destination)
    >>> route.geometry.wkt
"""

import random
import time
import typing
from datetime import datetime, timedelta

from .enum_types import AvoidOptions, RouteType, TravelMode
from .route import Route

# BEGIN: Update from 2023--09-19: PROME-2050


# Fix the date at insection to next monday also change on depart_at at __init__ on RouteOptions
def _next_monday(date):
    """Returns the next Monday from the given date, or the same date if it's already a Monday"""
    return date + timedelta(days=(7 - date.weekday()) % 7)


# END: Update from 2023-09-19: PROME-2050


def _is_flat_list(input_list):
    """Check if the input list is a flat list (i.e. a list of non-list items)"""
    return (isinstance(input_list, list) or isinstance(input_list, tuple)) and not all(
        isinstance(item, (list, tuple)) for item in input_list
    )


class RouteOptions:
    """Route options passed to BaseProvider.getRoute

    Encapsulates the various route options under the same interface, regardless of the routing API.

    Examples::

        >>> prov = Provider.from_properties(name='Genesis')
        >>> options = RouteOptions(route_type=RouteType.FASTEST, avoid=[AvoidOptions.UNPAVED], sections=[SectionType.MOTORWAY, SectionType.UNPAVED])
        >>> route = prov.getRoute(origin = [-3.854, 40.304], destination = [-3.842, 40.294], options = options)
        >>> route.geometry.wkt
    """

    def __init__(
        self,
        route_type=RouteType.FASTEST,
        travel_mode=TravelMode.CAR,
        avoid=None,
        traffic=False,
        depart_at=f"{_next_monday(datetime.now()).strftime('%Y-%m-%d')}T23:00:00",
        arrive_at=None,
        strict=False,
        sections=None,
        vehicle_weight: typing.Optional[float] = None,
        vehicle_height: typing.Optional[float] = None,
        vehicle_width: typing.Optional[float] = None,
        vehicle_length: typing.Optional[float] = None,
        # -1 Is the magic number to indicate no alternatives must return
        # (each provider checks if this param is valid).
        alternative_routes: int = -1,
    ):
        if avoid is None:
            avoid = [AvoidOptions.CARPOOL, AvoidOptions.FERRY, AvoidOptions.UNPAVED]

        if sections is None:
            sections = []

        if route_type not in RouteType:
            raise ValueError(f"{route_type} is not a valid option for RouteType")
        self.route_type = route_type

        if travel_mode not in TravelMode:
            raise ValueError(f"{travel_mode} is not a valid option for TravelMode")
        self.travel_mode = travel_mode

        for avoid_option in avoid:
            if avoid_option not in AvoidOptions:
                raise ValueError(
                    f"{avoid_option} is not a valid option for AvoidOptions"
                )
        self.avoid = avoid

        if not isinstance(traffic, bool):
            raise ValueError(f"traffic must be a boolean flag (traffic = {traffic})")
        self.traffic = traffic

        if depart_at is not None:
            # Just checking date format
            datetime.strptime(depart_at, "%Y-%m-%dT%H:%M:%S")
        self.depart_at = depart_at

        if arrive_at is not None:
            # Just checking date format
            datetime.strptime(arrive_at, "%Y-%m-%dT%H:%M:%S")
        self.arrive_at = arrive_at

        if (depart_at is not None) and (arrive_at is not None):
            raise ValueError("Arrival time is not supported when Departure time is set")

        if not isinstance(strict, bool):
            raise ValueError(f"strict must be a boolean flag (strict = {strict})")
        self.strict = strict

        # TODO: check type
        self.sections = sections

        # Truck restrictions
        if vehicle_weight is not None and not isinstance(vehicle_weight, int):
            raise ValueError(
                f"vehicle_weight must be a int (type(vehicle_weight) = {type(vehicle_weight)})"
            )
        self.vehicle_weight = vehicle_weight

        if vehicle_height is not None and not isinstance(vehicle_height, float):
            raise ValueError(
                f"vehicle_height must be a float (type(vehicle_height) = {type(vehicle_height)})"
            )
        self.vehicle_height = vehicle_height

        if vehicle_width is not None and not isinstance(vehicle_width, float):
            raise ValueError(
                f"vehicle_width must be a float (type(vehicle_width) = {type(vehicle_width)})"
            )
        self.vehicle_width = vehicle_width

        if vehicle_length is not None and not isinstance(vehicle_length, float):
            raise ValueError(
                f"vehicle_length must be a float (type(vehicle_length) = {type(vehicle_length)}))"
            )
        self.vehicle_length = vehicle_length

        if not isinstance(alternative_routes, int):
            raise ValueError(
                f"alternative_routes must be a int (type(alternative_routes) = {type(alternative_routes)})"
            )
        self.alternative_routes = alternative_routes


class BaseProvider:
    """Standard Base provider class"""

    __gg_api_calls = 0
    __tt_api_calls = 0
    __osm_api_calls = 0
    __here_api_calls = 0
    __bing_api_calls = 0
    __kakao_api_calls = 0
    __gt_api_calls = 0
    __mapbox_api_calls = 0
    __mmi_api_calls = 0
    __zenrin_api_calls = 0
    __genesysmap_api_calls = 0
    __orbis_api_calls = 0

    def __init__(
        self,
        name,
        api=None,
        endpoint="",
        api_key="",
        qps_limit=3,
        product="",
        gg_api_count=0,
        directions_api_count=0,
        valhalla_api_count=0,
        here_api_count=0,
        bing_api_count=0,
        kakao_api_count=0,
        gt_api_count=0,
        mapbox_api_count=0,
        mmi_api_count=0,
        zenrin_api_count=0,
        genesysmap_api_count=0,
        orbis_api_count=0,
    ):
        self.name = name
        self.api = api
        self.endpoint = endpoint
        self.api_key = api_key
        self.qps_limit = qps_limit
        self.product = product
        self.__tt_api_calls = directions_api_count
        self.__gg_api_calls = gg_api_count
        self.__osm_api_calls = valhalla_api_count
        self.__here_api_calls = here_api_count
        self.__bing_api_calls = bing_api_count
        self.__kakao_api_calls = kakao_api_count
        self.__gt_api_calls = gt_api_count
        self.__mapbox_api_calls = mapbox_api_count
        self.__mmi_api_calls = mmi_api_count
        self.__zenrin_api_calls = zenrin_api_count
        self.__genesysmap_api_calls = genesysmap_api_count
        self.__orbis_api_calls = orbis_api_count

    def get_route(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options=RouteOptions(),
    ) -> Route:
        return Route([], self.name, 0.0, 0.0)

    def get_route_with_supporting_points(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        supporting_route=None,
        route_options=RouteOptions(),
    ) -> Route:
        return Route([], self.name, 0.0, 0.0)

    def getRoute(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        heading_origin: typing.Optional[int] = None,
        heading_destination: typing.Optional[int] = None,
        supporting_route: typing.Optional[typing.List[float]] = None,
        route_options=RouteOptions(),
    ) -> Route:
        """Compute a route from origin to destination for this provider

        :param origin: Coordinates in EPSG:4326 (lon, lat) format
        :type origin: typing.List[float]
        :param destination: Coordinates in EPSG:4326 (lon, lat) format
        :type destination: typing.List[float]
        :param heading_origin: Heading at origin in degrees, defaults to None
        :type heading_origin: typing.Optional[int], optional
        :param heading_destination: Heading at destination in degrees, defaults to None
        :type heading_destination: typing.Optional[int], optional
        :return: Object with route properties
        :rtype: Route
        """
        self.__increase_api_calls()
        # Sleep randomly to match qps
        time.sleep(random.random() / self.qps_limit)

        # Needed parameters
        route_args = {
            "origin": origin,
            "destination": destination,
            "route_options": route_options,
        }

        # Check if heading is provided
        if heading_origin is not None:
            route_args["heading_origin"] = heading_origin
        if heading_destination is not None:
            route_args["heading_destination"] = heading_destination

        #  Check if supporting route is provided
        if supporting_route is not None:
            route_args["supporting_route"] = supporting_route
            return self.get_route_with_supporting_points(**route_args)

        return self.get_route(**route_args)

    def __increase_api_calls(self):
        if self.api in ["Directions", "Amigo-Alpha"]:
            self.__tt_api_calls += 1
        elif self.api == "Google":
            self.__gg_api_calls += 1
        elif self.api in ["Valhalla", "Valhalla-Strict"]:
            self.__osm_api_calls += 1
        elif self.api == "Here":
            self.__here_api_calls += 1
        elif self.api == "Bing":
            self.__bing_api_calls += 1
        elif self.api == "Kakao":
            self.__kakao_api_calls += 1
        elif self.api == "GT":
            self.__gt_api_calls += 1
        elif self.api == "MapBox":
            self.__mapbox_api_calls += 1
        elif self.api == "MMI":
            self.__mmi_api_calls += 1
        elif self.api == "Zenrin":
            self.__zenrin_api_calls += 1
        elif self.api == "Genesysmap":
            self.__genesysmap_api_calls += 1
        elif self.api == "Orbis":
            self.__orbis_api_calls += 1

    @property
    def api_calls(self):
        """Total calls to the different APIs during the current spark session"""
        return {
            "Directions": self.__tt_api_calls
            if isinstance(self.__tt_api_calls, int)
            else self.__tt_api_calls.value,
            "Google": self.__gg_api_calls
            if isinstance(self.__gg_api_calls, int)
            else self.__gg_api_calls.value,
            "Valhalla": self.__osm_api_calls
            if isinstance(self.__osm_api_calls, int)
            else self.__osm_api_calls.value,
            "Here": self.__here_api_calls
            if isinstance(self.__here_api_calls, int)
            else self.__here_api_calls.value,
            "Bing": self.__bing_api_calls
            if isinstance(self.__bing_api_calls, int)
            else self.__bing_api_calls.value,
            "Kakao": self.__kakao_api_calls
            if isinstance(self.__kakao_api_calls, int)
            else self.__kakao_api_calls.value,
            "GT": self.__gt_api_calls
            if isinstance(self.__gt_api_calls, int)
            else self.__gt_api_calls.value,
            "MapBox": self.__mapbox_api_calls
            if isinstance(self.__mapbox_api_calls, int)
            else self.__mapbox_api_calls.value,
            "MMI": self.__mmi_api_calls
            if isinstance(self.__mmi_api_calls, int)
            else self.__mmi_api_calls.value,
            "Zenrin": self.__zenrin_api_calls
            if isinstance(self.__zenrin_api_calls, int)
            else self.__zenrin_api_calls.value,
            "Genesysmap": self.__genesysmap_api_calls
            if isinstance(self.__genesysmap_api_calls, int)
            else self.__genesysmap_api_calls.value,
            "Orbis": self.__orbis_api_calls
            if isinstance(self.__orbis_api_calls, int)
            else self.__orbis_api_calls.value,
        }
