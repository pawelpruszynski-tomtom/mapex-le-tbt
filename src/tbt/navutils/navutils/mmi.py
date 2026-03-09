import json
import logging
import typing
import warnings

import requests

from .base_provider import BaseProvider, RouteOptions, _is_flat_list
from .enum_types import AvoidOptions, RouteType, TravelMode
from .route import Route
from tbt.utils.console_print import conditional_print

log = logging.getLogger(__name__)


class MMI(BaseProvider):
    def __parse_route_options(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options: RouteOptions,
    ) -> str:
        """
        Parse the route options and return the URL to request the route
        """
        # To avoid considering traffic conditions
        url = "route_adv/"

        if route_options.travel_mode == TravelMode.CAR:
            url += "driving/"
        # For future implementations
        # if route_options.travel_mode == TravelMode.TRUCK:
        #     url += "trucking/"
        else:
            warnings.warn("Requested route travel_mode not supported")
            url += "driving/"

        # Adding origin and destination
        url += f"{origin[0]},{origin[1]};{destination[0]},{destination[1]}?geometries=geojson"

        if route_options.route_type == RouteType.FASTEST:
            url += "&rtype=0"
        if route_options.route_type == RouteType.SHORTEST:
            url += "&rtype=1"

        # To skip the steps (sections), as they don't provide info about the section type
        # at least in the trial version
        url += "&steps=false"

        avoid_features = []
        if AvoidOptions.FERRY in route_options.avoid:
            avoid_features.append("ferry")
        if AvoidOptions.TOLL in route_options.avoid:
            avoid_features.append("toll")
        if AvoidOptions.MOTORWAY in route_options.avoid:
            avoid_features.append("motorway")

        if len(avoid_features) > 0:
            url += "&" + ",".join(avoid_features)

        # you have to specift the region using iso-code 2 or 3 that we will set for IND at the moment
        url += "&region=IND"

        # How many routes we want to get
        if route_options.alternative_routes > 0:
            total_routes = 1 + route_options.alternative_routes
            url += f"&alternatives={total_routes}"

        # Flexibility for start and origin in a radius of X meters
        url += "&radiuses=100;100"

        # geometry overviewed - simplified/false/full
        url += "&overview=full"

        return url

    def __parse_response(self, response):
        content = json.loads(response.content)

        routes_info = []
        for route in content["routes"]:
            routes_info.append(
                [
                    route["geometry"]["coordinates"],
                    route["distance"],
                    route["duration"],
                ]
            )

        return routes_info[0] if len(routes_info) == 1 else routes_info

    def get_route_with_supporting_points(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        supporting_route=None,
        route_options=RouteOptions(),
    ) -> Route:
        return NotImplementedError

    def get_route(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options=RouteOptions(),
    ) -> Route:
        # Building the URL
        url = self.endpoint + self.api_key + "/"
        url += self.__parse_route_options(origin, destination, route_options)

        try:
            response = requests.get(url, timeout=600)
            # If Error response
            if response.status_code != 200:
                return Route([], self.name, 0)

            log.info(f"URL: {url}")
            conditional_print(f"URL: {url}")
            log.info(f"Response from MMI: {response.content}")
            conditional_print(f"Response from MMI: {response.content}")

            routes_info = self.__parse_response(response)
            if _is_flat_list(routes_info):
                route, distance, time = routes_info
                return Route(
                    geometry=route,
                    provider=self.name,
                    length=distance,
                    time=time,
                )
            return [
                Route(
                    geometry=route,
                    provider=self.name,
                    length=distance,
                    time=time,
                )
                for route, distance, time in routes_info
            ]
        except Exception:
            return Route(
                geometry=[],
                provider=self.name,
            )
