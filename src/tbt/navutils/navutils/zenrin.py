import json
import typing
import warnings

import requests

from .base_provider import (
    BaseProvider,
    RouteOptions,
)
from .enum_types import AvoidOptions, RouteType, TravelMode
from .route import Route


def parse_zenrin_response(
    response_content: bytes,
) -> typing.Tuple[typing.List[float], float, float]:
    """Parse the response from Zenrin API and return the route geometry, route distance and route duration"""

    json_content = json.loads(response_content)
    # parse route coords
    route = json_content["result"]["item"][0]["route"]
    links = route["link"]
    # points = [[item['line']['coordinates'][0][0], item['line']['coordinates'][0][1]] for item in links]

    points = []
    for link in links:
        for item in link["line"]["coordinates"][
            :-1
        ]:  # avoid duplicate so ingnore last point for every link
            points.append([item[0], item[1]])
    # add last point for last link
    points.append(
        [
            links[len(links) - 1]["line"]["coordinates"][-1][0],
            links[len(links) - 1]["line"]["coordinates"][-1][1],
        ]
    )

    # parse route time

    distance = route["distance"]
    duration = route["time"] * 60  # convert to seconds

    return points, distance, duration


class Zenrin(BaseProvider):
    """Zenrin provider class"""

    def __parse_route_options(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options: RouteOptions,
    ) -> str:
        """
        Parse the route options and return the URL to request the route
        """
        # long+lat
        _from = f"{origin[0]},{origin[1]}"
        _to = f"{destination[0]},{destination[1]}"

        if route_options.travel_mode != TravelMode.CAR:
            warnings.warn("Requested travel_mode not supported")

        url = f"{self.endpoint}?from={_from}&to={_to}&to_type=2&from_type=2"

        # Route type
        if route_options.route_type == RouteType.FASTEST:
            url += "&search_type=1"
        elif route_options.route_type == RouteType.SHORTEST:
            url += "&search_type=4"
        else:
            url += "&search_type=1"

        # Ferry
        if AvoidOptions.FERRY in route_options.avoid:
            # ferry parameter: 0:Standard, 1:Preferred , 2:Avoided, 3:Prohibited(if no, return error)
            url += "&ferry=false"

        # Travel Mode (Car type)
        if route_options.travel_mode == TravelMode.CAR:
            url += "&toll_type=normal"
        elif route_options.travel_mode == TravelMode.TRUCK:
            url += "&toll_type=large"

        # Not supported
        if AvoidOptions.TOLL in route_options.avoid:
            warnings.warn("Requested avoid TOLL not supported")
        if AvoidOptions.MOTORWAY in route_options.avoid:
            warnings.warn("Requested avoid MOTORWAY not supported")
        if AvoidOptions.UNPAVED in route_options.avoid:
            warnings.warn("Requested avoid unpaved not supported")
        if AvoidOptions.LEZ in route_options.avoid:
            warnings.warn("Requested avoid low emission zone not supported")
        if AvoidOptions.CARPOOL in route_options.avoid:
            warnings.warn("Requested avoid carpool not supported")
        if not route_options.traffic:
            warnings.warn("Requested avoid road event/traffic not supported")
        return url

    def get_route(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options=RouteOptions(),
    ) -> Route:
        api_key = self.api_key
        try:
            url = self.__parse_route_options(
                origin=origin, destination=destination, route_options=route_options
            )
            header = {
                "x-api-key": api_key,
                "Referer": "https://api.tomtom.com",
                "Authorization": "referer",
            }
            request = requests.get(url, timeout=5000, headers=header)
            if request.status_code != 200:
                return Route(geometry=[], provider=self.name, length=0, time=0)

            points, distance, duration = parse_zenrin_response(request.content)
            return Route(
                geometry=points, length=distance, time=duration, provider=self.name
            )
        except:
            pass
        return Route(geometry=[], provider=self.name, length=0, time=0)

    def get_route_with_supporting_points(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        supporting_route=None,
        route_options=RouteOptions(),
    ) -> Route:
        return NotImplementedError
