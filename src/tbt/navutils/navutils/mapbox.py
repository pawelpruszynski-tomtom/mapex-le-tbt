import typing
import warnings

import requests

from .base_provider import BaseProvider, RouteOptions
from .enum_types import AvoidOptions, RouteType, TravelMode
from .route import Route


class MapBox(BaseProvider):
    """MapBox provider class"""

    def __parse_route_options(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options: RouteOptions,
    ) -> typing.Tuple[str, typing.Dict[str, str]]:
        # Create the URL
        url = self.endpoint
        params = {
            "geometries": "geojson",
            "overview": "full",
            "access_token": self.api_key,
        }

        # Add the travel mode to the URL and traffic if it's a car route
        if route_options.travel_mode == TravelMode.BICYCLE:
            url += "cycling/"
        elif route_options.travel_mode == TravelMode.PEDESTRIAN:
            url += "walking/"
        elif route_options.travel_mode == TravelMode.CAR:
            url += "driving-traffic/" if route_options.traffic else "driving/"
        else:
            raise NotImplementedError("Travel mode not implemented for MapBox")

        # Add the origin and destination to the URL
        url += f"{origin[0]},{origin[1]};{destination[0]},{destination[1]}"

        # MapBox only supports fastest route
        if route_options.route_type != RouteType.FASTEST:
            raise NotImplementedError("Route type not implemented for MapBox")

        # MapBox do not support a number of alternatives
        if route_options.alternative_routes > 0:
            raise NotImplementedError("Alternatives not implemented for MapBox")

        avoid_features = []
        # Parse the avoid options
        if AvoidOptions.UNPAVED in route_options.avoid:
            avoid_features.append("unpaved")
        if AvoidOptions.TOLL in route_options.avoid:
            avoid_features.append("toll")
        if AvoidOptions.MOTORWAY in route_options.avoid:
            avoid_features.append("motorway")
        if AvoidOptions.CARPOOL in route_options.avoid:
            warnings.warn("Carpool avoidance not supported by MapBox")
        if AvoidOptions.FERRY in route_options.avoid:
            avoid_features.append("ferry")
        if AvoidOptions.LEZ in route_options.avoid:
            warnings.warn("LEZ not supported by MapBox")

        if len(avoid_features) > 0:
            params["exclude"] = ",".join(avoid_features)

        return url, params

    def get_route(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options=RouteOptions(),
    ) -> Route:
        url, params = self.__parse_route_options(
            origin=origin, destination=destination, route_options=route_options
        )

        try:
            response = requests.get(url, params=params, timeout=600)
            geometry, distance, time = parse_mapbox_api_response(response)

            return Route(geometry, self.name, distance, time)

        except Exception as e:
            warnings.warn(f"Exception computing route: {e}")
            return Route([], self.name, 0.0, 0.0)

    def get_route_with_supporting_points(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        supporting_route=None,
        route_options=RouteOptions(),
    ) -> Route:
        return NotImplementedError(
            "Routing with supporting points is not implemented for MapBox"
        )


def parse_mapbox_api_response(
    response: requests.models.Response,
) -> typing.Tuple[typing.List[typing.Tuple[float, float]], float, float]:
    json_content = response.json()

    # Check if the API returned an error
    if json_content.get("code") != "Ok":
        warnings.warn(
            f"MapBox API returned an error: "
            f"code = {json_content.get('code')}, "
            f"message = {json_content.get('message')}"
        )
        return [], 0.0, 0.0

    # Check if the API returned a route
    routes = json_content.get("routes", 0)
    if routes == 0:
        warnings.warn("MapBox API did not return a route")
        return [], 0.0, 0.0

    route_path = routes[0]["geometry"]["coordinates"]
    travel_distance = routes[0]["distance"]  # seconds
    travel_time = routes[0]["duration"]  # meters

    return route_path, travel_distance, travel_time
