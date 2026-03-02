import json
import typing
import warnings

import polyline
import requests

from .base_provider import (
    AvoidOptions,
    BaseProvider,
    RouteOptions,
    TravelMode,
    _is_flat_list,
)
from .route import Route


class Valhalla(BaseProvider):
    """Class to encapsulate Valhalla get_route queries"""

    def __parse_route_options(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options: RouteOptions,
        heading_origin: typing.Optional[int] = None,
        heading_destination: typing.Optional[int] = None,
    ):
        payload = {}

        # Avoid options
        # set some default options that other routing engines use
        costing_options = {
            "gate_penalty": 9999,
            "private_access_penalty": 9999,
            "use_tracks": 0,
            "service_penalty": 9999,
            "service_factor": 9999,
            "country_crossing_penalty": 9999,
        }

        if AvoidOptions.UNPAVED in route_options.avoid:
            # avoid unpaved
            costing_options["exclude_unpaved"] = 1
        if AvoidOptions.TOLL in route_options.avoid:
            # Avoid tolls
            costing_options["toll_booth_cost"] = 999
            costing_options["toll_booth_penalty"] = 1
            costing_options["use_tolls"] = 0
        if AvoidOptions.MOTORWAY in route_options.avoid:
            costing_options["use_highways"] = 0
        if AvoidOptions.FERRY in route_options.avoid:
            costing_options["use_ferry"] = 0

        # Travel mode and costing options
        if route_options.travel_mode == TravelMode.CAR:
            payload["costing"] = "auto"
            payload["costing_options"] = {"auto": costing_options}
        elif route_options.travel_mode == TravelMode.TRUCK:
            payload["costing"] = "truck"
            payload["costing_options"] = {
                "truck": costing_options,
                "height": route_options.vehicle_height,
                "width": route_options.vehicle_width,
                "length": route_options.vehicle_length,
                "weight": route_options.vehicle_weight,
            }
        else:
            # TODO: Implement other travel modes here
            warnings.warn(
                "Requested route travel_mode not supported. Using costing auto"
            )
            payload["costing"] = "auto"
            payload["costing_options"] = {"auto": costing_options}

        # origin/destination
        if not route_options.strict:
            payload["locations"] = [
                {
                    "lat": origin[1],
                    "lon": origin[0],
                    "radius": 20,
                    "rank_candidates": False,
                    "street_side_tolerance": 10,
                    **({"heading": heading_origin} if heading_origin else {}),
                },
                {
                    "lat": destination[1],
                    "lon": destination[0],
                    "radius": 20,
                    "rank_candidates": False,
                    "street_side_tolerance": 10,
                    **({"heading": heading_destination} if heading_destination else {}),
                },
            ]
        else:
            payload["locations"] = [
                {
                    "lat": origin[1],
                    "lon": origin[0],
                    **({"heading": heading_origin} if heading_origin else {}),
                },
                {
                    "lat": destination[1],
                    "lon": destination[0],
                    **({"heading": heading_destination} if heading_destination else {}),
                },
            ]

        if route_options.alternative_routes > 0:
            payload["alternates"] = route_options.alternative_routes

        return payload

    def __parse_response(self, response):
        content = json.loads(response.content)

        first_trip = content["trip"]
        route = [
            (y, x)
            for (x, y) in polyline.decode(first_trip["legs"][0]["shape"], precision=6)
        ]

        length_m = float(first_trip["summary"]["length"]) * 1000
        time_s = float(first_trip["summary"]["time"])

        alternate_routes = content.get("alternates", 0)
        if alternate_routes == 0:
            return route, length_m, time_s

        return_info_routes = [[route, length_m, time_s]]

        for alternate_route in alternate_routes:
            route = [
                (y, x)
                for (x, y) in polyline.decode(
                    alternate_route["trip"]["legs"][0]["shape"], precision=6
                )
            ]
            length_m = float(alternate_route["trip"]["summary"]["length"]) * 1000
            time_s = float(alternate_route["trip"]["summary"]["time"])
            return_info_routes.append([route, length_m, time_s])

        # Return the first route if only one route is found
        return return_info_routes

    def get_route(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options=RouteOptions(),
        heading_origin: typing.Optional[int] = None,
        heading_destination: typing.Optional[int] = None,
    ) -> Route:
        """
        Get a route from valhalla. Coordinates are expected to be in (lon, lat) format.

        This method calculates a route between the given origin and destination coordinates,
        with optional supporting points. The route is calculated using the Valhalla routing engine.

        :param origin: The coordinates of the origin point.
        :type origin: list[float]
        :param destination: The coordinates of the destination point.
        :type destination: list[float]
        :param route_options: The route options to be used for the calculation.
        :type route_options: RouteOptions, optional
        :param heading_origin: The heading of the origin point in degrees.
        :type heading_origin: int, optional
        :param heading_destination: The heading of the destination point in degrees.
        :type heading_destination: int, optional
        :return: The calculated route.
        :rtype: Route
        """

        if self.api == "Valhalla-Strict":
            # Force using strict
            route_options.strict = True

        payload = self.__parse_route_options(
            origin, destination, route_options, heading_origin, heading_destination
        )

        try:
            response = requests.get(
                self.endpoint + "route",
                params={"json": json.dumps(payload, separators=(",", ":"))},
                timeout=300,
            )

            routes_info = self.__parse_response(response)
            if _is_flat_list(routes_info):
                route, length_m, time_s = routes_info
                return Route(route, self.name, length_m, time_s)
            return [
                Route(
                    route,
                    self.name,
                    length_m,
                    time_s,
                )
                for route, length_m, time_s in routes_info
            ]
        except Exception:
            return Route([], self.name)

    def get_route_with_supporting_points(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        supporting_route: typing.Optional[typing.List[typing.List[float]]] = None,
        route_options=RouteOptions(),  # Default options
        heading_origin: typing.Optional[int] = None,
        heading_destination: typing.Optional[int] = None,
    ) -> Route:
        """
        Get a route with supporting points. Coordinates are expected to be in (lon, lat) format.

        This method calculates a route between the given origin and destination coordinates,
        with optional supporting points. The route is calculated using the Valhalla routing engine.

        :param origin: The coordinates of the origin point.
        :type origin: list[float]
        :param destination: The coordinates of the destination point.
        :type destination: list[float]
        :param supporting_route: Optional supporting points along the route.
        :type supporting_route: list[list[float]], optional
        :param route_options: The route options to be used for the calculation.
        :type route_options: RouteOptions, optional
        :param heading_origin: The heading of the origin point in degrees.
        :type heading_origin: int, optional
        :param heading_destination: The heading of the destination point in degrees.
        :type heading_destination: int, optional
        :return: The calculated route.
        :rtype: Route
        """
        if self.api == "Valhalla-Strict":
            # Force using strict
            route_options.strict = True

        payload = self.__parse_route_options(origin, destination, route_options, heading_origin, heading_destination)

        # we pop the destination point to add it later at the end
        destination = payload["locations"].pop()

        for supporting_point in supporting_route:
            if not route_options.strict:
                payload["locations"].append(
                    {
                        "lat": supporting_point[1],
                        "lon": supporting_point[0],
                        "radius": 20,
                        "rank_candidates": False,
                        "street_side_tolerance": 10,
                        "type": "through",
                    }
                )
            else:
                payload["locations"].append(
                    {
                        "lat": supporting_point[1],
                        "lon": supporting_point[0],
                        "type": "through",
                    }
                )

        payload["locations"].append(destination)

        try:
            response = requests.get(
                self.endpoint + "route",
                params={"json": json.dumps(payload, separators=(",", ":"))},
                timeout=300,
            )

            routes_info = self.__parse_response(response)
            if _is_flat_list(routes_info):
                route, length_m, time_s = routes_info
                return Route(route, self.name, length_m, time_s)
            return [
                Route(
                    route,
                    self.name,
                    length_m,
                    time_s,
                )
                for route, length_m, time_s in routes_info
            ]
        except Exception:
            return Route([], self.name)
