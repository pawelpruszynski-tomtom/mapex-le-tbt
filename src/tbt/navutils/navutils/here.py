import json
import typing
import warnings

import flexpolyline
import numpy
import requests
import shapely.geometry

from .base_provider import BaseProvider, RouteOptions, _is_flat_list
from .enum_types import AvoidOptions, SectionType, TravelMode
from .route import Route, RouteSection


class HereAPI(BaseProvider):
    """Class to encapsulate route queries to Here"""

    def __parse_route_options(
        self,
        origin,
        destination,
        route_options: RouteOptions,
        heading_origin,
        heading_destination,
    ) -> str:
        url = f"origin={origin[1]},{origin[0]}{(';course='+str(heading_origin)) if heading_origin else ''}"
        url += f"&destination={destination[1]},{destination[0]}{(';course='+str(heading_destination)) if heading_destination else ''}"

        if route_options.travel_mode == TravelMode.CAR:
            url += "&transportMode=car"
        elif route_options.travel_mode == TravelMode.TRUCK:
            url += "&transportMode=truck"
        else:
            # TODO: Implement other travel modes here
            warnings.warn("Requested route travel_mode not supported")
            url += "&transportMode=car"

        # Avoid options
        avoid_features = []
        avoid_zonecategories = []

        if AvoidOptions.UNPAVED in route_options.avoid:
            # avoid unpaved
            avoid_features.append("dirtRoad")
        if AvoidOptions.TOLL in route_options.avoid:
            avoid_features.append("tollRoad")
        if AvoidOptions.MOTORWAY in route_options.avoid:
            avoid_features.append("motorway")
        if AvoidOptions.FERRY in route_options.avoid:
            avoid_features.append("ferry")
        if AvoidOptions.LEZ in route_options.avoid:
            avoid_zonecategories.append("environmental")

        if len(avoid_features) > 0:
            url += "&avoid[features]=" + ",".join(avoid_features)
        if len(avoid_zonecategories) > 0:
            url += "&avoid[zoneCategories]=" + ",".join(avoid_zonecategories)

        # Parse traffic options
        if not route_options.traffic:
            url += "&traffic[mode]=disabled"

        # Departure
        if route_options.depart_at is not None:
            url += f"&departureTime={route_options.depart_at}"
        # Arrival
        if route_options.arrive_at is not None:
            url += f"&arrivalTime={route_options.arrive_at}"

        # Route type ignored

        # Section types

        returns = ["summary", "polyline"]
        spans = []
        if SectionType.UNPAVED in route_options.sections:
            spans.append("streetAttributes")
        if SectionType.TOLL in route_options.sections:
            spans.append("tollSystems")
            returns.append("tolls")
        if (
            SectionType.CAR in route_options.sections
            or SectionType.TRUCK in route_options.sections
        ):
            spans.append("carAttributes,truckAttributes")
        if (
            SectionType.CARTRAIN in route_options.sections
            or SectionType.MOTORWAY in route_options.sections
            or SectionType.FERRY in route_options.sections
        ):
            spans.append("streetAttributes")

        url += f"&return={','.join(returns)}"
        if len(spans) > 0:
            url += f"&spans={','.join(spans)}"

        # truck restrictions
        if route_options.vehicle_weight is not None:
            url += f"&vehicle[grossWeight]={route_options.vehicle_weight}"
        if route_options.vehicle_height is not None:
            url += f"&vehicle[height]={int(round(route_options.vehicle_height*100))}"
        if route_options.vehicle_width is not None:
            url += f"&vehicle[width]={int(round(route_options.vehicle_width* 100))}"
        if route_options.vehicle_length is not None:
            url += f"&vehicle[length]={int(round(route_options.vehicle_length* 100))}"

        # Route alternatives
        if route_options.alternative_routes > 0:
            url += f"&alternatives={route_options.alternative_routes}"

        # API keys
        url += f"&apikey={self.api_key}"

        return url

    def __parse_response(self, response):
        # Parse json
        content = json.loads(response.content)

        return_info_routes = []
        for route_json in content["routes"]:
            # Check for route violations. HereAPI can give you the route,
            # but sometimes there is a violation that is necessary to complete
            # the route. We will avoid those

            flag_violation = False
            if "notices" in route_json["sections"][0]:
                for notice in route_json["sections"][0]["notices"]:
                    if notice["code"] == "violatedVehicleRestriction":
                        flag_violation = True

            # Decode route using flexpolyline library
            route = flexpolyline.decode(route_json["sections"][0]["polyline"])
            route = [(y, x) for (x, y) in route]

            # Route time
            time_s = float(route_json["sections"][0]["summary"]["duration"])

            # Extract sections
            if "spans" in route_json["sections"][0].keys():
                spans = route_json["sections"][0]["spans"]
            else:
                spans = []
            sections = []
            for i, span in enumerate(spans):
                start = span["offset"]
                if i < len(spans) - 1:
                    end = spans[i + 1]["offset"]
                else:
                    end = len(route)

                geometry = shapely.geometry.LineString(route[start : (end + 1)])

                # SectionType.CAR
                if "carAttributes" in span.keys():
                    if "open" in span["carAttributes"]:
                        sections.append(RouteSection(geometry, SectionType.CAR))
                    # SectionType.TOLL
                    if "tollRoad" in span["truckAttributes"]:
                        sections.append(RouteSection(geometry, SectionType.TOLL))

                # SectionType.TRUCK
                if "truckAttributes" in span.keys():
                    if "open" in span["truckAttributes"]:
                        sections.append(RouteSection(geometry, SectionType.TRUCK))

                # SectionType.MOTORWAY
                if "streetAttributes" in span.keys():
                    if "motorway" in span["streetAttributes"]:
                        sections.append(RouteSection(geometry, SectionType.MOTORWAY))

                    # SectionType.UNPAVED
                    if "dirtRoad" in span["streetAttributes"]:
                        sections.append(RouteSection(geometry, SectionType.UNPAVED))

            return_info_routes.append([route, time_s, sections, flag_violation])

        # Return the first route if only one route is found
        return (
            return_info_routes[0]
            if len(return_info_routes) == 1
            else return_info_routes
        )

    def get_route_with_supporting_points(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        supporting_route=None,
        route_options=RouteOptions(),
        heading_origin: typing.Optional[int] = None,
        heading_destination: typing.Optional[int] = None,
    ) -> Route:
        url = self.__parse_route_options(
            origin, destination, route_options, heading_origin, heading_destination
        )
        parsed_waypoints = None
        if supporting_route is not None:
            waypoints = [(y, x) for (x, y) in supporting_route]
            if len(waypoints) > 95:
                warnings.warn(
                    "Maximum number of waypoints exceeded (95)... truncating to 95"
                )
                choices = numpy.random.choice(range(len(waypoints)), 25, replace=False)
                choices.sort()
                waypoints = numpy.array(waypoints)[choices]
                waypoints = [list(x) for x in waypoints]

            parsed_waypoints = [
                f"via={x[0]},{x[1]}!passThrough=true" for x in waypoints
            ]
            parsed_waypoints = "&" + "&".join(parsed_waypoints)

        url += f"&{parsed_waypoints}" if parsed_waypoints else ""

        try:
            response = requests.get(self.endpoint + "routes?" + url, timeout=600)
            # If Error response
            if response.status_code != 200:
                return Route([], self.name, 0)

            routes_info = self.__parse_response(response)
            if len(routes_info) == 0:
                return Route([], self.name, 0)

            if _is_flat_list(routes_info):
                route, time_s, sections, flag_violation = routes_info
                return Route(
                    route,
                    self.name,
                    time=time_s,
                    sections=sections,
                    flag_violation=flag_violation,
                )
            return[
                Route(
                    route,
                    self.name,
                    time=time_s,
                    sections=sections,
                    flag_violation=flag_violation,
                )
                for route, time_s, sections, flag_violation in routes_info
            ]
        except:
            return Route([], self.name, 0, flag_violation=True)

    def get_route(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options=RouteOptions(),
        heading_origin: typing.Optional[int] = None,
        heading_destination: typing.Optional[int] = None,
    ) -> Route:
        return self.get_route_with_supporting_points(
            origin,
            destination,
            supporting_route=None,
            route_options=route_options,
            heading_origin=heading_origin,
            heading_destination=heading_destination,
        )
