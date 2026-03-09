import json
import typing
import warnings

import requests
import shapely.geometry

from .base_provider import BaseProvider, RouteOptions, _is_flat_list
from .enum_types import AvoidOptions, RouteType, SectionType, TravelMode
from .route import Route, RouteSection


def parse_pudre_api_response(response):
    content = response.content
    all_json = json.loads(content).get("routes", 0)
    if all_json == 0:
        return [], 0.0, 0.0, []

    return_info_routes = []

    for route in all_json:
        points = [p for leg in route["legs"] for p in leg["points"]]

        sequence = [(x["longitude"], x["latitude"]) for x in points]
        distance = float(
            sum(leg["summary"]["lengthInMeters"] for leg in route["legs"])
        )
        travel_time = float(
            sum(leg["summary"]["travelTimeInSeconds"] for leg in route["legs"])
        )

        flag_violation = False

        # Retrieve all sections
        sections = []
        start_point_index = 0
        end_point_index = 0

        found_truck_section = False
        found_other_after_truck_section = False

        for section in route["sections"]:
            start = int(section["startPointIndex"])
            end = int(section["endPointIndex"])
            if section["sectionType"] == "TRAVEL_MODE":
                if section["travelMode"] == "car":
                    sectiontype = SectionType.CAR
                    if end_point_index == 0:
                        # We only return the first car section for backward compatibility in TbT
                        start_point_index = start
                        end_point_index = end
                elif section["travelMode"] == "truck":
                    sectiontype = SectionType.TRUCK
                    # Check if there is a no route error
                    # TODO: Consider also violated flag when only "other" travel_mode appears in ALL
                    # Ex: Origin=POINT (5.53278 46.80387); destination=POINT (5.27564 45.83713)
                    found_truck_section = True
                    if end_point_index == 0:
                        start_point_index = start
                        end_point_index = end
                    if found_other_after_truck_section:
                        flag_violation = True
                else:
                    sectiontype = SectionType.OTHER
                    if found_truck_section:
                        found_other_after_truck_section = True
            elif section["sectionType"] == "TOLL_ROAD":
                sectiontype = SectionType.TOLL
            elif section["sectionType"] == "MOTORWAY":
                sectiontype = SectionType.MOTORWAY
            elif section["sectionType"] == "UNPAVED":
                sectiontype = SectionType.UNPAVED
            else:
                # Not implemented
                warnings.warn(
                    f"section type returned: {section['sectionType']} : not implemented."
                )
                sectiontype = SectionType.OTHER

            # Workaround to deal with only one point sections.
            # We duplicate that point to create the geo.
            section_geom = sequence[start:end]
            if len(section_geom) <= 1:
                section_geom.append(sequence[start:end][0])

            sections.append(
                RouteSection(
                    geometry=shapely.geometry.LineString(section_geom),
                    section_type=sectiontype,
                )
            )

        return_info_routes.append(
            [sequence, distance, travel_time, sections, flag_violation]
        )

    # Return the first route if only one route is found
    return return_info_routes[0] if len(return_info_routes) == 1 else return_info_routes


class Orbis(BaseProvider):
    """Class to encapsule prune get route requests"""

    def __parse_route_options(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options: RouteOptions,
        heading_origin: typing.Optional[int] = None,
        heading_destination: typing.Optional[int] = None,
    ):
        url = "&apiVersion=2"
        if route_options.travel_mode == TravelMode.CAR:
            url += "&travelMode=car"
        elif route_options.travel_mode == TravelMode.TRUCK:
            url += "&travelMode=truck"
        else:
            # TODO: Implement other travel modes here
            warnings.warn(
                "Requested route travel_mode not supported. Using default option car."
            )
            url += "&travelMode=car"

        # Avoid options
        if AvoidOptions.UNPAVED in route_options.avoid:
            # avoid unpaved
            url += "&avoid=unpavedRoads"
        if AvoidOptions.TOLL in route_options.avoid:
            url += "&avoid=tollRoads"
        if AvoidOptions.MOTORWAY in route_options.avoid:
            url += "&avoid=motorways"
        if AvoidOptions.FERRY in route_options.avoid:
            url += "&avoid=ferries"
        if AvoidOptions.LEZ in route_options.avoid:
            url += "&avoid=lowEmissionZones"

        # Traffic
        if route_options.traffic:
            url += "&traffic=true"
        else:
            url += "&traffic=historical"

        # Departure
        if route_options.depart_at is not None:
            url += f"&departAt={route_options.depart_at}"
        # Arrival
        if route_options.arrive_at is not None:
            url += f"&arriveAt={route_options.arrive_at}"

        # Route type
        if route_options.route_type == RouteType.FASTEST:
            url += "&routeType=fast"
        elif route_options.route_type == RouteType.SHORTEST:
            url += "&routeType=shortest"
        else:
            warnings.warn("Requested route type not supported")

        # Section types
        url += "&sectionType=travelMode"  # always travel mode

        # TODO: trucks
        if SectionType.UNPAVED in route_options.sections:
            url += "&sectionType=unpaved"
        if SectionType.TOLL in route_options.sections:
            url += "&sectionType=tollRoad"
        if SectionType.CARTRAIN in route_options.sections:
            url += "&sectionType=carTrain"
        if SectionType.MOTORWAY in route_options.sections:
            url += "&sectionType=motorway"
        if SectionType.HOV in route_options.sections:
            url += "&sectionType=carpool"
        if SectionType.FERRY in route_options.sections:
            url += "&sectionType=ferry"
        if SectionType.TUNNEL in route_options.sections:
            url += "&sectionType=tunnel"

        # truck restrictions
        if route_options.vehicle_weight is not None:
            url += f"&vehicleWeight={route_options.vehicle_weight}"
        if route_options.vehicle_height is not None:
            url += f"&vehicleHeight={route_options.vehicle_height}"
        if route_options.vehicle_width is not None:
            url += f"&vehicleWidth={route_options.vehicle_width}"
        if route_options.vehicle_length is not None:
            url += f"&vehicleLength={route_options.vehicle_length}"

        # Route alternatives
        if route_options.alternative_routes > 0:
            url += f"&maxAlternatives={route_options.alternative_routes}"

        # API keys
        url += f"&key={self.api_key}"

        # origin/destination
        locations = [origin, destination]
        checkpoints = ":".join([f"{loc[1]:.6f},{loc[0]:.6f}" for loc in locations])

        # Heading
        if heading_origin is not None:
            url += f"&vehicleHeading={heading_origin}"
        if heading_destination is not None:
            warnings.warn("Heading destination not supported for TomTom")

        return self.endpoint + f"{checkpoints}/json?" + url

    def get_route(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options=RouteOptions(),  # Default options
        heading_origin: typing.Optional[int] = None,
        heading_destination: typing.Optional[int] = None,
    ) -> Route:
        url = self.__parse_route_options(
            origin,
            destination,
            route_options,
            heading_origin=heading_origin,
            heading_destination=heading_destination,
        )
        try:
            try:
                # print(url)
                request = requests.get(url, timeout=600)
            except requests.exceptions.SSLError:
                # https://jira.tomtomgroup.com/browse/OMA-534 for KOR
                request = requests.get(url, timeout=600, verify=False)

            if request.status_code == 400:
                response_json = json.loads(request.content)
                if response_json["detailedError"]["code"] == "NO_ROUTE_FOUND":
                    return Route(
                        geometry=[],
                        provider=self.name,
                        length=0.0,
                        time=0.0,
                        sections=None,
                        flag_violation=True,
                    )

            routes_info = parse_pudre_api_response(request)
            if _is_flat_list(routes_info):
                (
                    sequence,
                    distance,
                    travel_time,
                    sections,
                    flag_violation,
                ) = routes_info
                return Route(
                    sequence, self.name, distance, travel_time, sections, flag_violation
                )
            else:
                return [
                    Route(
                        sequence,
                        self.name,
                        distance,
                        travel_time,
                        sections,
                        flag_violation,
                    )
                    for sequence, distance, travel_time, sections, flag_violation in routes_info
                ]
        except:
            return Route([], self.name, 0.0, 0.0)

    def get_route_with_supporting_points(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        supporting_route: typing.Optional[typing.List[typing.List[float]]] = None,
        route_options=RouteOptions(),  # Default options
        heading_origin: typing.Optional[int] = None,
        heading_destination: typing.Optional[int] = None,
    ):
        if supporting_route is None:
            supporting_route = []
        supporting_points = [
            {"latitude": loc[1], "longitude": loc[0]} for loc in supporting_route
        ]

        url = self.__parse_route_options(
            origin,
            destination,
            route_options,
            heading_origin=heading_origin,
            heading_destination=heading_destination,
        )
        try:
            request = requests.post(
                url,
                headers={"Content-Type": "application/json", "accept": "*/*"},
                data=json.dumps({"supportingPoints": supporting_points}),
                timeout=600,
            )

            if request.status_code == 400:
                response_json = json.loads(request.content)
                if response_json["detailedError"]["code"] == "NO_ROUTE_FOUND":
                    return Route(
                        geometry=[],
                        provider=self.name,
                        length=0.0,
                        time=0.0,
                        sections=None,
                        flag_violation=True,
                    )
            routes_info = parse_pudre_api_response(request)
            if _is_flat_list(routes_info):
                (
                    sequence,
                    distance,
                    travel_time,
                    sections,
                    flag_violation,
                ) = routes_info
                return Route(
                    sequence, self.name, distance, travel_time, sections, flag_violation
                )
            else:
                return [
                    Route(
                        sequence,
                        self.name,
                        distance,
                        travel_time,
                        sections,
                        flag_violation,
                    )
                    for sequence, distance, travel_time, sections, flag_violation in routes_info
                ]
        except:
            return Route([], self.name, 0.0, 0.0)
