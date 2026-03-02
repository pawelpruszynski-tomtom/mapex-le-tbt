from datetime import datetime
import typing
import warnings
import googlemaps
import numpy
import polyline
from .route import Route
from .base_provider import BaseProvider, RouteOptions
from .enum_types import TravelMode, AvoidOptions


class GoogleAPI(BaseProvider):
    """Class to encapsulate route queries to Google"""

    @staticmethod
    def __parse_route_options(route_options: RouteOptions):
        # See https://developers.google.com/maps/documentation/directions/get-directions
        # https://googlemaps.github.io/google-maps-services-python/docs/index.html

        mode = "driving"
        if route_options.travel_mode != TravelMode.CAR:
            warnings.warn(
                "Travel mode other than car is not implemented for Google as provider"
            )

        avoid = []
        if AvoidOptions.TOLL in route_options.avoid:
            avoid.append("tolls")
        if AvoidOptions.MOTORWAY in route_options.avoid:
            avoid.append("motorways")
        if AvoidOptions.FERRY in route_options.avoid:
            avoid.append("ferries")

        departure_time = route_options.depart_at
        if departure_time is not None and departure_time.strip() != "":
            departure_time = datetime.strptime(departure_time, "%Y-%m-%dT%H:%M:%S")
        else:
            departure_time = None

        arrival_time = route_options.arrive_at
        if arrival_time is not None and arrival_time.strip() != "":
            departure_time = datetime.strptime(arrival_time, "%Y-%m-%dT%H:%M:%S")
        else:
            arrival_time = None

        traffic_model = "best_guess"
        # if not route_options.traffic:
        #     warnings.warn("Not using Traffic in Google api is not possible")

        return mode, avoid, departure_time, arrival_time, traffic_model

    def __init__(self, *args):
        super().__init__(*args)
        self.gmaps = googlemaps.Client(key=self.api_key)

    def get_route_with_supporting_points(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        supporting_route=None,
        route_options=RouteOptions(),
    ) -> Route:
        o_t = [origin[1], origin[0]]
        d_t = [destination[1], destination[0]]
        try:
            (
                mode,
                avoid,
                departure_time,
                arrival_time,
                traffic_model,
            ) = self.__parse_route_options(route_options)

            if supporting_route is not None:
                waypoints = [(y, x) for (x, y) in supporting_route]
                if len(waypoints) > 10:
                    warnings.warn(
                        "Maximum number of waypoints exceeded (10)... truncating to 10"
                    )
                    choices = numpy.random.choice(
                        range(len(waypoints)), 25, replace=False
                    )
                    choices.sort()
                    waypoints = numpy.array(waypoints)[choices]
                    waypoints = [list(x) for x in waypoints]
            else:
                waypoints = None

            directions = self.gmaps.directions(
                o_t,
                d_t,
                mode=mode,
                waypoints=waypoints,
                avoid=avoid,
                departure_time=departure_time,
                arrival_time=arrival_time,
                traffic_model=traffic_model,
            )
            point_list = []
            for leg in directions[0]["legs"]:
                for step in leg["steps"]:
                    point_list += polyline.decode(step["polyline"]["points"])
            route = [(y, x) for (x, y) in point_list]
        except:
            return Route([], self.name)

        return Route(
            route,
            self.name,
            length=numpy.sum(
                [leg["distance"]["value"] for leg in directions[0]["legs"]]
            ),
            time=numpy.sum([leg["duration"]["value"] for leg in directions[0]["legs"]]),
        )

    def get_route(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options=RouteOptions(),  # default options
    ) -> Route:
        return self.get_route_with_supporting_points(
            origin, destination, supporting_route=None, route_options=route_options
        )
