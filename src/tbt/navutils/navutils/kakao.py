import json
import requests
import typing
import warnings

from .route import Route
from .base_provider import (
    BaseProvider,
    RouteOptions,
)
from .enum_types import AvoidOptions, RouteType, TravelMode, SectionType

def parse_kakao_response(response_content: bytes) -> typing.Tuple[typing.List[float], float, float]:
    """Parse the response from Kakao API and return the route geometry, route distance and route duration"""
    json_content = json.loads(response_content)
    
    # parse route coords
    route = json_content.get("routes")[0]["sections"][0]
    points = [road["vertexes"] for road in route["roads"]]
    points = [sublist[i:i+2] for sublist in points for i in range(0, len(sublist), 2)]

    # parse route time
    summary  = json_content.get("routes")[0]["summary"]
    distance = summary["distance"]
    duration = summary["duration"]

    return points, distance, duration

class Kakao(BaseProvider):
    """Kakao provider class"""
    def __parse_route_options(
        self, origin: typing.List[float], destination: typing.List[float], route_options: RouteOptions
    ) -> str:
        """
        Parse the route options and return the URL to request the route
        """

        url = f"{self.endpoint}?origin={origin[0]},{origin[1]}&destination={destination[0]},{destination[1]}"
        
        if route_options.route_type == RouteType.FASTEST:
            url += "&priority=TIME"
        elif route_options.route_type == RouteType.SHORTEST:
            url += "&priority=DISTANCE"

        if route_options.travel_mode != TravelMode.CAR:
            warnings.warn("Requested travel_mode not supported")
        
        to_avoid = []
        if AvoidOptions.TOLL in route_options.avoid:
            to_avoid += ["toll"]
        if AvoidOptions.MOTORWAY in route_options.avoid:
            to_avoid += ["motorway"]
        if AvoidOptions.FERRY in route_options.avoid:
            to_avoid += ["ferries"]
        
        if AvoidOptions.UNPAVED in route_options.avoid:
            warnings.warn("Requested avoid unpaved not supported")
        if AvoidOptions.LEZ in route_options.avoid:
            warnings.warn("Requested avoid low emission zone not supported")
        if AvoidOptions.CARPOOL in route_options.avoid:
            warnings.warn("Requested avoid carpool not supported")

        if not route_options.traffic:
            to_avoid += ["roadevent"]
        
        if len(to_avoid) > 0:
            url += f"&avoid={'|'.join(to_avoid)}"
        
        return url


    def get_route(self, origin: typing.List[float], destination: typing.List[float], route_options=RouteOptions()) -> Route:
        url = self.__parse_route_options(
            origin=origin,
            destination=destination,
            route_options=route_options
        )

        try:
            url = self.__parse_route_options(origin=origin, destination=destination, route_options=route_options)
            
            request = requests.get(url, headers={"Authorization": f"KakaoAK {self.api_key}"}, timeout=600)
            
            if request.status_code != 200:
                return Route(geometry=[], provider=self.name, length=0, time=0)
            
            points, distance, duration = parse_kakao_response(request.content)
            return Route(geometry=points, length=distance, time=duration, provider=self.name)
        except:
            pass
        return Route(geometry=[], provider=self.name, length=0, time=0)
        
        


    def get_route_with_supporting_points(self, origin: typing.List[float], destination: typing.List[float], supporting_route=None, route_options=RouteOptions()) -> Route:
        return NotImplementedError
