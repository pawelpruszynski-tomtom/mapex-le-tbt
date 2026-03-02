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
import polyline

def parse_genesysmap_response(
    response_content: bytes,
) -> typing.Tuple[typing.List[float], float, float]:
    """Parse the response from Genesysmap API and return the route geometry, route distance and route duration"""
  

    # parse route coords
    trip = response_content["trip"]
    links = polyline.decode(trip["legs"][0]['shape'], 6)
    # points = [[item['line']['coordinates'][0][0], item['line']['coordinates'][0][1]] for item in links]

    points = []
    for link in links:
        points.append([link[1],link[0]])

        # parse route time
    
        distance = trip["summary"]["length"] #unit km
        duration = trip["summary"]["time"] #unit seconds
    return points, distance, duration


class Genesysmap(BaseProvider):
    """Genesysmap provider class"""

    def __parse_route_options(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options: RouteOptions,
    ) -> str:
        """
        Parse the route options and return the URL to request the route
        """
        
      

        if route_options.travel_mode != TravelMode.CAR:
            warnings.warn("Requested travel_mode not supported")

        url = f"{self.endpoint}?api_key={self.api_key}"

        # long+lat
        params = {"locations":
                  [{"lat":origin[1],"lon":origin[0]},{"lat":destination[1],"lon":destination[0]}]
                  }

        params_costing_options = {}
        # Route type
        if route_options.route_type == RouteType.FASTEST:
            params_costing_options["shortest"] = False
        elif route_options.route_type == RouteType.SHORTEST:
            params_costing_options["shortest"] = True
        else:
            params_costing_options["shortest"] = False

        # Ferry
        if AvoidOptions.FERRY in route_options.avoid:
            # ferry parameter: 0:Standard, 1:Preferred , 2:Avoided, 3:Prohibited(if no, return error)
            params_costing_options["use_ferry"] = False

        # Travel Mode (Car type)
        params_costing= "auto"
        if route_options.travel_mode == TravelMode.CAR:
            params_costing= "auto"
        elif route_options.travel_mode == TravelMode.TRUCK:
            params_costing= "truck"
        elif route_options.travel_mode == TravelMode.BICYCLE:
            params_costing= "bicycle"

        # Not supported
        if AvoidOptions.TOLL in route_options.avoid:
            params_costing_options["use_tools"] = False
        if AvoidOptions.MOTORWAY in route_options.avoid:
            params_costing_options["use_highways"] = False
        if AvoidOptions.UNPAVED in route_options.avoid:
            warnings.warn("Requested avoid unpaved not supported")
        if AvoidOptions.LEZ in route_options.avoid:
            warnings.warn("Requested avoid low emission zone not supported")
        if AvoidOptions.CARPOOL in route_options.avoid:
            warnings.warn("Requested avoid carpool not supported")
        if not route_options.traffic:
            warnings.warn("Requested avoid road event/traffic not supported")
        params['costing']=params_costing
        params['costing_options']=params_costing_options
        return url, params

    def get_route(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options=RouteOptions(),
    ) -> Route:
        try:
            url,params = self.__parse_route_options(
                origin=origin, destination=destination, route_options=route_options
            )
      
            request = requests.post(url, json=params, timeout=600)
         
            if request.status_code != 200:
                return Route(geometry=[], provider=self.name, length=0, time=0)

            points, distance, duration = parse_genesysmap_response(request.json())
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
