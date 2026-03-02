import json
import requests
import typing
import warnings
from datetime import datetime

from .route import Route
from .base_provider import (
    BaseProvider,
    RouteOptions,
)
from .enum_types import AvoidOptions, RouteType, TravelMode

def parse_gt_response(response_content: bytes) -> typing.Tuple[typing.List[float], float, float]:
    """Parse the response from GT(Mapfan) API and return the route geometry, route distance and route duration"""
  
    json_content = json.loads(response_content)
    # parse route coords
    shape = json_content.get("shape")
    points = [[subitem['lon'],subitem['lat']] for sublist in shape for subitem in sublist['shapePoints']]

    # parse route time
    summary  = json_content.get("summary")
    distance = summary["totalDistance"]
    duration = summary["totalTravelTime"]

    return points, distance, duration

class GT(BaseProvider):
    """GT(Mapfan) provider class"""
    def __parse_route_options(
        self, origin: typing.List[float], destination: typing.List[float], route_options: RouteOptions
    ) -> str:
        """
        Parse the route options and return the URL to request the route
        """
        # long+lat
        _start_point = f"{origin[0]},{origin[1]}"
        _destination_point = f"{destination[0]},{destination[1]}"
        _ferry=""
        _tollway=""
        
        if route_options.travel_mode != TravelMode.CAR:
            warnings.warn("Requested travel_mode not supported")
        
        # Toll
        if AvoidOptions.TOLL in route_options.avoid:
            # start 3rd parameter: 0:general and toll roads, 1:only general roads , 2:only  toll roads
            _start_point+= ",1"
            # tollway parameter: 0:Standard, 1:Preferred , 2:Avoided, 3:Prohibited(if no, return error)
            _tollway="2"
        else:
            _start_point+= ",0"
            _tollway="0"

        url = f"{self.endpoint}?start={_start_point}&destination={_destination_point}"

        # Route type
        if _tollway!="":
            url += f"&tollway={_tollway}"
        if route_options.route_type == RouteType.FASTEST:
            url += "&priority=0"
        elif route_options.route_type == RouteType.SHORTEST:
            url += "&priority=1"

        # Ferry
        if AvoidOptions.MOTORWAY in route_options.avoid:
            warnings.warn("Requested avoid MOTORWAY not supported")
        if AvoidOptions.FERRY in route_options.avoid:
            # ferry parameter: 0:Standard, 1:Preferred , 2:Avoided, 3:Prohibited(if no, return error)
            _ferry="2"
        else:
            _ferry="0"
        if _ferry!="":
            url += f"&ferry={_ferry}"
        
        # Travel Mode (Car type)
        if route_options.travel_mode == TravelMode.CAR:
            url += f"&cartype=1"
        elif route_options.travel_mode == TravelMode.TRUCK:
            url += f"&cartype=3"
        else:
            url += f"&cartype=1"
        
        # Not supported
        if AvoidOptions.UNPAVED in route_options.avoid:
            warnings.warn("Requested avoid unpaved not supported")
        if AvoidOptions.LEZ in route_options.avoid:
            warnings.warn("Requested avoid low emission zone not supported")
        if AvoidOptions.CARPOOL in route_options.avoid:
            warnings.warn("Requested avoid carpool not supported")
        if not route_options.traffic:
            warnings.warn("Requested avoid road event/traffic not supported")
        
        

        return url


    def get_route(self, origin: typing.List[float], destination: typing.List[float], route_options=RouteOptions()) -> Route:
        appid=self.api_key



        dt_string = datetime.now().strftime("%Y%m%d%H%M%S")+"000"
        dynamic_key=''
        try:
            key_request = requests.get(f"https://api-auth.mapfan.com/v1/auth?appid={appid}&date={dt_string}", timeout=600)
            dynamic_key = key_request.json()['key']
        except:
            pass

    
        try:
            url = self.__parse_route_options(origin=origin, destination=destination, route_options=route_options)
            # TODO: request API key
            url+=f"&key={dynamic_key}"
            request = requests.get(url, timeout=600)
            
            if request.status_code != 200:
                return Route(geometry=[], provider=self.name, length=0, time=0)
            
            points, distance, duration = parse_gt_response(request.content)
            return Route(geometry=points, length=distance, time=duration, provider=self.name)
        except:
            pass
        return Route(geometry=[], provider=self.name, length=0, time=0)
        
        


    def get_route_with_supporting_points(self, origin: typing.List[float], destination: typing.List[float], supporting_route=None, route_options=RouteOptions()) -> Route:
        return NotImplementedError
