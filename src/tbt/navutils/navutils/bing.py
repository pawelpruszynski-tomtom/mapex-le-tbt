import typing
import warnings
import requests
import json

from .route import Route, RouteSection
from .base_provider import (
    BaseProvider,
    RouteOptions,
)
from .enum_types import TravelMode, AvoidOptions, RouteType, SectionType

class BingAPI(BaseProvider):
    def __parse_route_options(
        self, origin, destination, route_options: RouteOptions
    ) -> str:
        #url = "http://dev.virtualearth.net/REST/v1/Routes/{travelMode}?wayPoint.1={wayPoint1}&viaWaypoint.2={viaWaypoint2}&waypoint.3={waypoint3}&wayPoint.n={waypointN}&heading={heading}&optimize={optimize}&avoid={avoid}&distanceBeforeFirstTurn={distanceBeforeFirstTurn}&routeAttributes={routeAttributes}&timeType={timeType}&dateTime={dateTime}&maxSolutions={maxSolutions}&tolerances={tolerances}&distanceUnit={distanceUnit}&key={BingMapsKey}"
        
        url = ""
        # Parse travel mode
        if route_options.travel_mode == TravelMode.CAR:
            url += "Driving"
        elif route_options.travel_mode == TravelMode.TRUCK:
            url += "Truck"
        else:
            # TODO: Implement other travel modes here
            warnings.warn("Requested route travel_mode not supported. Using car mode")
            url += "Driving"
        
        # Parse origin/destination
        url += f"?wayPoint.1={origin[1]},{origin[0]}&wayPoint.2={destination[1]},{destination[0]}"
        
        avoid_features = []
        # Parse avoid options
        if AvoidOptions.UNPAVED in route_options.avoid:
            # avoid unpaved not supported. see https://learn.microsoft.com/en-us/bingmaps/rest-services/routes/calculate-a-route
            pass
        if AvoidOptions.TOLL in route_options.avoid:
            avoid_features.append("tolls")
        if AvoidOptions.MOTORWAY in route_options.avoid:
            avoid_features.append("highways")
        if AvoidOptions.FERRY in route_options.avoid:
            avoid_features.append("ferry")
        if AvoidOptions.LEZ in route_options.avoid:
            # Low emision zones not supported
            pass
        
        if len(avoid_features) > 0:
            url += "&avoid=" + ",".join(avoid_features)
        
        # distance unit
        url += "&distanceUnit=km"
        
        # optimize
        if route_options.route_type == RouteType.FASTEST:
            #traffic
            if route_options.traffic:
                url += "&optimize=timeWithTraffic"  
            else:  
                url += "&optimize=timeAvoidClosure"
        elif route_options.route_type == RouteType.SHORTEST:
            url += "&optimize=distance"
        
        # route output
        url += "&routePathOutput=Points"
        
        # api
        url += f"&key={self.api_key}"
        
        return self.endpoint + url
        

    def get_route_with_supporting_points(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        supporting_route=None,
        route_options=RouteOptions(),
    ) -> Route:
        # TODO: implement supporting points
        warnings.warn("Supporting points are not implemented for Bing: check bing.py")
        return self.get_route(origin, destination, route_options)
        
        

    def get_route(
        self,
        origin: typing.List[float],
        destination: typing.List[float],
        route_options=RouteOptions(),
    ) -> Route:
        url = self.__parse_route_options(origin, destination, route_options)
        try:
            response = requests.get(url, timeout=600)
            path, length, time = parse_bing_api_response(response)
            return Route(path, self.name, length, time)
        except:
            return Route([], self.name, 0.0, 0.0) 
        
def parse_bing_api_response(response):
    # TODO: Extract sections if possible
    c = json.loads(response.content)
    route_path = [(y,x) for (x,y) in c['resourceSets'][0]['resources'][0]['routePath']['line']['coordinates'] ]
    travel_distance = c['resourceSets'][0]['resources'][0]['travelDistance']*1000
    travel_time = c['resourceSets'][0]['resources'][0]['travelDuration']
    
    return route_path, travel_distance, travel_time