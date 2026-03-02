import requests
import json
import shapely
import shapely.geometry
from shapely.geometry import LineString
import logging
import pandas as pd
import numpy as np
from .. import geometry_utils
from .mapmatcher import MapMatcher


default_fields = '{\
    route{\
        type,\
        geometry{type,coordinates},\
        properties{\
            id,\
            speedLimits{value,unit,type},\
            speedRestrictions{maximumSpeed{value,unit}},\
            speedProfile{value,unit},\
            address{roadName,roadNumbers},\
            frc,\
            formOfWay,\
            roadUse,\
            laneInfo{numberOfLanes},\
            heightInfo{height,chainage},\
            trafficSigns{signType,chainage},\
            trafficLight,confidence\
                }\
        }\
    }'


class GenesisMapMatcher(MapMatcher):
    def __init__(self, api_key, mapmatching_url = 'https://api.tomtom.com/snap-to-roads/1/snap-to-roads', 
                 fields=default_fields, vehicle_type='PassengerCar') -> None:
        self.api_key = api_key
        self.mapmatching_url = mapmatching_url
        self.fields = fields
        self.vehicle_type=vehicle_type
        self.target_coords = 4000 # Tested Limit 5000
        self.max_request_length = 6500 # Max limit 7168

    def format_url(self, geom):
        coords = ';'.join([f"{loc[0]},{loc[1]}" for loc in geom.coords[:]])
        url = f"{self.mapmatching_url}?key={self.api_key}&points={coords}&fields={self.fields.replace(' ', '')}&vehicleType={self.vehicle_type}&measurementSystem=metric"
        return url

    
    def map_match_segment(self, geom: shapely.geometry.LineString)-> pd.DataFrame:
        """Single substring request to mapmatch with Snap2Roads API

        :param geom: Line to mapmatch
        :type geom: shapely.geometry.LineString
        :return: DataFrame with the mapmatched segment
        :rtype: pd.DataFrame
        """

        url = self.format_url(geom)        
        
        simplify_factor = 1e-7
        while len(geom.coords[:]) > self.target_coords or len(url) > self.max_request_length:
            geom = geom.simplify(simplify_factor)
            simplify_factor *= 2
            url = self.format_url(geom)
        
        try:
            r = requests.get(url)
            if r.status_code!=200:
                logging.warning(r.content)
                return pd.DataFrame()
            content = json.loads(r.content)
            if "route" not in content.keys() or len(content['route']) == 0:
                logging.warning("Not found route in response")
                return pd.DataFrame()
            
            route = pd.json_normalize(content['route'])
            route = route.filter(regex="(id|geometry|properties)")
            route.columns = [col.replace("properties.", "") for col in route.columns]
            route['geom'] = route['geometry.coordinates'].map(lambda x: LineString(x).wkt)
  
            route.rename(columns={'id': 'feature_id'}, inplace=True)
            route.drop(columns=['geometry.type', 'geometry.coordinates'], inplace=True)


        except Exception as ex:
            logging.warning(f"Error calling snap2roads: {ex}")
            return pd.DataFrame()
        return route


    def map_match_request(self,
        geom: shapely.geometry.LineString
    ) -> pd.DataFrame:
        """
        Map matched geometry of a route with Genesis map (TomTom API) given a list of the route coordinates
        As per snap2roads max distance to mapmatch we split the original geometry into subsegments of 2km max

        :param list trace: list with the coordinates for the points of the
            route (longitude, latitude)
        :return: DataFrame with all origin-destination pairs of the stretches in the
            route and their respective speed limits. The coordinates of the points are
            given with WKT format (longitude-latitude)
        :rtype: Pandas.DataFrame()
        """

        # measure geom length and split the trace if it's bigger than 2km -> max is 100km
        geom_length = geometry_utils.get_length(geom)
        n_splits = int(np.ceil(geom_length / 20000)) # request every 20 km
        total_coords = len(geom.coords)
        coords_per_segment = total_coords // n_splits
        segments_coords = [geom.coords[i:i + coords_per_segment] for i in range(0, total_coords, coords_per_segment) if len(geom.coords[i:i + coords_per_segment])>1]
        segments = [LineString(coords) for coords in segments_coords]
        segments_mapmatched = []

        for segment in segments:
            mapmatched = self.map_match_segment(segment)  
            if len(mapmatched) == 0:
                continue
            segments_mapmatched.append(mapmatched)

        if len(segments_mapmatched)==0:
            logging.warning(f"Weird segment. total_coords: {total_coords}, n_splits: {n_splits}, coords_per_segment: {coords_per_segment}, length: {geom_length}")
            return pd.DataFrame()
        
        return pd.concat(segments_mapmatched)

