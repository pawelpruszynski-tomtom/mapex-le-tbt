import requests
import json
import shapely
import shapely.geometry
import typing
import logging
import pandas as pd
from collections import Counter
from datetime import datetime
from ...common.decorators import repeat_if_fail
from .mapmatcher import MapMatcher



default_attributes = 'SPEED_LIMITS_FCn(*),\
    LINK_ATTRIBUTE_FCn(*),\
    TRAFFIC_SIGN_FCn(*),\
    LANE_FCn(*),\
    ADAS_ATTRIB_FCn(*),\
    LANE_CONN_FCn(*)'

class HereMapMatcher(MapMatcher):
    def __init__(self, api_key, mapmatching_url = 'https://routematching.hereapi.com/v8/match/routelinks',
                 attributes=default_attributes) -> None:
        self.api_key = api_key
        self.mapmatching_url = mapmatching_url
        self.attributes = attributes
        self.number_requests = 0

    def map_match_request(
        self,
        geom: shapely.geometry.LineString
    ) -> typing.Tuple[pd.DataFrame, dict]:
        """Makes the call to HERE map matching server.
        It gets the mapmatched trace and the edge atributes along the trace.
        Returns a pandas DataFrame with the mapmatched route and the edge atributes for each segment.

        :param geom: Geometry coming from GPS info
        :type geom: shapely.geometry.LineString
        :return: Dataframe with edge atributes along the trace
        :rtype: pd.DataFrame
        :return: Dict containing the json raw response
        :rtype: Dict
        """
        # today = datetime.today().strftime('%Y-%m-%d')

        # querystring={
        # 'apiKey': self.api_key
        # ,'mode':'car'
        # ,'routeAttributes':'-mo,sh,sp'
        # ,'legAttributes':'sh'
        # ,'routeMatch':'1'
        # ,"alternatives":"0"
        # ,"departure":f"{today}T23:00:00"
        # ,'attributes': self.attributes
        # }

        # target_coords = 450 # Tested Limit 500
        # simplify_factor = 1e-7
        # while len(geom.coords[:]) > target_coords:
        #     geom = geom.simplify(simplify_factor)
        #     simplify_factor *= 2


        # coords = geom.coords[:]
        # for indx,coord in enumerate(coords):
        #     querystring[f'waypoint{indx}'] = f"{coord[1]},{coord[0]}"

        # self.number_requests += 1
        # r = requests.get(self.mapmatching_url, params=querystring)
        # if r.status_code != 200:
        #     logging.warning(f"Something goes wrong with the HERE api: {r.content}")
        #     return pd.DataFrame()

        # content = json.loads(r.content)

        # # Extract the HERE response
        # if "response" not in content:
        #     logging.warning(f"There is no response in HERE request: {content}")
        #     return pd.DataFrame()
        # response = content['response']

        # # Extract the HERE route 0 (main route not alternatives)
        # if "route" not in response:
        #     logging.warning(f"There is no route in HERE request: {response}")
        #     return pd.DataFrame()
        # route = pd.json_normalize(response['route'][0])

        # # Extract links
        # links = pd.json_normalize(route['leg'].iloc[0])['link'].tolist()

        # ## Uncompress the links list. link[0] if we need main route and not all alternative paths
        # uncompressed_links = [link[0] if isinstance(link, list) else link for link in links[0]]
        # link_data = pd.json_normalize(uncompressed_links)
        # # Uncompress attributes
        # attrs_cols = link_data.filter(regex="attributes.+").columns
        # for attr_col in attrs_cols:
        #     link_data_df = link_data[attr_col].map(self.fillna_with_list)
        #     all_attributes = []
        #     for _, row in link_data_df.items():
        #         if len(row)==0:
        #             all_attributes.append({})
        #             continue
        #         processed_row = pd.json_normalize(row)
        #         transformed = {}
        #         for col in processed_row.columns:
        #                 if len(processed_row[col]) == 1:
        #                     transformed[col] = processed_row[col].values[0]
        #                 else:
        #                     transformed[col] = processed_row[col].tolist()
        #         all_attributes.append(transformed)
        #     link_data_df = pd.json_normalize(all_attributes)
        #     # Add prefix (layer_attribute) example: ADAS_ATTRIB_FCN_
        #     uncompressed_feature = link_data_df.add_prefix(f"{attr_col.replace('attributes.', '')}_")
        #     link_data = pd.concat([link_data, uncompressed_feature], axis=1)
        # link_data.drop(columns=attrs_cols, inplace=True)

        # # Convert shape into geom in linestring format
        # link_data['geom'] = link_data['shape'].map(self.create_line_string)

        # # Parse direction
        # link_data['ref_direction'] = link_data['linkId'].map(self.parse_direction)

        # # Filter out ferry and boat
        # if "LINK_ATTRIBUTE_FCN_BOAT_FERRY" in link_data.columns:
        #     link_data = link_data[link_data['LINK_ATTRIBUTE_FCN_BOAT_FERRY']!='Y']
        # if "LINK_ATTRIBUTE_FCN_RAIL_FERRY" in link_data.columns:
        #     link_data = link_data[link_data['LINK_ATTRIBUTE_FCN_RAIL_FERRY']!='Y']

        # # Extracting lane counts in sample direction
        # try:
        #     # Parse directed lane counts
        #     link_data['here_lane_count_sample_direction'] = link_data.apply(self.get_directed_lanes, axis=1)
        # except Exception as ex:
        #     logging.warning(f"Error extracting directed lanes, potential lack of layers {ex}")

        return pd.DataFrame()

    @staticmethod
    def fillna_with_list(value):
        if isinstance(value, list):  # Check if value is a list
            return [x if pd.notna(x) else False for x in value]  # Replace NaN with False in the list
        elif pd.isna(value):  # Check if value is NaN
            return []  # Return an empty list
        else:
            return value  # Return the original value

    @staticmethod
    def create_line_string(coords):
        points = [(coords[i+1], coords[i]) for i in range(0, len(coords), 2)]
        return shapely.geometry.LineString(points)

    @staticmethod
    def parse_direction(link_id):
        # leg>linkId: Permanent version id (HERE core map), negative sign means driven towards reference node = TO_REF
        if float(link_id)>0:
            return "FROM_REF"
        else:
            return "TO_REF"

    @staticmethod
    def get_directed_lanes(link:pd.Series)->int:
        """Process here lane info in order to return lane specific information in the driving direction.
        :param link: link to process containing all attributes requested
        :type link: pd.Series
        :return: Number of lanes in the driving direction
        :rtype: int
        """

        # Get lane travel direction and type
        direction = link['ref_direction']
        lane_travel_direction = link['LANE_FCN_LANE_TRAVEL_DIRECTION']
        lane_travel_type = link['LANE_FCN_LANE_TYPE']

        # Cast int to and from lanes:
        try:
            from_ref_lanes = int(link['LINK_ATTRIBUTE_FCN_FROM_REF_NUM_LANES'])
        except:
            from_ref_lanes = 0
        try:
            to_ref_lanes = int(link['LINK_ATTRIBUTE_FCN_TO_REF_NUM_LANES'])
        except:
            to_ref_lanes = 0
        try:
            physical_lanes = int(link['LINK_ATTRIBUTE_FCN_PHYSICAL_NUM_LANES'])
        except:
            physical_lanes = 0

        # If we dont know phisical or from+to=total physical take the correct
        if physical_lanes==0 or physical_lanes==(from_ref_lanes+to_ref_lanes):
            if direction == 'FROM_REF':
                return from_ref_lanes
            elif  direction == 'TO_REF':
                return to_ref_lanes
            else:
                return 0

        # We have physical but the sum is correct:
        if lane_travel_direction is not None:
            # Preprocess list
            if isinstance(lane_travel_direction, list):
                count_dict = dict(Counter(lane_travel_direction))
                if direction == 'FROM_REF':
                    return count_dict.get('F',0)+count_dict.get('B',0)
                elif direction == 'TO_REF':
                    return count_dict.get('T',0)+count_dict.get('B',0)
            else:
                if direction == 'FROM_REF':
                    return from_ref_lanes
                elif direction == 'TO_REF':
                    return to_ref_lanes


        return -1
