# import general libraries
import pandas as pd
import numpy as np
import json
import shapely.wkt
import shapely.geometry
import pyproj
import logging

# import FCD and SDO API call functions
from .call_FCD_API import FCDFeaturizer
from .call_SDO_API import wrapper_sdo
from tbt.utils.console_print import conditional_print_warning

log = logging.getLogger(__name__)

class Featurizer:
    """Class performing feature engineering 

    :param pdf: dataframe with input data
    :type pdf: pd.DataFrame with columns "run_id", "route_id", "case_id", "country", "provider", "route", "stretch"
    :param fcd_credentials: dictionary with credentials for FCD
    :type fcd_credentials: dict
    :param sample_metric: sample metric to be used for prediction, must be either "TbT" or "HDR"
    :type sample_metric: str
    """

    def __init__(self, pdf: pd.DataFrame, fcd_credentials: dict, sample_metric: str, spark):
        self.pdf = pdf
        self.fcd_credentials = fcd_credentials
        self.sample_metric = sample_metric
        self.spark = spark


    def calculate_general_features(self, row):
        """Calculate general features for one row of input data, used inside apply function

        :param row: row of input data, must contain columns "country"
        :type row: pd.Series
        :return: pd.Series with general features
        :rtype: pd.Series
        """

        # Define the countries where driving is on the left side
        left_side_countries = [
            "ATG", "AUS", "BHS", "BGD", "BRB", "BTN", "BWA", "BRU", "CHN", "CYP", 
            "DMA", "TMP", "SWZ", "FJI", "GRD", "GUY", "IND", "IDN", "IRL", "JAM", 
            "JPN", "KEN", "KIR", "LSO", "MWI", "MDV", "MLT", "MUS", "MOZ", "NAM", 
            "NRU", "NZL", "PAK", "PNG", "KNA", "LCA", "VCT", "WSM", "SYC", "SGP", 
            "SLB", "LKA", "SUR", "TZA", "THA", "TON", "TTO", "TUV", "UGA", "GBR", 
            "VGB", "CYM", "FLK", "GGY", "IMN", "JEY", "ZMB", "ZWE"
        ]

        # Check if the country in the row drives on the left side
        drive_left_side = 1.0 if row['country'] in left_side_countries else 0.0
        drive_right_side = float(abs(drive_left_side - 1))

        # Check the metric type for the row
        is_tbt = 1 if self.sample_metric == "TbT" else 0
        is_hdr = 1 if self.sample_metric == "HDR" else 0

        # Return the general features as a Series
        general_features = pd.Series({
            "is_tbt": is_tbt,
            "is_hdr": is_hdr,
            "drive_left_side": drive_left_side,
            "drive_right_side": drive_right_side
        })

        return general_features
        
        
    def calculate_geometric_features(self, row):
        """Calculate geometric features based on stretch and route in input data, used inside apply function
        
        :param row: row of input data, must contain columns "route" and "stretch"
        :type row: pd.Series
        :return: pd.Series with geometric features
        :rtype: pd.Series
        """

        # Functions for general features
        def unit_vector(vector: np.array) -> np.array:
            """Returns the unit vector of the vector."""
            norm = np.linalg.norm(vector)
            if norm > 1e-8:
                return vector / np.linalg.norm(vector)
            else:
                return vector

        def create_vector(start, end) -> np.array:
            return np.array(end) - np.array(start)

        def angle_between(v1: np.array, v2: np.array) -> float:
            v1_u = unit_vector(v1)
            v2_u = unit_vector(v2)

            angle = np.arccos(np.clip(np.dot(v1_u, v2_u), -1.0, 1.0)) * 180 / np.pi
            sign = np.sign(np.dot(v1_u[::-1], v2_u))

            return sign * angle

        def curvature(coords):
            # Get point coords
            x = np.array([coord[0] for coord in coords])
            y = np.array([coord[1] for coord in coords])

            # Get coords derivates
            dx_dt = np.gradient(x)
            dy_dt = np.gradient(y)
            ds_dt = np.sqrt(dx_dt * dx_dt + dy_dt * dy_dt)
            d2s_dt2 = np.gradient(ds_dt)
            d2x_dt2 = np.gradient(dx_dt)
            d2y_dt2 = np.gradient(dy_dt)

            curvature = np.abs(d2x_dt2 * dy_dt - dx_dt * d2y_dt2) / (dx_dt * dx_dt + dy_dt * dy_dt)**1.5

            return np.nan_to_num(curvature)

        def distance(start: np.array, end: np.array):
            geodesic = pyproj.Geod(ellps="WGS84")
            s_lon, s_lat = start
            e_lon, e_lat = end

            return geodesic.inv(s_lon, s_lat, e_lon, e_lat)[2]

        def all_angles(geometry: shapely.geometry.LineString):
            coords = np.array(geometry.coords)
            return np.array(
                [
                    angle_between(create_vector(start_1, end_1), create_vector(start_2, end_2))
                    for start_1, end_1, start_2, end_2 in zip(
                        coords, coords[1:], coords[1:], coords[2:]
                    )
                ]
            )

        def get_length(geom: shapely.geometry.LineString) -> float:
            coords = geom.coords[:]
            length = sum(distance(start, end) for start, end in zip(coords[:-1], coords[1:]))
            return length

        def convert_to_linestring(wkt_str: str) -> shapely.geometry.LineString:
            return shapely.wkt.loads(wkt_str)

        def stretch_start_position_in_route(
            route: shapely.geometry.LineString, stretch: shapely.geometry.LineString
        ) -> float:
            proj = route.project(stretch.interpolate(0, normalized=True), normalized=True)
            if not np.isnan(proj):
                return proj
            else:
                return 0

        def stretch_end_position_in_route(
            route: shapely.geometry.LineString, stretch: shapely.geometry.LineString
        ) -> float:
            proj = route.project(stretch.interpolate(1, normalized=True), normalized=True)
            if not np.isnan(proj):
                return proj
            else:
                return 0

        def heading(geometry: shapely.geometry.LineString) -> float:
            """Angle difference between start heading and end"""
            start_vector = unit_vector(
                np.array(geometry.coords[1]) - np.array(geometry.coords[0])
            )

            end_vector = unit_vector(
                np.array(geometry.coords[-1]) - np.array(geometry.coords[-2])
            )

            return angle_between(start_vector, end_vector)

        def turn_stats(all_angles):
            right_turns = all_angles > 45
            left_turns = all_angles < -45
            straight_turns = ~right_turns & ~left_turns

            right_angles = np.abs(all_angles[right_turns])
            left_angles = np.abs(all_angles[left_turns])

            return (
                np.sum(left_turns),     
                np.sum(straight_turns),
                np.sum(right_turns),
                np.sum(right_angles),
                np.max(right_angles, initial=0),
                np.sum(left_angles),
                np.max(left_angles, initial=0),
                np.sum(all_angles)
            )

        def curvature_values(geometry: shapely.geometry.LineString) -> float:
            coords = np.array(geometry.coords)
            curvature_vector = curvature(coords)

            return np.min(curvature_vector), np.max(curvature_vector), np.mean(curvature_vector)
        
        def distance_start_end_stretch(geometry: shapely.geometry.LineString) -> float:
            coords = np.array(geometry.coords)

            return distance(coords[0], coords[-1])

        def tortuosity(geometry: shapely.geometry.LineString, all_angles):
            num_segments = len(geometry.coords) - 1
            return np.sum(np.abs(all_angles)) / (num_segments * 180)

        def sinuoisty(geometry: shapely.geometry.LineString):
            stretch_length = get_length(geometry)
            return (
                get_length(
                    shapely.geometry.LineString([geometry.coords[0], geometry.coords[-1]])
                )
                / stretch_length
                if stretch_length
                else 0
            )

        def density(geometry: shapely.geometry.LineString):
            stretch_length = get_length(geometry)
            return len(geometry.coords) / stretch_length if stretch_length else 0
        
        # Calculate geometric features
        route = row['route']
        stretch = row['stretch']
        
        route_geometry = convert_to_linestring(route)
        stretch_geometry = convert_to_linestring(stretch)

        route_length = get_length(route_geometry)
        stretch_length = get_length(stretch_geometry)

        stretch_start_position_in_route_value = float(
            stretch_start_position_in_route(route_geometry, stretch_geometry)
        )
        stretch_end_position_in_route_value = float(
            stretch_end_position_in_route(route_geometry, stretch_geometry)
        )

        stretch_starts_at_the_route_start = float(
            stretch_start_position_in_route_value < 1e-8
        )
        stretch_ends_at_the_route_end = float(
            stretch_end_position_in_route_value > 0.9999999
        )

        heading_stretch = float(heading(stretch_geometry))

        angles_stretch = all_angles(stretch_geometry)
        turn_stats_values = turn_stats(angles_stretch)
        left_turns, straight_turns, right_turns = map(int, turn_stats_values[:3])
        (
            total_right_angle,
            max_right_angle,
            total_left_angle,
            max_left_angle,
            absolute_angle,
        ) = map(float, turn_stats_values[3:])

        has_left_turns = float(left_turns > 0)
        has_right_turns = float(right_turns > 0)

        min_curvature, max_curvature, mean_curvature = curvature_values(stretch_geometry)
        min_curvature = float(min_curvature)
        max_curvature = float(max_curvature)
        mean_curvature = float(mean_curvature)

        distance_start_end_stretch_value = float(
            distance_start_end_stretch(stretch_geometry)
        )

        coverage = stretch_length / route_length if route_length else 0
        route_coverage = float(coverage)

        stretch_covers_route = float(route_coverage == 1)

        tortuosity_value = float(tortuosity(stretch_geometry, angles_stretch))

        sinuoisty_value = float(sinuoisty(stretch_geometry))

        density_value = float(density(stretch_geometry))

        geometric_features = pd.Series({
            "right_turns": right_turns,
            "route_length": route_length,
            "stretch_length": stretch_length,
            "stretch_start_position_in_route": stretch_start_position_in_route_value,
            "stretch_end_position_in_route": stretch_end_position_in_route_value,
            "stretch_starts_at_the_route_start": stretch_starts_at_the_route_start,
            "stretch_ends_at_the_route_end": stretch_ends_at_the_route_end,
            "heading_stretch": heading_stretch,
            "left_turns": left_turns,
            "straight_turns": straight_turns,
            "total_right_angle": total_right_angle,
            "max_right_angle": max_right_angle,
            "total_left_angle": total_left_angle,
            "max_left_angle": max_left_angle,
            "absolute_angle": absolute_angle,
            "has_left_turns": has_left_turns,
            "has_right_turns": has_right_turns,
            "min_curvature": min_curvature,
            "max_curvature": max_curvature,
            "mean_curvature": mean_curvature,
            "distance_start_end_stretch": distance_start_end_stretch_value,
            "route_coverage": route_coverage,
            "stretch_covers_route": stretch_covers_route,
            "tortuosity": tortuosity_value,
            "sinuosity": sinuoisty_value,
            "density": density_value,
            })
        
        return geometric_features       

    def calculate_sdo_features(self, row, min_ocurrences=2):
        """Calculate SDO features for one row of input data, used inside apply function

        :param row: row of input data, must contain columns "route" and "stretch"
        :type row: pd.Series
        :param min_ocurrences: minimum number of ocurrences to consider a traffic sign, defaults to 2
        :type min_ocurrences: int, optional
        :return: pd.Series with SDO features
        :rtype: pd.Series
        """

        relevant_traffic_signs = [
                    "CONSTRUCTION_AHEAD_TT",
                    "MANDATORY_STRAIGHT_ONLY",
                    "MANDATORY_STRAIGHT_OR_LEFT",
                    "MANDATORY_TURN_RESTRICTION",
                    "MANDATORY_TURN_LEFT_ONLY",
                    "MANDATORY_TURN_LEFT_OR_RIGHT",
                    "MANDATORY_LEFT_OR_STRAIGHT_OR_RIGHT",
                    "MANDATORY_TURN_RIGHT_ONLY",
                    "MANDATORY_STRAIGHT_OR_RIGHT",
                    "NO_ENTRY",
                    "NO_MOTOR_VEHICLE",
                    "NO_CAR_OR_BIKE",
                    "NO_LEFT_OR_RIGHT_TT",
                    "NO_LEFT_TURN",
                    "NO_RIGHT_TURN",
                    "NO_VEHICLE",
                    "NO_STRAIGHT_OR_LEFT_TT",
                    "NO_STRAIGHT_OR_RIGHT_TT",
                    "NO_STRAIGHT_TT",
                    "NO_TURN_TT",
                    "NO_U_OR_LEFT_TURN",
                    "NO_U_TURN",
                    "ONEWAY_TRAFFIC_TO_STRAIGHT"
        ]

        # try loading the json response
        try:
            cache = json.loads(row['sdo_api_response'])
        except:
            log.warning("SDO: Exception when loading json response, SDO features set to 0 for this critical section")
            conditional_print_warning("SDO: Exception when loading json response, SDO features set to 0 for this critical section")
            cache = None

        # to keep counts by sign
        traffic_signs_data = dict(zip(relevant_traffic_signs, [0,]*len(relevant_traffic_signs)))
        
        if cache is None:
            # In case SDO response is null
            return pd.Series(traffic_signs_data)
        
        if len(cache['features']) == 0:
            return pd.Series(traffic_signs_data)
        
        for feature in cache['features']:
            traffic_sign = feature["properties"]["sign"]
            if traffic_sign in relevant_traffic_signs:
                ocurrences = feature["properties"]["CLUSTER_SIZE"] if "CLUSTER_SIZE" in feature["properties"].keys() else 1
                if ocurrences > min_ocurrences:
                    traffic_signs_data[traffic_sign] = 1

        return pd.Series(traffic_signs_data)


    def combine_features(self, general_features, geometric_features, fcd_features_old, fcd_features, sdo_features):
        """Combine all feature dataframes
        
        :param general_features: dataframe with general features
        :type general_features: pandas.DataFrame
        :param geometric_features: dataframe with geometric features
        :type geometric_features: pandas.DataFrame
        :param fcd_features: dataframe with fcd features
        :type fcd_features: pandas.DataFrame
        :param sdo_features: dataframe with sdo features
        :type sdo_features: pandas.DataFrame
        :return: dataframe with all features
        :rtype: pandas.DataFrame
        """

        # combine all the features
        combined_data = pd.concat([general_features, geometric_features, fcd_features_old, fcd_features, sdo_features], axis=1)
        
        return combined_data
    

    def featurize(self):
        """wrapper function for all the featurization steps.

        :return: featurized dataframe
        :rtype: pd.DataFrame
        """

        # calculate general features
        general_features = self.pdf.apply(self.calculate_general_features, axis=1)

        # calculate geometric features
        geometric_features = self.pdf.apply(self.calculate_geometric_features, axis=1)

        # calculate FCD features by batch
        fcd_featurizer = FCDFeaturizer(
            self.pdf,
            trace_retrieval_geometry="bbox_json",
            traces_limit=2000,
            fcd_credentials=self.fcd_credentials,
            spark_context=self.spark
        )
        
        fcd_features = fcd_featurizer.featurize()

        # Define the number of rows (should match the number of rows in your combined DataFrame)

        # add dataframe with old fcd features to be logged [pra, prb, prab, tot, lift] with all values set to -2.0
        num_rows = self.pdf.shape[0]
        fcd_features_old = pd.DataFrame(
            {
                "pra": [-2.0] * num_rows,
                "prb": [-2.0] * num_rows,
                "prab": [-2.0] * num_rows,
                "lift": [-2.0] * num_rows,
                "tot": [-2] * num_rows,
            }
        )

        # retrieve SDO responses
        sdo_pdf = self.pdf.apply(wrapper_sdo, axis=1)

        # parse SDO responses and calculate features
        sdo_features = sdo_pdf.apply(self.calculate_sdo_features, axis=1)

        # add column with raw sdo_response
        sdo_features["sdo_api_response"] = sdo_pdf["sdo_api_response"]

        # Combine data
        combined_feature_data = self.combine_features(general_features, geometric_features, fcd_features_old, fcd_features, sdo_features)

        return combined_feature_data

