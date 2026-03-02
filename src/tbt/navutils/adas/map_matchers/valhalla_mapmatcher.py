import requests
import time
import json
import shapely
import shapely.geometry
import typing
import logging
import numpy as np
import pandas as pd
from .. import geometry_utils
from .mapmatcher import MapMatcher



valhalla_renaming_const = {
    "speed": "speed_limit",
    "speed_limit": "speed_no_default",
    "lane_count": "lane_counts",
    "id": "edge_id",
    "traversability": "traversability",
    "road_class": "road_class",
    "way_id": "way_id",
}


class ValhallaMapMatcher(MapMatcher):
    def __init__(self, url, valhalla_renaming=valhalla_renaming_const, test_sleep_time=60) -> None:
        self.url = url
        self.valhalla_renaming = valhalla_renaming
        self.test_sleep_time = test_sleep_time

    @staticmethod
    def decode_valhalla_polyline(encoded: str) -> typing.List[typing.List[float]]:
        """Decodes the polyline format provided by Valhalla

        :param encoded: Encoded polyline from the Valhalla API response
        :type encoded: string
        :return: List of coordinates obtained from the polyline
        :rtype: list(list)
        """
        inv = 1.0 / 1e6
        decoded = []
        previous = [0, 0]
        i = 0
        # for each byte
        while i < len(encoded):
            # for each coord (lat, lon)
            ll = [0, 0]
            for j in [0, 1]:
                shift = 0
                byte = 0x20
                # keep decoding bytes until you have this coord
                while byte >= 0x20:
                    byte = ord(encoded[i]) - 63
                    i += 1
                    ll[j] |= (byte & 0x1F) << shift
                    shift += 5
                # get the final value adding the previous offset and remember it for the next
                ll[j] = previous[j] + (~(ll[j] >> 1) if ll[j] & 1 else (ll[j] >> 1))
                previous[j] = ll[j]
                # scale by the precision and chop off long coords also flip the positions so
                # its the far more standard lon,lat instead of lat,lon
                decoded.append(
                    [float("%.6f" % (ll[1] * inv)), float("%.6f" % (ll[0] * inv))]
                )
        # hand back the list of coordinates

        # remove intermediate useless coordinates
        decoded = decoded[1::2]
        return decoded

    def map_match_request(
        self,
        geom: shapely.geometry.LineString,
        extra_attributes: typing.List = [],
        costing: str = "auto",
    ) -> pd.DataFrame:
        """Makes the call to the trace attributes to the valhalla server in the constructor.
        It gets the mapmatched trace and the edge atributes along the trace.
        Returns a pandas DataFrame with the mapmatched route and the edge atributes for each segment.

        :param geom: Geometry coming from GPS info
        :type geom: shapely.geometry.LineString
        :param extra_attributes: If extra edge information is needed that is not present in the default extraction, defaults to []
        :type extra_attributes: typing.List, optional
        :param costing: Costing algorithm from valhalla routing, defaults to 'auto'
        :type costing: str, optional
        :return: Dataframe with edge atributes along the trace
        :rtype: pd.DataFrame
        """

        shape = [{"lat": coord[1], "lon": coord[0]} for coord in geom.coords]
        filters_string = {
            "attributes": [
                "edge.names",
                "edge.id",
                "edge.speed",
                "edge.speed_limit",
                "edge.length",
                "edge.road_class",
                "edge.lane_count",
                "edge.begin_shape_index",
                "edge.end_shape_index",
                "edge.way_id",
                "edge.traversability",
                "edge.use",
                "shape",
                *extra_attributes,
            ],
            "action": "include",
        }

        query_json = {
            "shape": shape,
            "costing": costing,
            "shape_match": "map_snap", # Change it to map_snap as it uses a more robust algorithm for the map matching
            "units": "kilometers",
            "filters": filters_string,
        }
        trace_attributes_url = f"{self.url}/trace_attributes"
        r = requests.post(trace_attributes_url, data=json.dumps(query_json), timeout=10)  # Timeouted request for avoiding infinite waiting
        if r.status_code != 200:
            logging.warning(f"Something goes wrong with the Valhalla API: {r.content}")
            return pd.DataFrame()
        content = json.loads(r.content)

        if "shape" not in content:
            return pd.DataFrame()
        content["shape"] = self.decode_valhalla_polyline(content["shape"])

        edges_df = pd.json_normalize(content["edges"])
        if len(edges_df) == 0:
            return pd.DataFrame()

        if "names" in edges_df.columns:
            edges_df["names"] = edges_df["names"].astype(str)
        # Transform functions
        to_geom = (
            lambda x: shapely.geometry.LineString(
                content["shape"][x["begin_shape_index"] : x["end_shape_index"] + 1]
            ).wkt
            if x["begin_shape_index"] < x["end_shape_index"]
            else None
        )
        # Group by same characteristics
        final_edges = []
        current_aggregation = None

        for _, edges_row in edges_df.iterrows():
            if current_aggregation is None:
                current_aggregation = edges_row.copy()
                current_aggregation["id"] = [edges_row["id"]]
                current_aggregation["geom"] = to_geom(current_aggregation)
            else:
                # Check if the current row and the aggregation have the same characteristics
                if "geom" in current_aggregation.index:
                    current_index_drop = ["end_shape_index", "begin_shape_index", "length", "id", "geom"]
                else:
                    current_index_drop = ["end_shape_index", "begin_shape_index", "length", "id"]
                edges_index_drop = ["end_shape_index", "begin_shape_index", "length", "id"]
                same_characteristics = current_aggregation.drop(index=current_index_drop).equals(edges_row.drop(index=edges_index_drop))

                if same_characteristics:
                    # Update the aggregation with the length and ID
                    current_aggregation["length"] += edges_row["length"]
                    current_aggregation["id"].append(edges_row["id"])
                    current_aggregation["end_shape_index"] = edges_row["end_shape_index"]
                else:
                    # Add the current aggregation to the final list
                    current_aggregation["geom"] = to_geom(current_aggregation)
                    final_edges.append(current_aggregation)
                    # Start a new aggregation with the current row
                    current_aggregation = edges_row.copy()
                    current_aggregation["id"] = [current_aggregation["id"]]


        # Add the last aggregation to the final list
        if current_aggregation is not None:
            current_aggregation["geom"] = to_geom(current_aggregation)
            final_edges.append(current_aggregation)

        final_edges = pd.DataFrame(final_edges)
        final_edges["real_length"] = final_edges["geom"].map(lambda x: geometry_utils.get_length(geometry_utils.ensure_geom(x)))
        final_edges = final_edges[ 2 * final_edges['length']*1000 > final_edges['real_length']] # 2 times mapmatched distance must be less than the real (filter map cuts)
        # Get geometries for the edges
        final_edges.drop(columns=["end_shape_index", "begin_shape_index", "real_length"], inplace=True)
        # Drop empty geoms (points or errors)
        final_edges.dropna(subset="geom", inplace=True)
        # Rename columns
        final_edges.rename(columns=self.valhalla_renaming, inplace=True)

        # Filter all traces that goes in ferry
        if "use" in final_edges.columns:
            final_edges = final_edges[final_edges["use"] != "ferry"]

        return final_edges
    
    def connection_test(
        self,
        rows: list,
        geom_col: str = "geom",
        allowed_error_connections: int = 5
    ):
        """Test the connection to the Valhalla API by requesting a few random items. Two things can happen for each request:
        - The request is successful and a response is retrieved, therefore there is no need to retry
        - The request fails but, to make sure this is not an exceptional case, it retries with a different geometry. This process
        continues until either the maximum number of allowed errors is reached or a response is retrieved

        :param rows: List of rows to test the connection
        :type rows: list
        :param geom_col: geometry column to mapmatch, defaults to 'geom'
        :type geom_col: str, optional
        :param allowed_error_connections: number of allowed failed attempts when connecting to the API, defaults to 5
        :type allowed_error_connections: int, optional
        """

        logging.info(f"Testing connection to the Valhalla API at {self.url}")
        error_count = 0  # Counts the number of errors
        attempt_count = 1  # Counts the number of attempts
    
        while error_count < allowed_error_connections:
            try:
                # Randomly select a row to test the connection
                index = np.random.default_rng(attempt_count).integers(0, len(rows)) if rows else 0
                test_request = self.map_match_request(rows[index][geom_col])
                if isinstance(test_request, pd.DataFrame):
                    # We only consider the connection to be successful if we get a response in the form of a DataFrame
                    logging.info(f"[Attempt {attempt_count}] Connection to the Valhalla API successful... Exiting test")
                    return  # Finish the test successfully
                else:
                    logging.warning(f"[Attempt {attempt_count}] Received response is not a DataFrame... Continuing test")
            except Exception as e:
                # Some geometries are expected to fail to map match, so we don't consider them strictly as errors
                logging.warning(f"[Attempt {attempt_count}] Error testing connection to the Valhalla API: {e}")
                error_count += 1
                # Wait before trying again
                logging.warning(f"Retrying again in {self.test_sleep_time}s")
                time.sleep(self.test_sleep_time)

            attempt_count += 1

        # When the number of failed geometries is big enough, we exit the loop and consider the connection to be broken
        raise Exception("Error testing connection to the Valhalla API")


# if __name__=='__main__': # Test purposes
#     from shapely import wkt
#     line = 'LINESTRING (21.13772 55.68529, 21.13765 55.68529, 21.13763 55.68528, 21.13762 55.68528, 21.13761 55.68528, 21.13761 55.68527, 21.1376 55.68527, 21.13759 55.68526, 21.13758 55.68526, 21.13758 55.68525, 21.13757 55.68525, 21.13758 55.68525, 21.13757 55.68525, 21.13758 55.68525, 21.13759 55.68525, 21.13758 55.68525, 21.13757 55.68525, 21.13754 55.68524, 21.13757 55.68525, 21.13759 55.68525, 21.1376 55.68524, 21.13759 55.68524, 21.13759 55.68523, 21.13758 55.68522, 21.13758 55.68523, 21.13758 55.68522, 21.13757 55.68522, 21.13757 55.68521, 21.13758 55.68521, 21.13759 55.68521, 21.13759 55.68522, 21.13758 55.68522, 21.13759 55.68522, 21.13759 55.68521, 21.1376 55.68522, 21.13761 55.68517, 21.13763 55.68516, 21.13763 55.68513, 21.13766 55.68512, 21.1377 55.68508, 21.1377 55.6851, 21.13775 55.68511, 21.13777 55.68512, 21.1378 55.68512, 21.13781 55.68512, 21.13778 55.68511, 21.13774 55.6851, 21.13773 55.68509, 21.1377 55.68511, 21.13763 55.68514, 21.1376 55.68514, 21.13758 55.68515, 21.13757 55.68515, 21.13751 55.68515, 21.13743 55.68515, 21.13739 55.68515, 21.1374 55.68518, 21.13749 55.68516, 21.13754 55.68515, 21.13757 55.68515, 21.13759 55.68516, 21.13758 55.68522, 21.13755 55.68526, 21.13753 55.68527, 21.1376 55.68526, 21.13764 55.68529, 21.13767 55.68529, 21.13773 55.68526, 21.13769 55.68526, 21.13765 55.68525, 21.13763 55.68521, 21.13756 55.68515, 21.13756 55.68507, 21.13759 55.685, 21.13761 55.68495, 21.13763 55.68492, 21.13767 55.68489, 21.13776 55.68486, 21.13785 55.68483, 21.13796 55.68481, 21.13809 55.68474, 21.13819 55.6847, 21.13826 55.68464, 21.13835 55.6846, 21.13851 55.68458, 21.13864 55.68456, 21.1387 55.68451, 21.13879 55.68447, 21.13887 55.68442, 21.13891 55.68436, 21.13894 55.68431, 21.13896 55.68429, 21.13899 55.68427, 21.13901 55.68425, 21.13904 55.68422, 21.13905 55.68419, 21.13904 55.6842, 21.13911 55.68426, 21.13922 55.68431, 21.1391 55.68437, 21.13905 55.68434, 21.13913 55.68425, 21.13928 55.68414, 21.13946 55.68399, 21.13963 55.68381, 21.13977 55.6836, 21.13996 55.68343, 21.14027 55.68337, 21.1407 55.6834, 21.14108 55.68345, 21.14151 55.68349, 21.14192 55.68355, 21.14231 55.68361, 21.14241 55.68382, 21.14228 55.68411, 21.14214 55.68445, 21.142 55.68479, 21.14186 55.68513, 21.14173 55.6855, 21.14159 55.68585, 21.14145 55.68619, 21.14129 55.68656, 21.14113 55.68691, 21.14069 55.68704, 21.14029 55.68714, 21.14018 55.68735, 21.14041 55.68752, 21.14094 55.68763, 21.14163 55.6878, 21.14237 55.68799, 21.14304 55.688, 21.14354 55.6878, 21.14379 55.68756, 21.14395 55.68741, 21.1441 55.68723, 21.14403 55.68704, 21.14362 55.68695, 21.1433 55.68686, 21.14309 55.68677, 21.14306 55.68671, 21.14306 55.68668, 21.1432 55.68649, 21.1435 55.68623, 21.14382 55.68594, 21.14411 55.68565, 21.14435 55.68542, 21.14452 55.68525, 21.14461 55.68515, 21.14471 55.68505, 21.14479 55.68497, 21.14484 55.68495, 21.14498 55.68483, 21.14517 55.68464, 21.14542 55.68444, 21.14551 55.68414, 21.14557 55.68379, 21.14564 55.6835, 21.14584 55.68331, 21.14635 55.68332, 21.14699 55.68335, 21.14761 55.68339)'
#     line = wkt.loads(line)
#     # url="http://tt-valhalla-api.westeurope.azurecontainer.io:8002"
#     url = "http://10.137.172.105:8014"
#     map_match_engine = ValhallaMapMatcher(url)

#     response = map_match_engine.map_match_request(line)
#     # print(routing.trace_attributes_req(line))

#     geodf = pd.DataFrame(data={'geom':[line]})
#     result = map_match_engine.map_match_df(geodf)
#     print(result)


#     # features = pd.read_csv("test_feat_valh.csv")
#     # features['geom'] = features['geom'].map(wkt.loads)
#     # routing.trace_attributes_req(features.iloc[0]['geom'])
#     # routing.process_trace(features.iloc[0])


#     # routing.trace_attributes_df(features)
