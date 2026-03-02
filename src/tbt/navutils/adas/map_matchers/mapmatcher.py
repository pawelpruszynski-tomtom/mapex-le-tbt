import shapely
from shapely.geometry import LineString
from shapely import wkt
import typing
import logging
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import geometry_utils
from ...common.decorators import repeat_if_fail


# If a mapmatched trace is more than MULTIPLIER_LENGTH_TO_DISCARD the original GPS length, discard because prossible error mapmatching
MULTIPLIER_LENGTH_TO_DISCARD = 3

class MapMatcher:  
    def map_match_request(self,
        geom: LineString
    ) -> pd.DataFrame:
        """
        Map matched geometry of a route given a linestring 

        :param list trace: list with the coordinates for the points of the
            route (longitude, latitude)
        :return: DataFrame with all origin-destination pairs of the stretches in the
            route and their respective speed limits. The coordinates of the points are
            given with WKT format (longitude-latitude)
        :rtype: Pandas.DataFrame()
        """
        raise NotImplementedError("This method should be implemented in a child class")

    @repeat_if_fail(num_retries=10, time_between_retries=120, default_return=pd.DataFrame())
    def map_match_trace(
        self, trace: pd.Series, geom_col: str = "geom", **kwargs
    ) -> pd.DataFrame:
        """Process a individual trace comming from a batch process given a pandas DataFrame.
        This frame is an individual row for the whole DataFrame

        :param trace: Trace to get the attributes
        :type trace: pd.Series
        :param geom_col: Column name for the geometry, defaults to 'geom'
        :type geom_col: str, optional
        :return: DataFrame for the given trace with rows based on its different attributes.
        :rtype: pd.DataFrame
        """

        mapmatched_trace = self.map_match_request(
            trace[geom_col], **kwargs
        )
        if len(mapmatched_trace) == 0:
            return pd.DataFrame()
        
        # Recover all trace columns that are not the geom one
        columns = [col for col in trace.index if col != geom_col]
        for col in columns:
            # if the column already exists put prefix
            if col in mapmatched_trace.columns:
                mapmatched_trace[f"original_{col}"] = trace[col]
            else:
                mapmatched_trace[col] = trace[col]

        # If the mapmatched length is more than MULTIPLIER_LENGTH_TO_DISCARD of the original, discard because prossible error mapmatching
        if mapmatched_trace['geom'].map(geometry_utils.get_length).sum() > geometry_utils.get_length(trace[geom_col]) * MULTIPLIER_LENGTH_TO_DISCARD:
            logging.warning(
                f"Mapmatched length is more than triple of the original, discard because prossible error mapmatching: {trace}"
            )
            return pd.DataFrame()
        return mapmatched_trace
    

    def map_match_df(
        self,
        trace_df: pd.DataFrame,
        geom_col: str = "geom",
        max_workers: typing.Union[int, None] = None,
        **kwargs,
    ):
        """Process a dataframe to get the mapmatched and trace attributes for all the rows in the dataframe using multithread and snap2roads

        :param trace_df: Dataframe to mapmatch
        :type trace_df: pd.DataFrame
        :param geom_col: geometry column to mapmatch, defaults to 'geom'
        :type geom_col: str, optional
        :param max_workers: number of workers for the pool of execution if not set it uses all, defaults to None
        :type max_workers: typing.Union[int,None], optional
        :return: Returns the mapmatched dataframe with the information along the trace for genesis attributes
        :rtype: _type_
        """
        executor = ThreadPoolExecutor(max_workers=max_workers)
        rows = []
        for _, row in tqdm(trace_df.iterrows(), total=len(trace_df)):
            if type(row[geom_col]) == str:
                row[geom_col] = wkt.loads(row[geom_col])
            if len(row[geom_col].coords) > 2 and isinstance(
                row[geom_col], shapely.geometry.LineString
            ):  # or row['length']>10
                rows.append(row)

        logging.info(f"Map matching {len(rows)} traces with correct geometries")

        # Test connection to the API by requesting a few random items
        self.connection_test(rows)
        
        futures = [
            executor.submit(self.map_match_trace, row, geom_col, **kwargs) for row in rows
        ]
        features = []
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Collecting results"
        ):
            result = future.result()
            features.append(result)

        features = pd.concat(features)

        return features


    def connection_test(
        self,
        rows: list,
        geom_col: str = "geom",
        allowed_error_connections: int = 5,
        sleep_time: int = 60
    ):
        """Test the connection to the API by requesting a few random items.

        :param rows: List of rows to test the connection
        :type rows: list
        :param geom_col: geometry column to mapmatch, defaults to 'geom'
        :type geom_col: str, optional
        :param allowed_error_connections: number of allowed failed attempts when connecting to the API, defaults to 5
        :type allowed_error_connections: int, optional
        :param sleep_time: time to sleep between attempts, defaults to 60
        :type sleep_time: int, optional
        """

        logging.info(f"There is no need to test the connection with the API")
        return