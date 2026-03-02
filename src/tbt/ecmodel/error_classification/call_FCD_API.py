# %pip install --extra-index-url https://svc-ar-maps-analytics-editor:<secret>@artifactory.tomtomgroup.com/artifactory/api/pypi/maps-fcd-pypi-release/simple fcd_py==5.0.615
import fcd_py

import pandas as pd
import geopandas as gpd
from pyspark import SparkContext
import pyspark

import shapely
import shapely.wkt
from shapely.geometry import MultiPolygon

from datetime import datetime, timedelta
import json


# The FCDFeaturizer class below relies upon this function, but it needs to be a static function and not a method of the class because of spark. 
def readOutput(cache):
    """Process FCD traces (raw) into WKT

    :param cache: FCD traces. Rows if RDD in format ({}, 'json')
    :type cache: list
    :return: traces
    :rtype: List[Dict]
    """
    
    traces = []
    org = cache["meta"]["ORGANISATION"]

    if cache["reports"] is None:
        return traces

    if len(cache["reports"]) > 1:
        reports = cache["reports"]
        thisTrace = [shapely.geometry.Point([reports[0]["lon"], reports[0]["lat"]])]
        for j in range(1, len(reports)):
            thisTrace.append(
                    shapely.geometry.Point([reports[j]["lon"], reports[j]["lat"]])
            )
        traces.append({"org": org, "trace": shapely.geometry.LineString(thisTrace).wkt})
    return traces


class FCDFeaturizer:
    """
    Class to get FCD features.
    """

    def __init__(
        self, 
        pdf: pd.DataFrame, 
        fcd_credentials: dict,
        traces_limit = 2000,
        trace_retrieval_geometry = "buffer_json", # bbox_json or buffer_json
        spark_context = None,
        use_date_column = False
        ):
        """
        :param pdf: pandas dataframe, needs to have the columns: 
            + "stretch": str, containing WKT Linestring
            + "date": str, optional, specifying error date. default is today - 30 days 
        :type pdf: pd.DataFrame
        :param fcd_credentials: credentials for FCD API
        :type fcd_credentials: dict
        :param date_start: start date for trace retrieval, defaults to today - 60 days
        :type date_start: str, optional
        :param date_end: end date for trace retrieval, defaults to today - 30 days
        :type date_end: str, optional
        :param traces_limit: maximum number of traces to retrieve, defaults to 1000
        :type traces_limit: int, optional
        :param trace_retrieval_geometry: 
            + column to use for trace retrieval, 
            + options are "bbox_json", "buffer_json", defaults to "bbox_json"
        :type trace_retrieval_geometry: str, optional
        """
        # convert to GeoDataFrame
        gdf = pdf.copy()

        # Check if 'date' column exists
        if 'date' not in gdf or not use_date_column:
            default_date = datetime.strftime(datetime.now()-timedelta(30), "%Y-%m-%d")
            print(f"Using default date of {default_date} for all stretches")
            gdf['date'] = default_date
        else:
            gdf['date'] = gdf['date'].astype(str)
            # Validate the date format for each date in the 'date' column and check if any date is in the future
            for date_str in gdf['date']:
                try:
                    date_object = datetime.strptime(date_str, "%Y-%m-%d")
                    if date_object > datetime.now():
                        raise ValueError(f"Date {date_str} is in the future. Dates should not be later than today's date.")
                except ValueError:
                    raise ValueError(f"Date {date_str} is not formatted correctly or is in the future. It should be in 'YYYY-MM-DD' format and not later than today's date.")
            print(f"Using date column provided in the dataframe")


        # Create a second column with dates 30 days before the 'date' column values
        gdf['date_start'] = pd.to_datetime(gdf['date']).apply(lambda x: datetime.strftime(x - timedelta(14), "%Y-%m-%d"))

        # rename date to date_end 
        gdf.rename({"date": "date_end"}, axis='columns', inplace=True)

        # # sort by stretch so to use the cache of FCD
        # gdf.sort_values(by="stretch", axis=0, inplace=True, ignore_index=True)

        # convert to GeoDataFrame
        gdf["stretch"] = gpd.GeoSeries.from_wkt(gdf["stretch"])
        self.gdf = gpd.GeoDataFrame(gdf, geometry="stretch", crs=4326)

        # Define remaining attributes 
        self.fcd_credentials = fcd_credentials
        self.traces_limit=traces_limit
        self.trace_retrieval_geometry = trace_retrieval_geometry
        self.sc = spark_context or SparkContext.getOrCreate()

    
    def add_geometric_attributes(
        self,
        buffer_stretch=0.0001,
        simplify_buffer_stretch=0.000001, 
        buffer_point=0.0001, 
        simplify_buffer_point=0.000001,
        ):
        """
        Add geometric attributes to the gdf that are needed for trace retrieval
        This includes buffers for stretch, start and endpoint
        Also adds GeoJSON for the trace retrieval geometry

        :param buffer_stretch: buffer for stretch, defaults to 0.0001
        :type buffer_stretch: float, optional
        :param simplify_buffer_stretch: simplification for stretch buffer, defaults to 0.000001
        :type simplify_buffer_stretch: float, optional
        :param buffer_point: buffer for start and endpoint, defaults to 0.0001
        :type buffer_point: float, optional
        :param simplify_buffer_point: simplification for start and endpoint buffer, defaults to 0.000001
        :type simplify_buffer_point: float, optional
        """
        gdf = self.gdf

        # helper function to determine if start and endpoint intersect
        def start_and_endpoint_intersect(start_point, end_point, distance):
            return start_point.distance(end_point) < distance

        # buffer for stretch
        gdf["buffer_stretch"] = gdf["stretch"].buffer(buffer_stretch).simplify(simplify_buffer_stretch)

        # start and endpoint of stretch
        gdf["start_point"] = gdf["stretch"].apply(lambda geom: shapely.geometry.Point(geom.coords[0]))
        gdf["end_point"] = gdf["stretch"].apply(lambda geom: shapely.geometry.Point(geom.coords[-1]))

        # start and endpoint buffers for trace selection (i.e. check if trace goes through start/endpoint)
        gdf["buffer_start_point_trace_selection"] = gdf["start_point"].buffer(buffer_point).simplify(simplify_buffer_point)
        gdf["buffer_end_point_trace_selection"] = gdf["end_point"].buffer(buffer_point).simplify(simplify_buffer_point)

        # extract start and endpoint buffers for checking if trace start/endpoint starts/ends at stretch start/endpoint
        # it needs to be slighly larger than trace selection buffer, 
        # so that points at the edge of trace selection buffer fall into this checking buffer
        gdf["buffer_start_point_trace_check"] = gdf["start_point"].buffer(1.1 * buffer_point).simplify(simplify_buffer_point)
        gdf["buffer_end_point_trace_check"] = gdf["end_point"].buffer(1.1 * buffer_point).simplify(simplify_buffer_point)

        # Check if the start and endpoint of the critical section are very close to each other
        # If they are, more often than not, it's a U-turn, so we want to have that as a feature as well. 
        gdf["start_and_endpoint_intersect"] = gdf.apply(lambda x: start_and_endpoint_intersect(x["start_point"], x["end_point"], buffer_point * 2.05), axis=1)

        # Add column with GeoJSON for the area to be supplied to FCD module
        gdf["buffer_json"] = gdf["buffer_stretch"].buffer(simplify_buffer_stretch).simplify(5 * simplify_buffer_stretch).apply(lambda x: gpd.GeoSeries([x]).to_json())

        # Add bounding box that can optionally be supplied to FCD module instead of GeoJSON
        gdf["bbox_json"] = gdf["buffer_stretch"].apply(lambda x: gpd.GeoSeries([shapely.geometry.box(*(x.bounds))]).to_json())

        self.gdf = gdf


    def query_traces(
        self,
        date_start, 
        date_end, 
        orgs=None, 
        clipping_mode = "ALL",
        areas=None, 
        credentials=None
        ):
        """
        Query FCD traces for a given area and time period
        Method relies on the readOutput function defined outside the class

        :param orgs: list of organizations, defaults to None
        :type orgs: list, optional
        :param clipping_mode: clipping mode, 
        :type clipping_mode: str, optional
        :param areas: GeoJSON or bounding box, defaults to None
        :type areas: str, optional
        :param credentials: credentials for FCD API, defaults to None
        :type credentials: dict, optional
        :return: traces
        :rtype: pd.DataFrame
        """

        builder = (
            fcd_py.TraceDF(self.sc)
            .with_auth(json.dumps(credentials))
            .with_test_archive(type="tiled")
            .with_date(date_start + ":" + date_end)
            .with_areas(areas)
            .with_random_sample(0.99) # to ensure randomness of traces
            # .with_validation_mode("VALIDATE")
        )

        if clipping_mode != "ALL": 
            # temporary workaround not supported by FCD, see https://tomtomslack.slack.com/archives/CDS338SB0/p1704729140707339
            builder._config["com.tomtom.trace.spark.filter.clipping.mode"] = clipping_mode
        
        if orgs:
            print("Trying orgs: ", orgs)
            traces = builder.with_orgs(orgs).build()
        else:
            traces = builder.build()
        
        # convert to pandas
        fcd_cache_full = pd.DataFrame(traces.rdd.flatMap(readOutput).take(self.traces_limit))

        if fcd_cache_full.shape[0] < 1:
            return fcd_cache_full
        else: 
            # convert to GeoDataFrame 
            fcd_cache_full["trace"] = gpd.GeoSeries.from_wkt(fcd_cache_full["trace"])
            fcd_cache_full = gpd.GeoDataFrame(fcd_cache_full, geometry="trace", crs=4326)

        return fcd_cache_full
    

    def generate_map(self, 
                     row, 
                     trace_df, 
                     trace_df_contained = None,
                     visualize_all_traces = False):

        viz_row = gpd.GeoDataFrame(row.to_frame().transpose(), geometry="stretch")

        # visualize all traces if available
        if trace_df.shape[0] < 1 or row["start_and_endpoint_intersect"]:
            print("No traces available to visualize, visualizing only stretch")
            m = gpd.GeoSeries(viz_row["buffer_stretch"], crs=4326).explore(color="lightblue")
        elif visualize_all_traces:
            m = gpd.GeoSeries(trace_df["trace"], crs=4326).explore()
            m = gpd.GeoSeries(viz_row["buffer_stretch"], crs=4326).explore(m=m, color="lightblue")
        else:
            m = gpd.GeoSeries(viz_row["buffer_stretch"], crs=4326).explore(color="lightblue")

        # visualize other buffers
        m = gpd.GeoSeries(viz_row["buffer_start_point_trace_check"], crs=4326).explore(m=m, color="green")
        m = gpd.GeoSeries(viz_row["buffer_end_point_trace_check"], crs=4326).explore(m=m, color="orange")

        # visualizing contained traces if available
        if trace_df_contained is None:
            print("No contained traces available")
        elif visualize_all_traces:
            print("Visualizing contained traces on top of all traces")
            # contained traces
            m = gpd.GeoSeries(trace_df_contained["geometry"], crs=4326).explore(m=m, color="yellow")
            m = gpd.GeoSeries(trace_df_contained["start_point"], crs=4326).explore(m=m, color="green")
            m = gpd.GeoSeries(trace_df_contained["end_point"], crs=4326).explore(m=m, color="orange")
        else:
            print("Visualizing contained traces only")
            m = gpd.GeoSeries(trace_df_contained["geometry"], crs=4326).explore(m=m, color="yellow")
            m = gpd.GeoSeries(trace_df_contained["start_point"], crs=4326).explore(m=m, color="green")
            m = gpd.GeoSeries(trace_df_contained["end_point"], crs=4326).explore(m=m, color="orange")

        # visualize stretch and start/endpoint last to be on top
        m = gpd.GeoSeries(viz_row["stretch"], crs=4326).explore(m=m, color="red")
        m = gpd.GeoSeries(viz_row["start_point"], crs=4326).explore(m=m, color="green")
        m = gpd.GeoSeries(viz_row["end_point"], crs=4326).explore(m=m, color="orange")

        return m
    

    def get_FCD_features_and_visualization(self, row, get_visualization = False, visualize_all_traces = False):
        """
        Get FCD features for a given row of the gdf
        This includes trace retrieval and feature calculation
        Optionally, a map can be returned that visualizes the stretch, start and endpoint, and the traces

        :param row: row of the gdf
        :type row: pd.Series
        :param get_visualization: whether to return a map, defaults to False
        :type get_visualization: bool, optional
        :return: fcd_features, m
        :rtype: pd.Series, str or folium.folium map
        """
        # default value for m 
        m = "No visualization requested"

        # default return value fcd_features
        fcd_features = pd.Series(
                {
                    # see definitions here: https://confluence.tomtomgroup.com/display/MANA/Analysis+-+FCD+Feature+Improvements#AnalysisFCDFeatureImprovements
                    "tot_new": 0,
                    "pra_new": -2.0,
                    "prb_new": -2.0,
                    "pra_not_b": -2.0,
                    "prb_not_a": -2.0,
                    "pra_and_b": -2.0,

                    "pra_to_b": -2.0,
                    "prb_to_a": -2.0, 

                    "tot_contained": 0, 
                    "pra_to_b_contained": -2.0,
                    "prb_to_a_contained": -2.0,
                    "pra_to_b_not_contained": -2.0,
                    "prb_to_a_not_contained": -2.0,
                    "traffic_direction": -2.0,
                    "traffic_direction_contained": -2.0,
                    "ab_intersect": 0
                }
            )
        
        # if start & endpoint of stretch are overlapping, don't calculate FCD features
        if row["start_and_endpoint_intersect"]:
            fcd_features["ab_intersect"] = 1
            if get_visualization:
                m = self.generate_map(row, trace_df=pd.DataFrame(), visualize_all_traces = visualize_all_traces)
            return fcd_features, m


        # get trace_df
        trace_df = self.query_traces(
            date_start=row["date_start"],
            date_end = row["date_end"],
            areas=row[self.trace_retrieval_geometry],
            credentials=self.fcd_credentials
            )

        # total number of traces
        tot = trace_df.shape[0]
        fcd_features["tot_new"] = tot

        if tot < 6:
            if get_visualization:
                m = self.generate_map(row, trace_df, visualize_all_traces = visualize_all_traces)
            return fcd_features, m


        ##### a, b, a_not_b, b_not_a, a_and_b ####

        # get tot_a, tot_b, tot_a_not_b, tot_b_not_a, tot_a_and_b
        tot_a = trace_df.intersects(row["buffer_start_point_trace_selection"])
        tot_b = trace_df.intersects(row["buffer_end_point_trace_selection"])
        tot_a_not_b = tot_a & ~tot_b
        tot_b_not_a = tot_b & ~tot_a
        tot_a_and_b = tot_a & tot_b

        # calculate probabilities
        fcd_features["pra_new"] = tot_a.sum()/tot
        fcd_features["prb_new"] = tot_b.sum()/tot
        fcd_features["pra_not_b"] = tot_a_not_b.sum()/tot
        fcd_features["prb_not_a"] = tot_b_not_a.sum()/tot
        fcd_features["pra_and_b"] = tot_a_and_b.sum()/tot

        
        ##### a_to_b, b_to_a ##### 

        # get intersections of traces with the start and endpoint
        start_and_endpoint = MultiPolygon([row["buffer_start_point_trace_selection"], row["buffer_end_point_trace_selection"]])
        traces_start_and_endpoint = trace_df[tot_a_and_b].intersection(start_and_endpoint)

        # convert to GeoDataFrame
        trace_df_intersect_a_b = gpd.GeoDataFrame(geometry=gpd.GeoSeries(traces_start_and_endpoint))
        
        # extract start and endpoints of contained trace intersections
        trace_df_intersect_a_b["start_point"] = trace_df_intersect_a_b["geometry"].apply(lambda geom: shapely.geometry.Point(geom.geoms[0].coords[0]))
        trace_df_intersect_a_b["end_point"] = trace_df_intersect_a_b["geometry"].apply(lambda geom: shapely.geometry.Point(geom.geoms[-1].coords[-1]))

        # check if trace start/endpoint falls into stretch start/endpoint buffer
        # start/start or end/end
        trace_df_intersect_a_b["trace_start_in_stretch_start"] = trace_df_intersect_a_b["start_point"].intersects(row["buffer_start_point_trace_check"])
        trace_df_intersect_a_b["trace_end_in_stretch_end"] = trace_df_intersect_a_b["end_point"].intersects(row["buffer_end_point_trace_check"])

        # start/end or end/start
        trace_df_intersect_a_b["trace_start_in_stretch_end"] = trace_df_intersect_a_b["start_point"].intersects(row["buffer_end_point_trace_check"])
        trace_df_intersect_a_b["trace_end_in_stretch_start"] = trace_df_intersect_a_b["end_point"].intersects(row["buffer_start_point_trace_check"])

        # traces A -> B or B -> A  
        trace_df_intersect_a_b["from_a_to_b"] = trace_df_intersect_a_b["trace_start_in_stretch_start"] & trace_df_intersect_a_b["trace_end_in_stretch_end"]
        trace_df_intersect_a_b["from_b_to_a"] = trace_df_intersect_a_b["trace_start_in_stretch_end"] & trace_df_intersect_a_b["trace_end_in_stretch_start"]

        from_a_to_b = trace_df_intersect_a_b["from_a_to_b"].sum()
        from_b_to_a = trace_df_intersect_a_b["from_b_to_a"].sum()

        # calculate probabilities
        fcd_features["pra_to_b"] = from_a_to_b / tot
        fcd_features["prb_to_a"] = from_b_to_a / tot

        # traffic flow direction
        fcd_features["traffic_direction"] = (from_a_to_b - from_b_to_a) / tot


        ##### contained traces only ##### 

        # GOAL: generate trace_df_contained, subset of trace_df with only traces that are going through A and B and never leave the stretch buffer

        # get intersection of traces with buffer_stretch and keep only non-empty Linestrings
        intersection_traces = trace_df.intersection(row["buffer_stretch"])
        contained_traces = intersection_traces[(intersection_traces.type == "LineString") & ~intersection_traces.is_empty]

        # get a_and_b of contained traces
        contained_tot_a_and_b = contained_traces.intersects(row["buffer_start_point_trace_check"]) & contained_traces.intersects(row["buffer_end_point_trace_check"])

        # traces that are going through A AND B AND never leave the buffer of the route 
        contained_traces_a_and_b = contained_traces[contained_tot_a_and_b]
        trace_df_contained = gpd.GeoDataFrame(geometry=gpd.GeoSeries(contained_traces_a_and_b))
        
        tot_contained = trace_df_contained.shape[0]
        fcd_features["tot_contained"] = tot_contained

        if tot_contained < 6:
            if get_visualization:
                m = self.generate_map(row, trace_df, visualize_all_traces = visualize_all_traces)
            return fcd_features, m

        # extract start and endpoints of contained traces
        trace_df_contained["start_point"] = trace_df_contained["geometry"].apply(lambda geom: shapely.geometry.Point(geom.coords[0]))
        trace_df_contained["end_point"] = trace_df_contained["geometry"].apply(lambda geom: shapely.geometry.Point(geom.coords[-1]))

        # check if the start/endpoints of contained traces start/end in the start/end points of trace
        # start/start or end/end
        trace_df_contained["trace_start_in_stretch_start"] = trace_df_contained["start_point"].intersects(row["buffer_start_point_trace_check"])
        trace_df_contained["trace_end_in_stretch_end"] = trace_df_contained["end_point"].intersects(row["buffer_end_point_trace_check"])

        # start/end or end/start
        trace_df_contained["trace_start_in_stretch_end"] = trace_df_contained["start_point"].intersects(row["buffer_end_point_trace_check"])
        trace_df_contained["trace_end_in_stretch_start"] = trace_df_contained["end_point"].intersects(row["buffer_start_point_trace_check"])

        # check if traces go from A --> B, or B --> A
        trace_df_contained["from_a_to_b"] = trace_df_contained["trace_start_in_stretch_start"] & trace_df_contained["trace_end_in_stretch_end"]
        trace_df_contained["from_b_to_a"] = trace_df_contained["trace_start_in_stretch_end"] & trace_df_contained["trace_end_in_stretch_start"]

        from_a_to_b_contained = trace_df_contained["from_a_to_b"].sum()
        from_b_to_a_contained = trace_df_contained["from_b_to_a"].sum()

        # probabilities of A -> B / B -> A given that trace is contained in the stretch 
        fcd_features["pra_to_b_contained"] = from_a_to_b_contained / tot_contained
        fcd_features["prb_to_a_contained"] = from_b_to_a_contained / tot_contained

        # probabilities of A -> B / B -> A given that trace is NOT contained in stretch
        if (tot - tot_contained) != 0:
            fcd_features["pra_to_b_not_contained"] = (from_a_to_b - from_a_to_b_contained) / (tot - tot_contained)
            fcd_features["prb_to_a_not_contained"] = (from_b_to_a - from_b_to_a_contained) / (tot - tot_contained)

        # traffic flow direction of contained traces
        fcd_features["traffic_direction_contained"] = (from_a_to_b_contained - from_b_to_a_contained) / tot_contained

        if get_visualization: 
            m = self.generate_map(
                row, 
                trace_df = trace_df, 
                trace_df_contained=trace_df_contained, 
                visualize_all_traces=visualize_all_traces
                )

        return fcd_features, m 
    

    def calculate_FCD_features(self, row):
        """
        Intermediate function that only passes on the fcd_features and not the map
        This has the purpose that it can be used in a .apply() function call. 
        """
        fcd_features, m = self.get_FCD_features_and_visualization(row)
        return fcd_features
    

    def visualize(self, row_index, visualize_all_traces = False):
        """
        Visualize the stretch, start and endpoint, and the traces for a given row of the gdf
        Relies on the .explore() method of GeoPandas, returning a folium map

        :param row_index: index of the row of the gdf
        :type row_index: int
        :return: folium map
        :rtype: folium.folium.Map
        """
        self.add_geometric_attributes()
        fcd_features, m = self.get_FCD_features_and_visualization(
            self.gdf.iloc[row_index,:], 
            get_visualization = True, 
            visualize_all_traces = visualize_all_traces
            )
        print(self.gdf.iloc[row_index,0:6])
        print(pd.DataFrame(fcd_features).to_markdown()) 
        return m
    

    def featurize(self, show_progress = False):
        """
        End2end featurization of the gdf
        This includes trace retrieval and feature calculation
        :param show_progress: whether to show a progress bar, defaults to False
        :type show_progress: bool, optional
        :return: fcd_features
        :rtype: pd.DataFrame
        """
        self.add_geometric_attributes()
        if show_progress:
            from tqdm import tqdm
            tqdm.pandas()
            fcd_features = self.gdf.progress_apply(self.calculate_FCD_features, axis=1)
        else:
            fcd_features = self.gdf.apply(self.calculate_FCD_features, axis=1)

        return fcd_features
