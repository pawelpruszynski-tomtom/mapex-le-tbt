# Databricks notebook source
# MAGIC %md
# MAGIC # Explore fcd_py library and potential to replace current FCD request process deployed in EC model

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %pip install --extra-index-url https://svc-ar-maps-analytics-editor:AP5GYCYPFsETQzbsgnE8a6cjhNEcSvTaNTUvzNmkDHQTRt9GhcqKa3zAe9j2@artifactory.tomtomgroup.com/artifactory/api/pypi/maps-fcd-pypi-release/simple fcd_py==5.0.615

# COMMAND ----------

# MAGIC %pip install shapely geopandas folium matplotlib mapclassify sqlalchemy

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import fcd_py
from pyspark.sql import SparkSession
import json
from pyspark.sql import functions as F
import sqlalchemy
import geopandas as gpd
import shapely
import shapely.wkt
from shapely.geometry import mapping, Polygon, MultiPolygon

from datetime import datetime, timedelta
import copy
from tqdm import tqdm
tqdm.pandas()

# plotting
import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %run "/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/notebooks/utils"

# COMMAND ----------

# FCD credentials
credentials = json.loads(dbutils.secrets.get(scope = "mdbf3", key = "fcd_credentials"))

# COMMAND ----------

# MAGIC %md
# MAGIC # New FCD features: Analysis
# MAGIC See this confluence page for details: https://confluence.tomtomgroup.com/display/MANA/Analysis+-+FCD+Feature+Improvements 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define functions for getting FCD data

# COMMAND ----------

# functions copied from trunqa repo: https://adb-8671683240571497.17.azuredatabricks.net/?o=8671683240571497#notebook/1722655489460006/command/2749705210464987

def readOutput(cache):
    """Process FCD traces (raw) into WKT

    :param cache: FCD traces. Rows if RDD in format ({}, 'json')
    :type cache: list
    :return: traces
    :rtype: List[Dict]
    """

    traces = []
    org = cache["meta"]["ORGANISATION"]
    vehicle_type = cache["meta"]["VEHICLE"]

    if cache["reports"] is None:
        return traces

    if len(cache["reports"]) > 1:
        reports = cache["reports"]
        thisTrace = [shapely.geometry.Point([reports[0]["lon"], reports[0]["lat"]])]
        for j in range(1, len(reports)):
            thisTrace.append(
                    shapely.geometry.Point([reports[j]["lon"], reports[j]["lat"]])
            )
        traces.append({"org": org, "vehicle_type": vehicle_type, "trace": shapely.geometry.LineString(thisTrace).wkt})
    return traces

def query_traces(orgs=None, limit=1000, clipping_mode = "ALL", 
                 date_start=None, date_end=None, areas=None, credentials=None):
    """
    Download traces using fcd_py library.
    """
    builder = (
        fcd_py.TraceDF(spark)
        .with_auth(json.dumps(credentials))
        .with_test_archive(type="tiled")
        .with_date(date_start + ":" + date_end)
        .with_areas(areas)
    )
    # temporary workaround not supported by FCD, see https://tomtomslack.slack.com/archives/CDS338SB0/p1704729140707339
    builder._config["com.tomtom.trace.spark.filter.clipping.mode"] = clipping_mode

    if orgs:
        print("Trying orgs: ", orgs)
        traces = builder.with_orgs(orgs).build()
    else:
        traces = builder.build()
    
    fcd_cache_full = pd.DataFrame(traces.rdd.flatMap(readOutput).take(limit))
    return fcd_cache_full

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import some critical sections for testing

# COMMAND ----------

# load a few critical section
# OPTION 1: sample with different countries
# ics = read_tbt_datalake("inspection_critical_sections.delta").select("case_id", "stretch", "fcd_state").sample(0.00001, seed = 1234).toPandas()

# OPTION 2: 
ics = read_tbt_datalake("inspection_critical_sections.delta").select("case_id", "stretch", "fcd_state").limit(5).toPandas()

# convert to GeoDataFrame
ics["stretch"] = gpd.GeoSeries.from_wkt(ics["stretch"])
gics = gpd.GeoDataFrame(ics, geometry="stretch", crs=4326)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function for geometric attributes

# COMMAND ----------

def add_geometric_attributes(
    gdf, 
    buffer_stretch=0.0001, 
    simplify_buffer_stretch=0.0000001, 
    buffer_point=0.0001, 
    simplify_buffer_point=0.0000001
    ):
    """
    For a geodataframe with stretches, add a buffer, start and end-point of each stretch
    geometry column must be named 'stretch'
    """
    # draw buffer for stretch
    gdf["buffer_stretch"] = gdf["stretch"].buffer(buffer_stretch).simplify(simplify_buffer_stretch)

    # extract start and endpoints and buffer for start and endpoint
    gdf["start_point"] = gdf["stretch"].apply(lambda geom: shapely.geometry.Point(geom.coords[0]))
    gdf["end_point"] = gdf["stretch"].apply(lambda geom: shapely.geometry.Point(geom.coords[-1]))
    gdf["buffer_start_point"] = gdf["start_point"].buffer(buffer_point).simplify(simplify_buffer_point)
    gdf["buffer_end_point"] = gdf["end_point"].buffer(buffer_point).simplify(simplify_buffer_point)

    # get buffer for trace retrieval, we want it to be larger than only the buffer for the stretch to capture more traces
    gdf["buffer_trace_retrieval"] = gdf["buffer_stretch"].buffer(1.5 * buffer_stretch).simplify(5 * simplify_buffer_stretch)

    # Add column with GeoJSON for the area to be supplied to FCD module
    gdf["buffer_json"] = gdf["buffer_trace_retrieval"].apply(lambda x: gpd.GeoSeries([x]).to_json())

    # Add bounding box that can optionally be supplied to FCD module instead of GeoJSON
    gdf["bbox"] = gdf["stretch"].apply(lambda x: gpd.GeoSeries([shapely.box(*(x.bounds))]))
    gdf["bbox_json"] = gdf["stretch"].apply(lambda x: gpd.GeoSeries([shapely.box(*(x.bounds))]).to_json())

    return gdf

# COMMAND ----------

gdf = add_geometric_attributes(gics)
gdf

# COMMAND ----------

# show traces
m = gdf["stretch"].explore()

# add stretches with buffer polygons
# m = gpd.GeoSeries(gdf["buffer_stretch"], crs=4326).explore(m=m, color="red")
m = gpd.GeoSeries(gdf["start_point"], crs=4326).explore(m=m, color="blue")
m = gpd.GeoSeries(gdf["end_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(gdf["buffer_trace_retrieval"], crs=4326).explore(m=m, color="green")

m

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the retrieval of traces for many stretches in bulk 

# COMMAND ----------

date_start = datetime.strftime(datetime.now()-timedelta(15+1), "%Y-%m-%d")
date_end = datetime.strftime(datetime.now()-timedelta(15), "%Y-%m-%d")

# COMMAND ----------

all_stretch_buffers = gdf["buffer_trace_retrieval"].to_json()

# COMMAND ----------

area = '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[8.422056880209162, 51.37003023696716], [8.422017283189538, 51.37003550338465], [8.421975802779098, 51.37006295693615], [8.421838508122333, 51.3702913411416], [8.421722444447449, 51.37042526076647], [8.421478188371852, 51.37059714467152], [8.421289066782068, 51.37064251587901], [8.421255352582877, 51.37066215670697], [8.421231720943746, 51.37069320433138], [8.421221769567511, 51.370730932032885], [8.421227013460994, 51.37076959611093], [8.421246654288948, 51.37080331031012], [8.421277701913363, 51.370826941949254], [8.421325242388242, 51.37083701557627], [8.421569527410222, 51.37077687367632], [8.421857055487685, 51.37057418599309], [8.42200116065599, 51.37040842044094], [8.422104693763174, 51.370234009513695], [8.422242237775686, 51.37023917911327], [8.42227783257, 51.370223197249565], [8.422304601877473, 51.370194810394736], [8.422318470313705, 51.370158340190095], [8.422317326534998, 51.37011933889371], [8.422301344671292, 51.3700837440994], [8.42227295781646, 51.37005697479192], [8.422236487611825, 51.37004310635569], [8.422056880209162, 51.37003023696716]]]}, "bbox": [8.421221769567511, 51.37003023696716, 8.422318470313705, 51.37083701557627]}], "bbox": [8.421221769567511, 51.37003023696716, 8.422318470313705, 51.37083701557627]}'

# COMMAND ----------

# get traces for all these stretches
trace_df = query_traces(areas=area, date_start=date_start, date_end=date_end, credentials=credentials)

# COMMAND ----------

trace_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define index of the one critical section that you want to highlight/investigate in more detail

# COMMAND ----------

# select first critical section
cs_index = 1

# COMMAND ----------

# get traces for the one stretch
trace_df = query_traces(areas=gdf["buffer_json"][cs_index], date_start=date_start, date_end=date_end, credentials=credentials)

# COMMAND ----------

trace_df["trace"] = gpd.GeoSeries.from_wkt(trace_df["trace"])
trace_df = gpd.GeoDataFrame(trace_df, geometry="trace", crs=4326)

# COMMAND ----------

# show traces
m = trace_df.explore()

# add stretches with buffer polygons
m = gdf["stretch"].explore(m=m, color="red")
m = gpd.GeoSeries(gdf["buffer_stretch"], crs=4326).explore(m=m, color="blue")
m = gpd.GeoSeries(gdf["start_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(gdf["end_point"], crs=4326).explore(m=m, color="orange")
m = gpd.GeoSeries(gdf["buffer_start_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(gdf["buffer_end_point"], crs=4326).explore(m=m, color="orange")
m

# COMMAND ----------

# get tot_a and tot_b, etc.
tot_a = trace_df.intersects(gdf["buffer_start_point"][cs_index])
tot_b = trace_df.intersects(gdf["buffer_end_point"][cs_index])
tot_a_not_b = tot_a & ~tot_b
tot_b_not_a = tot_b & ~tot_a
tot_a_and_b = tot_a & tot_b


for item in [tot_a, tot_b, tot_a_not_b, tot_b_not_a, tot_a_and_b]: 
    print(item.sum())

# COMMAND ----------

# MAGIC %md
# MAGIC ### tot_a

# COMMAND ----------

m = trace_df.explore()
m = gdf["stretch"].explore(m=m, color="red")
m

# COMMAND ----------

to_visualize = tot_a_and_b
m = trace_df.explore()
m = trace_df[to_visualize].explore(m=m, color = "yellow")
print(to_visualize.sum())

# add stretches with buffer polygons
m = gdf["stretch"].explore(m=m, color="red")
# m = gpd.GeoSeries(gdf["buffer_stretch"], crs=4326).explore(m=m, color="blue")
m = gpd.GeoSeries(gdf["start_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(gdf["end_point"], crs=4326).explore(m=m, color="orange")
m = gpd.GeoSeries(gdf["buffer_start_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(gdf["buffer_end_point"], crs=4326).explore(m=m, color="orange")
m

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get traces that are going from A to B and B to A

# COMMAND ----------

intersection_a_b_traces = trace_df[tot_a_and_b].intersection(MultiPolygon([gdf["buffer_start_point"][cs_index], gdf["buffer_end_point"][cs_index]]))
intersection_a_b_traces

# COMMAND ----------

# get intersections with the start and endpoint
intersection_a_b_traces = trace_df[tot_a_and_b].intersection(MultiPolygon([gdf["buffer_start_point"][cs_index], gdf["buffer_end_point"][cs_index]]))

# convert to dataframe
intersection_a_b_traces = gpd.GeoDataFrame(geometry=gpd.GeoSeries(intersection_a_b_traces))
intersection_a_b_traces

# extract start and endpoints of contained traces
intersection_a_b_traces["start_point"] = intersection_a_b_traces["geometry"].apply(lambda geom: shapely.geometry.Point(geom.geoms[0].coords[0]))
intersection_a_b_traces["end_point"] = intersection_a_b_traces["geometry"].apply(lambda geom: shapely.geometry.Point(geom.geoms[-1].coords[-1]))

# COMMAND ----------


m = intersection_a_b_traces["geometry"].explore()
m = gpd.GeoSeries(intersection_a_b_traces["start_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(intersection_a_b_traces["end_point"], crs=4326).explore(m=m, color="orange")
m

# COMMAND ----------

# 
intersection_a_b_traces["trace_start_in_stretch_start"] = gdf["buffer_start_point"][cs_index].buffer(0.00005).intersects(intersection_a_b_traces["start_point"])
intersection_a_b_traces["trace_end_in_stretch_end"] = gdf["buffer_end_point"][cs_index].buffer(0.00005).intersects(intersection_a_b_traces["end_point"])

intersection_a_b_traces["trace_start_in_stretch_end"] = gdf["buffer_end_point"][cs_index].buffer(0.00005).intersects(intersection_a_b_traces["start_point"])
intersection_a_b_traces["trace_end_in_stretch_start"] = gdf["buffer_start_point"][cs_index].buffer(0.00005).intersects(intersection_a_b_traces["end_point"])

intersection_a_b_traces["from_a_to_b"] = intersection_a_b_traces["trace_start_in_stretch_start"] & intersection_a_b_traces["trace_end_in_stretch_end"]
intersection_a_b_traces["from_b_to_a"] = intersection_a_b_traces["trace_start_in_stretch_end"] & intersection_a_b_traces["trace_end_in_stretch_start"]

# COMMAND ----------

m = trace_df.explore()
m = trace_df[tot_a_and_b][intersection_a_b_traces["from_b_to_a"]].explore(m=m, color = "yellow")
print(to_visualize.sum())

# add stretches with buffer polygons
m = gdf["stretch"].explore(m=m, color="red")
# m = gpd.GeoSeries(gdf["buffer_stretch"], crs=4326).explore(m=m, color="blue")
m = gpd.GeoSeries(gdf["start_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(gdf["end_point"], crs=4326).explore(m=m, color="orange")
m = gpd.GeoSeries(gdf["buffer_start_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(gdf["buffer_end_point"], crs=4326).explore(m=m, color="orange")
m

# COMMAND ----------

print(intersection_a_b_traces.shape[0])
from_a_to_b = intersection_a_b_traces["from_a_to_b"].sum()
from_b_to_a = intersection_a_b_traces["from_b_to_a"].sum()
print(f"A-->B: {from_a_to_b}, B-->A: {from_b_to_a}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get traces that are going through A and B AND never leave the buffer of the stretch
# MAGIC The traces included in tot_a_and_b might go through A and B, but not through the suggested route/stretch. This is what is calculated above 

# COMMAND ----------

# strech_and_point_buffer = shapely.ops.unary_union([gdf["buffer_stretch"], gdf["buffer_start_point"], gdf["buffer_end_point"]])

intersection_traces = trace_df.intersection(gdf["buffer_stretch"][cs_index])
intersection_traces.explore()

# COMMAND ----------

# only keep LineStrings that are not empty, discarding empty ones and MultiLinestrings
# we do not want MultiLinestrings as they might leave the buffer_stretch 
contained_traces = intersection_traces[(intersection_traces.type == "LineString") & ~intersection_traces.is_empty]

# COMMAND ----------

contained_traces.explore()

# COMMAND ----------

# get tot_a and tot_b, etc.
tot_a = contained_traces.intersects(gdf["buffer_start_point"][cs_index])
tot_b = contained_traces.intersects(gdf["buffer_end_point"][cs_index])
tot_a_not_b = tot_a & ~tot_b
tot_b_not_a = tot_b & ~tot_a
tot_a_and_b = tot_a & tot_b

for item in [tot_a, tot_b, tot_a_not_b, tot_b_not_a, tot_a_and_b]: 
    print(item.sum())

# COMMAND ----------

# traces that are going through A and B and never leave the buffer of the route! 
contained_traces_a_and_b = contained_traces[tot_a_and_b]
contained_traces_a_and_b.explore()

# COMMAND ----------

# traces that go from a to b
contained_traces_a_and_b_df = gpd.GeoDataFrame(geometry=gpd.GeoSeries(contained_traces_a_and_b))
contained_traces_a_and_b_df

# extract start and endpoints
for index, row in contained_traces_a_and_b_df.iterrows():
    coords = [(coords) for coords in list(row['geometry'].coords)]
    first_coord, last_coord = [ coords[i] for i in (0, -1) ]
    contained_traces_a_and_b_df.at[index,'start_point'] = shapely.geometry.Point(first_coord)
    contained_traces_a_and_b_df.at[index,'end_point'] = shapely.geometry.Point(last_coord)

# COMMAND ----------

# add stretches with buffer polygons

# traces
m = gpd.GeoSeries(contained_traces_a_and_b_df["geometry"], crs=4326).explore()
m = gpd.GeoSeries(contained_traces_a_and_b_df["start_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(contained_traces_a_and_b_df["end_point"], crs=4326).explore(m=m, color="orange")

# critical section
m = gpd.GeoSeries(gdf.iloc[cs_index:cs_index+1,:]["buffer_start_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(gdf.iloc[cs_index:cs_index+1,:]["buffer_end_point"], crs=4326).explore(m=m, color="orange")
m = gdf.iloc[cs_index:cs_index+1,:]["stretch"].explore(m=m, color="red")
m = gpd.GeoSeries(gdf.iloc[cs_index:cs_index+1,:]["start_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(gdf.iloc[cs_index:cs_index+1,:]["end_point"], crs=4326).explore(m=m, color="orange")
m

# COMMAND ----------

# get tot_a and tot_b, etc. again for contained traces only
contained_tot_a = contained_traces_a_and_b_df.intersects(gdf["buffer_start_point"][cs_index])
contained_tot_b = contained_traces_a_and_b_df.intersects(gdf["buffer_end_point"][cs_index])
contained_tot_a_not_b = tot_a & ~tot_b
contained_tot_b_not_a = tot_b & ~tot_a
contained_tot_a_and_b = tot_a & tot_b

for item in [contained_tot_a, contained_tot_b, 
             contained_tot_a_not_b, contained_tot_b_not_a, 
             contained_tot_a_and_b]: 
    print(item.sum())

# COMMAND ----------

intersection_traces.explore()

# COMMAND ----------

# recalculate some values we need
tot_a_extra = trace_df.intersects(gdf["buffer_start_point"][cs_index])
tot_b_extra = trace_df.intersects(gdf["buffer_end_point"][cs_index])
tot_a_and_b_extra = tot_a_extra & tot_b_extra


m = trace_df.explore()
m = trace_df[tot_a_and_b_extra][(intersection_traces.type == "LineString") & ~intersection_traces.is_empty].explore(m=m, color = "yellow")

# add stretches with buffer polygons
m = gdf["stretch"].explore(m=m, color="red")
m = gpd.GeoSeries(gdf["buffer_stretch"], crs=4326).explore(m=m, color="blue")
# m = gpd.GeoSeries(gdf["start_point"], crs=4326).explore(m=m, color="green")
# m = gpd.GeoSeries(gdf["end_point"], crs=4326).explore(m=m, color="orange")
# m = gpd.GeoSeries(gdf["buffer_start_point"], crs=4326).explore(m=m, color="green")
# m = gpd.GeoSeries(gdf["buffer_end_point"], crs=4326).explore(m=m, color="orange")
m

# COMMAND ----------

# MAGIC %md
# MAGIC ## Traces that are going FROM A to B and B to A and are contained inside the buffer

# COMMAND ----------

# 
contained_traces_a_and_b_df["trace_start_in_stretch_start"] = gdf["buffer_start_point"][cs_index].buffer(0.00001).intersects(contained_traces_a_and_b_df["start_point"])
contained_traces_a_and_b_df["trace_end_in_stretch_end"] = gdf["buffer_end_point"][cs_index].buffer(0.00001).intersects(contained_traces_a_and_b_df["end_point"])

contained_traces_a_and_b_df["trace_start_in_stretch_end"] = gdf["buffer_end_point"][cs_index].buffer(0.00001).intersects(contained_traces_a_and_b_df["start_point"])
contained_traces_a_and_b_df["trace_end_in_stretch_start"] = gdf["buffer_start_point"][cs_index].buffer(0.00001).intersects(contained_traces_a_and_b_df["end_point"])

contained_traces_a_and_b_df["from_a_to_b"] = contained_traces_a_and_b_df["trace_start_in_stretch_start"] & contained_traces_a_and_b_df["trace_end_in_stretch_end"]
contained_traces_a_and_b_df["from_b_to_a"] = contained_traces_a_and_b_df["trace_start_in_stretch_end"] & contained_traces_a_and_b_df["trace_end_in_stretch_start"]

# COMMAND ----------

contained_traces_a_and_b_df.shape[0]

# COMMAND ----------

from_a_to_b = contained_traces_a_and_b_df["from_a_to_b"].sum()
from_b_to_a = contained_traces_a_and_b_df["from_b_to_a"].sum()
print(f"A-->B: {from_a_to_b}, B-->A: {from_b_to_a}")

# COMMAND ----------

# recalculate some values we need
tot_a_extra = trace_df.intersects(gdf["buffer_start_point"][cs_index])
tot_b_extra = trace_df.intersects(gdf["buffer_end_point"][cs_index])
tot_a_and_b_extra = tot_a_extra & tot_b_extra


m = trace_df.explore()
# m = trace_df[tot_a_and_b_extra][(intersection_traces.type == "LineString") & ~intersection_traces.is_empty][contained_traces_a_and_b_df["from_b_to_a"]].explore(m=m, color = "yellow")

# add stretches with buffer polygons
m = gdf["stretch"].explore(m=m, color="red")
m = gpd.GeoSeries(gdf["buffer_stretch"], crs=4326).explore(m=m, color="blue")
m = gpd.GeoSeries(gdf["start_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(gdf["end_point"], crs=4326).explore(m=m, color="orange")
m = gpd.GeoSeries(gdf["buffer_start_point"], crs=4326).explore(m=m, color="green")
m = gpd.GeoSeries(gdf["buffer_end_point"], crs=4326).explore(m=m, color="orange")
m

# COMMAND ----------

# MAGIC %md
# MAGIC # ------ DEPRECATED FROM HERE ------
# MAGIC # Package functions from above into class calculating all new FCD features
# MAGIC It was moved from here to this file: ``github-maps-analytics-error-classification/error_classification/call_FCD_API_new.py``

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define FCDFeaturizer module

# COMMAND ----------

# Optional 
# import sys
# sys.path.append('/Workspace/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/error_classification')

# import FCDFeaturizer module
%run "/Workspace/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/error_classification/call_FCD_API_new.py"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test FCDFeaturizer module

# COMMAND ----------

ics_limit = read_tbt_datalake("inspection_critical_sections.delta").select("case_id", "stretch", "fcd_state").limit(10).toPandas()
fcd_featurizer = FCDFeaturizer(ics_limit, fcd_credentials=credentials, trace_retrieval_geometry="bbox_json")

# COMMAND ----------

fcd_featurizer.featurize(show_progress=True)

# COMMAND ----------

fcd_featurizer.visualize(0)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tests and Analyses

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test if a larger number of traces makes a large difference. 

# COMMAND ----------

fcd_featurizer_1000 = FCDFeaturizer(ics_limit, fcd_credentials=credentials)
fcd_features_new_1000 = fcd_featurizer_1000.featurize()

# COMMAND ----------

diff_df = fcd_features_new_1000 - fcd_features_new
sns.heatmap(diff_df.iloc[:,1:-1], annot=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Taking 1,000 or 10,000 traces does not make a large difference in the features --> stay with 1,000. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test old vs new FCD features 
# MAGIC - Check latency difference
# MAGIC - Check difference in features

# COMMAND ----------

# MAGIC %md
# MAGIC ### New features

# COMMAND ----------

ics_limit = read_tbt_datalake("inspection_critical_sections.delta").select("case_id", "stretch", "fcd_state").limit(10).toPandas()
fcd_featurizer = FCDFeaturizer(ics_limit, fcd_credentials=credentials)
fcd_features_new = fcd_featurizer.featurize()
fcd_features_new = fcd_features_new.add_suffix('_new')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Old features

# COMMAND ----------

# get old fcd featurization functions
%run "/Workspace/Repos/simon.jasansky@tomtom.com/github-maps-analytics-error-classification/error_classification/call_FCD_API.py"

# COMMAND ----------

ics_limit["stretch_length"] = 100
old_fcd_features = evaluate_with_RFC(ics_limit, fcd_credentials=credentials)


# COMMAND ----------

fcd_features_old = old_fcd_features.add_suffix('_old')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine and compare
# MAGIC

# COMMAND ----------

fcd_features_all = pd.concat([fcd_features_old, fcd_features_new], axis = 1)
selected_features = fcd_features_all[["pra_old", "prb_old", "prab_old", "lift_old", "pra_new", "prb_new", "pra_and_b_new", "pra_to_b_new"]]

# COMMAND ----------

corr_matrix = selected_features.corr()

sn.heatmap(corr_matrix, annot=True)
plt.show()

# COMMAND ----------

selected_features[["pra_old", "pra_new"]].plot()

# COMMAND ----------

selected_features[["prb_old", "prb_new"]].plot()

# COMMAND ----------

selected_features[["prab_old", "pra_to_b_new"]].plot()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check new approach of making start/endpoint buffer smaller in case it overlaps
# MAGIC

# COMMAND ----------

selh = read_tbt_datalake("scheduled_error_logs_history.delta")
selh.createOrReplaceTempView("selh")

test_data2 = spark.sql(f"""
select 
    selh.case_id,
    selh.stretch as stretch, 
    case
        when selh.error_type in ('discard', null)
            then 'no_error'
        else
            'error'
        end error_label,
    case
        when selh.error_type is null
            then 'discard'
        else
            selh.error_type
        end error_type
from selh
where selh.case_id in ('041cffda-15b5-48c3-8c82-fa757a961fea', '979f5ed8-3e9a-4646-9ec8-85f1c0201b59', 'e941a478-5bdc-4a8a-bb9b-24606f8ef95c')
""").toPandas()

# COMMAND ----------

test_data

# COMMAND ----------

fcd_featurizer = FCDFeaturizer(test_data2, fcd_credentials=credentials)
m = fcd_featurizer.visualize(2)
m

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test it on validated data and create test dataset
# MAGIC - Compare old and new fcd featurization method in terms of latency
# MAGIC - check if a pattern is visible in terms of features & error distribution
# MAGIC - create a small test dataset

# COMMAND ----------

# should be about 850 cases
selh = read_tbt_datalake("scheduled_error_logs_history.delta").sample(0.015, seed=8370897409)
selh.createOrReplaceTempView("selh")

test_data = spark.sql(f"""
select 
    selh.case_id,
    selh.stretch as stretch, 
    case
        when selh.error_type in ('discard', null)
            then 'no_error'
        else
            'error'
        end error_label,
    case
        when selh.error_type is null
            then 'discard'
        else
            selh.error_type
        end error_type
from selh
-- where selh.country in ('NLD', 'BEL', 'USA', 'GBR', 'DEU', 'FRA', 'ESP')
""").toPandas()

# COMMAND ----------

test_data

# COMMAND ----------

fcd_featurizer = FCDFeaturizer(test_data, fcd_credentials=credentials, spark_context=spark)

# COMMAND ----------

fcd_featurizer.gdf.loc[600,["case_id", "error_label", "error_type"]]

# COMMAND ----------

fcd_featurizer.visualize(650)

# COMMAND ----------

fcd_featurizer = FCDFeaturizer(selh, fcd_credentials=credentials, spark_context=spark)
fcd_features = fcd_featurizer.featurize(show_progress=True)

# COMMAND ----------

fcd_featurizer.gdf.iloc[800,:]

# COMMAND ----------

fcd_featurizer.visualize(950)

# COMMAND ----------

# add features to df
df_with_features = pd.concat([test_data, fcd_features], axis = 1)

# COMMAND ----------

# add numerical has_error column
df_with_features["has_error"] = df_with_features["error_label"].apply(lambda x: 1 if x == "error" else 0)

# COMMAND ----------

df_with_features

# COMMAND ----------

# Histograms for all features
df_with_features.hist(bins=15, figsize=(15, 10))
plt.show()

# COMMAND ----------

# Pairplot to visualize the distribution of features by target class
sns.pairplot(df_with_features, hue='has_error')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the data

# COMMAND ----------

# Save the data
(
    spark.createDataFrame(df_with_features)
    .write
    .mode("append")
    .format('delta')
    .option("overwriteSchema", "true")
    .save("dbfs:/error_classification/other/new_fcd_features.delta")
)

# COMMAND ----------

# Save tiny training table with FCD data only
(
    spark.createDataFrame(df_with_features.drop(["case_id", "stretch", "error_label", "error_type"], axis=1))
    .write
    .mode("append")
    .format('delta')
    .option("overwriteSchema", "true")
    .save("dbfs:/error_classification/other/new_fcd_features_training_table.delta")
)

# COMMAND ----------

spark.read.format("delta").load('dbfs:/error_classification/other/new_fcd_features_training_table.delta').count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Explore clustering of traces (tbd.)

# COMMAND ----------

from shapely.geometry import LineString, MultiLineString
from sklearn.cluster import DBSCAN
from scipy.spatial.distance import directed_hausdorff
import numpy as np

# Let's assume you have a list of LineString objects called linestrings
linestrings = trace_df.sample(20)["trace"]

# Define a distance matrix function
def calculate_distance_matrix(linestrings):
    num_lines = len(linestrings)
    print(num_lines)
    distance_matrix = np.zeros((num_lines, num_lines))
    for i in range(num_lines):
        for j in range(num_lines):
            line1 = linestrings[i]
            line2 = linestrings[j]
            # Using directed Hausdorff distance for the example, but you could choose another
            distance_matrix[i, j] = directed_hausdorff(line1.coords, line2.coords)[0]
    return distance_matrix

# Calculate the distance matrix
distance_matrix = calculate_distance_matrix(linestrings)

# Cluster the linestrings using the distance matrix
db = DBSCAN(eps=0.1, min_samples=2, metric='precomputed')  # Adjust parameters as necessary
clusters = db.fit_predict(distance_matrix)

# For each cluster, compute the average linestring
average_linestrings = []
for cluster_id in set(clusters):
    if cluster_id != -1:  # -1 is noise in DBSCAN
        cluster_linestrings = [linestrings[i] for i in range(len(linestrings)) if clusters[i] == cluster_id]
        # Compute average linestring (this is the tricky part, see strategies above)
        # For simplicity, let's assume all linestrings have the same number of points
        avg_coords = np.mean([np.array(line.coords) for line in cluster_linestrings], axis=0)
        average_linestrings.append(LineString(avg_coords))

# average_linestrings now contains the average linestring for each cluster


# COMMAND ----------

average_linestring

# COMMAND ----------

trace_df.sample(20)["trace"][0]
