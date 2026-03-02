from ...adas import geometry_utils
import pytest
import shapely
from shapely import wkt
from shapely.geometry import LineString
import pyproj
import pandas as pd


@pytest.mark.parametrize(
    "line,expected_bearing",
    [
        (
            "LINESTRING (19.4728012 51.7721309, 19.4731083 51.7721565)",
            82.327579736111,
        ),
        (
            "LINESTRING (19.4352219 51.7548198, 19.4354044 51.7543237, 19.4355552 51.753832, 19.435685 51.7533807, 19.4358237 51.7528416, 19.4359096 51.75237120000001)",
            167.1712849,
        ),
        (
            "LINESTRING (19.4354212 51.7548496, 19.4352831 51.7552377, 19.4351006 51.7556604, 19.4349369 51.7560032)",
            -12.42229794,
        ),
    ],
)
def test_get_las_bearing(line, expected_bearing):
    line = shapely.wkt.loads(line)
    bearing = geometry_utils.get_las_heading(line)
    assert round(bearing, 3) == pytest.approx(round(expected_bearing, 3), 5)


@pytest.mark.parametrize(
    "start_point,line,expected_distance,poly_container",
    [
        (
            "POINT (19.4728012 51.7721309)",
            "LINESTRING (19.4728012 51.7721309, 19.4731083 51.7721565)",  # 23m --> 10% =~ 2.3m
            2.3,
            "POLYGON ((19.47282849960832962 51.77213539511262752, 19.47282899386798505 51.77213072435887398, 19.47283556752142175 51.77213119390554397, 19.47283472728000575 51.77213593879825027, 19.47282849960832962 51.77213539511262752))",
        ),
        (
            "POINT (19.4352219 51.7548198)",
            "LINESTRING (19.4352219 51.7548198, 19.4354044 51.7543237, 19.4355552 51.753832, 19.435685 51.7533807, 19.4358237 51.7528416, 19.4359096 51.75237120000001)",  # 277m --> 10% =~ 27m
            27,
            "POLYGON ((19.43531270815789114 51.7545832125726335, 19.43530577534207637 51.7545805534104062, 19.43530814959406783 51.75457589987649953, 19.43531422767916439 51.75457798921825514, 19.43531270815789114 51.7545832125726335))",
        ),
    ],
)
def test_get_following_point(start_point, line, expected_distance, poly_container):
    line = shapely.wkt.loads(line)
    # initial point
    start_point = shapely.wkt.loads(start_point)
    following_point = geometry_utils.get_following_point(
        line, start_point, forward_perc=0.1
    )
    poly = wkt.loads(poly_container)
    assert poly.contains(following_point)
    geodesic = pyproj.Geod(ellps="WGS84")
    _, _, dist = geodesic.inv(
        start_point.x, start_point.y, following_point.x, following_point.y
    )
    assert dist == pytest.approx(expected_distance, abs=1)

    # Second point
    second_point = following_point
    following_point = geometry_utils.get_following_point(
        line, second_point, forward_perc=0.1
    )
    _, _, dist = geodesic.inv(
        second_point.x, second_point.y, following_point.x, following_point.y
    )
    assert dist == pytest.approx(expected_distance, abs=1)


@pytest.mark.parametrize(
    "line,point,expected_bearing",
    [
        (
            "LINESTRING (19.4352219 51.7548198, 19.4354044 51.7543237, 19.4355552 51.753832, 19.435685 51.7533807, 19.4358237 51.7528416, 19.4359096 51.75237120000001)",
            "POINT (19.43535569390394 51.75445610051646)",
            167.171184969607,
        ),
        (
            wkt.loads(
                "LINESTRING (19.4352219 51.7548198, 19.4354044 51.7543237, 19.4355552 51.753832, 19.435685 51.7533807, 19.4358237 51.7528416, 19.4359096 51.75237120000001)"
            ),
            wkt.loads("POINT (19.43535569390394 51.75445610051646)"),
            167.171184969607,
        ),
    ],
)
def test_get_point_bearing(line, point, expected_bearing):
    bearing = geometry_utils.get_point_bearing(point, line)
    assert round(bearing, 3) == pytest.approx(expected_bearing, 5)


@pytest.mark.parametrize(
    "line,point,old_heading,expected_bearing",
    [
        (
            "LINESTRING (19.4352219 51.7548198, 19.4354044 51.7543237, 19.4355552 51.753832, 19.435685 51.7533807, 19.4358237 51.7528416, 19.4359096 51.75237120000001)",
            "POINT (19.43535569390394 51.75445610051646)",
            167.171184969607,
            167.171184969607,
        ),
        (
            wkt.loads(
                "LINESTRING (19.4352219 51.7548198, 19.4354044 51.7543237, 19.4355552 51.753832, 19.435685 51.7533807, 19.4358237 51.7528416, 19.4359096 51.75237120000001)"
            ),
            wkt.loads("POINT (19.43535569390394 51.75445610051646)"),
            167.171184969607,
            167.171184969607,
        ),
        (
            wkt.loads("POINT (19.43535569390394 51.75445610051646)"),
            wkt.loads("POINT (19.43535569390394 51.75445610051646)"),
            167.171184969607,
            167.171184969607,
        ),
    ],
)
def test_get_point_bearing_or_heading(line, point, old_heading, expected_bearing):
    bearing = geometry_utils.get_point_bearing_or_heading(point, line, old_heading)
    assert round(bearing, 3) == pytest.approx(expected_bearing, abs=5)



def test_get_bearing():
    line = "LINESTRING (19.484374 51.761565, 19.484356 51.761564)"
    assert geometry_utils.get_bearing(wkt.loads(line)) == pytest.approx(-95.115, abs=2)



@pytest.mark.parametrize(
    "angle_a,angle_b,expected",
    [
        (5,355,10),
        (5,12,7),
        (5,-5,10),

    ],
)
def test_get_delta_angle(angle_a,angle_b,expected):
    assert abs(geometry_utils.get_delta_angle(angle_a,angle_b))==expected


def test_get_angle():
    line = "LINESTRING (19.484374 51.761565, 19.484356 51.761564)"
    assert geometry_utils.get_angle(wkt.loads(line)) == pytest.approx(93.179, abs=0.01)



def test_is_continuous_segments():
    line1 = "LINESTRING (19.51521 51.82301, 19.5123 51.82173)"
    line2 = "LINESTRING (19.50935 51.82043, 19.50687 51.81936, 19.50498 51.81796)"

    line_no_consecurtive = "LINESTRING (19.36532 51.81347, 19.36623 51.81303, 19.36751 51.81232)"
    assert geometry_utils.is_continuous_segments(line1, line2) == True
    assert geometry_utils.is_continuous_segments(line1, line_no_consecurtive) == False



def test_is_same_directed():
    line1 = "LINESTRING (19.51521 51.82301, 19.5123 51.82173)"
    line2 = "LINESTRING (19.50935 51.82043, 19.50687 51.81936, 19.50498 51.81796)"

    line_reverse2 = "LINESTRING (19.50498 51.81796, 19.50687 51.81936, 19.50935 51.82043)"    
    line3 = "LINESTRING (19.41719 51.77285, 19.41837 51.77166)"

    assert geometry_utils.is_same_directed(line1, line2) == True
    assert geometry_utils.is_same_directed(line1, line_reverse2) == False
    # Check type
    # assert geometry_utils.is_same_directed(123, line_reverse2) == False
    # assert geometry_utils.is_same_directed(line1, 345) == False
    # different angle and line
    assert geometry_utils.is_same_directed(line1, line3) == False




def test_intersects_gdf():
   
    gdf_a = pd.DataFrame(data={
    'geom':[LineString([(0, 0), (0, 1), (0, 2)]), LineString([(0, 2), (0, 4), (0, 5)])],
    'lanes': [1,2],
    'feature_id_uuid_provider': ['p1','p2'],
    'feature_id_uuid_competitor': ['c1','c2'],
    'trace_id':'trace1'
    }) 
    gdf_b = pd.DataFrame(data={
        'geom':[LineString([(0, 1), (0, 2), (0, 3)]), LineString([(5, 2), (5, 3), (5, 4)])],
        'lanes': [3,4]
    }) 
    # lanes 1 and 3 intersects 100%
    # lanes 2 and 3 intersets 50%
    intersection = geometry_utils.intersects_gdf(gdf_a,gdf_b,buffer_distance=1)
    # Round the coordinates of the result to two decimal places
    intersection['intersected_geom'] = intersection['intersected_geom'].apply(lambda x: LineString([(round(coord[0], 2), round(coord[1], 2)) for coord in x.coords]))

    assert intersection['intersected_geom'].iloc[0].wkt == 'LINESTRING (0 1, 0 2)'
    assert intersection['intersected_geom'].iloc[1].wkt == 'LINESTRING (0 2, 0 3)'

    intersection = geometry_utils.intersects_gdf(gdf_a,gdf_b,buffer_distance=2)
    assert intersection['intersected_geom'].iloc[0].wkt == 'LINESTRING (0 1, 0 2)'
    assert intersection['intersected_geom'].iloc[1].wkt == 'LINESTRING (0 2, 0 3)' # Flat buffer now

    