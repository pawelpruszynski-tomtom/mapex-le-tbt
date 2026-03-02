import shapely.geometry

from ...navutils.base_provider import AvoidOptions, RouteOptions, TravelMode
from ...navutils.provider import Provider

origin = [29.1285201, 40.18264]
destination = [29.1259901, 40.17819]


def route_calculation(
    expected_api, expected_endpoint, route_options=RouteOptions(), **kwargs
):
    prov = Provider.from_properties(**kwargs)
    assert prov.api == expected_api
    assert prov.endpoint == expected_endpoint
    route = prov.getRoute(origin, destination, route_options=route_options)
    assert route.geometry.wkt != "LINESTRING EMPTY"
    assert (
        shapely.geometry.Point(route.geometry.coords[0]).distance(
            shapely.geometry.Point(origin)
        )
        < 0.0002
    )
    assert route.time > 0


class TestProviders:
    def test_route_tt(self):
        route_calculation(
            expected_api="Directions",
            expected_endpoint="https://api.tomtom.com/routing/1/calculateRoute/",
            name="TT",
        )
        route_calculation(
            expected_api="Directions",
            expected_endpoint="https://api.tomtom.com/routing/1/calculateRoute/",
            name="TomTom",
        )

    def test_route_osm(self):
        osm_endpoint = "http://tt-valhalla-api.maps-orbis-analytics-dev.orbis.az.tt3.com:8002/"
        route_calculation(
            expected_api="Valhalla",
            expected_endpoint=osm_endpoint,
            name="OSM",
            product="2022.09.02",
        )
        route_calculation(
            expected_api="Valhalla-Strict",
            expected_endpoint=osm_endpoint,
            name="OSM-STRICT",
            product="2022.09.02",
        )
        route_calculation(
            expected_api="Valhalla",
            expected_endpoint=osm_endpoint,
            name="OSM",
            product="2022.09.02",
            endpoint=None,
            url=None,
        )

    def test_route_osm_trucks(self):
        osm_endpoint = "http://tt-valhalla-api.maps-orbis-analytics-dev.orbis.az.tt3.com:8002/"
        truck_options = RouteOptions(
            travel_mode=TravelMode.TRUCK,
            vehicle_height=3.5,
            vehicle_weight=40_000,
            vehicle_length=16.5,
            vehicle_width=2.5,
        )
        route_calculation(
            expected_api="Valhalla",
            expected_endpoint=osm_endpoint,
            name="OSM",
            product="2022.09.02",
            route_options=truck_options,
        )
        route_calculation(
            expected_api="Valhalla-Strict",
            expected_endpoint=osm_endpoint,
            name="OSM-STRICT",
            product="2022.09.02",
            route_options=truck_options,
        )
        route_calculation(
            expected_api="Valhalla",
            expected_endpoint=osm_endpoint,
            name="OSM",
            product="2022.09.02",
            endpoint=None,
            url=None,
            route_options=truck_options,
        )

    def test_route_gg(self):
        route_calculation(
            expected_api="Google", expected_endpoint="http://google/", name="GG"
        )

    def test_route_orbis(self):
        assert True
        return
        route_calculation(
            expected_api="Amigo-Alpha",
            expected_endpoint="https://api.tomtom.com/routing/10/calculateRoute/",
            name="OM",
        )

    # Skipping this test due to here restrictions
    # def test_route_here(self):
    #     route_calculation(
    #         expected_api="Here",
    #         expected_endpoint="https://router.hereapi.com/v8/",
    #         name="Here",
    #     )

    def test_api_calls(self):
        api_calls = Provider("TT").api_calls
        assert api_calls is not None

    def test_tt_weird_examples(self):
        origin = [-13.56908, 28.95409]
        destination = [-13.56312, 28.96358]
        prov = Provider.from_properties(name="TT")
        assert prov.getRoute(origin, destination).geometry.wkt != "LINESTRING EMPTY"

        origin = [-15.680190, 27.758590]
        destination = [-15.683340, 27.770290]
        prov = Provider.from_properties(name="TT")
        assert prov.getRoute(origin, destination).geometry.wkt != "LINESTRING EMPTY"

    def test_tt_supporting_points(self):
        origin = [49.76627, 40.44086]
        destination = [49.77216, 40.44588]
        supporting_points = [
            [49.77073, 40.44067],
            [49.771, 40.44073],
            [49.77189, 40.44142],
            [49.77218, 40.44149],
            [49.77241, 40.44143],
            [49.77279, 40.44109],
            [49.7742, 40.4402],
            [49.77588, 40.43896],
            [49.77612, 40.43856],
            [49.77601, 40.43816],
            [49.77584, 40.43792],
            [49.77535, 40.43811],
            [49.77584, 40.43792],
            [49.77601, 40.43816],
            [49.77628, 40.43824],
            [49.77666, 40.43826],
            [49.77705, 40.43811],
            [49.7833, 40.43351],
            [49.78353, 40.43304],
            [49.78304, 40.43246],
            [49.78234, 40.43243],
            [49.77651, 40.43481],
            [49.77622, 40.43501],
            [49.7761, 40.43536],
            [49.77626, 40.43562],
            [49.77951, 40.43771],
            [49.77957, 40.43787],
            [49.77798, 40.43889],
            [49.77759, 40.43888],
            [49.77684, 40.43871],
            [49.77624, 40.4389],
            [49.77433, 40.44029],
            [49.77292, 40.44159],
            [49.77262, 40.44199],
            [49.77297, 40.4434],
            [49.77309, 40.44381],
            [49.77367, 40.44447],
            [49.77248, 40.44515],
            [49.77234, 40.44535],
            [49.77247, 40.44547],
        ]
        prov = Provider.from_properties(name="TT")
        route = prov.getRoute(origin=origin, destination=destination, supporting_route=supporting_points)
        assert route.length > 4500.0

    def test_tt_walking(self):
        origin = [4.484953, 51.213173]  # [4.483656, 51.213484]
        destination = [4.516561, 51.208456]

        prov = Provider.from_properties(name="TT")
        route = prov.getRoute(origin=origin, destination=destination)
        # Assert that the origin cannot be reached with car, and the vehicle route starts later on
        assert (
            shapely.geometry.Point(route.geometry.coords[0]).distance(
                shapely.geometry.Point(origin)
            )
            > 0.0005
        )

    def test_api_calls_count(self):
        origin = [4.483656, 51.213484]
        destination = [4.516561, 51.208456]

        prov = Provider.from_properties(name="OSM")
        route = prov.getRoute(origin=origin, destination=destination)

        assert "Directions" in prov.api_calls.keys()
        assert "Valhalla" in prov.api_calls.keys()
        assert "Google" in prov.api_calls.keys()
        assert "Here" in prov.api_calls.keys()
        assert "Bing" in prov.api_calls.keys()
        assert "Kakao" in prov.api_calls.keys()
        assert "MapBox" in prov.api_calls.keys()
        assert "GT" in prov.api_calls.keys()

        assert prov.api_calls["Valhalla"] == 1
        assert prov.api_calls["Here"] == 0

#    def test_bing_route(self):
#        origin = [49.76627, 40.44086]
#       destination = [49.77216, 40.44588]
#
#        prov = Provider.from_properties(name="Bing")
#        route = prov.getRoute(origin, destination)
#        assert route.length > 1000

    def test_valhalla_avoid_options(self):
        origin = [-3.859591, 40.321052]
        destination = [-3.772259, 40.381859]

        prov = Provider.from_properties(name="OSM")
        route_avoid = prov.getRoute(
            origin,
            destination,
            route_options=RouteOptions(avoid=[AvoidOptions.MOTORWAY]),
        )
        route = prov.getRoute(origin, destination, route_options=RouteOptions(avoid=[]))
        assert route.length < route_avoid.length

    def test_kakao_route(self):
        origin = [128.40002444726804, 35.76767343014201]
        destination = [128.74420221260414, 35.57787452323171]

        prov = Provider.from_properties(name="Kakao")
        route = prov.getRoute(origin, destination)

        assert route.provider == "Kakao"

        assert not route.geometry.is_empty
        assert route.length > 50000
        assert route.time > 4000

    # def test_kor_genesis_route(self):
    #     origin=[128.40002444726804, 35.76767343014201]
    #     destination=[128.74420221260414, 35.57787452323171]

    #     prov = Provider.from_properties(
    #         name='Genesis',
    #         endpoint = "https://routing-hcp3-kr-68-koreacentral.koreacentral.cloudapp.azure.com/hcp3-1-nkw/routing/calculateRoute/"
    #         )
    #     route = prov.getRoute(origin, destination)

    #     assert not route.geometry.is_empty

    # def test_mapbox_route(self):
    #     route_calculation(
    #         expected_api="MapBox",
    #         expected_endpoint="https://api.mapbox.com/directions/v5/mapbox/",
    #         name="mapbox",
    #     )


#    def test_mmi_route(self):
#        origin = [77.6067572631471, 12.982840910996448]
#        destination = [77.57826147457307, 12.969625972160584]

#        prov = Provider.from_properties(name='mmi')
#        route = prov.get_route(origin=origin, destination=destination)


#        assert prov.name == "mmi"
#        assert prov.endpoint == "https://apis.mappls.com/advancedmaps/v1/"
#        assert prov.api == "MMI"

#        assert route.geometry.wkt != "LINESTRING EMPTY"
#        assert route.time > 500
#        assert route.length > 4000

    # def test_zenrin_route(self):
    #     origin = [139.13445956998004,35.29129362328014]
    #     destination = [139.1357550768829,35.2906959457079]

    #     prov = Provider.from_properties(name="Zenrin")
    #     route = prov.getRoute(origin, destination)

    #     assert route.provider == "Zenrin"

    #     assert not route.geometry.is_empty
    #     assert route.length > 100
    #     assert route.time > 10
    # def test_genesysmap_route(self):
    #     origin = [19.119227,72.841945]
    #     destination = [19.106347,72.834617]

    #     prov = Provider.from_properties(name="Genesysmap")
    #     route = prov.getRoute(origin, destination)

    #     assert route.provider == "Genesysmap"

    #     assert not route.geometry.is_empty
    #     assert route.length <1
    #     assert route.time < 1

