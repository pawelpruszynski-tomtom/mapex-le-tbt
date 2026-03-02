"""
This module contains an example test.

Tests should be placed in ``src/tests``, in modules that mirror your
project's structure, and in files named test_*.py. They are simply functions
named ``test_*`` which test a unit of logic.

To run the tests, run ``kedro test`` from the project root directory.
"""

import random

import pandas as pd
import pyspark.sql
import pyspark.sql.types as T
import pytest
import shapely.geometry
import shapely.wkt

import tbt.pipelines.inspection
import tbt.pipelines.sampling
from tbt.navutils.navutils.provider import Provider
from tbt.navutils.navutils.route import Route
from tbt.pipelines.sampling.nodes import (
    compliance_check,
    read_traces_fcd,
    tbt_sampling_entrypoint,
)

m14scope_raw = [
    ("9f70524", "Q1", "ESP"),
    ("ca8b17c", "Q1", "ESP"),
    ("ca8b4ca", "Q1", "ESP"),
    ("ca89e09", "Q1", "ESP"),
]

traces_raw = [
    (
        "9f70524",
        [
            {
                "org": "ps_psa",
                "trace": [
                    (-4.52542, 36.61394),
                    (-4.52386, 36.61434),
                    (-4.52233, 36.61473),
                    (-4.52096, 36.61534),
                    (-4.51984, 36.61614),
                    (-4.51902, 36.61705),
                ],
            }
        ],
    ),
    (
        "9f70524",
        [
            {
                "org": "ps_psa",
                "trace": [
                    (-4.5196, 36.61685),
                    (-4.52067, 36.61596),
                    (-4.52204, 36.61519),
                    (-4.52336, 36.6148),
                    (-4.52475, 36.61445),
                    (-4.52625, 36.61392),
                ],
            }
        ],
    ),
    (
        "9f70524",
        [
            {
                "org": "ps_psa",
                "trace": [
                    (-4.52003, 36.61635),
                    (-4.52165, 36.61534),
                    (-4.52354, 36.61477),
                    (-4.52552, 36.61419),
                ],
            }
        ],
    ),
    (
        "9f70524",
        [
            {
                "org": "ps_psa",
                "trace": [
                    (-4.51916, 36.61725),
                    (-4.52058, 36.61588),
                    (-4.52255, 36.615),
                    (-4.52478, 36.61447),
                ],
            }
        ],
    ),
    (
        "9f70524",
        [
            {
                "org": "ps_psa",
                "trace": [
                    (-4.51935, 36.61701),
                    (-4.52069, 36.61584),
                    (-4.52226, 36.61507),
                    (-4.52398, 36.61461),
                    (-4.52564, 36.61417),
                ],
            }
        ],
    ),
    (
        "9f70524",
        [
            {
                "org": "ps_tomtom_bridge",
                "trace": [
                    (-4.52582, 36.61376),
                    (-4.52531, 36.61393),
                    (-4.52451, 36.61414),
                    (-4.52398, 36.61426),
                    (-4.52318, 36.61445),
                    (-4.52266, 36.61459),
                    (-4.52191, 36.61485),
                    (-4.52144, 36.61507),
                    (-4.52078, 36.61545),
                    (-4.52017, 36.61593),
                    (-4.51962, 36.6164),
                    (-4.51929, 36.61674),
                    (-4.51887, 36.61729),
                ],
            }
        ],
    ),
    (
        "9f70524",
        [
            {
                "org": "ps_psa",
                "trace": [
                    (-4.51944, 36.61695),
                    (-4.52058, 36.61593),
                    (-4.52205, 36.61517),
                    (-4.5237, 36.61472),
                    (-4.52532, 36.61432),
                ],
            }
        ],
    ),
    (
        "9f70524",
        [
            {
                "org": "ps_psa",
                "trace": [
                    (-4.5201, 36.61628),
                    (-4.52158, 36.61536),
                    (-4.52323, 36.61481),
                    (-4.52485, 36.61439),
                ],
            }
        ],
    ),
    (
        "9f70524",
        [
            {
                "org": "ps_psa",
                "trace": [
                    (-4.5194, 36.61688),
                    (-4.52074, 36.61576),
                    (-4.52246, 36.61499),
                    (-4.52431, 36.61454),
                    (-4.52602, 36.61399),
                ],
            }
        ],
    ),
]


@pytest.fixture
def spark() -> pyspark.sql.SparkSession:
    _spark = (
        pyspark.sql.SparkSession.builder.master("local[*]")
        .appName("local-tests")
        .enableHiveSupport()
        .getOrCreate()
    )
    return _spark


@pytest.fixture
def traces(spark):
    return spark.createDataFrame(
        traces_raw,
        schema=T.StructType(
            [
                T.StructField("tile_id", T.StringType(), True),
                T.StructField(
                    "traces",
                    T.ArrayType(
                        T.StructType(
                            [
                                T.StructField("org", T.StringType(), True),
                                T.StructField(
                                    "trace",
                                    T.ArrayType(T.ArrayType(T.FloatType())),
                                    True,
                                ),
                            ]
                        )
                    ),
                ),
            ]
        ),
    ).rdd.map(lambda x: (x["tile_id"], x["traces"]))


@pytest.fixture
def tbt_m14scope_delta(spark):
    return spark.createDataFrame(
        m14scope_raw,
        schema=T.StructType(
            [
                T.StructField("tile_id", T.StringType()),
                T.StructField("quality", T.StringType()),
                T.StructField("country", T.StringType()),
            ]
        ),
    )


def mocked_compliance_check(origin, destination, avoid, endpoint):
    return random.random() < 0.5


def mocked_getRoute(origin, destination, supporting_route):
    return Route(
        geometry=shapely.wkt.loads(
            "LINESTRING (-6.17884 36.64473, -6.17868 36.64482, -6.17815 36.64514, -6.17783 36.64536, -6.17732 36.64576, -6.17727 36.6458, -6.17699 36.64606, -6.17683 36.64624, -6.17643 36.64672, -6.17606 36.64728, -6.17596 36.64746, -6.1757 36.64799, -6.17551 36.6485, -6.17538 36.64885, -6.1752 36.64916, -6.17512 36.64929, -6.17496 36.64954, -6.17466 36.64995, -6.17436 36.65033, -6.17432 36.65039, -6.17431 36.65039, -6.17428 36.65043, -6.17408 36.65071, -6.17358 36.65133, -6.173 36.6522, -6.17244 36.65307, -6.17221 36.65339, -6.17217 36.65344, -6.17213 36.65348, -6.17182 36.65375, -6.17085 36.65446, -6.17064 36.65462, -6.17022 36.65492, -6.1695 36.65551, -6.1694 36.6556, -6.16876 36.65624, -6.16846 36.65656, -6.16732 36.65793, -6.16695 36.6584, -6.16688 36.65849, -6.16675 36.65864, -6.1661 36.65938, -6.16584 36.65965, -6.16563 36.65985, -6.16525 36.66015, -6.1651 36.66025, -6.16479 36.66043, -6.16272 36.66151, -6.16163 36.66207, -6.16072 36.66256, -6.15996 36.66303, -6.15936 36.6635, -6.15877 36.66401, -6.15813 36.66458, -6.15764 36.66502, -6.15678 36.6658, -6.15621 36.66629, -6.15586 36.66661, -6.15545 36.66704, -6.15517 36.6674, -6.15498 36.66772, -6.15479 36.66801, -6.15465 36.66838, -6.15463 36.66847, -6.15459 36.66858, -6.15454 36.66871, -6.15452 36.66876, -6.1545 36.66883, -6.15357 36.67137, -6.15321 36.67229, -6.15317 36.67236, -6.15302 36.67266, -6.15286 36.67291, -6.15257 36.67325, -6.15234 36.67345, -6.15222 36.67357, -6.15194 36.67376, -6.1512 36.6741, -6.14977 36.67485, -6.14942 36.67505, -6.14918 36.67502, -6.14894 36.67504, -6.14864 36.67514, -6.14845 36.67527, -6.14834 36.67538, -6.14832 36.67542, -6.14828 36.67548, -6.14823 36.67558, -6.14823 36.67562, -6.14822 36.67564, -6.1482 36.67578, -6.148 36.67602, -6.1479 36.67611, -6.14783 36.67614, -6.14752 36.67626, -6.1474 36.67632, -6.14712 36.67645, -6.14672 36.67666, -6.14603 36.67702, -6.14523 36.67748, -6.14499 36.67762, -6.14495 36.67764, -6.14476 36.67774, -6.14461 36.67783, -6.14454 36.67781, -6.1445 36.6778, -6.14447 36.6778, -6.14442 36.67781, -6.14438 36.67781, -6.14436 36.67782, -6.1443 36.67786, -6.14427 36.67789, -6.14425 36.67794, -6.14424 36.678, -6.14425 36.67803, -6.14387 36.67823, -6.14369 36.67833, -6.14314 36.67862, -6.14299 36.6786, -6.14283 36.67859, -6.14228 36.67851, -6.14215 36.67849, -6.14179 36.67844, -6.1415 36.6784, -6.14134 36.67838, -6.14097 36.67834, -6.14075 36.67834, -6.14047 36.67834, -6.14029 36.67834, -6.1401 36.67834, -6.1397 36.67835, -6.13917 36.67836, -6.13906 36.67836, -6.13875 36.67834, -6.13781 36.6781, -6.13774 36.6781, -6.13752 36.67805, -6.13742 36.67802, -6.13735 36.67799, -6.13722 36.67792, -6.13704 36.67782, -6.1369 36.67776, -6.13674 36.67767, -6.13623 36.67743, -6.1356 36.67715, -6.1353 36.677, -6.13497 36.67689, -6.13492 36.67687, -6.13471 36.67682, -6.13438 36.67671, -6.13371 36.67655, -6.13333 36.67644, -6.13326 36.67642, -6.13311 36.67643, -6.13263 36.67654, -6.13201 36.67668, -6.13144 36.67683, -6.13126 36.67691, -6.13118 36.67696, -6.131 36.67716, -6.13053 36.67772, -6.13037 36.67795, -6.13031 36.67804, -6.13009 36.67833, -6.12993 36.67856, -6.12983 36.67863, -6.12981 36.67866, -6.12978 36.6787, -6.12974 36.67884, -6.12968 36.67894, -6.12964 36.67903, -6.12951 36.67928, -6.12937 36.6795, -6.12918 36.67982, -6.12906 36.68002, -6.12886 36.6804, -6.12869 36.68066, -6.12866 36.68069, -6.12862 36.68072, -6.1286 36.68073, -6.12857 36.68074, -6.12847 36.68074, -6.12837 36.68075, -6.12831 36.68074, -6.12826 36.68073, -6.12821 36.68073, -6.12802 36.68067, -6.12786 36.68064, -6.12715 36.68057, -6.12692 36.68057, -6.12682 36.6806, -6.12675 36.68062, -6.12658 36.68069, -6.12648 36.68073, -6.12643 36.68075, -6.12638 36.68076, -6.12635 36.68076, -6.12627 36.68076, -6.12625 36.68073, -6.12622 36.68071, -6.12619 36.6807, -6.12613 36.68069, -6.12608 36.68069, -6.12604 36.6807, -6.12569 36.68083, -6.12567 36.68085, -6.12566 36.68087, -6.12565 36.68089, -6.12564 36.68092, -6.12565 36.68095, -6.12566 36.68099, -6.12569 36.68102, -6.12572 36.68104, -6.12556 36.68115, -6.12545 36.68123, -6.12543 36.68125, -6.12537 36.6813, -6.12526 36.68139, -6.12511 36.68151, -6.12469 36.68187, -6.12435 36.68224, -6.12366 36.68307, -6.12307 36.6838, -6.12301 36.68388, -6.12289 36.68401, -6.12276 36.68417, -6.12231 36.68472, -6.12224 36.68482, -6.12221 36.68485, -6.12216 36.68491, -6.1221 36.68495, -6.12206 36.68497, -6.12201 36.68499, -6.12192 36.68502, -6.12187 36.68501, -6.12181 36.68501, -6.12176 36.68502, -6.12171 36.68504, -6.12166 36.68507, -6.12164 36.6851, -6.12159 36.68516, -6.12158 36.6852, -6.12158 36.68521, -6.12158 36.68526, -6.12159 36.6853, -6.12161 36.68533, -6.12165 36.68538, -6.12168 36.6854, -6.12174 36.68543, -6.12177 36.68557, -6.12176 36.68622, -6.12175 36.68704, -6.12176 36.68729, -6.12173 36.68776, -6.12171 36.68787, -6.12168 36.68795, -6.12162 36.68804, -6.1216 36.68804, -6.12154 36.68806, -6.1215 36.68809, -6.12146 36.68813, -6.12144 36.68817, -6.12143 36.68824, -6.12143 36.6883, -6.12147 36.68836, -6.12151 36.6884, -6.12156 36.68843, -6.12159 36.68844, -6.12166 36.68851, -6.12169 36.68856, -6.12172 36.68866, -6.12179 36.68898, -6.12179 36.68904, -6.12179 36.68919, -6.12177 36.68937, -6.12161 36.68963, -6.1212 36.69028, -6.1211 36.69046, -6.12096 36.6908, -6.12085 36.69113, -6.1205 36.69216, -6.12041 36.69232, -6.12035 36.69239, -6.12021 36.69252, -6.12013 36.69257, -6.12002 36.69263, -6.11987 36.69269, -6.11969 36.69273, -6.11953 36.69274, -6.11928 36.69273, -6.11912 36.6927, -6.11884 36.69267, -6.11866 36.69266, -6.11855 36.69268, -6.11828 36.69277, -6.11814 36.69283, -6.11798 36.69294, -6.11786 36.69305, -6.11766 36.69336, -6.11718 36.69429, -6.11707 36.69429, -6.11705 36.69429, -6.11683 36.69422, -6.11677 36.69417, -6.11667 36.69401, -6.11663 36.69394, -6.11652 36.6938, -6.11644 36.69372, -6.11638 36.69367, -6.11615 36.69354, -6.1159 36.69346, -6.11563 36.69338, -6.11542 36.69328, -6.11519 36.69313, -6.11462 36.69273, -6.11401 36.6923, -6.11386 36.6922, -6.11359 36.69201, -6.11333 36.69182, -6.1128 36.69144, -6.11209 36.69095)"
        ).coords[::],
        provider="TT",
        length=700,
        time=120,
    )


class TestPipelines:
    def test_tbt_sampling_entrypoint(self, traces, tbt_m14scope_delta, mocker):
        mocker.patch(
            "tbt.pipelines.sampling.nodes.read_traces_fcd", return_value=traces
        )
        mocker.patch(
            "tbt.pipelines.sampling.nodes.read_traces_fcd", return_value=traces
        )
        mocker.patch(
            "tbt.pipelines.sampling.nodes.compliance_check", mocked_compliance_check
        )
        mocker.patch("tbt.pipelines.sampling.nodes.Provider.getRoute", mocked_getRoute)

        tbt_db = {}
        mocker.patch(
            "tbt.pipelines.sampling.nodes.country_data_query",
            return_value={
                "feat_id": None,
                "geom": shapely.geometry.Polygon(
                    [(-180, -90), (180, -90), (180, 90), (-180, 90)]
                ),
                "country_iso": "ESP",
            },
        )

        routes, metadata = tbt_sampling_entrypoint(
            sample_options={
                "country": "ESP",
                "max_routes": {"Q1": 10, "Q2": 8, "Q3": 2},
                "trace_limit": 10000,
                "endpoint": "http://tt-valhalla-api.westeurope.azurecontainer.io:8002/",
                "osm_map_version": "2022.06.24",
                "sample_q": "development",
            },
            tbt_m14scope_delta=tbt_m14scope_delta,
            fcd_credentials=None,
            run_id="auto",
            tbt_db=tbt_db,
        )

    def test_tbt_sampling(self):
        pipeline = tbt.pipelines.sampling.create_pipeline()

    def test_compliance_check(self):
        origin = [-3.526829957962036, 40.34077835083008]
        destination = [-3.532499074935913, 40.33879470825195]
        prov = Provider.from_properties(name="OSM-STRICT")
        comp = compliance_check(
            origin=origin,
            destination=destination,
            avoid=[
                "motorway",
                "trunk",
                "primary",
                "cycleway",
                "service",
                "pedestrian",
                "track",
                "construction",
                "service_other",
                "path",
                "dirt",
            ],
            endpoint=prov.endpoint,
        )
        assert comp


TRACES = [
    {
        "org": "muskoka",
        "trace": [
            (-3.70328, 40.41668),
            (-3.70328, 40.41668),
            (-3.70328, 40.41668),
            (-3.70328, 40.41668),
            (-3.70329, 40.41661),
            (-3.70329, 40.4166),
            (-3.70326, 40.41614),
            (-3.70324, 40.41584),
            (-3.70323, 40.41524),
            (-3.70326, 40.41506),
            (-3.70327, 40.41503),
            (-3.70331, 40.41488),
            (-3.70328, 40.41668),
            (-3.70328, 40.41668),
            (-3.70328, 40.41668),
            (-3.70328, 40.41668),
            (-3.70329, 40.41661),
            (-3.70329, 40.4166),
            (-3.70326, 40.41614),
            (-3.70324, 40.41584),
            (-3.70323, 40.41524),
            (-3.70326, 40.41506),
            (-3.70327, 40.41503),
            (-3.70331, 40.41488),
        ],
    },
    {
        "org": "muskoka",
        "trace": [
            (-3.70328, 40.41668),
            (-3.70329, 40.41661),
            (-3.70329, 40.4166),
            (-3.70326, 40.41614),
            (-3.70324, 40.41584),
            (-3.70323, 40.41524),
            (-3.70326, 40.41506),
            (-3.70327, 40.41503),
            (-3.70331, 40.41488),
            (-3.70328, 40.41668),
            (-3.70328, 40.41668),
            (-3.70328, 40.41668),
            (-3.70328, 40.41668),
            (-3.70329, 40.41661),
            (-3.70329, 40.4166),
            (-3.70326, 40.41614),
            (-3.70324, 40.41584),
            (-3.70323, 40.41524),
            (-3.70326, 40.41506),
            (-3.70327, 40.41503),
            (-3.70331, 40.41488),
        ],
    },
    {
        "org": "muskoka",
        "trace": [
            (-3.70328, 40.41668),
            (-3.70329, 40.41661),
            (-3.70329, 40.4166),
            (-3.70326, 40.41614),
            (-3.70324, 40.41584),
            (-3.70323, 40.41524),
            (-3.70326, 40.41506),
            (-3.70327, 40.41503),
            (-3.70331, 40.41488),
            (-3.70328, 40.41668),
            (-3.70328, 40.41668),
            (-3.70328, 40.41668),
            (-3.70328, 40.41668),
            (-3.70329, 40.41661),
            (-3.70329, 40.4166),
            (-3.70326, 40.41614),
            (-3.70324, 40.41584),
            (-3.70323, 40.41524),
            (-3.70326, 40.41506),
            (-3.70327, 40.41503),
            (-3.70331, 40.41488),
        ],
    },
]


def test_stream_route_sampler(mocker):
    tiles_df = pd.DataFrame(
        data=[
            ["tile_id_1", TRACES, "Q1", "GBR", "run_id"],
            ["tile_id_2", TRACES, "Q1", "GBR", "run_id"],
            ["tile_id_3", TRACES, "Q1", "GBR", "run_id"],
        ],
        columns=["tile_id", "traces", "quality", "country", "run_id"],
    )
    iterator = iter([tiles_df])
    provider = "OSM-STRICT"
    endpoint = "fake_enpoint"
    mocker.patch(
        "tbt.navutils.navutils.provider.Valhalla.get_route",
        return_value=Route(
            geometry=[
                [-18.00422, 27.75411],
                [-18.01009, 27.75498],
                [-18.01009, 27.75498],
                [-18.01009, 27.75498],
                [-18.01009, 27.75498],
                [-18.01009, 27.75498],
                [-18.01009, 27.75498],
                [-18.01009, 27.75498],
                [-18.01009, 27.75498],
                [-18.01009, 27.75498],
                [-18.01009, 27.75498],
                [-18.01009, 27.75498],
                [-18.01009, 27.75498],
            ],  # Just origin and destination
            provider="TT",
            length=1000.0,  # Fake length
            time=100.0,  # Fake time
        ),
    )
    mocker.patch("shapely.geometry.Point.distance", return_value=0.006)
    mocker.patch("tbt.pipelines.sampling.nodes.compliance_check", return_value=True)
    mocker.patch("tbt.pipelines.sampling.nodes.overlap_cond", return_value=False)

    result = next(
        tbt.pipelines.sampling.nodes.stream_route_sampler(iterator, provider, endpoint)
    )
    assert isinstance(result, pd.DataFrame)
    assert result.shape == (9, 8)

    assert "tile_id" in result.columns
    tile_ids = result.tile_id.unique().tolist()
    assert "tile_id_1" in tile_ids
    assert "tile_id_2" in tile_ids
    assert "tile_id_3" in tile_ids

    assert "route_id" in result.columns
    route_ids = result.route_id.unique().tolist()
    for route_id in route_ids:
        assert isinstance(route_id, str)
        random_uuid = route_id.split("-")
        assert len(random_uuid[0]) == 8
        assert len(random_uuid[1]) == 4
        assert len(random_uuid[2]) == 4
        assert len(random_uuid[3]) == 4
        assert len(random_uuid[4]) == 12

    assert "origin" in result.columns
    origins = result.origin.unique().tolist()
    for origin in origins:
        assert isinstance(origin, str)
        shapely.wkt.loads(origin)

    assert "destination" in result.columns
    destinations = result.destination.unique().tolist()
    for destination in destinations:
        assert isinstance(destination, str)
        shapely.wkt.loads(destination)

    assert "org" in result.columns
    orgs = result.org.unique()
    assert len(orgs) == 1
    assert orgs[0] == "muskoka"

    assert "run_id" in result.columns
    run_ids = result.run_id.unique()
    assert len(run_ids) == 1
    assert run_ids[0] == "run_id"

    assert "quality" in result.columns
    qualitys = result.quality.unique()
    assert len(qualitys) == 1
    assert qualitys[0] == "Q1"

    assert "country" in result.columns
    countrys = result.country.unique()
    assert len(countrys) == 1
    assert countrys[0] == "GBR"
