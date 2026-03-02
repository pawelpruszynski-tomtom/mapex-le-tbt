# """
# This module contains an example test.

# Tests should be placed in ``src/tests``, in modules that mirror your
# project's structure, and in files named test_*.py. They are simply functions
# named ``test_*`` which test a unit of logic.

# To run the tests, run ``kedro test`` from the project root directory.
# """

from datetime import date, datetime

import pandas
import pyspark.sql
import pyspark.sql.types as T
import pytest

import tbt.pipelines.inspection
import tbt.pipelines.inspection.nodes


@pytest.fixture
def spark() -> pyspark.sql.SparkSession:
    _spark = (
        pyspark.sql.SparkSession.builder.master("local[*]")
        .appName("local-tests")
        .enableHiveSupport()
        .getOrCreate()
    )
    return _spark


tbt_sampling_metadata_example = [
    {
        "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
        "metric": "TbT",
    }
]

tbt_sampling_example = [
    {
        "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
        "tile_id": "9ff6c6f",
        "origin": "POINT (-1.8671220541000366 43.32010269165039)",
        "destination": "POINT (-1.8618359565734863 43.323184967041016)",
        "route_id": "5cc09f6d-5124-409a-bdcc-441d534b90a6",
        "quality": "Q2",
        "country": "ESP",
        "date_generated": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        "org": "fleet-tomtom",
    },
    {
        "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
        "tile_id": "9ff6c6f",
        "origin": "POINT (-1.8505699634552002 43.32994842529297)",
        "destination": "POINT (-1.8579200506210327 43.32857131958008)",
        "route_id": "537b4755-1583-45ed-991d-f7f42a00ce5d",
        "quality": "Q2",
        "country": "ESP",
        "date_generated": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        "org": "muskoka",
    },
    {
        "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
        "tile_id": "9ff6c6f",
        "origin": "POINT (-1.8667099475860596 43.32741165161133)",
        "destination": "POINT (-1.857159972190857 43.32851028442383)",
        "route_id": "38417d31-433a-40cd-b04c-dfe992d56f31",
        "quality": "Q2",
        "country": "ESP",
        "date_generated": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        "org": "ps_hyundai",
    },
    {
        "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
        "tile_id": "9f7dc9d",
        "origin": "POINT (-0.5375000238418579 38.434959411621094)",
        "destination": "POINT (-0.5434399843215942 38.43162155151367)",
        "route_id": "fb88752a-6fa3-40f0-bf23-47fb7f141023",
        "quality": "Q2",
        "country": "ESP",
        "date_generated": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        "org": "perseus_alliance_st30",
    },
    {
        "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
        "tile_id": "9f7dc9d",
        "origin": "POINT (-0.5376799702644348 38.440128326416016)",
        "destination": "POINT (-0.5306400060653687 38.43408966064453)",
        "route_id": "c0dbb1c2-8632-4f92-be7b-ab76a791f7d6",
        "quality": "Q2",
        "country": "ESP",
        "date_generated": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        "org": "muskoka-batch",
    },
    {
        "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
        "tile_id": "9f7dc9d",
        "origin": "POINT (-0.5469899773597717 38.437068939208984)",
        "destination": "POINT (-0.5410199761390686 38.43558883666992)",
        "route_id": "34b3e3c5-923e-48a6-a566-6243122b4668",
        "quality": "Q2",
        "country": "ESP",
        "date_generated": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        "org": "hk",
    },
    {
        "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
        "tile_id": "9fd9098",
        "origin": "POINT (-4.128320217132568 40.89313888549805)",
        "destination": "POINT (-4.129059791564941 40.90121841430664)",
        "route_id": "b7b2ba6a-b2ce-431e-bb05-216edb99ca4d",
        "quality": "Q2",
        "country": "ESP",
        "date_generated": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        "org": "muskoka-batch",
    },
    {
        "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
        "tile_id": "9fd3934",
        "origin": "POINT (-3.725260019302368 40.479190826416016)",
        "destination": "POINT (-3.7202301025390625 40.48128890991211)",
        "route_id": "ff526198-076d-4b9e-9a42-e818eda12ac2",
        "quality": "Q1",
        "country": "ESP",
        "date_generated": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        "org": "muskoka-batch",
    },
    {
        "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
        "tile_id": "9fd3934",
        "origin": "POINT (-3.7145299911499023 40.48006820678711)",
        "destination": "POINT (-3.7188799381256104 40.47502136230469)",
        "route_id": "59a7c900-070e-4308-92e4-798cdbf1ced3",
        "quality": "Q1",
        "country": "ESP",
        "date_generated": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        "org": "ps_renault",
    },
    {
        "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
        "tile_id": "9fd3934",
        "origin": "POINT (-3.731300115585327 40.477149963378906)",
        "destination": "POINT (-3.723759889602661 40.4810791015625)",
        "route_id": "70c13f8c-6925-4193-a6b0-943b74df559b",
        "quality": "Q1",
        "country": "ESP",
        "date_generated": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        "org": "muskoka-batch",
    },
]

tbt_new_critical_sections_example_raw = [
    {
        "route_id": "63db61e6-6c6e-422a-a686-db805a9029a4",
        "case_id": "7ed924d2-c906-4005-90c4-7b95fffeff1a",
        "stretch": "LINESTRING (5.477674910380154 51.2598238578413, 5.477572111937919 51.26021042281678)",
        "stretch_length": 43.575195,
    },
    {
        "route_id": "63e4e18a-df1c-44f1-9956-328be44677cd",
        "case_id": "9c321959-4cb5-46e5-aea5-733ebf3aae82",
        "stretch": "LINESTRING (4.300347431487025 52.11176749720925, 4.300215 52.111807, 4.299772 52.111911, 4.299454 52.111978, 4.299326 52.112001, 4.299009 52.112058, 4.298687 52.112107, 4.298159 52.11216, 4.297444 52.11222, 4.296271 52.112427, 4.295499 52.112572, 4.294695 52.112724, 4.294554 52.112753, 4.294037 52.11289, 4.293758 52.112975, 4.293645 52.112996, 4.293423 52.113031, 4.293063 52.113128, 4.292925 52.113157, 4.292761 52.113188, 4.292423 52.113229, 4.291572 52.113353, 4.289601 52.113612, 4.288590947107238 52.11373833109927)",
        "stretch_length": 834.82,
    },
]


@pytest.fixture
def tbt_options_fcd():
    return {
        "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
        "provider": "TT",
        "competitor": "GG",
        "endpoint": None,
        "product": "2022.06.24",
        "mapdate": date(2022, 6, 24),
        "ignore_previous_inspections": True,
        "error_classification_mode": "ML",
    }


@pytest.fixture
def tbt_sampling_example_sdf(spark):
    return spark.createDataFrame(
        tbt_sampling_example,
        schema=T.StructType(
            [
                T.StructField("sample_id", T.StringType()),
                T.StructField("tile_id", T.StringType()),
                T.StructField("origin", T.StringType()),
                T.StructField("destination", T.StringType()),
                T.StructField("route_id", T.StringType()),
                T.StructField("quality", T.StringType()),
                T.StructField("country", T.StringType()),
                T.StructField("date_generated", T.DateType()),
                T.StructField("org", T.StringType()),
            ]
        ),
    )


@pytest.fixture
def tbt_sampling_metadata_example_sdf(spark):
    return spark.createDataFrame(
        tbt_sampling_metadata_example,
        schema=T.StructType(
            [
                T.StructField("sample_id", T.StringType()),
                T.StructField("metric", T.StringType()),
            ]
        ),
    )

@pytest.fixture
def ml_model_outcome():
    pdf = pandas.DataFrame(
        [
            {
                "route_id": "b7b2ba6a-b2ce-431e-bb05-216edb99ca4d",
                "case_id": "901bb733-8d33-44bd-88fa-73719017d2a4",
                "stretch": "LINESTRING (-4.132589220214242 40.90164285921445, -4.13253 40.90186, -4.13244 40.90201, -4.13231 40.90194, -4.13223 40.9019, -4.13218 40.90189, -4.13212 40.90186, -4.132 40.90184, -4.13192 40.90182, -4.13152 40.90169, -4.13139 40.90166, -4.13093 40.90164, -4.13086 40.90163, -4.13085 40.90163, -4.13082 40.90163, -4.13066 40.90158, -4.13061 40.90157, -4.13055 40.90157, -4.1305 40.90158, -4.13045 40.90159, -4.13034 40.90164, -4.13031 40.90165, -4.13021 40.90166, -4.13011 40.90166, -4.12988 40.90165, -4.12945 40.90159, -4.12935 40.90158, -4.12933 40.90159, -4.12931 40.90159, -4.12928 40.90162)",
                "stretch_length": 323.8656005859375,
                "fcd_state": "potential_error",
                "pra": -1.0,
                "prb": -1.0,
                "prab": -1.0,
                "lift": -1.0,
                "tot": -1,
            },
            {
                "route_id": "c0dbb1c2-8632-4f92-be7b-ab76a791f7d6",
                "case_id": "98424cfb-fa0d-479b-b7b9-fa6783f1fb3a",
                "stretch": "LINESTRING (-0.5319001165694026 38.43638003138407, -0.5319 38.43638, -0.53178 38.43633, -0.53161 38.43626, -0.53116 38.43603, -0.5311399999999999 38.43602, -0.53113 38.43601, -0.53101 38.43589, -0.53085 38.43568, -0.53083 38.43566, -0.53069 38.4354, -0.53059 38.43518, -0.5305800000000001 38.43516, -0.5305299999999999 38.43496, -0.53047 38.4348, -0.53046 38.43476, -0.53047 38.43472, -0.53048 38.43469, -0.5306860094859545 38.43452261729266)",
                "stretch_length": 265.1271667480469,
                "fcd_state": "potential_error",
                "pra": -1.0,
                "prb": -1.0,
                "prab": -1.0,
                "lift": -1.0,
                "tot": -1,
            },
            {
                "route_id": "3c55ee9f-c913-4cf5-b272-8f610b633b4f",
                "case_id": "31703eb9-1067-40f9-aeb8-221338a2cc4a",
                "stretch": "LINESTRING (-8.84168 43.05197, -8.84206 43.05198, -8.84212 43.05199, -8.842650000000001 43.05206, -8.8429 43.05208, -8.84342 43.05207, -8.843540000000001 43.05208, -8.843590000000001 43.0521, -8.84365 43.05214, -8.8437 43.05212, -8.843769999999999 43.05211, -8.8439 43.05212, -8.84401012789951 43.05212)",
                "stretch_length": 193.40196228027344,
                "fcd_state": "potential_error",
                "pra": -1.0,
                "prb": -1.0,
                "prab": -1.0,
                "lift": -1.0,
                "tot": -1,
            },
            {
                "route_id": "7b99a669-a32e-43ee-8bd4-1b90909cbf83",
                "case_id": "866f2358-bcdd-4cf3-a5a4-e1cf7496ef99",
                "stretch": "LINESTRING (-3.821331914835427 40.30526025034327, -3.82116 40.30535, -3.82106 40.30539, -3.82087 40.30546, -3.82065 40.30552, -3.820500033595686 40.30544033034771)",
                "stretch_length": 80.44650268554688,
                "fcd_state": "potential_error",
                "pra": -1.0,
                "prb": -1.0,
                "prab": -1.0,
                "lift": -1.0,
                "tot": -1,
            },
        ]
    )
    ec_model_log_table = pandas.DataFrame(
        [
            {
                "inspection_date": date(2023, 6, 8),
                "country": "ESP",
                "provider": "TT",
                "run_id": "dcc56735-b9ff-468a-9ab5-9743e6665ed2",
                "route_id": "b7b2ba6a-b2ce-431e-bb05-216edb99ca4d",
                "case_id": "7b6ad27a-ad5f-4179-84a2-2701a0e41c9a",
                "route": "LINESTRING (-4.13157 40.89393, -4.13152 40.89407, -4.13138 40.89477, -4.13137 40.89491, -4.13141 40.89516, -4.13148 40.89538, -4.13161 40.89562, -4.13186 40.89598, -4.1319 40.89602, -4.13194 40.89606, -4.13209 40.89617, -4.13212 40.8962, -4.13236 40.89632, -4.13255 40.89645, -4.13263 40.89651, -4.13272 40.89658, -4.13274 40.89661, -4.13362 40.89775, -4.13374 40.89793, -4.13379 40.89802, -4.13384 40.89816, -4.13389 40.89837, -4.13395 40.89996, -4.13387 40.90026, -4.13383 40.90035, -4.13375 40.90045, -4.13369 40.9005, -4.13282 40.90128, -4.13269 40.90143, -4.13262 40.90153, -4.13253 40.90186, -4.13244 40.90201, -4.13231 40.90194, -4.13223 40.9019, -4.13218 40.90189, -4.13212 40.90186, -4.132 40.90184, -4.13192 40.90182, -4.13152 40.90169, -4.13139 40.90166, -4.13093 40.90164, -4.13086 40.90163, -4.13085 40.90163, -4.13082 40.90163, -4.13066 40.90158, -4.13061 40.90157, -4.13055 40.90157, -4.1305 40.90158, -4.13045 40.90159, -4.13034 40.90164, -4.13031 40.90165, -4.13021 40.90166, -4.13011 40.90166, -4.12988 40.90165, -4.12945 40.90159, -4.12935 40.90158, -4.12933 40.90159, -4.12931 40.90159, -4.12928 40.90162)",
                "stretch": "LINESTRING (-4.132589220214242 40.90164285921445, -4.13253 40.90186, -4.13244 40.90201, -4.13231 40.90194, -4.13223 40.9019, -4.13218 40.90189, -4.13212 40.90186, -4.132 40.90184, -4.13192 40.90182, -4.13152 40.90169, -4.13139 40.90166, -4.13093 40.90164, -4.13086 40.90163, -4.13085 40.90163, -4.13082 40.90163, -4.13066 40.90158, -4.13061 40.90157, -4.13055 40.90157, -4.1305 40.90158, -4.13045 40.90159, -4.13034 40.90164, -4.13031 40.90165, -4.13021 40.90166, -4.13011 40.90166, -4.12988 40.90165, -4.12945 40.90159, -4.12935 40.90158, -4.12933 40.90159, -4.12931 40.90159, -4.12928 40.90162)",
                "route_length": 1284.85302734375,
                "stretch_length": 324.43621826171875,
                "stretch_start_position_in_route": 0.7155143618583679,
                "stretch_end_position_in_route": 1.0,
                "stretch_starts_at_the_route_start": 0.0,
                "stretch_ends_at_the_route_end": 1.0,
                "heading_stretch": 29.74488067626953,
                "left_turns": 0,
                "straight_turns": 26,
                "right_turns": 2,
                "total_right_angle": 132.33700561523438,
                "max_right_angle": 87.33699798583984,
                "total_left_angle": 0.0,
                "max_left_angle": 0.0,
                "absolute_angle": 123.71407318115234,
                "has_left_turns": 0.0,
                "has_right_turns": 1.0,
                "min_curvature": 4.58146858215332,
                "max_curvature": 27106.921875,
                "mean_curvature": 3781.6982421875,
                "distance_start_end_stretch": 278.846923828125,
                "route_coverage": 0.25250840187072754,
                "stretch_covers_route": 0.0,
                "tortuosity": 0.07429777830839157,
                "sinuosity": 0.8594815731048584,
                "density": 0.09246809780597687,
                "drive_right_side": 0.0,
                "sdo_api_response": None,
                "traffic_signs": None,
                "pra": 0.0,
                "prb": 0.0,
                "prab": 0.0,
                "lift": -1.0,
                "tot": 0.0,
                "logged_model": "runs:/6b175f24a701411b8d2034a0ce74a300/model",
                "prediction": "potential_error",
                "probability": 0.2004166692495346,
            },
            {
                "inspection_date": date(2023, 6, 8),
                "country": "ESP",
                "provider": "TT",
                "run_id": "dcc56735-b9ff-468a-9ab5-9743e6665ed2",
                "route_id": "c0dbb1c2-8632-4f92-be7b-ab76a791f7d6",
                "case_id": "2c200523-4937-43ec-b48b-c703811ccfea",
                "route": "LINESTRING (-0.5377 38.44012, -0.53766 38.44002, -0.53765 38.44001, -0.53764 38.44, -0.53742 38.43976, -0.53741 38.43975, -0.53697 38.4395, -0.53695 38.43949, -0.53683 38.43939, -0.5368 38.43936, -0.53665 38.43918, -0.53643 38.43884, -0.53642 38.43883, -0.53624 38.43849, -0.53623 38.43847, -0.53621 38.43846, -0.53612 38.4384, -0.53596 38.43829, -0.53592 38.43827, -0.53571 38.43817, -0.5357 38.43816, -0.53554 38.43806, -0.5355 38.43803, -0.535 38.43775, -0.53471 38.43757, -0.5347 38.43756, -0.53468 38.43755, -0.53459 38.43742, -0.53453 38.43733, -0.5345 38.43728, -0.53449 38.43726, -0.5344 38.43705, -0.5344 38.43703, -0.53429 38.43685, -0.53428 38.43684, -0.53427 38.43682, -0.53425 38.43681, -0.53408 38.43672, -0.53406 38.43672, -0.53392 38.43669, -0.53344 38.4366, -0.53301 38.43654, -0.53221 38.43646, -0.53216 38.43645, -0.5319 38.43638, -0.53178 38.43633, -0.53161 38.43626, -0.53116 38.43603, -0.53114 38.43602, -0.53113 38.43601, -0.53101 38.43589, -0.53085 38.43568, -0.53083 38.43566, -0.53069 38.4354, -0.53059 38.43518, -0.53058 38.43516, -0.53053 38.43496, -0.53047 38.4348, -0.53046 38.43476, -0.53047 38.43472, -0.53048 38.43469, -0.5308 38.43443, -0.53082 38.4344, -0.53083 38.43437, -0.53081 38.43431, -0.53079 38.43427, -0.5307 38.43416, -0.53069 38.43414, -0.53065 38.43408)",
                "stretch": "LINESTRING (-0.5319001165694026 38.43638003138407, -0.5319 38.43638, -0.53178 38.43633, -0.53161 38.43626, -0.53116 38.43603, -0.53114 38.43602, -0.53113 38.43601, -0.53101 38.43589, -0.53085 38.43568, -0.53083 38.43566, -0.53069 38.4354, -0.53059 38.43518, -0.53058 38.43516, -0.53053 38.43496, -0.53047 38.4348, -0.53046 38.43476, -0.53047 38.43472, -0.53048 38.43469, -0.5306860094859545 38.434522617292664)",
                "route_length": 1024.6610107421875,
                "stretch_length": 265.0667419433594,
                "stretch_start_position_in_route": 0.6967644095420837,
                "stretch_end_position_in_route": 0.9468985199928284,
                "stretch_starts_at_the_route_start": 0.0,
                "stretch_ends_at_the_route_end": 0.0,
                "heading_stretch": -125.83765411376953,
                "left_turns": 0,
                "straight_turns": 17,
                "right_turns": 0,
                "total_right_angle": 0.0,
                "max_right_angle": 0.0,
                "total_left_angle": 0.0,
                "max_left_angle": 0.0,
                "absolute_angle": -30.120302200317383,
                "has_left_turns": 0.0,
                "has_right_turns": 0.0,
                "min_curvature": 41.300621032714844,
                "max_curvature": 586136000.0,
                "mean_curvature": 30856002.0,
                "distance_start_end_stretch": 231.8358154296875,
                "route_coverage": 0.25868725776672363,
                "stretch_covers_route": 0.0,
                "tortuosity": 0.049384281039237976,
                "sinuosity": 0.8746318221092224,
                "density": 0.07168006151914597,
                "drive_right_side": 0.0,
                "sdo_api_response": None,
                "traffic_signs": None,
                "pra": 0.0,
                "prb": 0.0,
                "prab": 0.0,
                "lift": -1.0,
                "tot": 0.0,
                "logged_model": "runs:/6b175f24a701411b8d2034a0ce74a300/model",
                "prediction": "potential_error",
                "probability": 0.14983156323432922,
            },
            {
                "inspection_date": date(2023, 6, 8),
                "country": "ESP",
                "provider": "TT",
                "run_id": "dcc56735-b9ff-468a-9ab5-9743e6665ed2",
                "route_id": "3c55ee9f-c913-4cf5-b272-8f610b633b4f",
                "case_id": "32db6cf8-a183-44ec-b3e1-8b5c1c5b3e5c",
                "route": "LINESTRING (-8.84168 43.05197, -8.84206 43.05198, -8.84212 43.05199, -8.84265 43.05206, -8.8429 43.05208, -8.84342 43.05207, -8.84354 43.05208, -8.84359 43.0521, -8.84365 43.05214, -8.8437 43.05212, -8.84377 43.05211, -8.8439 43.05212, -8.84402 43.05212, -8.84438 43.05217, -8.84473 43.0522, -8.84488 43.05224, -8.84509 43.05236, -8.84513 43.05238, -8.84533 43.05239, -8.84538 43.0524, -8.84549 43.05243, -8.84555 43.05246, -8.84563 43.05231, -8.84578 43.05217, -8.84586 43.05212, -8.84603 43.05203, -8.84615 43.05194, -8.84641 43.05168, -8.8465 43.05145, -8.847 43.05051, -8.84716 43.05026, -8.84743 43.04994, -8.84754 43.04983, -8.84766 43.04969, -8.84773 43.04955, -8.84775 43.04946, -8.84773 43.04927, -8.84775 43.04921, -8.84777 43.04918, -8.84784 43.04914, -8.84801 43.04929, -8.84845 43.04965, -8.84876 43.04988, -8.84881 43.04993, -8.84951 43.05054, -8.84971 43.05075, -8.84987 43.05098, -8.84998 43.05116, -8.85018 43.05163, -8.85034 43.05195, -8.85037 43.05199, -8.85061 43.05232, -8.85084 43.05258, -8.85104 43.05275, -8.85138 43.05301, -8.85142 43.05303, -8.85154 43.0531, -8.85188 43.0533)",
                "stretch": "LINESTRING (-8.84168 43.05197, -8.84206 43.05198, -8.84212 43.05199, -8.84265 43.05206, -8.8429 43.05208, -8.84342 43.05207, -8.84354 43.05208, -8.84359 43.0521, -8.84365 43.05214, -8.8437 43.05212, -8.84377 43.05211, -8.8439 43.05212, -8.84401012789951 43.05212)",
                "route_length": 1327.7518310546875,
                "stretch_length": 193.89430236816406,
                "stretch_start_position_in_route": 0.0,
                "stretch_end_position_in_route": 0.1680978238582611,
                "stretch_starts_at_the_route_start": 1.0,
                "stretch_ends_at_the_route_end": 0.0,
                "heading_stretch": -1.5074357986450195,
                "left_turns": 1,
                "straight_turns": 10,
                "right_turns": 0,
                "total_right_angle": 0.0,
                "max_right_angle": 0.0,
                "total_left_angle": 55.491477966308594,
                "max_left_angle": 55.491477966308594,
                "absolute_angle": -87.0007553100586,
                "has_left_turns": 1.0,
                "has_right_turns": 0.0,
                "min_curvature": 14.591257095336914,
                "max_curvature": 7226.9716796875,
                "mean_curvature": 1226.5576171875,
                "distance_start_end_stretch": 190.57041931152344,
                "route_coverage": 0.14603203535079956,
                "stretch_covers_route": 0.0,
                "tortuosity": 0.06453749537467957,
                "sinuosity": 0.9828572273254395,
                "density": 0.06704683601856232,
                "drive_right_side": 0.0,
                "sdo_api_response": None,
                "traffic_signs": None,
                "pra": 0.25,
                "prb": 0.75,
                "prab": 0.0,
                "lift": 0.0,
                "tot": 4.0,
                "logged_model": "runs:/6b175f24a701411b8d2034a0ce74a300/model",
                "prediction": "potential_error",
                "probability": 0.16186904907226562,
            },
            {
                "inspection_date": date(2023, 6, 8),
                "country": "ESP",
                "provider": "TT",
                "run_id": "dcc56735-b9ff-468a-9ab5-9743e6665ed2",
                "route_id": "e11caac5-0981-43f5-a49b-26cf30d188f2",
                "case_id": "3db65a6e-8471-4bcb-a7a2-2b3624d7bdf2",
                "route": "LINESTRING (-8.84349 43.04725, -8.84336 43.04726, -8.84283 43.04734, -8.84204 43.04747, -8.84056 43.04776, -8.84015 43.04777, -8.83968 43.04772, -8.83938 43.04766, -8.83945 43.04756, -8.83959 43.0476, -8.83978 43.04764, -8.83994 43.04766, -8.84028 43.04768, -8.8406 43.04767, -8.8408 43.04765, -8.84117 43.0476, -8.84123 43.04759, -8.84127 43.04755, -8.84151 43.04749, -8.8423 43.04734, -8.8429 43.04724, -8.8432 43.0472, -8.84361 43.04717, -8.84401 43.04717, -8.84405 43.04719, -8.8444 43.04722, -8.84464 43.04725, -8.84482 43.04728, -8.84504 43.04733, -8.84509 43.04726, -8.84511 43.04723, -8.84512 43.04719, -8.8451 43.04714, -8.84509 43.04708, -8.8451 43.04703, -8.84513 43.04697, -8.8452 43.0469, -8.84524 43.04687, -8.84538 43.04679, -8.84575 43.04657, -8.84633 43.04623, -8.84809 43.04517)",
                "stretch": "LINESTRING (-8.841974100653468 43.04748291270979, -8.84056 43.04776, -8.84015 43.04777, -8.83968 43.04772, -8.83938 43.04766, -8.83945 43.04756, -8.83959 43.0476, -8.839648289073379 43.04761227138387)",
                "route_length": 1179.000732421875,
                "stretch_length": 246.3857421875,
                "stretch_start_position_in_route": 0.11098980903625488,
                "stretch_end_position_in_route": 0.3249109983444214,
                "stretch_starts_at_the_route_start": 0.0,
                "stretch_ends_at_the_route_end": 0.0,
                "heading_stretch": 157.02493286132812,
                "left_turns": 1,
                "straight_turns": 4,
                "right_turns": 1,
                "total_right_angle": 70.95337677001953,
                "max_right_angle": 70.95337677001953,
                "total_left_angle": 113.68209075927734,
                "max_left_angle": 113.68209075927734,
                "absolute_angle": -49.8033332824707,
                "has_left_turns": 1.0,
                "has_right_turns": 1.0,
                "min_curvature": 16.62090301513672,
                "max_curvature": 6745.578125,
                "mean_curvature": 2291.862548828125,
                "distance_start_end_stretch": 190.0467529296875,
                "route_coverage": 0.20897844433784485,
                "stretch_covers_route": 0.0,
                "tortuosity": 0.1675305962562561,
                "sinuosity": 0.7713382840156555,
                "density": 0.032469410449266434,
                "drive_right_side": 0.0,
                "sdo_api_response": None,
                "traffic_signs": None,
                "pra": 0.0,
                "prb": 0.0,
                "prab": 0.0,
                "lift": -1.0,
                "tot": 0.0,
                "logged_model": "runs:/6b175f24a701411b8d2034a0ce74a300/model",
                "prediction": "potential_error",
                "probability": 0.4541666805744171,
            },
            {
                "inspection_date": date(2023, 6, 8),
                "country": "ESP",
                "provider": "TT",
                "run_id": "dcc56735-b9ff-468a-9ab5-9743e6665ed2",
                "route_id": "7b99a669-a32e-43ee-8bd4-1b90909cbf83",
                "case_id": "59714aa3-3c3b-4c05-a39e-8e93a883d5da",
                "route": "LINESTRING (-3.81999 40.30668, -3.82021 40.30658, -3.82049 40.30645, -3.82073 40.30636, -3.82098 40.30631, -3.82112 40.30628, -3.82121 40.30627, -3.8214 40.30625, -3.82146 40.30625, -3.822 40.3063, -3.82227 40.30634, -3.82233 40.30638, -3.82238 40.30639, -3.82246 40.30639, -3.82249 40.30638, -3.82254 40.30636, -3.82265 40.30632, -3.82269 40.3063, -3.82274 40.30628, -3.8229 40.30616, -3.82297 40.30601, -3.82302 40.30579, -3.82285 40.30534, -3.82282 40.30529, -3.82277 40.30511, -3.82278 40.30506, -3.82285 40.30495, -3.82296 40.30485, -3.82306 40.30471, -3.82324 40.30458, -3.82392 40.3041, -3.82426 40.3038, -3.82455 40.3035, -3.82482 40.30322, -3.82501 40.303, -3.82551 40.30242, -3.82563 40.30238, -3.82597 40.30208, -3.82601 40.30205, -3.82608 40.302, -3.82632 40.30183, -3.82636 40.30179, -3.82638 40.30178, -3.8265 40.30166, -3.82658 40.30158, -3.82667 40.30147, -3.82715 40.30093, -3.82722 40.30093, -3.82732 40.30091, -3.82752 40.30082, -3.82764 40.30075, -3.82771 40.30066, -3.82776 40.30056, -3.82778 40.3005, -3.82779 40.30045, -3.82779 40.30039, -3.82778 40.30033, -3.82776 40.30026, -3.82772 40.30017, -3.82765 40.3001, -3.82757 40.30004, -3.82749 40.3, -3.82736 40.29995, -3.82731 40.29993, -3.82721 40.29991, -3.82714 40.29991, -3.82705 40.29991, -3.82692 40.29994, -3.82684 40.29996, -3.82677 40.29999, -3.82669 40.30004, -3.82661 40.30011, -3.82653 40.30019, -3.82651 40.30022, -3.82648 40.3003, -3.82647 40.30044, -3.82648 40.30053, -3.82651 40.30058, -3.82653 40.30062, -3.82645 40.30073, -3.8259 40.30135, -3.82573 40.30159, -3.82562 40.30179, -3.82543 40.30217, -3.82543 40.30227, -3.82493 40.30282, -3.82447 40.3033, -3.82442 40.30334, -3.82431 40.30345, -3.82416 40.3036, -3.82361 40.30407, -3.82324 40.30435, -3.82313 40.30443, -3.82297 40.30446, -3.82278 40.30456, -3.82252 40.30464, -3.82116 40.30535, -3.82106 40.30539, -3.82087 40.30546, -3.82065 40.30552, -3.82033 40.30535, -3.82007 40.30522, -3.81997 40.30516, -3.81978 40.30506, -3.8196 40.30496, -3.81869 40.30438, -3.81847 40.30417, -3.81838 40.30408, -3.81758 40.30324, -3.81734 40.30286, -3.81714 40.30252, -3.8171 40.3023, -3.81704 40.30181)",
                "stretch": "LINESTRING (-3.821331914835427 40.30526025034327, -3.82116 40.30535, -3.82106 40.30539, -3.82087 40.30546, -3.82065 40.30552, -3.820500033595686 40.30544033034771)",
                "route_length": 2575.725341796875,
                "stretch_length": 80.57774353027344,
                "stretch_start_position_in_route": 0.7684556245803833,
                "stretch_end_position_in_route": 0.8023914098739624,
                "stretch_starts_at_the_route_start": 0.0,
                "stretch_ends_at_the_route_end": 0.0,
                "heading_stretch": -55.54668045043945,
                "left_turns": 0,
                "straight_turns": 4,
                "right_turns": 0,
                "total_right_angle": 0.0,
                "max_right_angle": 0.0,
                "total_left_angle": 0.0,
                "max_left_angle": 0.0,
                "absolute_angle": -30.922504425048828,
                "has_left_turns": 0.0,
                "has_right_turns": 0.0,
                "min_curvature": 143.84756469726562,
                "max_curvature": 2708.32861328125,
                "mean_curvature": 1123.457763671875,
                "distance_start_end_stretch": 73.49273681640625,
                "route_coverage": 0.03128351271152496,
                "stretch_covers_route": 0.0,
                "tortuosity": 0.06171853467822075,
                "sinuosity": 0.9120723605155945,
                "density": 0.07446224987506866,
                "drive_right_side": 0.0,
                "sdo_api_response": None,
                "traffic_signs": None,
                "pra": 0.906000018119812,
                "prb": 0.9700000286102295,
                "prab": 0.6169999837875366,
                "lift": 0.7020778059959412,
                "tot": 1000.0,
                "logged_model": "runs:/6b175f24a701411b8d2034a0ce74a300/model",
                "prediction": "discard",
                "probability": 0.009285714477300644,
            },
        ]
    )

    return pdf, ec_model_log_table


@pytest.fixture
def tbt_new_critical_sections_example(spark):
    return spark.createDataFrame(
        tbt_new_critical_sections_example_raw,
        schema=T.StructType(
            [
                T.StructField("route_id", T.StringType()),
                T.StructField("case_id", T.StringType()),
                T.StructField("stretch", T.StringType()),
                T.StructField("stretch_length", T.FloatType()),
            ]
        ),
    )


class TestPipelines:
    def test_tbt_inspection(self):
        pipeline = tbt.pipelines.inspection.create_pipeline()
        pipeline_outputs = pipeline.outputs()

        assert "tbt_error_logs_output_sql" in pipeline_outputs
        assert (
            "tbt_critical_sections_with_mcp_feedback_output_spark" in pipeline_outputs
        )
        assert "tbt_error_logs_output_spark" in pipeline_outputs
        assert "tbt_inspection_metadata_output_sql" in pipeline_outputs
        assert "tbt_inspection_routes_output_spark" in pipeline_outputs
        assert "tbt_critical_sections_with_mcp_feedback_output_sql" in pipeline_outputs
        assert "tbt_inspection_metadata_output_spark" in pipeline_outputs
        assert "tbt_inspection_critical_sections_output_spark" in pipeline_outputs
        assert "tbt_inspection_critical_sections_output_sql" in pipeline_outputs
        assert "tbt_inspection_routes_output_sql" in pipeline_outputs
        assert "ec_model_log_table" in pipeline_outputs

    def test_get_provider_routes_genesis(
        self, tbt_sampling_example_sdf, tbt_sampling_metadata_example_sdf
    ):
        tbt_options = {
            "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
            "provider": "TT",
            "competitor": "GG",
            "endpoint": "https://api.tomtom.com/routing/1/calculateRoute/",
            "product": "2022.06.24",
            "mapdate": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        }

        tbt.pipelines.inspection.nodes.get_provider_routes(
            tbt_options, tbt_sampling_example_sdf, tbt_sampling_metadata_example_sdf, True
        )

    def test_get_provider_routes_osm(
        self, tbt_sampling_example_sdf, tbt_sampling_metadata_example_sdf
    ):
        tbt_options = {
            "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
            "provider": "OSM",
            "competitor": "GG",
            "endpoint": "http://tt-valhalla-api.westeurope.azurecontainer.io:8002/",
            "product": "2022.06.24",
            "mapdate": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        }
        tbt.pipelines.inspection.nodes.get_provider_routes(
            tbt_options, tbt_sampling_example_sdf, tbt_sampling_metadata_example_sdf, True
        )

    def test_get_provider_routes_osm_no_endpoint(
        self, tbt_sampling_example_sdf, tbt_sampling_metadata_example_sdf
    ):
        tbt_options = {
            "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
            "provider": "OSM",
            "competitor": "GG",
            "endpoint": "",
            "product": "2022.06.24",
            "mapdate": datetime.strptime("2022-11-04", "%Y-%m-%d"),
        }
        tbt.pipelines.inspection.nodes.get_provider_routes(
            tbt_options, tbt_sampling_example_sdf, tbt_sampling_metadata_example_sdf, True
        )

    def test_inspection_end_to_end(
        self,
        ml_model_outcome,
        tbt_sampling_example_sdf,
        tbt_sampling_metadata_example_sdf,
        spark,
        mocker,
    ):
        tbt_options = {
            "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
            "provider": "OSM",
            "competitor": "OSM-STRICT",
            "endpoint": "",
            "product": "2022.06.24",
            "mapdate": datetime.strptime("2022-11-04", "%Y-%m-%d"),
            "ignore_previous_inspections": True,
            "error_classification_mode": "ML",
            "competitor_endpoint": ""
        }


        mocker.patch(
            "tbt.pipelines.inspection.nodes.fcd.evaluate_with_ml_model",
            return_value=ml_model_outcome,
        )

        emp_RDD = spark.sparkContext.emptyRDD()
        tbt_inspection_routes = spark.createDataFrame(
            data=emp_RDD,
            schema=T.StructType(
                [
                    T.StructField("route_id", T.StringType(), True),
                    T.StructField("provider_route", T.StringType(), True),
                    T.StructField("competitor_route", T.StringType(), True),
                    T.StructField("rac_state", T.StringType(), True),
                    T.StructField("provider_route_length", T.StringType(), True),
                    T.StructField("provider_route_time", T.StringType(), True),
                    T.StructField("competitor_route_length", T.StringType(), True),
                    T.StructField("competitor_route_time", T.StringType(), True),
                    T.StructField("competitor", T.StringType(), True),
                    T.StructField("country", T.StringType(), True),
                    T.StructField("sample_id", T.StringType(), True),
                    T.StructField("run_id", T.StringType(), True),
                ]
            ),
        )

        tbt_inspection_critical_sections = spark.createDataFrame(
            data=emp_RDD,
            schema=T.StructType(
                [
                    T.StructField("route_id", T.StringType(), True),
                    T.StructField("mcp_state", T.StringType(), True),
                    T.StructField("case_id", T.StringType(), True),
                    T.StructField("reference_case_id", T.StringType(), True),
                    T.StructField("run_id", T.StringType(), True),
                    T.StructField("stretch", T.StringType(), True),
                    T.StructField("stretch_length", T.StringType(), True),
                    T.StructField("fcd_state", T.StringType(), True),
                    T.StructField("pra", T.StringType(), True),
                    T.StructField("prb", T.StringType(), True),
                    T.StructField("prab", T.StringType(), True),
                    T.StructField("lift", T.StringType(), True),
                    T.StructField("tot", T.StringType(), True),
                ]
            ),
        )

        tbt_inspection_metadata = spark.createDataFrame(
            data=emp_RDD,
            schema=T.StructType(
                [
                    T.StructField("run_id", T.StringType(), True),
                    T.StructField("sample_id", T.StringType(), True),
                    T.StructField("provider", T.StringType(), True),
                    T.StructField("endpoint", T.StringType(), True),
                    T.StructField("mapdate", T.DateType(), True),
                    T.StructField("product", T.StringType(), True),
                    T.StructField("country", T.StringType(), True),
                    T.StructField("mode", T.StringType(), True),
                    T.StructField("competitor", T.StringType(), True),
                    T.StructField("mcp_tasks", T.StringType(), True),
                    T.StructField("completed", T.StringType(), True),
                    T.StructField("inspection_date", T.DateType(), True),
                    T.StructField("comment", T.StringType(), True),
                    T.StructField("rac_elapsed_time", T.FloatType(), True),
                    T.StructField("fcd_elapsed_time", T.FloatType(), True),
                    T.StructField("total_elapsed_time", T.FloatType(), True),
                    T.StructField("api_calls", T.StringType(), True),
                    T.StructField("sanity_fail", T.BooleanType(), True),
                    T.StructField("sanity_msg", T.StringType(), True),
                ]
            ),
        )

        tbt_critical_sections_with_mcp_feedback = spark.createDataFrame(
            data=emp_RDD,
            schema=T.StructType(
                [
                    T.StructField("route_id", T.StringType(), True),
                    T.StructField("mcp_state", T.StringType(), True),
                    T.StructField("error_subtype", T.StringType(), True),
                    T.StructField("case_id", T.StringType(), True),
                    T.StructField("run_id", T.StringType(), True),
                ]
            ),
        )

        # test get_provider_routes
        (
            provider_routes,
            total_time,
            sample_metric,
            provider_api_calls,
        ) = tbt.pipelines.inspection.nodes.get_provider_routes(
            tbt_options, tbt_sampling_example_sdf, tbt_sampling_metadata_example_sdf, True
        )

        # test reuse_static_routes
        (
            provider_routes_unknown,
            provider_routes_known,
            inspection_critical_sections_known,
            total_time,
        ) = tbt.pipelines.inspection.nodes.reuse_static_routes(
            provider_routes,
            tbt_inspection_routes,
            tbt_inspection_critical_sections,
            tbt_critical_sections_with_mcp_feedback,
            tbt_inspection_metadata,
            tbt_options,
        )

        # test get_competitor_routes
        (
            competitor_routes,
            total_time,
            competitor_api_calls
        ) = tbt.pipelines.inspection.nodes.get_competitor_routes(
            tbt_options, provider_routes_unknown, tbt_inspection_routes, provider_api_calls
        )

        # test get_rac_state
        (
            routes,
            tbt_new_critical_sections,
            api_calls,
            total_time,
        ) = tbt.pipelines.inspection.nodes.get_rac_state(
            tbt_options, provider_routes_unknown, competitor_routes, competitor_api_calls
        )
        
        tbt_new_critical_sections.show()
        print(sample_metric)
        
        # test get_fcd_state
        (
            run_id,
            tbt_critical_sections_with_fcd_state,
            ec_model_log_table,
            total_time,
        ) = tbt.pipelines.inspection.nodes.get_fcd_state(
            tbt_options,
            "auto",
            tbt_new_critical_sections,
            {},
            {},
            sample_metric,
        )

        # test merge_inspection_data
        (
            inspection_routes,
            inspection_critical_sections,
            critical_sections_with_mcp_feedback,
            error_logs,
            inspection_metadata,
        ) = tbt.pipelines.inspection.nodes.merge_inspection_data(
            tbt_options,
            "841b4735-b5df-4c03-8a96-c48681613fed",
            provider_routes_known,
            inspection_critical_sections_known,
            routes,
            tbt_critical_sections_with_fcd_state,
            0.0,
            0.0,
            0.0,
            {},
            0.0,
            0.0,
        )


    def test_avoid_duplicates_notcheck(
        self,
    ):
        tbt_options = {
            "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
            "provider": "OSM",
            "competitor": "OSM-STRICT",
            "endpoint": "",
            "product": "2022.06.24",
            "mapdate": datetime.strptime("2022-11-04", "%Y-%m-%d"),
            "ignore_previous_inspections": True,
            "avoid_duplicates":False
        }
        
        tbt_inspection_metadata = pandas.DataFrame(
            [
                {
                    "run_id": "841b4735-b5df-4c03-8a96-c48681613fed",
                    "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
                    "provider": "OSM",
                    "endpoint": "",
                    "mapdate": datetime.strptime("2022-11-04", "%Y-%m-%d"),
                    "product": "2022.06.24",
                    "country": "ESP",
                    "mode": "auto",
                    "competitor": "OSM-STRICT",
                    "mcp_tasks": "auto",
                    "completed": False,
                    "inspection_date": datetime.strptime("2023-06-08", "%Y-%m-%d"),
                    "comment": "",
                    "rac_elapsed_time": 0.0,
                    "fcd_elapsed_time": 0.0,
                    "total_elapsed_time": 0.0,
                    "api_calls": "",
                    "sanity_fail": False,
                    "sanity_msg": "",
                }
            ]
        )

        assert tbt.pipelines.inspection.nodes.check_duplicates(tbt_options,tbt_inspection_metadata)

    def test_avoid_duplicates_check_no_duplicates(
        self,
    ):
        tbt_options = {
            "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
            "provider": "GG",
            "competitor": "OSM-STRICT",
            "endpoint": "",
            "product": "2022.06.24",
            "mapdate": datetime.strptime("2022-11-04", "%Y-%m-%d"),
            "ignore_previous_inspections": True,
            "avoid_duplicates":True
        }
        
        tbt_inspection_metadata = pandas.DataFrame(
            [
                {
                    "run_id": "841b4735-b5df-4c03-8a96-c48681613fed",
                    "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
                    "provider": "OSM",
                    "endpoint": "",
                    "mapdate": datetime.strptime("2022-11-04", "%Y-%m-%d"),
                    "product": "2022.06.24",
                    "country": "ESP",
                    "mode": "auto",
                    "competitor": "OSM-STRICT",
                    "mcp_tasks": "auto",
                    "completed": False,
                    "inspection_date": datetime.strptime("2023-06-08", "%Y-%m-%d"),
                    "comment": "",
                    "rac_elapsed_time": 0.0,
                    "fcd_elapsed_time": 0.0,
                    "total_elapsed_time": 0.0,
                    "api_calls": "",
                    "sanity_fail": False,
                    "sanity_msg": "",
                }
            ]
        )

        assert tbt.pipelines.inspection.nodes.check_duplicates(tbt_options,tbt_inspection_metadata)

    def test_avoid_duplicates_check_with_duplicates(
        self,
    ):
        tbt_options = {
            "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
            "provider": "OSM",
            "competitor": "OSM-STRICT",
            "endpoint": "",
            "product": "2022.06.24",
            "mapdate": datetime.strptime("2022-11-04", "%Y-%m-%d"),
            "ignore_previous_inspections": True,
            "avoid_duplicates":True
        }
        
        tbt_inspection_metadata = pandas.DataFrame(
            [
                {
                    "run_id": "841b4735-b5df-4c03-8a96-c48681613fed",
                    "sample_id": "841b4735-b5df-4c03-8a96-c48681613fed",
                    "provider": "OSM",
                    "endpoint": "",
                    "mapdate": datetime.strptime("2022-11-04", "%Y-%m-%d"),
                    "product": "2022.06.24",
                    "country": "ESP",
                    "mode": "auto",
                    "competitor": "OSM-STRICT",
                    "mcp_tasks": "auto",
                    "completed": False,
                    "inspection_date": datetime.strptime("2023-06-08", "%Y-%m-%d"),
                    "comment": "",
                    "rac_elapsed_time": 0.0,
                    "fcd_elapsed_time": 0.0,
                    "total_elapsed_time": 0.0,
                    "api_calls": "",
                    "sanity_fail": False,
                    "sanity_msg": "",
                }
            ]
        )

        with pytest.raises(ValueError) as e_info:
           tbt.pipelines.inspection.nodes.check_duplicates(tbt_options, tbt_inspection_metadata)
           
        assert str(e_info.value) == "Inspection already exists for this sample_id, provider, competitor and product"


   

       
