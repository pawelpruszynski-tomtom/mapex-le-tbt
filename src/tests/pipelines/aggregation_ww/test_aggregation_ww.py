""" Tests for ww aggregation pipeline """
import json
import uuid

import pandas
import math

import tbt.pipelines.aggregation_ww
from tbt.pipelines.aggregation_ww.nodes import (
    get_latest_products,
    get_inspections,
    get_results,
    get_ww_agg_inspections,
    update_agg_source,
    unify_error_type_json,
    data_to_binary
)

test_aggregation_ww_options = {
    "run_ids": [
        "f4489ffc-2182-461d-a5d4-1a04a819d1a4",
        "6d66d8a1-1c4e-4760-b428-8092f4d3d2e5",
        "625fdc75-b4f4-49c8-8d08-6660f5254f2f",
        "69950f94-bf32-448c-bc59-4d34f0c8b017",
        "06c329a4-aaaa-4889-a912-fe930a364999",
    ],
    "base_run_id_ww": "065400a4-64c8-415d-bdc3-a52a69778668",
    "scope": "WorldWide",
    "provider": "OM-VAL",
    "comment": "test",
    "email":"jonathan.suarez@tomtom.com"
}

test_latest_products = pandas.DataFrame(
    {
        'product': {0: '2022.06.03', 1: '2023.01.02', 2: '2023.02.14', 3: '2023.03.000', 4: '2023.03.20', 5: '22490.000-Orbis-Enterprise-11', 6: 'OV_23140.000'},
        'provider': {0: 'MAPBOX', 1: 'OSM', 2: 'HERE', 3: 'TT', 4: 'GG', 5: 'OM-VAL', 6: 'OM_CUSTOM'},
        'mapdate': {0: '2022-06-03', 1: '2023-01-02', 2: '2023-02-14', 3: '2023-02-20', 4: '2023-03-23', 5: '2022-12-08', 6: '2023-04-03'}
    }
)

test_base_ww_agg_inspections_available = pandas.DataFrame(
    {
        'provider': {0: 'OM_CUSTOM', 1: 'OM_CUSTOM', 2: 'OM-VAL', 3: 'OM-VAL', 4: 'OSM', 5: 'OSM', 6: 'TT', 7: 'TT'},
        'scope': {0: 'WorldWide', 1: 'Top40', 2: 'WorldWide', 3: 'Top40', 4: 'WorldWide', 5: 'Top40', 6: 'WorldWide', 7: 'Top40'},
        'run_id': {0: 'cbe86f3d-fcda-4c49-b63c-6a8fe76b5f0e', 1: '664c390f-1be1-43bb-ac96-8f9ddaae5189', 2: '065400a4-64c8-415d-bdc3-a52a69778668', 3: '719edcb4-a63f-4bb6-8f83-a3db995c5eb1', 4: '8e3e816a-0fea-4ad3-9f91-8d1076fc2e27', 5: 'a359b0c3-06eb-490a-b2b4-daef285e38e4', 6: 'cc6a9c49-c4d6-4af7-ab8a-f3ba4c0eab78', 7: 'ca308515-a460-4ff7-835c-3d2043168d90'}
    }
)

test_inspections_data = pandas.DataFrame(
    {
        "run_id": {
            0: "69950f94-bf32-448c-bc59-4d34f0c8b017",
            1: "f4489ffc-2182-461d-a5d4-1a04a819d1a4",
            2: "625fdc75-b4f4-49c8-8d08-6660f5254f2f",
            3: "6d66d8a1-1c4e-4760-b428-8092f4d3d2e5",
            4: "06c329a4-aaaa-4889-a912-fe930a364999",
        },
        "provider": {0: "OM-VAL", 1: "OM-VAL", 2: "OM-VAL", 3: "OM-VAL", 4: "OM-VAL"},
        "country": {0: "NLD", 1: "BEL", 2: "GBR", 3: "US-CA", 4: "USA"},
        "product": {
            0: "22510.000-Orbis-Enterprise-13",
            1: "22510.000-Orbis-Enterprise-13",
            2: "22510.000-Orbis-Enterprise-13",
            3: "22510.000-Orbis-Enterprise-13",
            4: "22510.000-Orbis-Enterprise-13",
        },
        "eph": {0: 0.27, 1: 0.21, 2: 0.08, 3: 0.2, 4: 0.13},
        "metrics_per_error_type": {
            0: {
                "turnrest": {"eph": 0.07, "lower": 0.03, "upper": 0.11, "errors": 9},
                "oneway": {"eph": 0.07, "lower": 0.04, "upper": 0.11, "errors": 9},
                "nonnav": {"eph": 0.12, "lower": 0.07, "upper": 0.18, "errors": 15},
                "wdtrf": {"eph": 0.07, "lower": 0.04, "upper": 0.11, "errors": 9},
                "mman": {"eph": 0.02, "lower": 0.01, "upper": 0.05, "errors": 3},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.05, "lower": 0.02, "upper": 0.08, "errors": 6},
                "implicit": {"eph": 0.03, "lower": 0.01, "upper": 0.06, "errors": 4},
                "wrc": {"eph": 0.08, "lower": 0.04, "upper": 0.12, "errors": 10},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "sgeo": {"eph": 0.03, "lower": 0.01, "upper": 0.06, "errors": 4},
            },
            1: {
                "turnrest": {"eph": 0.08, "lower": 0.04, "upper": 0.12, "errors": 9},
                "oneway": {"eph": 0.11, "lower": 0.06, "upper": 0.17, "errors": 12},
                "nonnav": {"eph": 0.02, "lower": 0.0, "upper": 0.04, "errors": 2},
                "wdtrf": {"eph": 0.11, "lower": 0.06, "upper": 0.17, "errors": 12},
                "mman": {"eph": 0.03, "lower": 0.0, "upper": 0.05, "errors": 3},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.05, "lower": 0.02, "upper": 0.09, "errors": 6},
                "implicit": {"eph": 0.01, "lower": 0.0, "upper": 0.03, "errors": 1},
                "wrc": {"eph": 0.02, "lower": 0.0, "upper": 0.04, "errors": 2},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "sgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
            },
            2: {
                "turnrest": {"eph": 0.03, "lower": 0.01, "upper": 0.05, "errors": 4},
                "oneway": {"eph": 0.03, "lower": 0.01, "upper": 0.05, "errors": 4},
                "nonnav": {"eph": 0.03, "lower": 0.01, "upper": 0.05, "errors": 4},
                "wdtrf": {"eph": 0.03, "lower": 0.01, "upper": 0.05, "errors": 4},
                "mman": {"eph": 0.03, "lower": 0.01, "upper": 0.05, "errors": 4},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "implicit": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "wrc": {"eph": 0.02, "lower": 0.0, "upper": 0.04, "errors": 3},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "sgeo": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
            },
            3: {
                "turnrest": {"eph": 0.11, "lower": 0.02, "upper": 0.27, "errors": 3},
                "oneway": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "nonnav": {"eph": 0.08, "lower": 0.0, "upper": 0.24, "errors": 1},
                "wdtrf": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mman": {"eph": 0.03, "lower": 0.0, "upper": 0.08, "errors": 2},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.08, "lower": 0.0, "upper": 0.25, "errors": 1},
                "implicit": {"eph": 0.02, "lower": 0.0, "upper": 0.05, "errors": 1},
                "wrc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "sgeo": {"eph": 0.08, "lower": 0.0, "upper": 0.24, "errors": 1},
            },
            4: {
                "turnrest": {"eph": 0.07, "lower": 0.03, "upper": 0.1, "errors": 9},
                "oneway": {"eph": 0.02, "lower": 0.0, "upper": 0.04, "errors": 3},
                "nonnav": {"eph": 0.04, "lower": 0.01, "upper": 0.07, "errors": 5},
                "wdtrf": {"eph": 0.02, "lower": 0.0, "upper": 0.04, "errors": 3},
                "mman": {"eph": 0.05, "lower": 0.02, "upper": 0.09, "errors": 7},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.01, "lower": 0.0, "upper": 0.04, "errors": 2},
                "implicit": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "wrc": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "sgeo": {"eph": 0.02, "lower": 0.01, "upper": 0.05, "errors": 3},
            },
        },
        "mapdate": {
            0: "2022-12-27",
            1: "2022-12-27",
            2: "2022-12-27",
            3: "2022-12-27",
            4: "2022-12-27",
        },
    }
)
test_aggregation_ww_data = pandas.DataFrame(
    {
        "run_id": {0: "065400a4-64c8-415d-bdc3-a52a69778668"},
        "metadata": {
            0: '{"measured_eph": 0.635305003168465, "measured_share": 0.11283333100000004, "worst_measures_avg": 1.8460000038146973, "countries_with_no_weights": [], "run_ids": ["4d399306-78f4-48e0-bf44-2805a8d10984", "3b7dd8a7-4b0a-43e3-aca3-46e07e8edebd", "7fa68171-0428-4ad8-b5f0-9f09980a4498", "e2711409-3f5b-4313-9656-6ca91316a0c0", "52c4c45a-d856-477e-95ca-403c923f8dca", "551d3d51-980f-4548-a55e-81b8c0c2e5d7", "d60cdac9-ed09-47cf-b556-96993b2253bb", "70a5bc08-0708-4bf1-b966-e0551e6a50ad", "da1f65aa-0223-4e23-8173-4e386706884e", "cab7ddee-ae78-40d1-9887-bab2a536c6c0", "b703a853-9657-47ad-95a5-c4bc7bd07b14", "3923f60d-8bc0-4211-a4d3-fdc2a9575666", "f011da84-a393-4d9d-922a-b9a4a27bfb18", "a17d8b73-d924-4be7-a25e-593c9721645e", "d8ff7332-ffee-417f-afc9-f44a24b127ee", "de19e376-752a-41a3-bd50-6cd54a6a80c2", "9891d40b-5d95-4ec9-b7de-616377b50321", "4e5b9692-b5df-4839-a92b-69b0d4f23634", "e1b7838f-8711-46b8-95c8-f6e5640ee9e2", "84cde84f-ad70-434a-a95b-02e890d09a6d", "8003fef7-22b7-4ea8-b2bc-e9e7944fc68d", "55b813a3-094f-4e3c-ad29-4d2a4f3f487e", "2866f326-351e-462b-8f37-ce3f5e2516b1", "9561bff5-5d1f-4797-8fbb-837f31c53baf", "e9d03766-8cc2-4bf0-895d-3ae2d999269d", "0185bf36-fffe-4089-b32c-016ee257217e", "da6e120a-e022-4e93-8f7e-beb3d0792dca", "65f19c56-6dd4-4b9d-af17-2275cea3b26e", "6e93c212-a11f-46e8-853f-971caccd0974", "dcd7c309-e20d-4ac4-af6e-12040c42e5d9", "313bf6bb-278f-4a68-bc05-5ef9a2747b9c", "b3b12e72-4f90-41d3-9ab3-c7707b51a493", "8e9e4d06-7ea2-41a4-9acf-00641c777b24", "58720889-116f-4fdd-b630-43fdab24413a", "f57118f0-0aa1-44c2-b7f1-b757070f5f38", "421c5489-a452-4e94-8fc4-c9e5f29aca22", "47e0840c-57ca-4dda-8202-a645acc5ae4a", "6a5072fa-1bfe-4c77-89c0-35925f03f386", "07750a6d-e237-4deb-bc71-edf654816fb2", "e9828834-69bb-4f69-bb7d-c9c35951e1e9", "44b0d52d-ec37-4509-8a65-eefce9b15f37", "47b8867f-a47e-4879-9979-feb84faf7c63", "fe84d24c-6180-4b97-acc1-a6bc969181c5", "f8730a91-a17d-4a4e-bc1d-d202736800d8", "fc25436d-16ea-4579-b00b-c2592bf0fc7e", "21d452e4-2f90-4350-a1fc-7133435842b8", "3192ba66-6078-409b-a5c1-5ba9d26604bf", "70775a1f-7c6a-4246-bbc3-a03301db6d33", "292d65fb-d885-4059-9885-a6a90e539a26", "8c3c2a63-2c84-4a0a-9fed-da7f5c046de6", "cbce4a2b-a563-4cb8-8cce-ca373fae53bf", "d71b7360-bd5c-46b6-b1a4-795824eeceb6", "3eb6b93a-64f7-4adf-bb9d-41b31fbab9e9", "64b4613f-d9a0-4586-9447-5fa9819e0416", "093cc956-a936-4c6c-8838-a723d8648615", "53a6ec99-df1b-4f4c-bd2a-7c1dfc0ddb6d", "5b776d56-49d2-4324-b662-97082570b56f", "30e40f06-51ab-463e-b691-2441d7290ba0", "b2d21f7f-086e-4014-9d19-a6011f69052c", "512050bc-630d-4a49-84ab-24a1ab456475", "2ddf0eea-6bdc-4f0c-b67e-c5a7dacd3b36", "cff2c2d9-585e-4591-ae60-1847056463f3", "a4fc30f8-0338-4f00-82a1-ca2383e820a3", "a0a547f9-2e8f-42ad-800b-ce03282b7fce", "167c74b1-ab25-4f0f-90a3-4897c2af9a5a", "0c196c79-74e5-4b41-8075-e97198eacad6", "f705e298-b402-42f7-b92f-4cb124b5957e", "45d6c4e5-5c29-49bd-aaff-c654e3c540d1", "1540f595-451d-408e-8c6d-259c093555e6", "2463de67-76f3-458d-bf43-a841d3bab505", "4672c19d-e481-4906-9b9f-92b21bdcb4c1", "833ef2fd-b42d-4f39-95ad-3c569ca8118a", "89db0356-fb20-4309-a0b8-0b8fc1af7414", "37980c73-65ab-4ac1-ad56-7ace156f96fc", "ad705477-6e26-42e1-b046-278d6f09967d"]}'
        },
    }
)
test_weights = pandas.DataFrame(
    {
        "country": {
            0: "USA",
            1: "REU",
            2: "MTQ",
            3: "MYT",
            4: "GLP",
            5: "FRA",
            6: "MCO",
            7: "GBR",
            8: "ITA",
            9: "SMR",
            10: "VAT",
            11: "MLT",
            12: "BRA",
            13: "DEU",
            14: "CAN",
            15: "GIB",
            16: "ESP",
            17: "AND",
            18: "MEX",
            19: "IND",
            20: "RUS",
            21: "TUR",
            22: "ARG",
            23: "NLD",
            24: "LUX",
            25: "BEL",
            26: "AUS",
            27: "THA",
            28: "POL",
            29: "SAU",
        },
        "weight": {
            0: 0.05,
            1: None,
            2: None,
            3: None,
            4: None,
            5: 0.05,
            6: None,
            7: 0.05,
            8: 0.05,
            9: None,
            10: None,
            11: None,
            12: 0.05,
            13: 0.05,
            14: 0.05,
            15: None,
            16: 0.05,
            17: None,
            18: 0.05,
            19: 0.05,
            20: 0.015,
            21: 0.015,
            22: 0.015,
            23: 0.015,
            24: None,
            25: 0.015,
            26: 0.015,
            27: 0.015,
            28: 0.015,
            29: 0.015,
        },
        "ranking": {
            0: 1,
            1: 2,
            2: 2,
            3: 2,
            4: 2,
            5: 2,
            6: 2,
            7: 3,
            8: 4,
            9: 4,
            10: 4,
            11: 4,
            12: 5,
            13: 6,
            14: 7,
            15: 8,
            16: 8,
            17: 8,
            18: 9,
            19: 10,
            20: 11,
            21: 12,
            22: 13,
            23: 14,
            24: 15,
            25: 15,
            26: 16,
            27: 17,
            28: 18,
            29: 19,
        },
    }
)

test_base_ww_agg_inspections = pandas.DataFrame(
    {
        "run_id": {
            0: "65f19c56-6dd4-4b9d-af17-2275cea3b26e",
            1: "70a5bc08-0708-4bf1-b966-e0551e6a50ad",
            2: "3192ba66-6078-409b-a5c1-5ba9d26604bf",
            3: "9891d40b-5d95-4ec9-b7de-616377b50321",
            4: "167c74b1-ab25-4f0f-90a3-4897c2af9a5a",
            5: "b3b12e72-4f90-41d3-9ab3-c7707b51a493",
            6: "d60cdac9-ed09-47cf-b556-96993b2253bb",
            7: "5b776d56-49d2-4324-b662-97082570b56f",
            8: "84cde84f-ad70-434a-a95b-02e890d09a6d",
            9: "6a5072fa-1bfe-4c77-89c0-35925f03f386",
        },
        "provider": {
            0: "OM-VAL",
            1: "OM-VAL",
            2: "OM-VAL",
            3: "OM-VAL",
            4: "OM-VAL",
            5: "OM-VAL",
            6: "OM-VAL",
            7: "OM-VAL",
            8: "OM-VAL",
            9: "OM-VAL",
        },
        "country": {
            0: "TUR",
            1: "FRA",
            2: "RUS",
            3: "FIN",
            4: "EGY",
            5: "HUN",
            6: "IRL",
            7: "HKG",
            8: "ZAF",
            9: "ARE",
        },
        "product": {
            0: "22460.001-Orbis-Enterprise",
            1: "22460.001-Orbis-Enterprise",
            2: "22460.001-Orbis-Enterprise",
            3: "22460.001-Orbis-Enterprise",
            4: "22460.001-Orbis-Enterprise",
            5: "22460.001-Orbis-Enterprise",
            6: "22460.001-Orbis-Enterprise",
            7: "22460.001-Orbis-Enterprise",
            8: "22460.001-Orbis-Enterprise",
            9: "22460.001-Orbis-Enterprise",
        },
        "eph": {
            0: 2.04,
            1: 0.75,
            2: 0.72,
            3: 0.17,
            4: 0.72,
            5: 0.64,
            6: 0.49,
            7: 1.43,
            8: 0.36,
            9: 0.35,
        },
        "metrics_per_error_type": {
            0: {
                "turnrest": {"eph": 0.43, "lower": 0.34, "upper": 0.52, "errors": 63},
                "oneway": {"eph": 0.85, "lower": 0.73, "upper": 0.97, "errors": 125},
                "nonnav": {"eph": 0.76, "lower": 0.65, "upper": 0.88, "errors": 110},
                "wdtrf": {"eph": 0.85, "lower": 0.73, "upper": 0.97, "errors": 125},
                "mman": {"eph": 0.29, "lower": 0.21, "upper": 0.36, "errors": 42},
                "mgsc": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "mbp": {"eph": 0.14, "lower": 0.09, "upper": 0.19, "errors": 20},
                "implicit": {"eph": 0.09, "lower": 0.05, "upper": 0.13, "errors": 13},
                "wrc": {"eph": 0.23, "lower": 0.17, "upper": 0.3, "errors": 33},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.11, "lower": 0.06, "upper": 0.15, "errors": 15},
                "sgeo": {"eph": 0.43, "lower": 0.34, "upper": 0.52, "errors": 62},
            },
            1: {
                "turnrest": {"eph": 0.23, "lower": 0.17, "upper": 0.3, "errors": 32},
                "oneway": {"eph": 0.45, "lower": 0.36, "upper": 0.54, "errors": 61},
                "nonnav": {"eph": 0.08, "lower": 0.04, "upper": 0.12, "errors": 10},
                "wdtrf": {"eph": 0.45, "lower": 0.36, "upper": 0.54, "errors": 61},
                "mman": {"eph": 0.15, "lower": 0.1, "upper": 0.21, "errors": 21},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.08, "lower": 0.04, "upper": 0.13, "errors": 11},
                "implicit": {"eph": 0.07, "lower": 0.04, "upper": 0.11, "errors": 10},
                "wrc": {"eph": 0.05, "lower": 0.02, "upper": 0.08, "errors": 6},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.01, "lower": 0.0, "upper": 0.04, "errors": 2},
                "sgeo": {"eph": 0.01, "lower": 0.0, "upper": 0.04, "errors": 2},
            },
            2: {
                "turnrest": {"eph": 0.34, "lower": 0.26, "upper": 0.42, "errors": 48},
                "oneway": {"eph": 0.08, "lower": 0.05, "upper": 0.12, "errors": 12},
                "nonnav": {"eph": 0.3, "lower": 0.22, "upper": 0.38, "errors": 41},
                "wdtrf": {"eph": 0.08, "lower": 0.05, "upper": 0.12, "errors": 12},
                "mman": {"eph": 0.28, "lower": 0.21, "upper": 0.35, "errors": 38},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.06, "lower": 0.03, "upper": 0.1, "errors": 10},
                "implicit": {"eph": 0.05, "lower": 0.02, "upper": 0.08, "errors": 8},
                "wrc": {"eph": 0.07, "lower": 0.03, "upper": 0.11, "errors": 8},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.02, "lower": 0.0, "upper": 0.04, "errors": 3},
                "sgeo": {"eph": 0.21, "lower": 0.14, "upper": 0.28, "errors": 30},
            },
            3: {
                "turnrest": {"eph": 0.06, "lower": 0.02, "upper": 0.09, "errors": 6},
                "oneway": {"eph": 0.07, "lower": 0.04, "upper": 0.12, "errors": 8},
                "nonnav": {"eph": 0.04, "lower": 0.01, "upper": 0.07, "errors": 4},
                "wdtrf": {"eph": 0.07, "lower": 0.04, "upper": 0.12, "errors": 8},
                "mman": {"eph": 0.05, "lower": 0.02, "upper": 0.08, "errors": 5},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.01, "lower": 0.0, "upper": 0.03, "errors": 1},
                "implicit": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wrc": {"eph": 0.03, "lower": 0.0, "upper": 0.06, "errors": 3},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "sgeo": {"eph": 0.01, "lower": 0.0, "upper": 0.03, "errors": 1},
            },
            4: {
                "turnrest": {"eph": 0.05, "lower": 0.0, "upper": 0.1, "errors": 3},
                "oneway": {"eph": 0.16, "lower": 0.08, "upper": 0.25, "errors": 13},
                "nonnav": {"eph": 0.52, "lower": 0.38, "upper": 0.66, "errors": 44},
                "wdtrf": {"eph": 0.16, "lower": 0.08, "upper": 0.25, "errors": 13},
                "mman": {"eph": 0.04, "lower": 0.0, "upper": 0.1, "errors": 2},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "implicit": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "wrc": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.17, "lower": 0.08, "upper": 0.26, "errors": 12},
                "sgeo": {"eph": 0.34, "lower": 0.23, "upper": 0.47, "errors": 31},
            },
            5: {
                "turnrest": {"eph": 0.32, "lower": 0.22, "upper": 0.42, "errors": 28},
                "oneway": {"eph": 0.19, "lower": 0.13, "upper": 0.26, "errors": 22},
                "nonnav": {"eph": 0.12, "lower": 0.07, "upper": 0.18, "errors": 15},
                "wdtrf": {"eph": 0.19, "lower": 0.13, "upper": 0.26, "errors": 22},
                "mman": {"eph": 0.3, "lower": 0.2, "upper": 0.4, "errors": 25},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.02, "lower": 0.0, "upper": 0.05, "errors": 3},
                "implicit": {"eph": 0.06, "lower": 0.02, "upper": 0.11, "errors": 5},
                "wrc": {"eph": 0.09, "lower": 0.05, "upper": 0.14, "errors": 11},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "sgeo": {"eph": 0.03, "lower": 0.01, "upper": 0.06, "errors": 4},
            },
            6: {
                "turnrest": {"eph": 0.32, "lower": 0.19, "upper": 0.46, "errors": 16},
                "oneway": {"eph": 0.12, "lower": 0.05, "upper": 0.2, "errors": 8},
                "nonnav": {"eph": 0.05, "lower": 0.01, "upper": 0.11, "errors": 3},
                "wdtrf": {"eph": 0.12, "lower": 0.05, "upper": 0.2, "errors": 8},
                "mman": {"eph": 0.21, "lower": 0.11, "upper": 0.33, "errors": 10},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.11, "lower": 0.04, "upper": 0.19, "errors": 6},
                "implicit": {"eph": 0.04, "lower": 0.0, "upper": 0.09, "errors": 3},
                "wrc": {"eph": 0.03, "lower": 0.0, "upper": 0.08, "errors": 2},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "sgeo": {"eph": 0.02, "lower": 0.0, "upper": 0.07, "errors": 1},
            },
            7: {
                "turnrest": {"eph": 0.93, "lower": 0.52, "upper": 1.39, "errors": 18},
                "oneway": {"eph": 0.47, "lower": 0.15, "upper": 0.85, "errors": 6},
                "nonnav": {"eph": 0.03, "lower": 0.0, "upper": 0.07, "errors": 2},
                "wdtrf": {"eph": 0.47, "lower": 0.15, "upper": 0.85, "errors": 6},
                "mman": {"eph": 0.92, "lower": 0.5, "upper": 1.38, "errors": 17},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.02, "lower": 0.0, "upper": 0.05, "errors": 1},
                "implicit": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wrc": {"eph": 0.02, "lower": 0.0, "upper": 0.05, "errors": 1},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "sgeo": {"eph": 0.02, "lower": 0.0, "upper": 0.05, "errors": 1},
            },
            8: {
                "turnrest": {"eph": 0.12, "lower": 0.08, "upper": 0.17, "errors": 18},
                "oneway": {"eph": 0.01, "lower": 0.0, "upper": 0.03, "errors": 2},
                "nonnav": {"eph": 0.23, "lower": 0.17, "upper": 0.29, "errors": 37},
                "wdtrf": {"eph": 0.01, "lower": 0.0, "upper": 0.03, "errors": 2},
                "mman": {"eph": 0.05, "lower": 0.02, "upper": 0.08, "errors": 7},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.08, "lower": 0.04, "upper": 0.12, "errors": 11},
                "implicit": {"eph": 0.01, "lower": 0.0, "upper": 0.03, "errors": 2},
                "wrc": {"eph": 0.08, "lower": 0.05, "upper": 0.13, "errors": 14},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.02, "lower": 0.01, "upper": 0.05, "errors": 4},
                "sgeo": {"eph": 0.12, "lower": 0.08, "upper": 0.16, "errors": 19},
            },
            9: {
                "turnrest": {"eph": 0.08, "lower": 0.04, "upper": 0.12, "errors": 12},
                "oneway": {"eph": 0.05, "lower": 0.02, "upper": 0.08, "errors": 8},
                "nonnav": {"eph": 0.22, "lower": 0.16, "upper": 0.28, "errors": 38},
                "wdtrf": {"eph": 0.05, "lower": 0.02, "upper": 0.08, "errors": 8},
                "mman": {"eph": 0.05, "lower": 0.02, "upper": 0.08, "errors": 8},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.02, "lower": 0.01, "upper": 0.05, "errors": 4},
                "implicit": {"eph": 0.1, "lower": 0.06, "upper": 0.14, "errors": 16},
                "wrc": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.05, "lower": 0.03, "upper": 0.08, "errors": 9},
                "sgeo": {"eph": 0.16, "lower": 0.11, "upper": 0.21, "errors": 28},
            },
        },
        "mapdate": {
            0: "2022-11-17",
            1: "2022-11-17",
            2: "2022-11-17",
            3: "2022-11-17",
            4: "2022-11-17",
            5: "2022-11-17",
            6: "2022-11-17",
            7: "2022-11-17",
            8: "2022-11-17",
            9: "2022-11-17",
        },
    }
)

test_new_inspections = pandas.DataFrame(
    {
        "run_id": {
            0: "69950f94-bf32-448c-bc59-4d34f0c8b017",
            1: "f4489ffc-2182-461d-a5d4-1a04a819d1a4",
            2: "625fdc75-b4f4-49c8-8d08-6660f5254f2f",
            3: "6d66d8a1-1c4e-4760-b428-8092f4d3d2e5",
            4: "06c329a4-aaaa-4889-a912-fe930a364999",
        },
        "provider": {0: "OM-VAL", 1: "OM-VAL", 2: "OM-VAL", 3: "OM-VAL", 4: "OM-VAL"},
        "country": {0: "NLD", 1: "BEL", 2: "GBR", 3: "US-CA", 4: "USA"},
        "product": {
            0: "22510.000-Orbis-Enterprise-13",
            1: "22510.000-Orbis-Enterprise-13",
            2: "22510.000-Orbis-Enterprise-13",
            3: "22510.000-Orbis-Enterprise-13",
            4: "22510.000-Orbis-Enterprise-13",
        },
        "eph": {0: 0.27, 1: 0.21, 2: 0.08, 3: 0.2, 4: 0.13},
        "metrics_per_error_type": {
            0: {
                "turnrest": {"eph": 0.07, "lower": 0.03, "upper": 0.11, "errors": 9},
                "oneway": {"eph": 0.07, "lower": 0.04, "upper": 0.11, "errors": 9},
                "nonnav": {"eph": 0.12, "lower": 0.07, "upper": 0.18, "errors": 15},
                "wdtrf": {"eph": 0.07, "lower": 0.04, "upper": 0.11, "errors": 9},
                "mman": {"eph": 0.02, "lower": 0.01, "upper": 0.05, "errors": 3},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.05, "lower": 0.02, "upper": 0.08, "errors": 6},
                "implicit": {"eph": 0.03, "lower": 0.01, "upper": 0.06, "errors": 4},
                "wrc": {"eph": 0.08, "lower": 0.04, "upper": 0.12, "errors": 10},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "sgeo": {"eph": 0.03, "lower": 0.01, "upper": 0.06, "errors": 4},
            },
            1: {
                "turnrest": {"eph": 0.08, "lower": 0.04, "upper": 0.12, "errors": 9},
                "oneway": {"eph": 0.11, "lower": 0.06, "upper": 0.17, "errors": 12},
                "nonnav": {"eph": 0.02, "lower": 0.0, "upper": 0.04, "errors": 2},
                "wdtrf": {"eph": 0.11, "lower": 0.06, "upper": 0.17, "errors": 12},
                "mman": {"eph": 0.03, "lower": 0.0, "upper": 0.05, "errors": 3},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.05, "lower": 0.02, "upper": 0.09, "errors": 6},
                "implicit": {"eph": 0.01, "lower": 0.0, "upper": 0.03, "errors": 1},
                "wrc": {"eph": 0.02, "lower": 0.0, "upper": 0.04, "errors": 2},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "sgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
            },
            2: {
                "turnrest": {"eph": 0.03, "lower": 0.01, "upper": 0.05, "errors": 4},
                "oneway": {"eph": 0.03, "lower": 0.01, "upper": 0.05, "errors": 4},
                "nonnav": {"eph": 0.03, "lower": 0.01, "upper": 0.05, "errors": 4},
                "wdtrf": {"eph": 0.03, "lower": 0.01, "upper": 0.05, "errors": 4},
                "mman": {"eph": 0.03, "lower": 0.01, "upper": 0.05, "errors": 4},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "implicit": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "wrc": {"eph": 0.02, "lower": 0.0, "upper": 0.04, "errors": 3},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "sgeo": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
            },
            3: {
                "turnrest": {"eph": 0.11, "lower": 0.02, "upper": 0.27, "errors": 3},
                "oneway": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "nonnav": {"eph": 0.08, "lower": 0.0, "upper": 0.24, "errors": 1},
                "wdtrf": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mman": {"eph": 0.03, "lower": 0.0, "upper": 0.08, "errors": 2},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.08, "lower": 0.0, "upper": 0.25, "errors": 1},
                "implicit": {"eph": 0.02, "lower": 0.0, "upper": 0.05, "errors": 1},
                "wrc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "sgeo": {"eph": 0.08, "lower": 0.0, "upper": 0.24, "errors": 1},
            },
            4: {
                "turnrest": {"eph": 0.07, "lower": 0.03, "upper": 0.1, "errors": 9},
                "oneway": {"eph": 0.02, "lower": 0.0, "upper": 0.04, "errors": 3},
                "nonnav": {"eph": 0.04, "lower": 0.01, "upper": 0.07, "errors": 5},
                "wdtrf": {"eph": 0.02, "lower": 0.0, "upper": 0.04, "errors": 3},
                "mman": {"eph": 0.05, "lower": 0.02, "upper": 0.09, "errors": 7},
                "mgsc": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "mbp": {"eph": 0.01, "lower": 0.0, "upper": 0.04, "errors": 2},
                "implicit": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "wrc": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "mgeo": {"eph": 0.0, "lower": 0.0, "upper": 0.0, "errors": 0},
                "wgeo": {"eph": 0.01, "lower": 0.0, "upper": 0.02, "errors": 1},
                "sgeo": {"eph": 0.02, "lower": 0.01, "upper": 0.05, "errors": 3},
            },
        },
        "mapdate": {
            0: "2022-12-27",
            1: "2022-12-27",
            2: "2022-12-27",
            3: "2022-12-27",
            4: "2022-12-27",
        },
    }
)

test_ww_agg_inspections = pandas.DataFrame(
    {
        "country": {
            0: "TUR",
            1: "FRA",
            2: "RUS",
            3: "FIN",
            4: "EGY",
            5: "HUN",
            6: "IRL",
            7: "HKG",
            8: "ZAF",
            9: "ARE",
        },
        "provider": {
            0: "OM-VAL",
            1: "OM-VAL",
            2: "OM-VAL",
            3: "OM-VAL",
            4: "OM-VAL",
            5: "OM-VAL",
            6: "OM-VAL",
            7: "OM-VAL",
            8: "OM-VAL",
            9: "OM-VAL",
        },
        "product": {
            0: "22460.001-Orbis-Enterprise",
            1: "22460.001-Orbis-Enterprise",
            2: "22460.001-Orbis-Enterprise",
            3: "22460.001-Orbis-Enterprise",
            4: "22460.001-Orbis-Enterprise",
            5: "22460.001-Orbis-Enterprise",
            6: "22460.001-Orbis-Enterprise",
            7: "22460.001-Orbis-Enterprise",
            8: "22460.001-Orbis-Enterprise",
            9: "22460.001-Orbis-Enterprise",
        },
        "mapdate": {
            0: "2022-11-17",
            1: "2022-11-17",
            2: "2022-11-17",
            3: "2022-11-17",
            4: "2022-11-17",
            5: "2022-11-17",
            6: "2022-11-17",
            7: "2022-11-17",
            8: "2022-11-17",
            9: "2022-11-17",
        },
        "run_id": {
            0: "65f19c56-6dd4-4b9d-af17-2275cea3b26e",
            1: "70a5bc08-0708-4bf1-b966-e0551e6a50ad",
            2: "3192ba66-6078-409b-a5c1-5ba9d26604bf",
            3: "9891d40b-5d95-4ec9-b7de-616377b50321",
            4: "167c74b1-ab25-4f0f-90a3-4897c2af9a5a",
            5: "b3b12e72-4f90-41d3-9ab3-c7707b51a493",
            6: "d60cdac9-ed09-47cf-b556-96993b2253bb",
            7: "5b776d56-49d2-4324-b662-97082570b56f",
            8: "84cde84f-ad70-434a-a95b-02e890d09a6d",
            9: "6a5072fa-1bfe-4c77-89c0-35925f03f386",
        },
        "eph": {
            0: 2.04,
            1: 0.75,
            2: 0.72,
            3: 0.17,
            4: 0.72,
            5: 0.64,
            6: 0.49,
            7: 1.43,
            8: 0.36,
            9: 0.35,
        },
        "metrics_per_error_type": {
            0: {
                "implicit": {"eph": 0.09, "errors": 13, "lower": 0.05, "upper": 0.13},
                "mbp": {"eph": 0.14, "errors": 20, "lower": 0.09, "upper": 0.19},
                "mgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mgsc": {"eph": 0.01, "errors": 1, "lower": 0.0, "upper": 0.02},
                "mman": {"eph": 0.29, "errors": 42, "lower": 0.21, "upper": 0.36},
                "nonnav": {"eph": 0.76, "errors": 110, "lower": 0.65, "upper": 0.88},
                "oneway": {"eph": 0.85, "errors": 125, "lower": 0.73, "upper": 0.97},
                "sgeo": {"eph": 0.43, "errors": 62, "lower": 0.34, "upper": 0.52},
                "turnrest": {"eph": 0.43, "errors": 63, "lower": 0.34, "upper": 0.52},
                "wdtrf": {"eph": 0.85, "errors": 125, "lower": 0.73, "upper": 0.97},
                "wgeo": {"eph": 0.11, "errors": 15, "lower": 0.06, "upper": 0.15},
                "wrc": {"eph": 0.23, "errors": 33, "lower": 0.17, "upper": 0.3},
            },
            1: {
                "implicit": {"eph": 0.07, "errors": 10, "lower": 0.04, "upper": 0.11},
                "mbp": {"eph": 0.08, "errors": 11, "lower": 0.04, "upper": 0.13},
                "mgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mgsc": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mman": {"eph": 0.15, "errors": 21, "lower": 0.1, "upper": 0.21},
                "nonnav": {"eph": 0.08, "errors": 10, "lower": 0.04, "upper": 0.12},
                "oneway": {"eph": 0.45, "errors": 61, "lower": 0.36, "upper": 0.54},
                "sgeo": {"eph": 0.01, "errors": 2, "lower": 0.0, "upper": 0.04},
                "turnrest": {"eph": 0.23, "errors": 32, "lower": 0.17, "upper": 0.3},
                "wdtrf": {"eph": 0.45, "errors": 61, "lower": 0.36, "upper": 0.54},
                "wgeo": {"eph": 0.01, "errors": 2, "lower": 0.0, "upper": 0.04},
                "wrc": {"eph": 0.05, "errors": 6, "lower": 0.02, "upper": 0.08},
            },
            2: {
                "implicit": {"eph": 0.05, "errors": 8, "lower": 0.02, "upper": 0.08},
                "mbp": {"eph": 0.06, "errors": 10, "lower": 0.03, "upper": 0.1},
                "mgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mgsc": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mman": {"eph": 0.28, "errors": 38, "lower": 0.21, "upper": 0.35},
                "nonnav": {"eph": 0.3, "errors": 41, "lower": 0.22, "upper": 0.38},
                "oneway": {"eph": 0.08, "errors": 12, "lower": 0.05, "upper": 0.12},
                "sgeo": {"eph": 0.21, "errors": 30, "lower": 0.14, "upper": 0.28},
                "turnrest": {"eph": 0.34, "errors": 48, "lower": 0.26, "upper": 0.42},
                "wdtrf": {"eph": 0.08, "errors": 12, "lower": 0.05, "upper": 0.12},
                "wgeo": {"eph": 0.02, "errors": 3, "lower": 0.0, "upper": 0.04},
                "wrc": {"eph": 0.07, "errors": 8, "lower": 0.03, "upper": 0.11},
            },
            3: {
                "implicit": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mbp": {"eph": 0.01, "errors": 1, "lower": 0.0, "upper": 0.03},
                "mgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mgsc": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mman": {"eph": 0.05, "errors": 5, "lower": 0.02, "upper": 0.08},
                "nonnav": {"eph": 0.04, "errors": 4, "lower": 0.01, "upper": 0.07},
                "oneway": {"eph": 0.07, "errors": 8, "lower": 0.04, "upper": 0.12},
                "sgeo": {"eph": 0.01, "errors": 1, "lower": 0.0, "upper": 0.03},
                "turnrest": {"eph": 0.06, "errors": 6, "lower": 0.02, "upper": 0.09},
                "wdtrf": {"eph": 0.07, "errors": 8, "lower": 0.04, "upper": 0.12},
                "wgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "wrc": {"eph": 0.03, "errors": 3, "lower": 0.0, "upper": 0.06},
            },
            4: {
                "implicit": {"eph": 0.01, "errors": 1, "lower": 0.0, "upper": 0.02},
                "mbp": {"eph": 0.01, "errors": 1, "lower": 0.0, "upper": 0.02},
                "mgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mgsc": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mman": {"eph": 0.04, "errors": 2, "lower": 0.0, "upper": 0.1},
                "nonnav": {"eph": 0.52, "errors": 44, "lower": 0.38, "upper": 0.66},
                "oneway": {"eph": 0.16, "errors": 13, "lower": 0.08, "upper": 0.25},
                "sgeo": {"eph": 0.34, "errors": 31, "lower": 0.23, "upper": 0.47},
                "turnrest": {"eph": 0.05, "errors": 3, "lower": 0.0, "upper": 0.1},
                "wdtrf": {"eph": 0.16, "errors": 13, "lower": 0.08, "upper": 0.25},
                "wgeo": {"eph": 0.17, "errors": 12, "lower": 0.08, "upper": 0.26},
                "wrc": {"eph": 0.01, "errors": 1, "lower": 0.0, "upper": 0.02},
            },
            5: {
                "implicit": {"eph": 0.06, "errors": 5, "lower": 0.02, "upper": 0.11},
                "mbp": {"eph": 0.02, "errors": 3, "lower": 0.0, "upper": 0.05},
                "mgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mgsc": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mman": {"eph": 0.3, "errors": 25, "lower": 0.2, "upper": 0.4},
                "nonnav": {"eph": 0.12, "errors": 15, "lower": 0.07, "upper": 0.18},
                "oneway": {"eph": 0.19, "errors": 22, "lower": 0.13, "upper": 0.26},
                "sgeo": {"eph": 0.03, "errors": 4, "lower": 0.01, "upper": 0.06},
                "turnrest": {"eph": 0.32, "errors": 28, "lower": 0.22, "upper": 0.42},
                "wdtrf": {"eph": 0.19, "errors": 22, "lower": 0.13, "upper": 0.26},
                "wgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "wrc": {"eph": 0.09, "errors": 11, "lower": 0.05, "upper": 0.14},
            },
            6: {
                "implicit": {"eph": 0.04, "errors": 3, "lower": 0.0, "upper": 0.09},
                "mbp": {"eph": 0.11, "errors": 6, "lower": 0.04, "upper": 0.19},
                "mgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mgsc": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mman": {"eph": 0.21, "errors": 10, "lower": 0.11, "upper": 0.33},
                "nonnav": {"eph": 0.05, "errors": 3, "lower": 0.01, "upper": 0.11},
                "oneway": {"eph": 0.12, "errors": 8, "lower": 0.05, "upper": 0.2},
                "sgeo": {"eph": 0.02, "errors": 1, "lower": 0.0, "upper": 0.07},
                "turnrest": {"eph": 0.32, "errors": 16, "lower": 0.19, "upper": 0.46},
                "wdtrf": {"eph": 0.12, "errors": 8, "lower": 0.05, "upper": 0.2},
                "wgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "wrc": {"eph": 0.03, "errors": 2, "lower": 0.0, "upper": 0.08},
            },
            7: {
                "implicit": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mbp": {"eph": 0.02, "errors": 1, "lower": 0.0, "upper": 0.05},
                "mgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mgsc": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mman": {"eph": 0.92, "errors": 17, "lower": 0.5, "upper": 1.38},
                "nonnav": {"eph": 0.03, "errors": 2, "lower": 0.0, "upper": 0.07},
                "oneway": {"eph": 0.47, "errors": 6, "lower": 0.15, "upper": 0.85},
                "sgeo": {"eph": 0.02, "errors": 1, "lower": 0.0, "upper": 0.05},
                "turnrest": {"eph": 0.93, "errors": 18, "lower": 0.52, "upper": 1.39},
                "wdtrf": {"eph": 0.47, "errors": 6, "lower": 0.15, "upper": 0.85},
                "wgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "wrc": {"eph": 0.02, "errors": 1, "lower": 0.0, "upper": 0.05},
            },
            8: {
                "implicit": {"eph": 0.01, "errors": 2, "lower": 0.0, "upper": 0.03},
                "mbp": {"eph": 0.08, "errors": 11, "lower": 0.04, "upper": 0.12},
                "mgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mgsc": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mman": {"eph": 0.05, "errors": 7, "lower": 0.02, "upper": 0.08},
                "nonnav": {"eph": 0.23, "errors": 37, "lower": 0.17, "upper": 0.29},
                "oneway": {"eph": 0.01, "errors": 2, "lower": 0.0, "upper": 0.03},
                "sgeo": {"eph": 0.12, "errors": 19, "lower": 0.08, "upper": 0.16},
                "turnrest": {"eph": 0.12, "errors": 18, "lower": 0.08, "upper": 0.17},
                "wdtrf": {"eph": 0.01, "errors": 2, "lower": 0.0, "upper": 0.03},
                "wgeo": {"eph": 0.02, "errors": 4, "lower": 0.01, "upper": 0.05},
                "wrc": {"eph": 0.08, "errors": 14, "lower": 0.05, "upper": 0.13},
            },
            9: {
                "implicit": {"eph": 0.1, "errors": 16, "lower": 0.06, "upper": 0.14},
                "mbp": {"eph": 0.02, "errors": 4, "lower": 0.01, "upper": 0.05},
                "mgeo": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mgsc": {"eph": 0.0, "errors": 0, "lower": 0.0, "upper": 0.0},
                "mman": {"eph": 0.05, "errors": 8, "lower": 0.02, "upper": 0.08},
                "nonnav": {"eph": 0.22, "errors": 38, "lower": 0.16, "upper": 0.28},
                "oneway": {"eph": 0.05, "errors": 8, "lower": 0.02, "upper": 0.08},
                "sgeo": {"eph": 0.16, "errors": 28, "lower": 0.11, "upper": 0.21},
                "turnrest": {"eph": 0.08, "errors": 12, "lower": 0.04, "upper": 0.12},
                "wdtrf": {"eph": 0.05, "errors": 8, "lower": 0.02, "upper": 0.08},
                "wgeo": {"eph": 0.05, "errors": 9, "lower": 0.03, "upper": 0.08},
                "wrc": {"eph": 0.01, "errors": 1, "lower": 0.0, "upper": 0.02},
            },
        },
        "weight": {
            0: 0.015,
            1: 0.05,
            2: 0.015,
            3: 0.0025,
            4: 0.0025,
            5: 0.0025,
            6: 0.0025,
            7: 0.0025,
            8: 0.015,
            9: 0.0025,
        },
        "ranking": {
            0: 12.0,
            1: 2.0,
            2: 11.0,
            3: 45.0,
            4: 39.0,
            5: 36.0,
            6: 44.0,
            7: 61.0,
            8: 24.0,
            9: 32.0,
        },
        "turnrest_eph": {
            0: 0.43,
            1: 0.23,
            2: 0.34,
            3: 0.06,
            4: 0.05,
            5: 0.32,
            6: 0.32,
            7: 0.93,
            8: 0.12,
            9: 0.08,
        },
        "oneway_eph": {
            0: 0.85,
            1: 0.45,
            2: 0.08,
            3: 0.07,
            4: 0.16,
            5: 0.19,
            6: 0.12,
            7: 0.47,
            8: 0.01,
            9: 0.05,
        },
        "nonnav_eph": {
            0: 0.76,
            1: 0.08,
            2: 0.3,
            3: 0.04,
            4: 0.52,
            5: 0.12,
            6: 0.05,
            7: 0.03,
            8: 0.23,
            9: 0.22,
        },
        "wdtrf_eph": {
            0: 0.85,
            1: 0.45,
            2: 0.08,
            3: 0.07,
            4: 0.16,
            5: 0.19,
            6: 0.12,
            7: 0.47,
            8: 0.01,
            9: 0.05,
        },
        "mman_eph": {
            0: 0.29,
            1: 0.15,
            2: 0.28,
            3: 0.05,
            4: 0.04,
            5: 0.3,
            6: 0.21,
            7: 0.92,
            8: 0.05,
            9: 0.05,
        },
        "mgsc_eph": {
            0: 0.01,
            1: 0.0,
            2: 0.0,
            3: 0.0,
            4: 0.0,
            5: 0.0,
            6: 0.0,
            7: 0.0,
            8: 0.0,
            9: 0.0,
        },
        "mbp_eph": {
            0: 0.14,
            1: 0.08,
            2: 0.06,
            3: 0.01,
            4: 0.01,
            5: 0.02,
            6: 0.11,
            7: 0.02,
            8: 0.08,
            9: 0.02,
        },
        "implicit_eph": {
            0: 0.09,
            1: 0.07,
            2: 0.05,
            3: 0.0,
            4: 0.01,
            5: 0.06,
            6: 0.04,
            7: 0.0,
            8: 0.01,
            9: 0.1,
        },
        "wrc_eph": {
            0: 0.23,
            1: 0.05,
            2: 0.07,
            3: 0.03,
            4: 0.01,
            5: 0.09,
            6: 0.03,
            7: 0.02,
            8: 0.08,
            9: 0.01,
        },
        "mgeo_eph": {
            0: 0.0,
            1: 0.0,
            2: 0.0,
            3: 0.0,
            4: 0.0,
            5: 0.0,
            6: 0.0,
            7: 0.0,
            8: 0.0,
            9: 0.0,
        },
        "wgeo_eph": {
            0: 0.11,
            1: 0.01,
            2: 0.02,
            3: 0.0,
            4: 0.17,
            5: 0.0,
            6: 0.0,
            7: 0.0,
            8: 0.02,
            9: 0.05,
        },
        "sgeo_eph": {
            0: 0.43,
            1: 0.01,
            2: 0.21,
            3: 0.01,
            4: 0.34,
            5: 0.03,
            6: 0.02,
            7: 0.02,
            8: 0.12,
            9: 0.16,
        },
    }
)

test_error_type_json = {
    "WRC": {"errors": 1, "eph": 0.2982848620432513, "fr": 0.009009009009009009},
    "WDTRF": {"errors": 1, "eph": 0.2982848620432513, "fr": 0.009009009009009009},
    "SGEO": {"errors": 1, "eph": 0.2982848620432513, "fr": 0.009009009009009009},
    "MGSC": {"errors": 1, "eph": 0.2982848620432513, "fr": 0.009009009009009009},
    "MMAN": {"errors": 2, "eph": 0.5965697240865026, "fr": 0.018018018018018018},
    "MBP": {"errors": 2, "eph": 0.5965697240865026, "fr": 0.018018018018018018},
    "IMPLICIT": {"errors": 1, "eph": 0.2982848620432513, "failed_routes": 0.009009009009009009}
}


class TestPipeline:
    """Pipeline tests"""

    def test_tbt_aggregation_ww_pipeline(self):
        """Test pipeline outputs"""
        pipeline = tbt.pipelines.aggregation_ww.create_pipeline()
        pipeline_outputs = pipeline.outputs()

        assert "tbt_aggregation_ww_results" in pipeline_outputs

    def test_get_latest_products(self):
        test_results = get_latest_products(
                test_aggregation_ww_options,
                test_latest_products,
                test_inspections_data,
                test_base_ww_agg_inspections_available
            )
        
        assert isinstance(test_results[0], list)
        assert isinstance(test_results[1], str)

        if len(test_results[0]) > 0:
            assert len(test_results[1]) > 0

    def test_get_inspections(self):
        """Test get correct inspections"""
        test_results = get_inspections(
            test_aggregation_ww_options["run_ids"],
            test_aggregation_ww_options["base_run_id_ww"],
            test_inspections_data,
            test_aggregation_ww_data
        )

        assert isinstance(test_results[0], pandas.DataFrame)
        assert isinstance(test_results[1], pandas.DataFrame)

        assert test_results[0].shape[0] > 0

        assert test_results[0].mapdate.values[0] is not None
        assert test_results[0].shape[0] > 0

    def test_update_agg_source(self):
        """Test update agg_source node function"""
        test_results = update_agg_source(
            test_aggregation_ww_options,
            test_weights,
            test_base_ww_agg_inspections,
            test_new_inspections,
        )

        assert isinstance(test_results, pandas.DataFrame)
        assert test_results.shape[0] > 0
        assert test_results.mapdate.values[0] is not None
        assert "mbp_eph" in test_results.columns

    def test_get_results(self):
        """Test get_results node function"""

        test_results = get_results(
            str(uuid.uuid4()), test_aggregation_ww_options, test_ww_agg_inspections
        )

        assert isinstance(test_results[0], str)

        assert test_results[1].shape[0] > 0
        assert math.isclose(test_results[1].eph.values[0], 1.10128)
        assert test_results[1].publish_value_stream.values[0] is None
        assert test_results[1].publish_harold.values[0] is None
        assert test_results[1].mapdate.values[0] is not None
        assert isinstance(json.loads(test_results[1].metadata.values[0]), dict)
        assert isinstance(json.loads(test_results[1].eph_types.values[0]), dict)

        assert isinstance(test_results[1], pandas.DataFrame)

    def test_get_ww_agg_inspections(self):
        """Test get_ww_agg_inspcetion node function"""

        test_results = get_ww_agg_inspections(
            test_inspections_data, test_aggregation_ww_options["run_ids"]
        )

        assert isinstance(test_results, pandas.DataFrame)

    def test_unify_error_type_json(self):
        """Test get_ww_agg_inspcetion node function"""

        test_results = unify_error_type_json(
            test_error_type_json
        )

        assert test_results is not None
        assert len(test_results) > 0
        assert 'turnrest' in test_results.keys()
        assert 'oneway' in test_results.keys()
        assert 'nonnav' in test_results.keys()
        assert test_results['wdtrf']['eph'] == 0.2982848620432513
        assert test_results['wgeo']['eph'] == 0

    def test_data_to_binary(self):
        test_results = data_to_binary(
            {"df1":test_base_ww_agg_inspections_available}
        )

        assert isinstance(test_results, bytes)