"""Test for rocco pipeline nodes functions"""

import pytest
import pandas

from tbt.pipelines.rocco.nodes import (
    retrieve_run_ids,
    build_query,
    compute_anomalous_routes,
)


def test_retrieve_run_ids():
    hdr_latest_inspections = pandas.DataFrame([
        {'run_id': '39603acb-cced-4952-a740-fdc3d243d2a9'},
        {'run_id': '21e1cb6d-912e-4fa7-98eb-d3545b75e70d'},
    ])

    rocco_inspections = pandas.DataFrame(columns=['run_id', 'fr'])

    run_ids = retrieve_run_ids(
        hdr_latest_inspections=hdr_latest_inspections,
        rocco_inspections=rocco_inspections
    )

    assert type(run_ids) == list
    assert len(run_ids) > 0
    assert '39603acb-cced-4952-a740-fdc3d243d2a9' in run_ids
    assert '21e1cb6d-912e-4fa7-98eb-d3545b75e70d' in run_ids


def test_build_query():
    run_ids = ['39603acb-cced-4952-a740-fdc3d243d2a9', '21e1cb6d-912e-4fa7-98eb-d3545b75e70d']
    run_ids_ = str(run_ids)[1:-1]

    expected_query_1 = f"""
    select 
            run_id::text as run_id,
        route_id::text as route_id,
        st_astext(st_linesubstring(provider_route, 0, 500/provider_route_length)) as provider_route,
        case when competitor_route_length > 500
        	then st_astext(st_linesubstring(competitor_route, 0, 500/competitor_route_length))
        	else st_astext(competitor_route)
        	end as competitor_route,
        rac_state,
        provider_route_length,
        competitor_route_length,
        provider_route_time,
        competitor_route_time,
        case when (provider_route_length - competitor_route_length)/provider_route_length < -1 then -1 else 
            case when (provider_route_length - competitor_route_length)/provider_route_length > 1 then 1
            else (provider_route_length - competitor_route_length)/provider_route_length end end as rel_delta_len,
        provider_route_length - competitor_route_length as delta_len_meters,
        (provider_route_time - competitor_route_time)/60 as delta_time_mins
    from inspection_routes ir 
    where run_id in ({run_ids_})
    and rac_state in ('routing_issue', 'potential_error')
    and provider_route_length > 6250
    and ST_DistanceSphere(origin, destination) > 6250
    """

    expected_query_2 = f"""
    with counts as (
        select 
        run_id::text
            , count(*) as inspection_routes
            , sum(provider_route_length) provider_kms
            , sum(provider_route_time) provider_hours
            , sum(competitor_route_length) competitor_kms
            , sum(competitor_route_time) competitor_hours
        from inspection_routes
        where run_id in ({run_ids_})
        group by run_id
        )
    select
        counts.*
        , im.sample_id::text
        , im.provider 
        , im.country 
        , im.product 
        , now() metric_computation_date
        , im.mapdate 
        , 'ROCCO' metric
        , false publish_value_stream
        , false publish_harold
        , sm.final_routes as sampled_routes
    from counts
    left join inspection_metadata im 
    on counts.run_id = im.run_id::text 
    left join sampling_metadata sm 
    on im.sample_id = sm.sample_id 
    """

    query_inspections_runs, query_inspections_routes_counts, build_query_dummy = build_query(run_ids)

    assert build_query_dummy
    assert type(query_inspections_runs) == str
    assert type(query_inspections_routes_counts) == str
    assert query_inspections_runs == expected_query_1
    assert query_inspections_routes_counts == expected_query_2


def test_compute_anomalous_routes():
    test_rocco_inspections_runs = pandas.read_parquet('src/tests/pipelines/rocco/test_rocco_inspections_runs.pq')
    test_rocco_inspections_routes_counts = pandas.read_parquet('src/tests/pipelines/rocco/test_rocco_inspections_routes_counts.pq')
    test_metric = pandas.read_parquet('src/tests/pipelines/rocco/test_metric.pq')

    metric = compute_anomalous_routes(
        rocco_inspections_runs=test_rocco_inspections_runs,
        rocco_inspections_routes_counts=test_rocco_inspections_routes_counts,
        build_query_success=True
    )

    assert test_metric.shape[0] > 0
    assert '21e1cb6d-912e-4fa7-98eb-d3545b75e70d' in test_metric['run_id'].to_list()
    assert '39603acb-cced-4952-a740-fdc3d243d2a9' in test_metric['run_id'].to_list()
    assert metric.iloc[0, 1] == test_metric.iloc[0, 1] # errors run_id 1
    assert metric.iloc[1, 1] == test_metric.iloc[1, 1] # errors run_id 2
    assert metric.iloc[0, 2] == test_metric.iloc[0, 2] # routes run_id 1
    assert metric.iloc[1, 2] == test_metric.iloc[1, 2] # routes run_id 2