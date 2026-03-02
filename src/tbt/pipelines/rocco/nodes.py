"""Nodes used by the ROCCO inspection pipeline"""

import pandas

import json

import shapely.wkt
import shapely.ops

import typing
import logging

log = logging.getLogger(__name__)

def retrieve_run_ids(
    hdr_latest_inspections: pandas.DataFrame,
    rocco_inspections: pandas.DataFrame,
) -> typing.List[str]:
    """It retrieves the run ids for the latest HDR inspections for which there are no ROCCO metrics yet.

    :param hdr_latest_inspections: Latest HDR inspections (only considering last 30 days)
    :type hdr_latest_inspections: pandas.DataFrame
    :param rocco_inspections: All available ROCCO inspections
    :type rocco_inspections: pandas.DataFrame
    :return: list of run ids
    :rtype: list
    """
    # Merge hdr_latest_inspections (the ones that could be inspected with ROCCO)
    # with rocco_inspections (hdr_inspections that were already inspected with ROCCO)
    new_hdr_inspections = hdr_latest_inspections.merge(
        rocco_inspections,
        how='left',
        on='run_id'
    )

    # After the merge, the run_ids with no fr computed are new ones, so we want to compute ROCCO for those
    new_hdr_inspections = new_hdr_inspections[new_hdr_inspections['fr'].isna()]
    return new_hdr_inspections['run_id'].to_list() # We just need the run ids


def build_query(
        run_ids:typing.List
) -> typing.Tuple[str, str, bool]:
    """It builds the query to retrieve the information corresponding to the given run ids
    
    :param run_ids: list of run ids for which there are no ROCCO metrics
    :type rocco_inspections: list
    :return: query_inspections_runs, query_inspections_routes_counts, boolean dummy
    :rtype: str, str, bool
    """
    # If there are no new run_ids to be inspected with ROCCO, just return empty queries
    if len(run_ids) < 1:
        log.info("No run_ids supplied. Returning empty query")
        query = "select null as col where False"
        return query, query, False

    run_ids = str(run_ids)[1:-1]
    log.info(
        "Using run_id in (%s)", run_ids
    )
    # This query is to get the hdr routes and the characteristics to compute the anomalous routes per run_id
    # Routing algorithm starts optimizing RCs for routes longer than 6.25 kms. That's why we'll only focus on routes whose ODs are farther away
    # Fo the anomalous part of the analysis, we only need the first 500 meters of the routes to make sure both provider and competitor have the same start
    # and they are not starting in opposite directions because of slightly moved origins for instance. That's why we cut the routes and only take the first 500 meters.
    query_inspections_runs = f"""
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
    where run_id in ({run_ids})
    and rac_state in ('routing_issue', 'potential_error')
    and provider_route_length > 6250
    and ST_DistanceSphere(origin, destination) > 6250
    """

    # These are the counts by run_id to compute statistics later on: FR
    query_inspections_routes_counts = f"""
    with counts as (
        select 
        run_id::text
            , count(*) as inspection_routes
            , sum(provider_route_length) provider_kms
            , sum(provider_route_time) provider_hours
            , sum(competitor_route_length) competitor_kms
            , sum(competitor_route_time) competitor_hours
        from inspection_routes
        where run_id in ({run_ids})
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

    return query_inspections_runs, query_inspections_routes_counts, True


def compute_anomalous_routes(
        rocco_inspections_runs: pandas.DataFrame,
        rocco_inspections_routes_counts: pandas.DataFrame,
        build_query_success: bool,
) -> pandas.DataFrame:
    """It computes the anomalous routes metric for the given HDR inspections

    :param rocco_inspections_runs: routes and related information for the given inspections
    :type rocco_inspections_runs: pandas.DataFrame
    :param rocco_inspections_routes_counts: Additional metadata for the given inspections, such as sample_id, provider, etc...
    :type rocco_inspections_routes_counts: pandas.DataFrame
    :param build_query_success: dummy variable to keep the nodes order
    :type build_query_success: bool
    :return: metrics for the given run ids
    :rtype: pandas.DataFrame
    """    
    # If there are no new rocco inspections, just return an empty dataframe that won't be saved to the DB
    if len(rocco_inspections_runs) < 1:
        log.info("No results from built query")
        return pandas.DataFrame()
    
    # Filter out empty routes
    rocco_inspections_runs = rocco_inspections_runs[
        ~(rocco_inspections_runs['provider_route']=='LINESTRING EMPTY')&
        ~(rocco_inspections_runs['competitor_route']=='LINESTRING EMPTY')
    ]

    # Compute if routes have the same start
    # This is to avoid the cases where routes have origins slightly different, causing the routes to be completely different because of the Origin and not the map features
    rocco_inspections_runs['same_start'] = rocco_inspections_runs.apply(lambda row: routes_have_the_same_start(
        shapely.wkt.loads(row['provider_route']),
        shapely.wkt.loads(row['competitor_route'])
        ), axis=1)
    
    log.info("Routing issue: (%s)", len(rocco_inspections_runs))
    log.info("More than 20% relative difference: (%s)", len(rocco_inspections_runs[(rocco_inspections_runs.rel_delta_len > 0.2)]))
    log.info("Same start: (%s)", len(rocco_inspections_runs[rocco_inspections_runs.same_start & (rocco_inspections_runs.rel_delta_len > 0.2)]))

    # Spot routes' outliers by length
    rocco_inspections_runs['length_outlier'] = rocco_inspections_runs.rel_delta_len > 0.2

    # Get the rac_state to save it in the metrics table
    routes_rac = (
            rocco_inspections_runs
            .groupby(['run_id', 'rac_state'], as_index=False)
            .agg({'route_id':'count'})
            .pivot(index='run_id', columns='rac_state', values='route_id')
            .reset_index()
        )
    routes_rac['rac_state'] = (
            routes_rac[['potential_error', 'routing_issue']]
            .fillna(0)
            .astype(int)
            .to_dict(orient='records')
        )
    routes_rac['rac_state'] = routes_rac['rac_state'].apply(json.dumps)

    # Get the anomalous routes
    routes_anomalous = rocco_inspections_runs[rocco_inspections_runs['length_outlier']&rocco_inspections_runs['same_start']]

    # In case we don't find any anomalous routes, we set the errors to 0
    if len(routes_anomalous) < 1:
        log.info("No anomalous routes found")
        metric = rocco_inspections_routes_counts
        metric['errors'] = 0
    else:
        # Merging and pivoting data
        metric = (
            routes_anomalous
            .groupby(['run_id'], as_index=False)
            .agg({'route_id':'count'})
            .rename({'route_id':'errors'}, axis=1)
        )

        # Appending additional information about inspection: the counts for the statistics (fr)
        metric = metric.merge(rocco_inspections_routes_counts, how='left', on='run_id')

    # Appending rac_state and computing fr
    metric = metric.merge(routes_rac.loc[:, ['run_id', 'rac_state']], how='left', on='run_id')
    metric['fr'] = metric['errors'] / metric['inspection_routes']
    
    log.info("Saving to database results for: (%s)", metric.run_id.to_list())
    return metric


##################### SUPPORT FUNCTIONS #####################
def routes_have_the_same_start(prov, comp):
    return (shapely.ops.substring(prov, 0, 0.001).buffer(0.0002).contains(shapely.ops.substring(comp, 0, 0.001)) 
            and 
            shapely.ops.substring(comp, 0, 0.001).buffer(0.0002).contains(shapely.ops.substring(prov, 0, 0.001))
    )
