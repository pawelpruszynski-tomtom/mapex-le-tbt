"""Functions to pipelines that run tbt inspection"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

import pandas
import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
import sqlalchemy

from tbt.navutils.common.dict_ops import dict_update

from .metric import create_hdr_metric, create_tbt_metric

log = logging.getLogger(__name__)

def populate_critical_sections_with_mcp_feedback_queries(
    completed_new_runs: pandas.DataFrame,
):
    """It receives the completed new inspections and adds the mcp feedback to the critical sections
    If completed_new_runs is empty it stops the execution of the metric calculation

    :param completed_new_runs: Completed new runs to be populated with MCP feedback
    :type completed_new_runs: pandas.DataFrame
    :param az_db_creds: azure database credentials with keys `host, port, database, user, password`.
    :type az_db_creds: Dict
    :return: Critical sections with MCP feedback and Error logs
    :rtype: [pandas.DataFrame, pandas.DataFrame]
    """

    run_ids = completed_new_runs["run_id"].to_list()
    run_ids_str = str(run_ids)[1:-1] if len(run_ids) > 0 else "null"

    log.info(f"Number of inspections to compute metric for = {len(run_ids)}")

    error_logs_query = f"""
    select
        run_id::text run_id
        , route_id::text route_id
        , case_id::text case_id
        , st_astext(stretch) as stretch
        , st_astext(provider_route) as provider_route
        , st_astext(competitor_route) as competitor_route
        , error_type
        , review_date
        , evaluated_by
        , comment
        , error_subtype
        , country
        , provider
        , product
        , competitor
    from tbt.error_logs
    where run_id in ({run_ids_str})
    """

    critical_sections_with_mcp_feedback_query = f"""
    select
        el.run_id::text run_id
        , el.route_id::text route_id
        , el.case_id::text case_id
        , el.error_type as mcp_state
        , error_subtype
    from tbt.error_logs el
    where el.run_id in ({run_ids_str})
    """

    critical_sections_with_mcp_feedback_history_query = f"""
    select
        run_id::text run_id
        , route_id::text route_id
        , case_id::text case_id
        , mcp_state
    from tbt.critical_sections_with_mcp_feedback cs
    where cs.run_id in ({run_ids_str})
    """

    return (
        completed_new_runs,
        error_logs_query,
        critical_sections_with_mcp_feedback_query,
        critical_sections_with_mcp_feedback_history_query,
    )


def get_routes_data_queries(
    completed_new_runs_from_memory: pandas.DataFrame,
) -> Tuple[str, str]:
    run_ids = completed_new_runs_from_memory["run_id"].to_list()
    run_ids_str = str(run_ids)[1:-1] if len(run_ids) > 0 else "null"

    inspection_routes_query = f"""
    select
        ir.run_id::text run_id,
        ir.route_id::text route_id,
        ir.provider_route_length/1000 as prov_km,
        ir.provider_route_time/3600 as prov_hours,
        ir.competitor_route_length/1000 as comp_km,
        ir.competitor_route_time/3600 as comp_hours,
        ir.rac_state,
        ss.quality,
        sm.metric like '%%HDR%%' hdr
    from tbt.inspection_routes ir
    left join directions_shared_components.sampling_samples ss on ss.sample_id=ir.sample_id and ss.route_id = ir.route_id
    left join directions_shared_components.sampling_metadata sm on sm.sample_id = ss.sample_id
    where ir.run_id in ({run_ids_str})
    """

    return (inspection_routes_query, True)


def prepare_data_for_metric_computation(
    critical_sections_with_mcp_feedback: pandas.DataFrame,
    critical_sections_with_mcp_feedback_history: pandas.DataFrame,
    inspection_routes: pandas.DataFrame,
    dummy: bool,
):
    # Adding together newly evaluated critical sections with the existing history for the given run_ids
    critical_sections_with_mcp_feedback_full = pandas.concat(
        [
            critical_sections_with_mcp_feedback,  # New critical sections (the ones still in error_logs table)
            critical_sections_with_mcp_feedback_history,  # Reused critical sections (the ones that were moved to critical_sections_with_mcp_feedback in inspection pipelin after matching with existing logs)
        ]
    )

    # Joining everything together to compute the metrics in next nodes
    metric_computation_data = inspection_routes.merge(
        critical_sections_with_mcp_feedback_full,
        how="left",
        on=["run_id", "route_id"],
    )

    return metric_computation_data


def compute_metrics(
    completed_new_runs_from_memory: pandas.DataFrame,
    metric_computation_data: pandas.DataFrame,
):
    """It computes TbT 4.x metrics for completed new runs.

    .. note::
        This takes effect after the automatic part of the process
        and after MCP evaluation of the potential errors.

    :param completed_new_runs: Completed new runs
    :type completed_new_runs: pandas.DataFrame
    :param az_db_creds: azure database credentials
    :type az_db_creds: Dict
    :return: Metrics for the new completed runs
    :rtype: pandas.DataFrame
    """


    metrics = pandas.DataFrame()
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    bootstrap_sdf = spark.createDataFrame(
        [],
        schema=T.StructType(
            [
                T.StructField("seed", T.IntegerType()),
                T.StructField("eph", T.FloatType()),
                T.StructField("failed_routes", T.FloatType()),
                T.StructField("provider_hours", T.FloatType()),
                T.StructField("provider_km", T.FloatType()),
                T.StructField("competitor_hours", T.FloatType()),
                T.StructField("competitor_km", T.FloatType()),
                T.StructField("errors", T.IntegerType()),
                T.StructField("routes", T.IntegerType()),
                T.StructField("run_id", T.StringType()),
            ]
        ),
    )
    
    
    if completed_new_runs_from_memory.empty:
        log.info('There are no inspections to compute the metric')
        return (metrics, bootstrap_sdf, False) 

    if len(metric_computation_data) > 0:
        run_ids = metric_computation_data["run_id"].unique()

        for run_id in run_ids:
            log.info("Computing metrics for run_id='%s'", run_id)

            # Filtering the data for the specific inspection we want to compute metrics for
            metric_computation_data_i = metric_computation_data[
                metric_computation_data["run_id"] == run_id
            ]

            # Make the metric more robust, removing routes with more than 3 (actual) errors
            invalid_routes_num, invalid_routes = filter_routes_num_errors(
                metric_computation_data_i
            )

            log.info(
                f"There are {invalid_routes_num} with more than three errors,"
                " removed from metric computation."
            )

            metric_computation_data_i = metric_computation_data_i[
                ~metric_computation_data_i["route_id"].isin(invalid_routes)
            ]
            is_hdr = metric_computation_data_i["hdr"].values[0]

            metrics_i, bootstrap_i = metric_calculation_run_id(
                run_id=run_id,
                df_routes=metric_computation_data_i,
                is_hdr=is_hdr,
                resamples=1000,
            )

            metrics_i["inspection_routes"] = metric_computation_data_i.drop_duplicates(
                subset="route_id"
            ).shape[0]
            metrics_i["invalid_routes"] = invalid_routes_num > 0
            metrics_i["invalid_routes_info"] = json.dumps(
                {
                    "num_routes_invalid": invalid_routes_num,
                    "invalid_routes": invalid_routes,
                }
            )

            bootstrap_i = bootstrap_i.withColumn("run_id", F.lit(run_id))

            bootstrap_sdf = bootstrap_sdf.unionByName(bootstrap_i)

            metrics = pandas.concat([metrics, metrics_i])

    metrics = metrics[
        [
            "run_id",
            "eph",
            "failed_routes",
            "provider_hours",
            "provider_km",
            "competitor_hours",
            "competitor_km",
            "errors",
            "eph_lower",
            "failed_routes_lower",
            "eph_upper",
            "failed_routes_upper",
            "metrics_per_error_type",
            "rac_state",
            "metrics_per_mqs",
            "invalid_routes",
            "invalid_routes_info",
            "inspection_routes",
        ]
    ]

    return (metrics, bootstrap_sdf, True)


def get_inspection_info_query(
    completed_new_runs_from_memory: pandas.DataFrame,
    dummy: bool,
):
    """Function to get sample info query

    :param completed_new_runs_from_memory: completed_new_runs_from_memory table with extra info for metrics table
    :type completed_new_runs_from_memory: pandas.DataFrame
    :return: same outputs as input
    :rtype: pandas.DataFrame
    """

    run_ids = completed_new_runs_from_memory["run_id"].to_list()
    run_ids_str = str(run_ids)[1:-1] if len(run_ids) > 0 else "null"

    query = f"""
            select
                im.run_id::text
                , im.sample_id::text
                , im.provider 
                , im.country 
                , im.product 
                , im.mapdate 
                , im.sanity_fail as sanity_failed
                , im.sanity_msg as sanity_info
                , sm.final_routes as sampled_routes
            from tbt.inspection_metadata im 
            left join directions_shared_components.sampling_metadata sm on im.sample_id = sm.sample_id 
            where im.run_id in ({run_ids_str})
                ;
            """

    return (query, dummy)


# Missing: parameters, inspection_routes


def append_inspection_info(
    scheduled_inspection_info: pandas.DataFrame, metrics: pandas.DataFrame, dummy: bool
):
    
    if not dummy:
        return (metrics, False)
    
    metrics = metrics.merge(scheduled_inspection_info, how="left", on="run_id")
    # To avoid errors when setting publish_vs and publish_harold
    metrics["sanity_failed"] = metrics["sanity_failed"].replace([None], [False])

    # Renaming fields
    metrics = metrics.rename(
        columns={
            "provider_km": "provider_kms",
            "competitor_km": "competitor_kms",
            "failed_routes": "fr",
            "failed_routes_lower": "fr_lower",
            "failed_routes_upper": "fr_upper",
            "metrics_per_mqs": "metrics_per_level",
        }
    )

    # Adding missing parameters
    metrics["metric"] = "TbT"
    metrics["metric_computation_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    metrics["publish_value_stream"] = ~metrics["sanity_failed"]
    metrics["publish_harold"] = ~metrics["sanity_failed"]
    metrics.loc[
        ~(
            (~metrics["provider"].str.contains("Orbis", case=False))
            | (
                metrics["provider"].str.contains("Orbis", case=False)
                & metrics["product"].str.contains("ONA")
            )
        ),
        "publish_harold",
    ] = False

    metrics["publish_value_stream"] = ~metrics["sanity_failed"]
    metrics["publish_harold"] = ~metrics["sanity_failed"]
    metrics = metrics.drop(columns="sanity_failed")
    metrics["sanity_info"] = metrics["sanity_info"].apply(json.dumps)
    return metrics, True


def export_to_sql(
    metrics: pandas.DataFrame,
    critical_sections_with_mcp_feedback: pandas.DataFrame,
    error_logs: pandas.DataFrame,
    dummy: bool,
):
    """Dummy function to explicitly export to SQL with kedro

    :param metrics: scheduled_metrics table
    :type metrics: pandas.DataFrame
    :param critical_sections_with_mcp_feedback: critical_sections_with_mcp_feedback table
    :type critical_sections_with_mcp_feedback: pandas.DataFrame
    :param error_logs: error_logs table
    :type error_logs: pandas.DataFrame
    :return: same outputs as input
    :rtype: **pandas.DataFrame
    """
    return (metrics, critical_sections_with_mcp_feedback, error_logs, dummy)


def export_to_spark(
    metrics: pandas.DataFrame,
    bootstrap_sdf: pyspark.sql.DataFrame,
    critical_sections_with_mcp_feedback: pandas.DataFrame,
    error_logs: pandas.DataFrame,
    dummy: bool,
):
    """Converts pandas tables to spark DataFrames to export from kedro

    :param metrics: scheduled_metrics table
    :type metrics: pandas.DataFrame
    :param bootstrap_sdf: scheduled_bootstrap table
    :type bootstrap_sdf: pyspark.sql.DataFrame
    :param critical_sections_with_mcp_feedback: critical_sections_with_mcp_feedback table
    :type critical_sections_with_mcp_feedback: pandas.DataFrame
    :param error_logs: error_logs table
    :type error_logs: pandas.DataFrame
    :return: converted tables
    :rtype: **pyspark.sql.DataFrame
    """
    if len(metrics) == 0:
        metrics = []
        critical_sections_with_mcp_feedback = []
        error_logs = []
    else:
        metrics = metrics.rename(
            columns={
                "provider_kms": "provider_km",
                "competitor_kms": "competitor_km",
                "inspection_routes": "routes",
                "metrics_per_level": "metrics_per_mqs",
                "fr": "failed_routes",
                "fr_lower": "failed_routes_lower",
                "fr_upper": "failed_routes_upper",
            }
        )
        metrics = metrics[
            [
                "run_id",
                "eph",
                "provider_hours",
                "provider_km",
                "competitor_hours",
                "competitor_km",
                "errors",
                "routes",
                "metrics_per_error_type",
                "metrics_per_mqs",
                "publish_value_stream",
                "rac_state",
                "eph_lower",
                "eph_upper",
                "failed_routes",
                "failed_routes_lower",
                "failed_routes_upper",
                "invalid_routes",
                "invalid_routes_info",
            ]
        ]
        
        critical_sections_with_mcp_feedback["reference_case_id"] = None

    bootstrap_sdf = bootstrap_sdf.select(
        "seed",
        "eph",
        "provider_hours",
        "provider_km",
        "competitor_hours",
        "competitor_km",
        "errors",
        "routes",
        "run_id",
        "failed_routes",
    )
    bootstrap_sdf = bootstrap_sdf.withColumn("seed", F.col("seed").cast("int"))
    bootstrap_sdf = bootstrap_sdf.withColumn("eph", F.col("eph").cast("float"))
    bootstrap_sdf = bootstrap_sdf.withColumn(
        "failed_routes", F.col("failed_routes").cast("float")
    )
    bootstrap_sdf = bootstrap_sdf.withColumn(
        "provider_hours", F.col("provider_hours").cast("float")
    )
    bootstrap_sdf = bootstrap_sdf.withColumn(
        "provider_km", F.col("provider_km").cast("float")
    )
    bootstrap_sdf = bootstrap_sdf.withColumn(
        "competitor_hours", F.col("competitor_hours").cast("float")
    )
    bootstrap_sdf = bootstrap_sdf.withColumn(
        "competitor_km", F.col("competitor_km").cast("float")
    )
    bootstrap_sdf = bootstrap_sdf.withColumn("routes", F.col("routes").cast("int"))

    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    return (
        spark.createDataFrame(
            metrics,  # type: ignore
            schema=T.StructType(
                [
                    T.StructField("run_id", T.StringType()),
                    T.StructField("eph", T.FloatType()),
                    T.StructField("provider_hours", T.FloatType()),
                    T.StructField("provider_km", T.FloatType()),
                    T.StructField("competitor_hours", T.FloatType()),
                    T.StructField("competitor_km", T.FloatType()),
                    T.StructField("errors", T.IntegerType()),
                    T.StructField("routes", T.IntegerType()),
                    T.StructField("metrics_per_error_type", T.StringType()),
                    T.StructField("metrics_per_mqs", T.StringType()),
                    T.StructField("publish_value_stream", T.BooleanType()),
                    T.StructField("rac_state", T.StringType()),
                    T.StructField("eph_lower", T.FloatType()),
                    T.StructField("eph_upper", T.FloatType()),
                    T.StructField("failed_routes", T.FloatType()),
                    T.StructField("failed_routes_lower", T.FloatType()),
                    T.StructField("failed_routes_upper", T.FloatType()),
                    T.StructField("invalid_routes", T.BooleanType()),
                    T.StructField("invalid_routes_info", T.StringType()),
                ]
            ),
        ),
        spark.createDataFrame(
            critical_sections_with_mcp_feedback,  # type: ignore
            schema=T.StructType(
                [
                    T.StructField("run_id", T.StringType()),
                    T.StructField("route_id", T.StringType()),
                    T.StructField("case_id", T.StringType()),
                    T.StructField("mcp_state", T.StringType()),
                    T.StructField("reference_case_id", T.StringType()),
                    T.StructField("error_subtype", T.StringType()),
                ]
            ),
        ),
        spark.createDataFrame(
            error_logs,  # type: ignore
            schema=T.StructType(
                [
                    T.StructField("run_id", T.StringType()),
                    T.StructField("route_id", T.StringType()),
                    T.StructField("case_id", T.StringType()),
                    T.StructField("stretch", T.StringType()),
                    T.StructField("provider_route", T.StringType()),
                    T.StructField("competitor_route", T.StringType()),
                    T.StructField("error_type", T.StringType()),
                    T.StructField("review_date", T.DateType()),
                    T.StructField("evaluated_by", T.StringType()),
                    T.StructField("comment", T.StringType()),
                    T.StructField("error_subtype", T.StringType()),
                    T.StructField("country", T.StringType()),
                    T.StructField("provider", T.StringType()),
                    T.StructField("product", T.StringType()),
                    T.StructField("competitor", T.StringType()),
                ]
            ),
        ),
        bootstrap_sdf,
        dummy,
    )


def delete_error_logs(
    completed_new_runs: pandas.DataFrame,
    az_db_creds: Dict,
    tbt_scheduled_spark_ok: bool,
):
    """It removes the error logs from the database for the completed inspections.

    :param completed_new_runs: Completed new runs
    :type completed_new_runs: pandas.DataFrame
    :param az_db_creds: azure database credentials with keys `host, port, database, user, password`.
    :type az_db_creds: Dict
    """
    if len(completed_new_runs) > 0 and tbt_scheduled_spark_ok:
        run_ids = completed_new_runs.run_id.values
        run_ids_str = ",".join([f"'{str(x)}'" for x in run_ids])
        az_db = sqlalchemy.create_engine(
            f'postgresql+psycopg2://{az_db_creds["user"]}:{az_db_creds["password"]}'
            f'@{az_db_creds["host"]}:{az_db_creds["port"]}/{az_db_creds["database"]}'
        ).execution_options(autocommit=True)

        az_db.execute(f"delete from tbt.error_logs where run_id in ({run_ids_str})")

        az_db.dispose()

    return 0


def metric_calculation_run_id(
    run_id: str,
    df_routes: pandas.DataFrame,
    is_hdr: bool = False,
    resamples: int = 1000,
) -> Tuple[pandas.DataFrame, pyspark.sql.DataFrame]:
    """Computes metrics and bootstrap using Metrics funtions

    :param run_id: Inspection run_id
    :type run_id: str
    :param df_routes: All the routes as they come from DB
    :type df_routes: pandas.DataFrame
    :param is_hdr: checks if the metric is hdr, defaults to False
    :type is_hdr: bool, optional
    :return: A pandas DataFrame with one row
    :rtype: pandas.DataFrame
    """

    # Remove routes with no source
    df_routes = df_routes.query("mcp_state != 'no source'")

    # Create QualityMetric object
    if is_hdr:
        quality_metric = create_hdr_metric(routes_df=df_routes, error_col="mcp_state")
    else:
        quality_metric, quality_metric_mqs = create_tbt_metric(
            routes_df=df_routes, error_col="mcp_state"
        )

    # Compute metrics and stats
    metrics_dict = quality_metric.compute_metrics()
    metrics_per_error_type = metrics_dict.pop("per_error_type")
    metrics_per_error_subtype = metrics_dict.pop("per_error_subtype")
    metrics_rac_state = metrics_dict.pop("rac_state")

    # Compute bootstrap
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    bootstrap_dict, bootstrap_df_ = quality_metric.compute_bootstrap(
        resamples=resamples, spark_context=spark.sparkContext
    )
    bootstrap_per_error_type = bootstrap_dict.pop("per_error_type")
    bootstrap_per_error_subtype = bootstrap_dict.pop("per_error_subtype")
    metrics_dict = dict_update(metrics_dict, bootstrap_dict)

    # Adapt bootstrap_df to fit DL schema
    for column in bootstrap_df_.columns:
        if str(bootstrap_df_.schema[column].dataType) == "DoubleType":
            bootstrap_df_ = bootstrap_df_.withColumn(
                column, F.col(column).cast("float")
            )
        elif str(bootstrap_df_.schema[column].dataType) == "LongType":
            bootstrap_df_ = bootstrap_df_.withColumn(column, F.col(column).cast("int"))
    bootstrap_df_ = bootstrap_df_.withColumn("errors", F.col("errors").cast("int"))

    metrics_per_error_type = {
        **dict_update(metrics_per_error_type, bootstrap_per_error_type),
        **dict_update(metrics_per_error_subtype, bootstrap_per_error_subtype),
    }

    # Convert to pd.DataFrame to save on DB
    metrics = pandas.DataFrame(
        {
            "run_id": [run_id],
            **metrics_dict,
        }
    )
    metrics["metrics_per_error_type"] = [json.dumps(metrics_per_error_type)]
    metrics["rac_state"] = json.dumps(metrics_rac_state)

    if is_hdr:
        metrics["metrics_per_mqs"] = {}
        return metrics, bootstrap_df_

    # Compute metrics by mqs (just TbT)
    mqs_results_dict = {}
    for mqs in df_routes.quality.unique():
        routes_df_mqs = df_routes.query(f"quality=='{mqs}'")
        metrics_dict = quality_metric_mqs.compute_metrics(routes_df=routes_df_mqs)
        bootstrap_dict, _ = quality_metric_mqs.compute_bootstrap(
            routes_df=routes_df_mqs,
            resamples=resamples,
            spark_context=spark.sparkContext,
        )
        mqs_results_dict[mqs] = dict_update(metrics_dict, bootstrap_dict)

    metrics["metrics_per_mqs"] = [json.dumps(mqs_results_dict)]

    return metrics, bootstrap_df_


def filter_routes_num_errors(
    df_routes: pandas.DataFrame, errors_threshold: int = 3
) -> Tuple[int, List[str]]:
    """Function to remove routes with more than errors_threshold actual errors

    :param df_routes: Dataframe with the routes
    :type df_routes: pd.DataFrame
    :param errors_threshold: number of erros to filter out a route, defaults to 3
    :type errors_threshold: int, optional
    :return: number of invalid routes and list with the route_id of removed routes
    :rtype: Tuple[pd.DataFrame, int, List[str]]
    """
    tmp_df_routes = df_routes.copy()[
        df_routes.mcp_state.notna()
        & ~(df_routes.mcp_state.isin(["IMPLICIT", "no source", "discard"]))
    ]

    routes_valid = (
        (tmp_df_routes.groupby("route_id").size() <= errors_threshold)
        .rename("valid")
        .reset_index()
    )
    invalid_routes_num = routes_valid[~routes_valid.valid].groupby("route_id").ngroups
    invalid_routes = (
        routes_valid[~routes_valid.valid].groupby("route_id").size().index.tolist()
    )

    return invalid_routes_num, invalid_routes


def _dict_update(dictionary, update):
    for key, value in update.items():
        if isinstance(value, dict):
            dictionary[key] = _dict_update(dictionary.get(key, {}), value)
        else:
            dictionary[key] = value
    return dictionary
