"""Node: get_fcd_state — FCD evaluation + ML model."""

import logging
import uuid
from time import time

import pyspark.sql
import pyspark.sql.types as T
from pyspark.sql.functions import lit

import tbt.navutils.common.decorators as decorators
from tbt.ecmodel.error_classification.ec_end2end import ErrorClassification
from tbt.pipelines.inspection.domain.geometry import convert_to_linestring, get_length
from tbt.utils.console_print import conditional_print

log = logging.getLogger(__name__)


@decorators.timing
def get_fcd_state(
    tbt_options: dict,
    run_id: str,
    tbt_new_critical_sections: pyspark.sql.DataFrame,
    fcd_credentials: dict,
    ml_model_options: dict,
    sample_metric: str,
):
    """Evaluate critical sections and assign a provisional fcd_state using an ML model.

    :param tbt_options: Pipeline options.
    :param run_id: Unique identifier for the inspection (or ``"auto"`` to generate one).
    :param tbt_new_critical_sections: Critical sections not yet evaluated by MCP.
    :param fcd_credentials: Credentials for FCD data access.
    :param ml_model_options: ML model configuration.
    :param sample_metric: ``"TbT"`` or ``"HDR"``.
    :return: (run_id, critical_sections_with_fcd_state, ec_model_log_table, elapsed_time)
    """
    total_time = time()

    if run_id == "auto":
        run_id = str(uuid.uuid4())

    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    if (
        tbt_new_critical_sections.count() > 0
        and tbt_options["error_classification_mode"] == "ML"
    ):
        tbt_new_critical_sections = tbt_new_critical_sections.withColumn(
            "run_id", lit(run_id)
        )
        pdf, ec_model_log_table = evaluate_with_ml_model(
            tbt_new_critical_sections=tbt_new_critical_sections,
            fcd_credentials=fcd_credentials,
            ml_model_options=ml_model_options,
            sample_metric=sample_metric,
        )

    elif (
        tbt_new_critical_sections.count() > 0
        and tbt_options["error_classification_mode"] != "ML"
    ):
        pdf = tbt_new_critical_sections.toPandas()
        pdf["stretch_length"] = (
            pdf["stretch"].apply(convert_to_linestring).apply(get_length)
        )
        pdf["fcd_state"] = "potential_error"
        pdf["pra"] = -1.0
        pdf["prb"] = -1.0
        pdf["prab"] = -1.0
        pdf["lift"] = -1.0
        pdf["tot"] = -1
        pdf = pdf[
            [
                "route_id", "case_id", "stretch", "stretch_length",
                "fcd_state", "pra", "prb", "prab", "lift", "tot",
            ]
        ]
        ec_model_log_table = []

    else:
        pdf = []
        ec_model_log_table = []

    tbt_critical_sections_with_fcd_state = spark.createDataFrame(
        pdf,
        schema=T.StructType(
            [
                T.StructField("route_id", T.StringType()),
                T.StructField("case_id", T.StringType()),
                T.StructField("stretch", T.StringType()),
                T.StructField("stretch_length", T.FloatType()),
                T.StructField("fcd_state", T.StringType()),
                T.StructField("pra", T.FloatType()),
                T.StructField("prb", T.FloatType()),
                T.StructField("prab", T.FloatType()),
                T.StructField("lift", T.FloatType()),
                T.StructField("tot", T.IntegerType()),
            ]
        ),
    )

    ec_model_log_table = spark.createDataFrame(
        ec_model_log_table,
        schema=T.StructType(
            [
                # metadata
                T.StructField("run_id", T.StringType()),
                T.StructField("country", T.StringType()),
                T.StructField("provider", T.StringType()),
                T.StructField("route_id", T.StringType()),
                T.StructField("case_id", T.StringType()),
                T.StructField("route", T.StringType()),
                T.StructField("stretch", T.StringType()),
                # General features
                T.StructField("is_tbt", T.IntegerType()),
                T.StructField("is_hdr", T.IntegerType()),
                T.StructField("drive_left_side", T.IntegerType()),
                T.StructField("drive_right_side", T.IntegerType()),
                # Geometrical features
                T.StructField("route_length", T.FloatType()),
                T.StructField("stretch_length", T.FloatType()),
                T.StructField("stretch_start_position_in_route", T.FloatType()),
                T.StructField("stretch_end_position_in_route", T.FloatType()),
                T.StructField("stretch_starts_at_the_route_start", T.FloatType()),
                T.StructField("stretch_ends_at_the_route_end", T.FloatType()),
                T.StructField("heading_stretch", T.FloatType()),
                T.StructField("left_turns", T.FloatType()),
                T.StructField("straight_turns", T.FloatType()),
                T.StructField("right_turns", T.FloatType()),
                T.StructField("total_right_angle", T.FloatType()),
                T.StructField("max_right_angle", T.FloatType()),
                T.StructField("total_left_angle", T.FloatType()),
                T.StructField("max_left_angle", T.FloatType()),
                T.StructField("absolute_angle", T.FloatType()),
                T.StructField("has_left_turns", T.FloatType()),
                T.StructField("has_right_turns", T.FloatType()),
                T.StructField("min_curvature", T.FloatType()),
                T.StructField("max_curvature", T.FloatType()),
                T.StructField("mean_curvature", T.FloatType()),
                T.StructField("distance_start_end_stretch", T.FloatType()),
                T.StructField("route_coverage", T.FloatType()),
                T.StructField("stretch_covers_route", T.FloatType()),
                T.StructField("tortuosity", T.FloatType()),
                T.StructField("sinuosity", T.FloatType()),
                T.StructField("density", T.FloatType()),
                # old FCD features
                T.StructField("pra", T.FloatType()),
                T.StructField("prb", T.FloatType()),
                T.StructField("prab", T.FloatType()),
                T.StructField("lift", T.FloatType()),
                T.StructField("tot", T.IntegerType()),
                # new FCD features
                T.StructField("tot_new", T.IntegerType()),
                T.StructField("pra_new", T.FloatType()),
                T.StructField("prb_new", T.FloatType()),
                T.StructField("pra_not_b", T.FloatType()),
                T.StructField("prb_not_a", T.FloatType()),
                T.StructField("pra_and_b", T.FloatType()),
                T.StructField("pra_to_b", T.FloatType()),
                T.StructField("prb_to_a", T.FloatType()),
                T.StructField("tot_contained", T.IntegerType()),
                T.StructField("pra_to_b_contained", T.FloatType()),
                T.StructField("prb_to_a_contained", T.FloatType()),
                T.StructField("pra_to_b_not_contained", T.FloatType()),
                T.StructField("prb_to_a_not_contained", T.FloatType()),
                T.StructField("traffic_direction", T.FloatType()),
                T.StructField("traffic_direction_contained", T.FloatType()),
                T.StructField("ab_intersect", T.IntegerType()),
                # SDO Features
                T.StructField("CONSTRUCTION_AHEAD_TT", T.IntegerType()),
                T.StructField("MANDATORY_STRAIGHT_ONLY", T.IntegerType()),
                T.StructField("MANDATORY_STRAIGHT_OR_LEFT", T.IntegerType()),
                T.StructField("MANDATORY_TURN_RESTRICTION", T.IntegerType()),
                T.StructField("MANDATORY_TURN_LEFT_ONLY", T.IntegerType()),
                T.StructField("MANDATORY_TURN_LEFT_OR_RIGHT", T.IntegerType()),
                T.StructField("MANDATORY_LEFT_OR_STRAIGHT_OR_RIGHT", T.IntegerType()),
                T.StructField("MANDATORY_TURN_RIGHT_ONLY", T.IntegerType()),
                T.StructField("MANDATORY_STRAIGHT_OR_RIGHT", T.IntegerType()),
                T.StructField("NO_ENTRY", T.IntegerType()),
                T.StructField("NO_MOTOR_VEHICLE", T.IntegerType()),
                T.StructField("NO_CAR_OR_BIKE", T.IntegerType()),
                T.StructField("NO_LEFT_OR_RIGHT_TT", T.IntegerType()),
                T.StructField("NO_LEFT_TURN", T.IntegerType()),
                T.StructField("NO_RIGHT_TURN", T.IntegerType()),
                T.StructField("NO_VEHICLE", T.IntegerType()),
                T.StructField("NO_STRAIGHT_OR_LEFT_TT", T.IntegerType()),
                T.StructField("NO_STRAIGHT_OR_RIGHT_TT", T.IntegerType()),
                T.StructField("NO_STRAIGHT_TT", T.IntegerType()),
                T.StructField("NO_TURN_TT", T.IntegerType()),
                T.StructField("NO_U_OR_LEFT_TURN", T.IntegerType()),
                T.StructField("NO_U_TURN", T.IntegerType()),
                T.StructField("ONEWAY_TRAFFIC_TO_STRAIGHT", T.IntegerType()),
                # sdo response
                T.StructField("sdo_api_response", T.StringType()),
                # error label and probability
                T.StructField("probability", T.FloatType()),
                T.StructField("error_label", T.StringType()),
                # Model metadata
                T.StructField("ml_model_options", T.StringType()),
                T.StructField("prediction_date", T.StringType()),
                T.StructField("sample_metric", T.StringType()),
                T.StructField("model_run_id", T.StringType()),
                T.StructField("time_per_critical_section", T.FloatType()),
            ]
        ),
    )

    total_time = time() - total_time
    log.info("FCD state node finished")
    conditional_print("FCD state node finished")

    return run_id, tbt_critical_sections_with_fcd_state, ec_model_log_table, total_time


@decorators.timing
def evaluate_with_ml_model(
    tbt_new_critical_sections: pyspark.sql.DataFrame,
    fcd_credentials: dict,
    ml_model_options: dict,
    sample_metric: str,
):
    """Internal: call the EC model for critical sections."""
    log.info(
        "Calling the EC Model for %i critical sections (using code in submodule ecmodel)",
        tbt_new_critical_sections.count(),
    )
    conditional_print(
        "Calling the EC Model for %i critical sections (using code in submodule ecmodel)",
        tbt_new_critical_sections.count(),
    )

    tbt_new_sections_to_predict = tbt_new_critical_sections.select(
        "run_id", "country", "provider", "route_id", "case_id", "route", "stretch",
    ).toPandas()

    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    error_classification = ErrorClassification(
        pdf=tbt_new_sections_to_predict,
        fcd_credentials=fcd_credentials,
        ml_model_options=ml_model_options,
        sample_metric=sample_metric,
        spark=spark,
    )

    predictions, ec_model_log_table = error_classification.run()

    tbt_new_sections_to_predict["stretch_length"] = ec_model_log_table["stretch_length"]
    tbt_new_sections_to_predict["fcd_state"] = predictions
    tbt_new_sections_to_predict["pra"] = -1.0
    tbt_new_sections_to_predict["prb"] = -1.0
    tbt_new_sections_to_predict["prab"] = -1.0
    tbt_new_sections_to_predict["lift"] = -1.0
    tbt_new_sections_to_predict["tot"] = -1

    return tbt_new_sections_to_predict[
        [
            "route_id", "case_id", "stretch", "stretch_length",
            "fcd_state", "pra", "prb", "prab", "lift", "tot",
        ]
    ], ec_model_log_table



