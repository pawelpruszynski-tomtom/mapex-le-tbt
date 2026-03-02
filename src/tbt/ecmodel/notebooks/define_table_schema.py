# Databricks notebook source
"""
This notebook is to define table schemas used throughout the model training process.
It can be run by running `%run "./define_table_schemas.ipynb"`
"""

# internal data
internal_data_path = f'{BLOB_URL}/error_classification/training/intermediate/internal_data.parquet'

internal_data_schema = T.StructType(
    [
        T.StructField("route_id", T.StringType()),
        T.StructField("case_id", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("country", T.StringType()),
        T.StructField("provider", T.StringType()),
        T.StructField("stretch", T.StringType()),
        T.StructField("route", T.StringType()),
        T.StructField("stretch_length", T.FloatType()),
        T.StructField("metric", T.StringType()),
        T.StructField("error_label", T.StringType()),
        T.StructField("error_type", T.StringType()),
    ]
)


# FCD intermediate data
FCD_intermediate_data_path = f'{BLOB_URL}/error_classification/training/intermediate/FCD_intermediate_data.delta'

FCD_intermediate_data_schema = T.StructType(
    [
        T.StructField("case_id", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("provider", T.StringType()),
        T.StructField("stretch_length", T.FloatType()),
        T.StructField("pra", T.FloatType()),
        T.StructField("prb", T.FloatType()),
        T.StructField("prab", T.FloatType()),
        T.StructField("lift", T.FloatType()),
        T.StructField("tot", T.IntegerType()),
    ]
)


# SDO intermediate data
SDO_intermediate_data_path = f'{BLOB_URL}/error_classification/training/intermediate/SDO_intermediate_data.delta'

SDO_intermediate_data_schema = T.StructType(
    [
        T.StructField("case_id", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("provider", T.StringType()),
        T.StructField("sdo_response", T.StringType()),
        T.StructField("sdo_data_start_date", T.DateType()),
        T.StructField("sdo_data_end_date", T.DateType()),
        T.StructField("sdo_api_call_date", T.DateType()),
    ]
)
 

# intermediate data
intermediate_data_path = f'{BLOB_URL}/error_classification/training/intermediate/intermediate_data.delta'

intermediate_data_schema = T.StructType(
    [
        T.StructField("case_id", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("country", T.StringType()),
        T.StructField("provider", T.StringType()),
        T.StructField("stretch", T.StringType()),
        T.StructField("route", T.StringType()),
        T.StructField("metric", T.StringType()),
        T.StructField("pra", T.FloatType()),
        T.StructField("prb", T.FloatType()),
        T.StructField("prab", T.FloatType()),
        T.StructField("lift", T.FloatType()),
        T.StructField("tot", T.IntegerType()),
        T.StructField("sdo_response", T.StringType()),
        T.StructField("sdo_data_start_date", T.DateType()),
        T.StructField("sdo_data_end_date", T.DateType()),
        T.StructField("sdo_api_call_date", T.DateType()),
        T.StructField("error_label", T.StringType()),
        T.StructField("error_type", T.StringType()),
    ]
)


# feature table
feature_table_path = f'{BLOB_URL}/error_classification/training/feature_table.delta'

feature_table_schema = T.StructType(
    [
        # metadata
        T.StructField("case_id", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("provider", T.StringType()),
        T.StructField("metric", T.StringType()),
        T.StructField("date", T.DateType()),
        # General features
        T.StructField("is_tbt", T.IntegerType()),
        T.StructField("is_hdr", T.IntegerType()),
        T.StructField("drive_left_side", T.IntegerType()),
        T.StructField("drive_right_side", T.IntegerType()),
        # Geometrical features
        # T.StructField("route", T.StringType()),
        # T.StructField("stretch", T.StringType()),
        T.StructField("route_length", T.FloatType()),
        T.StructField("stretch_length", T.FloatType()),
        T.StructField("stretch_start_position_in_route", T.FloatType()),
        T.StructField("stretch_end_position_in_route", T.FloatType()),
        T.StructField("stretch_starts_at_the_route_start", T.FloatType()),
        T.StructField("stretch_ends_at_the_route_end", T.FloatType()),
        T.StructField("heading_stretch", T.FloatType()),
        T.StructField("left_turns", T.IntegerType()),
        T.StructField("straight_turns", T.IntegerType()),
        T.StructField("right_turns", T.IntegerType()),
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
        # T.StructField("sdo_api_response", T.MapType(T.StringType(), T.StringType())),
        # T.StructField("traffic_signs", T.MapType(T.StringType(), T.IntegerType())),
        # FCD features
        T.StructField("pra", T.FloatType()),
        T.StructField("prb", T.FloatType()),
        T.StructField("prab", T.FloatType()),
        T.StructField("lift", T.FloatType()),
        T.StructField("tot", T.FloatType()),
        # Target Labels 
        T.StructField("error_type", T.StringType()),
        T.StructField("error_label", T.StringType()),
        T.StructField("has_error", T.IntegerType())
    ]
)

# deduplicated feature table 
feature_table_deduplicated_path = f'{BLOB_URL}/error_classification/training/feature_table_deduplicated.delta'

# deduplicated feature table on country and stretch length
feature_table_deduplicated_country_stretchlength_path = f'{BLOB_URL}/error_classification/training/feature_table_deduplicated_on_country_stretchlength.delta'

# training table (contains only relevant features for training, whereas feature table contains all potential features)
training_table_path = f'{BLOB_URL}/error_classification/training/training_table.delta'

# test table
test_table_path = f'{BLOB_URL}/error_classification/training/test_table.delta'

# test table with old model predictions
test_table_with_old_model_predictions_path = f'{BLOB_URL}/error_classification/training/test_table_with_old_model_predictions.delta'

# balanced dataset
balanced_data_path = f'{BLOB_URL}/error_classification/training/balanced_data.delta'

# feature_table_w_new_fcd_features
feature_table_w_new_fcd_features_path = f'{BLOB_URL}/error_classification/training/feature_table_w_new_fcd_features_path.delta'

# log tables
prod_log_table_path = "dbfs:/mnt/tbt/delta-tables/ec_model_submodule/ec_model_logs.delta"
dev_log_table_path = "dbfs:/mnt/tbt/dev/ec_model_submodule/ec_model_logs.delta"

print("Success: All table schemas defined")
