import pandas as pd
import pytest 
import yaml
from pathlib import Path
import pyspark

from ..error_classification.ec_end2end import ErrorClassification


@pytest.fixture
def spark() -> pyspark.sql.SparkSession:
    _spark = (
        pyspark.sql.SparkSession.builder.master("local[*]")
        .appName("local-tests")
        .enableHiveSupport()
        .getOrCreate()
    )
    return _spark

@pytest.fixture(scope="session")
def ec_test_df():
    current_dir = Path(__file__).parent
    ec_test_df_dir = current_dir / "test_input_data/ec_test_df.csv"
    pdf = pd.read_csv(ec_test_df_dir)
    return pdf

@pytest.fixture
def tbt_new_critical_sections_example(ec_test_df):
    return ec_test_df[['run_id', 'route_id', 'case_id', 'country', 'provider', 'route', 'stretch']]

@pytest.fixture
def featurized_df_example(ec_test_df):
    return ec_test_df.iloc[:,7:-7]

@pytest.fixture
def predictions_example(ec_test_df):
    return ec_test_df[["error_label", "probability"]]

@pytest.fixture
def log_table_example(ec_test_df):
    return ec_test_df

@pytest.fixture
def ml_model_options_example():
    current_dir = Path(__file__).parent
    ml_model_dir = current_dir / "test_input_data/ml_model.yml"
    with open(ml_model_dir, 'r') as file:
        ml_model_options = yaml.safe_load(file)["ml_model_options"]
    return ml_model_options

class MockMLModel:
    def __init__(self, *args, **kwargs):
        pass

    def predict(self, *args, **kwargs):
        current_dir = Path(__file__).parent
        ec_test_df_dir = current_dir / "test_input_data/ec_test_df.csv"
        pdf = pd.read_csv(ec_test_df_dir)
        return pdf[["error_label", "probability"]]
    
    def get_model_options(self, *args, **kwargs):
        model_options = {
            "model_features": ["feature1", "feature2", "feature3"],
            "probability_threshold": 0.5,
            "model_run_id": "test_run_id"
        }
        return model_options


@pytest.fixture
def error_classification_fixture(mocker, tbt_new_critical_sections_example, ml_model_options_example, spark):
    
    # mock fcd
    mock_fcd_return_value = pd.DataFrame(
        {
            "tot_new": [0]*5,
            "pra_new": [-2.0]*5,
            "prb_new": [-2.0]*5,
            "pra_not_b": [-2.0]*5,
            "prb_not_a": [-2.0]*5,
            "pra_and_b": [-2.0]*5,
            "pra_to_b": [-2.0]*5,
            "prb_to_a": [-2.0]*5, 
            "tot_contained": [0]*5, 
            "pra_to_b_contained": [-2.0]*5,
            "prb_to_a_contained": [-2.0]*5,
            "pra_to_b_not_contained": [-2.0]*5,
            "prb_to_a_not_contained": [-2.0]*5,
            "traffic_direction": [-2.0]*5,
            "traffic_direction_contained": [-2.0]*5,
            "ab_intersect": [0]*5
        }
    )
    mocker.patch(
        'tbt.ecmodel.error_classification.featurization.FCDFeaturizer.featurize', 
        return_value=mock_fcd_return_value
    )

    # mock predictions
    mocker.patch(
        'tbt.ecmodel.error_classification.ec_end2end.MLModel', 
        new=MockMLModel
    )

    return ErrorClassification(
        pdf=tbt_new_critical_sections_example,
        fcd_credentials={},
        ml_model_options=ml_model_options_example,
        sample_metric="TbT",
        spark=spark
    )


def test_featurize(error_classification_fixture, featurized_df_example):
    featurized_df = error_classification_fixture.featurize()
    assert not featurized_df.empty
    assert set(featurized_df.columns) == set(featurized_df_example.columns)

def test_predict_on_df(error_classification_fixture, featurized_df_example, predictions_example):
    predictions = error_classification_fixture.predict_on_df(featurized_df_example)
    assert not predictions.empty
    assert set(predictions.columns) == set(predictions_example.columns)

def test_create_log_table(error_classification_fixture, featurized_df_example, predictions_example, log_table_example):
    log_table = error_classification_fixture.create_log_table(featurized_df_example, predictions_example)
    log_table_example.drop("time_per_critical_section", axis=1, inplace=True)
    assert not log_table.empty
    assert set(log_table.columns) == set(log_table_example.columns)

def test_run(error_classification_fixture, log_table_example):
    predictions, log_table = error_classification_fixture.run()
    assert isinstance(predictions, pd.Series)
    assert not predictions.empty
    assert not log_table.empty
    log_table.drop("time_per_critical_section", axis=1, inplace=True)
    assert set(log_table.columns) == set(log_table_example.columns)
