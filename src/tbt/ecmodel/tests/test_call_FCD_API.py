import pytest
import pyspark.sql
import pyspark.sql.types as T
import pandas as pd
from pathlib import Path
import yaml
import os

from ..error_classification.call_FCD_API import FCDFeaturizer

# FCD credentials
if os.getenv("GITHUB_ACTIONS"):
    FCD_CREDENTIALS_YAML = os.getenv("FCD_CREDENTIALS_YAML")
    fcd_credentials = yaml.safe_load(FCD_CREDENTIALS_YAML)["fcd_credentials"]
else: 
    with open("/workspace/conf/dev/credentials.yml", 'r') as file:
        fcd_credentials = yaml.safe_load(file)["fcd_credentials"] 


@pytest.fixture
def spark() -> pyspark.sql.SparkSession:
    _spark = (
        pyspark.sql.SparkSession.builder.master("local[*]")
        .appName("local-tests")
        .enableHiveSupport()
        .getOrCreate()
    )
    return _spark

# get pandas df from csv
current_dir = Path(__file__).parent
ec_test_df_dir = current_dir / "test_input_data/ec_test_df.csv"
pdf = pd.read_csv(ec_test_df_dir)



@pytest.fixture
def tbt_new_critical_sections_example(ec_test_df):
    return ec_test_df[['run_id', 'route_id', 'case_id', 'country', 'provider', 'route', 'stretch']]


def test_FCDFeaturizer(mocker, spark):
    # Test if the class is initialized correctly
    fcd_featurizer = FCDFeaturizer(
        pdf,
        trace_retrieval_geometry="bbox_json",
        traces_limit=2000,
        fcd_credentials=fcd_credentials,
        spark_context=spark
    )

    mocker.patch(
        'tbt.ecmodel.error_classification.featurization.FCDFeaturizer.query_traces', 
        return_value = pd.DataFrame()
    )


    fcd_featurizer.add_geometric_attributes()
    fcd_featurizer.featurize()
    assert fcd_featurizer is not None
    assert fcd_featurizer.trace_retrieval_geometry == "bbox_json"
    assert fcd_featurizer.traces_limit == 2000
