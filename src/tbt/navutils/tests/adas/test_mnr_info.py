import pandas as pd
import inspect
from mock import patch, MagicMock
from ...adas import kedro_utils, mnr_info
from shapely import wkt


def test_select_mnr_server(mocker):

    df_region_mock = pd.DataFrame({"region_wb": ["Europe & Central Asia"]})
    df_schema_mock = pd.DataFrame({"schema_name":["gbr_test_schema"]})

    read_sql_mock = mocker.patch("pandas.read_sql", side_effect = [df_region_mock, df_schema_mock])

    mock_credentials = (
        inspect.getmodule(kedro_utils.get_credentials).__name__ + ".get_credentials"
    )
    mocker.patch(
        mock_credentials,
        return_value={
            "db-credentials-speed-limits": {
                "con": "postgresql+psycopg2://asdf:asdf@11.111.111.111:5432/asdf"
            }
        },
    )    

    mnr_server, schema = mnr_info.select_mnr_server('GBR')

    assert mnr_server == "caprod-cpp-pgmnr-001.flatns.net"
    assert schema == "gbr_test_schema"
    assert read_sql_mock.call_count == 2
