""" Test for functions on Navutils ops """
import pandas
import pytest
import shapely.geometry

from ...navutils.ops import country_data_query

GBR_FAKE_POLYGON = "POLYGON ((30 20, 45 40, 10 40, 30 20))"
GBR_OTHER_FAKE_POLYGON = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"


def test_country_data_query_country(mocker):
    """Test to pass if correct"""
    mocker.patch("sqlalchemy.create_engine", return_value=None)

    mocker.patch(
        "pandas.read_sql_query",
        return_value=pandas.DataFrame(
            [
                {
                    "feat_id": "e65b7071-dc72-4285-992f-bd9b5fd61def",
                    "name": "United Kingdom",
                    "geom": GBR_FAKE_POLYGON,
                    "iso_a3": "GBR",
                    "eng_name": "United Kingdom",
                }
            ],
            columns=["feat_id", "name", "geom", "iso_a3", "eng_name"],
        ),
    )

    result = country_data_query(
        country="GBR",
        db_params={
            "user": "",
            "password": "",
            "host": "",
            "port": "",
            "database": "",
        },
    )

    assert result["feat_id"] == "e65b7071-dc72-4285-992f-bd9b5fd61def"
    assert result["name"] == "United Kingdom"
    assert result["geom"] == shapely.wkt.loads(GBR_FAKE_POLYGON)
    assert result["country_iso"] == "GBR"


def test_country_data_query_country_not_found(mocker):
    """Test on country not found"""

    mocker.patch("sqlalchemy.create_engine", return_value=None)

    mocker.patch(
        "pandas.read_sql_query",
        return_value=pandas.DataFrame(),
    )
    with pytest.warns(UserWarning):
        result = country_data_query(
            country="fake",
            db_params={
                "user": "",
                "password": "",
                "host": "",
                "port": "",
                "database": "",
            },
        )
    assert result["feat_id"] is None
    assert result["geom"] == shapely.geometry.Polygon(
        [(-180, -90), (180, -90), (180, 90), (-180, 90)]
    )
    assert result["country_iso"] == "fake"


def test_country_data_query_country_too_much_found(mocker):
    """Test  on two country found"""

    mocker.patch("sqlalchemy.create_engine", return_value=None)

    mocker.patch(
        "pandas.read_sql_query",
        return_value=pandas.DataFrame(
            [
                {
                    "feat_id": "e65b7071-dc72-4285-992f-bd9b5fd61def",
                    "name": "United Kingdom",
                    "geom": GBR_FAKE_POLYGON,
                    "iso_a3": "GBR",
                    "eng_name": "United Kingdom",
                },
                {
                    "feat_id": "not_real_uuid",
                    "name": "not_real_name",
                    "geom": GBR_OTHER_FAKE_POLYGON,
                    "iso_a3": "GBR",
                    "eng_name": "not_real_eng_name",
                },
            ],
            columns=["feat_id", "name", "geom", "iso_a3", "eng_name"],
        ),
    )
    with pytest.warns(UserWarning):
        result = country_data_query(
            country="GBR",
            db_params={
                "user": "",
                "password": "",
                "host": "",
                "port": "",
                "database": "",
            },
        )
    assert result["feat_id"] == "e65b7071-dc72-4285-992f-bd9b5fd61def"
    assert result["name"] == "United Kingdom"
    assert result["geom"] == shapely.wkt.loads(GBR_FAKE_POLYGON)
    assert result["country_iso"] == "GBR"
