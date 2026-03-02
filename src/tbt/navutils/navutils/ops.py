"""
Common operations
"""

import warnings

import pandas
import shapely.geometry
import shapely.wkt
import sqlalchemy


def country_data_query(country: str, db_params: dict) -> dict:
    """Function to query the country data

    :param country: Country iso
    :type country: str
    :param db_params: tbt db credentials
    :type db_params: dict
    :return: Dictionary with feat_id, geom and country_iso
    :rtype: dict
    """

    con = sqlalchemy.create_engine(
        f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}'
        f'@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}'
    )
    sql = (
        f"SELECT feat_id, st_astext(geom) geom, iso_a3 as country_iso"
        f" FROM golden_routes.country_data where  iso_a3 = '{country}'"
    )
    country_data_df = pandas.read_sql_query(sql, con)
    if len(country_data_df) == 1:
        country_data_df["geom"] = [shapely.wkt.loads(x) for x in country_data_df.geom]
        country_data_df["country_iso"] = country
        return country_data_df.to_dict(orient="records")[0]

    if len(country_data_df) > 1:
        warnings.warn(
            f"Found several matching ISO for '{country}' in country_data..."
            " continuing with the first polygon"
        )
        country_data_df["geom"] = [shapely.wkt.loads(x) for x in country_data_df.geom]
        country_data_df["country_iso"] = country
        return country_data_df.to_dict(orient="records")[0]

    warnings.warn(
        f"Country iso '{country}' not found in country_data table... "
        "continuing with world polygon"
    )
    return {
        "feat_id": None,
        "geom": shapely.geometry.Polygon(
            [(-180, -90), (180, -90), (180, 90), (-180, 90)]
        ),
        "country_iso": country,
    }
