"""Functions to calculate Worldwide aggregation"""
import datetime
import json
import typing
import uuid

import numpy
import pandas

import typing

import email, smtplib, ssl

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from io import BytesIO

import logging
log = logging.getLogger(__name__)


def get_latest_products(
    aggregation_ww_options: dict,
    latest_products: pandas.DataFrame,
    inspections_available: pandas.DataFrame,
    base_ww_aggregations_available: pandas.DataFrame
):
    
    run_ids = aggregation_ww_options["run_ids"]

    if len(run_ids) > 0:
        base_run_id_ww = aggregation_ww_options["base_run_id_ww"]
        return run_ids, base_run_id_ww
    

    provider = aggregation_ww_options["provider"]
    scope = aggregation_ww_options["scope"]

    base_run_id_ww = base_ww_aggregations_available.loc[
        (base_ww_aggregations_available['provider']==provider)
        & (base_ww_aggregations_available['scope']==scope),
        'run_id'
    ].values[0]

    providers = [provider]

    latest_products = latest_products.loc[latest_products['provider'].isin(providers), 'product'].values
    # Keep OSM & Orbis inspections
    filtered_inspections = inspections_available.loc[inspections_available['product'].isin(latest_products)]
    # If Orbis available, drop OSM
    run_ids = (
        filtered_inspections
        .sort_values(by=['country', 'provider'])
        .drop_duplicates(subset=['country'], keep='first'))['run_id'].values
    
    return run_ids, base_run_id_ww


def get_inspections(
    run_ids: list,
    base_run_id_ww: str,
    inspections_data: pandas.DataFrame,
    aggregation_ww_data: pandas.DataFrame,
) -> typing.Tuple[pandas.DataFrame, pandas.DataFrame]:
    """This function retrieves data from the inspections associated to the provided run ids.

    :param aggregation_ww_options: aggregation worldwide options
    :type aggregation_ww_options: dict
    :return: new inspections based on the provided run_ids
    :rtype: pandas.DataFrame
    """

    # Get new inspections data
    new_inspections = inspections_data.loc[inspections_data["run_id"].isin(run_ids), :]

    # Get base ww aggregation's inspections data
    if len(base_run_id_ww) > 0:
        # Get inspections' run_ids from base ww aggregation
        base_ww_agg = aggregation_ww_data.loc[
            aggregation_ww_data["run_id"] == base_run_id_ww, "metadata"
        ].values[0]

        base_ww_agg_run_ids = json.loads(base_ww_agg)["run_ids"]

        # Get the inspections belonging to the base ww aggregation
        base_ww_agg_inspections = get_ww_agg_inspections(
            inspections_data=inspections_data, run_ids=base_ww_agg_run_ids
        )
    else:
        base_ww_agg_inspections = pandas.DataFrame(
            {
                "run_id": [],
                "provider": [],
                "country": [],
                "product": [],
                "eph": [],
                "metrics_per_error_type": [],
                "mapdate": [],
            }
        )
    return new_inspections, base_ww_agg_inspections


def update_agg_source(
    aggregation_ww_options: dict,
    weights: pandas.DataFrame,
    base_ww_agg_inspections: pandas.DataFrame,
    new_inspections: pandas.DataFrame,
) -> pandas.DataFrame:
    """This function puts together the data from new inspections and from the base ww aggregation inspections.

    :param aggregation_ww_options: aggregation worldwide options
    :type aggregation_ww_options: dict
    :param AZ_DB: tbt database credentials
    :type AZ_DB: dict
    :param base_ww_agg_inspections: base ww aggregation inspections based on the provided ww aggregation run_id
    :type base_ww_agg_inspections: pandas.DataFrame
    :param new_inspections: new inspections based on the provided run_ids
    :type new_inspections: pandas.DataFrame
    :return: inspections data ready for the new ww aggregation
    :rtype: pandas.DataFrame
    """
    scope = aggregation_ww_options["scope"]

    # Do I use a base ww agg or just calculate a new one from scratch?
    if base_ww_agg_inspections.shape[0] > 0:
        ww_agg_inspections = base_ww_agg_inspections.loc[
            ~base_ww_agg_inspections["country"].isin(new_inspections.country.values)
        ]
        new_ww_agg_inspections = pandas.concat([ww_agg_inspections, new_inspections])

        new_ww_agg_inspections = new_ww_agg_inspections.merge(
            weights, how="left", on="country"
        ).dropna(subset=["weight"])
    else:
        new_ww_agg_inspections = new_inspections.merge(
            weights, how="left", on="country"
        ).dropna(subset=["weight"])

    # What's the scope of the WW agg?
    if scope.lower() != "worldwide":
        top = int(scope[3:])
        new_ww_agg_inspections = (
            new_ww_agg_inspections.loc[new_ww_agg_inspections.weight > 0, :]
            .sort_values(by="ranking")
            .iloc[:top]
        )

        total_weight = new_ww_agg_inspections.weight.sum()
        new_ww_agg_inspections["weight"] = (
            new_ww_agg_inspections["weight"] / total_weight
        )

    def unpack_error_type(inspections):
        inspections_df = inspections.copy()
        # Error types processing
        features = [
            "turnrest",
            "oneway",
            "nonnav",
            "wdtrf",
            "mman",
            "mgsc",
            "mbp",
            "implicit",
            "wrc",
            "mgeo",
            "wgeo",
            "sgeo",
        ]
        inspections_df.loc[:, features] = inspections_df[
            "metrics_per_error_type"
        ].apply(pandas.Series)

        for feature in [
            "turnrest",
            "oneway",
            "nonnav",
            "wdtrf",
            "mman",
            "mgsc",
            "mbp",
            "implicit",
            "wrc",
            "mgeo",
            "wgeo",
            "sgeo",
        ]:
            tmp = inspections_df[feature].apply(pandas.Series)
            tmp.columns = [feature + "_" + colname for colname in tmp.columns]
            for col in tmp.columns:
                inspections_df[col] = tmp[col]

        return inspections_df.loc[
            :,
            [
                "country",
                "provider",
                "product",
                "mapdate",
                "run_id",
                "eph",
                "metrics_per_error_type",
                "weight",
                "ranking",
                "turnrest_eph",
                "oneway_eph",
                "nonnav_eph",
                "wdtrf_eph",
                "mman_eph",
                "mgsc_eph",
                "mbp_eph",
                "implicit_eph",
                "wrc_eph",
                "mgeo_eph",
                "wgeo_eph",
                "sgeo_eph",
            ],
        ]

    new_ww_agg_inspections['metrics_per_error_type'] = new_ww_agg_inspections['metrics_per_error_type'].apply(unify_error_type_json)
    new_ww_agg_inspections = unpack_error_type(inspections=new_ww_agg_inspections)
    return new_ww_agg_inspections


def get_results(
    run_id: str,
    aggregation_ww_options: dict,
    ww_agg_inspections: pandas.DataFrame,
) -> pandas.DataFrame:
    """This function computes the ww aggregation.

    :param run_id: run id
    :type run_id: str
    :param aggregation_ww_options: aggregation worldwide options
    :type aggregation_ww_options: dict
    :param ww_agg_inspections: inspections data ready for the new ww aggregation
    :type ww_agg_inspections: pandas.DataFrame
    :return: ww aggregation results
    :rtype: pandas.DataFrame
    """

    if run_id == "auto":
        run_id = str(uuid.uuid4())

    def compute_ww_aggregation(
        ww_agg_inspections, feature="eph", include_metadata=False
    ):
        # Read weights and merge them
        ww_agg_inspections[f"weighted_{feature}"] = (
            ww_agg_inspections[f"{feature}"] * ww_agg_inspections["weight"]
        )

        # Remove NaN countries and save them to know which were not able to calculate the error
        countries_with_no_weights = list(
            ww_agg_inspections.loc[ww_agg_inspections.weight.isna(), "run_id"].values
        )
        ww_agg_inspections = ww_agg_inspections.dropna(subset="weight")

        # Calculates WW aggregation
        measured_feature = ww_agg_inspections[f"weighted_{feature}"].sum()
        remaining_share = 1 - ww_agg_inspections.weight.values.sum()
        # Take the five worst results, average them and multiply that by the remaining weight ** Agreed asignation of the weight pending
        # Keep sorting by eph independently of the feature, to have the same order as de WW aggregation and get comparable numbers
        worst_measures_avg = numpy.mean(
            ww_agg_inspections.sort_values(by="eph", ascending=False)
            .head(5)[f"{feature}"]
            .values
        )

        if include_metadata:
            returnvalue = (
                measured_feature,
                remaining_share,
                worst_measures_avg,
                countries_with_no_weights,
                ww_agg_inspections,
            )
        else:
            returnvalue = measured_feature + (remaining_share * worst_measures_avg)
        return returnvalue

    (
        measured_eph,
        remaining_share,
        worst_measures_avg,
        countries_with_no_weights,
        df_aggregation,
    ) = compute_ww_aggregation(
        ww_agg_inspections=ww_agg_inspections, feature="eph", include_metadata=True
    )

    return run_id, pandas.DataFrame(
        {
            "run_id": run_id,
            "provider": aggregation_ww_options["provider"],
            "scope": aggregation_ww_options["scope"],
            "countries": df_aggregation.shape[0],
            "metric": "TbT",
            "mapdate": ww_agg_inspections.mapdate.max(),
            "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "eph": measured_eph + (remaining_share * worst_measures_avg),
            "publish_harold": None,
            "publish_value_stream": None,
            "metadata": str(
                {
                    "measured_eph": measured_eph,
                    "measured_share": remaining_share,
                    "worst_measures_avg": worst_measures_avg,
                    "countries_with_no_weights": countries_with_no_weights,
                    "run_ids": list(df_aggregation.run_id.values),
                    "providers_composition":(ww_agg_inspections
                                             .groupby(['provider', 'product'])
                                             .agg({'country': 'count'})
                                             .reset_index()
                                             .rename({'country':'n_countries'}, axis=1)
                                             .to_dict(orient='records'))
                }
            ).replace("'", '"'),
            "eph_types": str(
                {
                    feat: compute_ww_aggregation(
                        ww_agg_inspections=ww_agg_inspections,
                        feature=feat,
                        include_metadata=False,
                    )
                    for feat in (
                        "turnrest_eph",
                        "oneway_eph",
                        "nonnav_eph",
                        "wdtrf_eph",
                        "mman_eph",
                        "mgsc_eph",
                        "mbp_eph",
                        "implicit_eph",
                        "wrc_eph",
                        "mgeo_eph",
                        "wgeo_eph",
                        "sgeo_eph",
                    )
                }
            ).replace("'", '"'),
            "comment":aggregation_ww_options["comment"],
        },
        index=[0],
    )

def send_results(
    aggregation_ww_options:dict,
    sender_email_credentials: dict,
    run_id_old:str,
    run_id_new:str,
    aggregation_ww:pandas.DataFrame,
    tbt_inspections:pandas.DataFrame,
    weights:pandas.DataFrame
):    
    receiver_email = aggregation_ww_options["email"]
    if len(receiver_email) < 1:
        log.info(f"No WW Aggregation variation report computed")
        return None

    def compute_variation(
            run_id_old:str,
            run_id_new:str,
            aggregation_ww:pandas.DataFrame,
            tbt_inspections:pandas.DataFrame,
            weights:pandas.DataFrame
    ) -> typing.Tuple[dict, pandas.DataFrame, pandas.DataFrame]:
        variation_email_message = {
            "provider_old":aggregation_ww.loc[aggregation_ww['run_id']==run_id_old, 'provider'].values[0],
            "provider_new":aggregation_ww.loc[aggregation_ww['run_id']==run_id_new, 'provider'].values[0],
            "scope_old":aggregation_ww.loc[aggregation_ww['run_id']==run_id_old, 'scope'].values[0],
            "scope_new":aggregation_ww.loc[aggregation_ww['run_id']==run_id_new, 'scope'].values[0],
            "mapdate_old":aggregation_ww.loc[aggregation_ww['run_id']==run_id_old, 'mapdate'].values[0],
            "mapdate_new":aggregation_ww.loc[aggregation_ww['run_id']==run_id_new, 'mapdate'].values[0],
            "run_id_old":run_id_old,
            "run_id_new":run_id_new,
            "eph_old":aggregation_ww.loc[aggregation_ww['run_id']==run_id_old, 'eph'].values[0],
            "eph_new":aggregation_ww.loc[aggregation_ww['run_id']==run_id_new, 'eph'].values[0],
        }
        # Extract metadata
        metadata_old = json.loads(aggregation_ww.loc[aggregation_ww['run_id']==run_id_old, 'metadata'].values[0])
        metadata_new = json.loads(aggregation_ww.loc[aggregation_ww['run_id']==run_id_new, 'metadata'].values[0])

        run_id_old = run_id_old[:6]
        run_id_new = run_id_new[:6]
        # Get inspections used in the aggregations from the metadata
        ww_agg_inspections_left = tbt_inspections[tbt_inspections["run_id"].isin(metadata_old["run_ids"])]
        ww_agg_inspections_right = tbt_inspections[tbt_inspections["run_id"].isin(metadata_new["run_ids"])]

        ww_agg_inspections_left = ww_agg_inspections_left.rename({
            "run_id":f"run_id_{run_id_old}",
            "provider":f"provider_{run_id_old}",
            "product":f"product_{run_id_old}",
            "eph":f"eph_{run_id_old}",
            "mapdate":f"mapdate_{run_id_old}",
            "run_id":f"run_id_{run_id_old}",
        }, axis=1).drop('metrics_per_error_type', axis=1)

        ww_agg_inspections_right = ww_agg_inspections_right.rename({
            "run_id":f"run_id_{run_id_new}",
            "provider":f"provider_{run_id_new}",
            "product":f"product_{run_id_new}",
            "eph":f"eph_{run_id_new}",
            "mapdate":f"mapdate_{run_id_new}",
            "run_id":f"run_id_{run_id_new}",
        }, axis=1).drop('metrics_per_error_type', axis=1)

        comparison = ww_agg_inspections_left.merge(ww_agg_inspections_right, how='outer', on='country')

        comparison['mapdate_variation'] = comparison.apply(lambda x: x[f'mapdate_{run_id_old}'] != x[f'mapdate_{run_id_new}'], axis=1)
        comparison['provider_variation'] = comparison.apply(lambda x: x[f'provider_{run_id_old}'] != x[f'provider_{run_id_new}'], axis=1)
        comparison['eph_variation'] = comparison[f'eph_{run_id_new}'] - comparison[f'eph_{run_id_old}']

        # Filter by the countries that change
        variation = comparison.loc[comparison.eph_variation != 0,:].sort_values(by='eph_variation')

        # More detailed report
        detailed = comparison.merge(weights, how='left', on='country').sort_values(by='ranking')

        detailed[f'weighted_eph_{run_id_old}'] = detailed[f'eph_{run_id_old}'] * detailed['weight']
        detailed[f'weighted_eph_{run_id_new}'] = detailed[f'eph_{run_id_new}'] * detailed['weight']
        detailed['eph_variaton'] = detailed[f'eph_{run_id_new}'] - detailed[f'eph_{run_id_old}']
        detailed['weighted_eph_variaton'] = detailed[f'weighted_eph_{run_id_new}'] - detailed[f'weighted_eph_{run_id_old}']

        return variation_email_message, variation, detailed.loc[:,
                                                            [
                                                            'country',
                                                            'weight',
                                                            'ranking',
                                                            f'run_id_{run_id_old}',
                                                            f'provider_{run_id_old}',
                                                            f'product_{run_id_old}',
                                                            f'mapdate_{run_id_old}',
                                                            f'run_id_{run_id_new}',
                                                            f'provider_{run_id_new}',
                                                            f'product_{run_id_new}',
                                                            f'mapdate_{run_id_new}',
                                                            f'eph_{run_id_old}',
                                                            f'eph_{run_id_new}',
                                                            f'weighted_eph_{run_id_old}',
                                                            f'weighted_eph_{run_id_new}',
                                                            'eph_variaton',
                                                            'weighted_eph_variaton'
                                                            ]].sort_values(by='eph_variaton')

    variation_email_message, variation, detailed = compute_variation(
            run_id_old=run_id_old,
            run_id_new=run_id_new,
            aggregation_ww=aggregation_ww,
            tbt_inspections=tbt_inspections,
            weights=weights
    )

    sender_email = sender_email_credentials["user"]
    password = sender_email_credentials["password"]
    body = f"""
    Hi,

    Variation summary (Previous | New):
    - Run_id = {variation_email_message["run_id_old"]} | {variation_email_message["run_id_new"]}
    - Provider = {variation_email_message["provider_old"]} | {variation_email_message["provider_new"]}
    - Scope = {variation_email_message["scope_old"]} | {variation_email_message["scope_new"]}
    - Mapdate = {variation_email_message["mapdate_old"]} | {variation_email_message["mapdate_new"]}
    - Eph = {variation_email_message["eph_old"]} | {variation_email_message["eph_new"]}
    
    Attached you can find a detailed report on the variation causes.

    Kind regards,
    """

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = f"""
    WorldWide Aggregation for {variation_email_message["provider_new"]} - {variation_email_message["scope_new"]} - {variation_email_message["mapdate_new"]}
    """

    message.attach(MIMEText(body, "plain"))

    filename = "aggregation_ww_comparison.xlsx"
    attachment = data_to_binary(dfs={
        "variation":variation,
        "detailed":detailed
    })
    part = MIMEBase("application", "octet-stream")
    part.set_payload(attachment)

    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    # Add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()

    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)
    
    log.info(f"WW Aggregation variation report succesfully sent to {receiver_email}")

############ Support functions ############
def get_map_date(x):
    try:
        cache = json.loads(x)
        return_value = cache["mapdate"]
    except:
        return_value = "2000-01-01"
    return return_value


def get_ww_agg_inspections(
    inspections_data: pandas.DataFrame, run_ids: list
) -> pandas.DataFrame:
    ww_agg_source_inspections = inspections_data.loc[
        inspections_data["run_id"].isin(run_ids)
    ]

    return ww_agg_source_inspections

def unify_error_type_json(error_type_json):
    keys_to_check = ['turnrest', 'oneway', 'nonnav', 'wdtrf', 'mman', 'mgsc', 'mbp', 'implicit', 'wrc', 'mgeo', 'wgeo', 'sgeo']

    if all([error_type in error_type_json.keys() for error_type in keys_to_check]):
        return error_type_json

    error_type_json = {k.lower(): v for k, v in error_type_json.items()}
    new_error_type_json = {}
    for error_type in keys_to_check:
        if error_type in error_type_json.keys():
            if error_type_json[error_type] is not None:
                new_error_type_json[error_type] = {"eph":error_type_json[error_type]["eph"], "lower":0, "upper":0, "errors":error_type_json[error_type]["errors"]}
            else:
                new_error_type_json[error_type] = {"eph":0, "lower":0, "upper":0, "errors":0}

        else:
            new_error_type_json[error_type] = {"eph":0, "lower":0, "upper":0, "errors":0}
            
    return new_error_type_json

def data_to_binary(dfs):
    output = BytesIO()
    with pandas.ExcelWriter(output, engine="xlsxwriter") as writer:
        for name, df in dfs.items():
            df.to_excel(writer, sheet_name=name, index=False)
            
    processed_data = output.getvalue()
    return processed_data