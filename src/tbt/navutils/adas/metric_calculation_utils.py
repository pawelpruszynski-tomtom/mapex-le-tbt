import pandas as pd
import numpy as np
from scipy.optimize import curve_fit
from scipy.optimize import differential_evolution
from statsmodels.distributions.empirical_distribution import ECDF
import warnings


def compute_eph(
    all_observations,
    gtd_observations,
    seed,
    fitted_parameters=None,
    groupper_cols=["sl_group"],
    length_col=None,
    proxy_group_col="sl_group",
    groups_resampling_frac={1: 1, 2: 1, 3: 1, 4: 1},
):
    """Determine, for a seed, and for both the gtd and all_observations data, the following:
        - Total EPH
        - Total error count
        - Total hours
        - Total driven hours with errors
        - Total percentage of driven hours with errors
        * If length column is defined:
        - Total EPKm
        - Total Km
        - Total driven km with errors
        - Total percentage of driven km with errors

    :param all_observations: DataFrame containing all the observations to be considered in the metric
    :type all_observations: pandas.DataFrame
    :param gtd_observations: DataFrame containing all the observations with ground truth data (MCP, Map Experts)
    :type gtd_observations: pandas.DataFrame
    :param seed: Random seed used during the internal bootstraping process
    :type seed: int
    :param fitted_parameters: defaults to None. If a value is given, it's used as the fitted parameters for the support function and
    no new fitting is performed
    :type fitted_parameters: array
    :param groupper_cols: List of all columns we wish to group by, for example sl_group if we want to compute the metric for
    groups 1,2,3,4 or type in lanecounts if we want to compute the metric in
    matching,provider_empty,competitor_emtpy,both_emtpy,potential_error.
    This is a list because we can use also [sl_group,frc] if we want the metric computed by the road class
    :type groupper_cols: list

    :param length_col: Name of the column that contains geometries lengths if we want to compute the metric by KM.
    This column must contain the length in KM
    :type length_col: str
    :param proxy_group_col: Name for the column that contains the proxy categorizations
    :type proxy_group_col: str
    :param groups_resampling_frac: Dictionary containing all the proxy groups and the selected frac for random sample.
    keys are the groups and values the size of the resample. For example, `frac=0.5` means a sample half of the size of the original one,
        while a `frac=2` means a sample twice as big as the original. Defaults to `frac=1`
    :type groups_resampling_frac: dict
    :return: dictionary containing all the aforementioned values for the given sample and metric
    :rtype: dict
    :return: DataFrame containing a detailed view of the different errors in sample and metric DataFrames
    :rtype: pandas.DataFrame
    """

    # FUNCTION DEFINITION
    def sigmoid(x, amplitude, x0, k):
        return amplitude * 1.0 / (1.0 + np.exp(-x0 * (x - k)))

    # Function for genetic algorithm to minimize (sum of squared error)
    def sumOfSquaredError(parameterTuple):
        warnings.filterwarnings("ignore")  # do not print warnings by genetic algorithm
        val = sigmoid(xData, *parameterTuple)
        return np.sum((yData - val) ** 2.0)

    def generate_Initial_Parameters(xData, yData):
        # Min and max used for bounds
        maxX = max(xData)
        minX = min(xData)
        maxY = max(yData)
        minY = min(yData)

        parameterBounds = []
        parameterBounds.append([minY, maxY])  # search bounds for amplitude
        parameterBounds.append([minX, maxX])  # search bounds for x0
        parameterBounds.append([minX, maxX])  # search bounds for k

        # "seed" the numpy random number generator for repeatable results
        result = differential_evolution(sumOfSquaredError, parameterBounds, seed=3)
        return result.x

    # BOOTSTRAPING
    # Filter each group
    if seed is None:
        pass
    else:
        df_resampled = []
        for group, frac in groups_resampling_frac.items():
            df_resampled.append(
                gtd_observations.loc[gtd_observations[proxy_group_col] == group].sample(
                    frac=frac, replace=True, random_state=seed
                )
            )
        # Combine all the group samples in one dataframe
        gtd_observations = pd.concat(df_resampled, ignore_index=True)

    # GENERAL DATA PREPROCESSING
    # CALCULATE TOTAL SAMPLE AND METRIC HOURS AND KM
    # Grouped time by FRC and Group (SAMPLE and METRIC DataFrames)
    if length_col is not None:
        agg_dict = {"time_in_seconds": lambda x: x.sum() / 3600, length_col: "sum"}
    else:
        agg_dict = {"time_in_seconds": lambda x: x.sum() / 3600}

    df_time_gtd_observations = (
        gtd_observations.groupby(groupper_cols).agg(agg_dict).reset_index()
    )
    df_time_metric_all_observations = (
        all_observations.groupby(groupper_cols).agg(agg_dict).reset_index()
    )

    # Convert groupby objects into DataFrames
    # df_time_gtd_observations = df_time_gtd_observations.reset_index()
    # df_time_metric_all_observations = df_time_metric_all_observations.reset_index()

    # Rename time columns
    df_time_gtd_observations = df_time_gtd_observations.rename(
        {"time_in_seconds": "gtd_hours"}, axis=1
    )
    df_time_metric_all_observations = df_time_metric_all_observations.rename(
        {"time_in_seconds": "all_sampled_hours"}, axis=1
    )

    # Rename in case length
    if length_col is not None:
        df_time_gtd_observations = df_time_gtd_observations.rename(
            {length_col: "gtd_km"}, axis=1
        )
        df_time_metric_all_observations = df_time_metric_all_observations.rename(
            {length_col: "all_sampled_km"}, axis=1
        )

    # Merge both DataFrames
    df_time = df_time_gtd_observations.merge(
        df_time_metric_all_observations, on=groupper_cols, how="outer"
    )
    df_time.fillna(
        0, inplace=True  # FIXME: I feel this is wrong
    )  # Fill NaN's with 0, as those are values that didn't appeared in the sample
    df_time.sort_values(groupper_cols, ascending=True, inplace=True)

    # CALCULATE THE DRIVEN HOURS WITH ERRORS FOR GTD AND ALL
    # Calculate the number of driven hours with errors
    # TODO: FIX
    df_metrics = gtd_observations.loc[
        gtd_observations["error_type_copy"].notnull()
    ]  # Get the cases where there was an error

    if length_col is not None:
        agg_dict = {"time_in_seconds": lambda x: x.sum() / 3600, length_col: "sum"}
    else:
        agg_dict = {"time_in_seconds": lambda x: x.sum() / 3600}

    df_metrics = (
        df_metrics.groupby([*groupper_cols, "error_type_copy"]).agg(agg_dict).reset_index()
    )  # Sum total seconds and km

    # Rename seconds to hours
    df_metrics.rename({"time_in_seconds": "error_hours"}, axis=1, inplace=True)
    # Rename in case length
    if length_col is not None:
        df_metrics.rename({length_col: "error_km"}, axis=1, inplace=True)

    # Add total and gtd hours and km to resulting DataFrame for later calculations
    df_metrics = df_metrics.merge(df_time, how="right", on=groupper_cols)
    df_metrics.fillna(
        {"error_type_copy": "no error", "error_hours": 0.0, "error_km": 0.0}, inplace=True
    )  # Fill NaN's (cases with no errors in sample) # FIXME: review this case

    # Calculate the estimated driven hours with errors (extrapolation)
    df_metrics["estimated_error_hours"] = (
        df_metrics["error_hours"] / df_metrics["gtd_hours"]
    ) * df_metrics["all_sampled_hours"]

    if length_col is not None:
        df_metrics["estimated_error_km"] = (
            df_metrics["error_km"] / df_metrics["gtd_km"]
        ) * df_metrics["all_sampled_km"]

    df_metrics.fillna(
        {"estimated_error_hours": 0, "estimated_error_km": 0}, inplace=True
    )  # Fill NaN's with 0 as they are due to cases where gtd_hours == 0

    # Fit an ECDF function for the errors: this only aply to hours
    # TODO: FIX
    xData = np.sort(
        gtd_observations.loc[
            gtd_observations["error_type_copy"].notnull(), "time_in_seconds"
        ]
    )
    ecdf = ECDF(xData)
    yData = np.array([ecdf(x) for x in xData])

    # If no parameters have been passed as input, then fit new parameters.
    # Else, reuse the ones saved in the database
    if len(fitted_parameters) == 0:
        # By default, differential_evolution completes by calling curve_fit() using parameter bounds
        geneticParameters = generate_Initial_Parameters(xData, yData)

        # Now call curve_fit without passing bounds from the genetic algorithm,
        # just in case the best fit parameters are aoutside those bounds
        fitted_parameters, pcov = curve_fit(sigmoid, xData, yData, geneticParameters)
    else:
        pass

    # Projected errors in the support function
    projected_err = sigmoid(xData, *fitted_parameters)

    # Add projected errors into a DataFrame (SAMPLE)
    # TODO: FIX
    df_error = (
        gtd_observations.loc[gtd_observations["error_type_copy"].notnull()]
        .sort_values("time_in_seconds")
        .reset_index(drop=True)
    )
    df_error["error_type_count"] = projected_err
    df_error = (
        df_error.groupby([*groupper_cols, "error_type_copy"])["error_type_count"]
        .sum()
        .reset_index()
    )

    df_metrics = df_metrics.merge(
        df_error, how="left", on=[*groupper_cols, "error_type_copy"]
    ).fillna(0.0)
    df_metrics["estimated_errors"] = (
        df_metrics["error_type_count"] / df_metrics["gtd_hours"]
    ) * df_metrics["all_sampled_hours"]
    df_metrics["estimated_errors"].replace(
        {np.inf: 0.0}, inplace=True
    )  # Replace inf with 0 (cases with 0/0 division)
    df_metrics["estimated_errors"].fillna(
        0.0, inplace=True
    )  # Fill NaN's with 0 (cases with division by 0)

    # CALCULATE THE FINAL METRICS
    # Sampled and metric hours (Total)
    gtd_hours = df_time["gtd_hours"].sum()
    all_sampled_hours = df_time["all_sampled_hours"].sum()

    # KM
    if length_col is not None:
        gtd_km = df_time["gtd_km"].sum()
        all_sampled_km = df_time["all_sampled_km"].sum()

    # Error count for sample and estimated error count for metric (Total)
    # TODO: FIX
    gtd_err = gtd_observations.loc[
        gtd_observations["error_type_copy"].notnull(), "error_type_copy"
    ].count()  # Original errors without transforming with support function
    all_observations_err = df_metrics[
        "estimated_errors"
    ].sum()  # Extrapolated and transformed errors with the support function

    # Hours driven with errors in gtd and estimated for metric (Total)
    gtd_error_hours = df_metrics["error_hours"].sum()
    all_observations_error_hours = df_metrics["estimated_error_hours"].sum()

    # KM driven with errors in gtd and estimated for metric (Total)
    if length_col is not None:
        gtd_error_km = df_metrics["error_km"].sum()
        all_observations_error_km = df_metrics["estimated_error_km"].sum()

    # Estimated percentage of driven hours with errors (Total)
    perc_metric_error_hours = all_observations_error_hours / all_sampled_hours

    # EPH for the metric (Total)
    total_eph = all_observations_err / all_sampled_hours


    df_metrics['eph'] = df_metrics['estimated_errors']/df_metrics['all_sampled_hours'].sum()
    df_metrics['%eph'] = df_metrics['estimated_error_hours']/df_metrics['all_sampled_hours'].sum()

    if length_col is not None:
        df_metrics['epkm'] = df_metrics['estimated_errors']/df_metrics['all_sampled_km'].sum()
        df_metrics['%epkm'] = df_metrics['estimated_error_km']/df_metrics['all_sampled_km'].sum()


    if length_col is not None:
        km_results = {
            "gtd_km": gtd_km,
            "all_sampled_km": all_sampled_km,
            "gtd_error_km": gtd_error_km,
            "all_observations_error_km": all_observations_error_km,
            "perc_epkm": all_observations_error_km / all_sampled_km,
            "total_epkm": all_observations_err / all_sampled_km,
        }
    else:
        km_results = {}
    # GENERATE A DICTIONARY WITH THE RESULTS
    results = {
        "seed": seed,
        "gtd_hours": gtd_hours,
        "all_observations_hours": all_sampled_hours,
        "gtd_err": gtd_err,
        "all_observations_err": all_observations_err,
        "gtd_error_hours": gtd_error_hours,
        "all_observations_error_hours": all_observations_error_hours,
        "perc_metric_error_hours": perc_metric_error_hours,
        "total_eph": total_eph,
        **km_results,
    }

    df_metrics.drop(columns=['error_type_copy'],inplace=True)

    return results, df_metrics, fitted_parameters
