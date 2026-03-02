""" Sanity checks functions """
from enum import Enum, auto

import pandas as pd
import numpy

import re
import time

import requests
import socket

import logging
log = logging.getLogger(__name__)


class CheckResult(Enum):
    """Class for results encapsulation"""

    WARNING = auto()
    INFO = auto()
    OK = auto()


def check_missing_length(routes_df, geom_col, len_col, threshold=0.01):
    """Check that confirms that routes with geometry have length attributed"""

    # checks that the geometry column is not empty and addtionally the length is 0
    # to find cases where the length is not saved
    cases = routes_df.loc[
        (~routes_df[geom_col].apply(lambda x: x.is_empty)) & (routes_df[len_col] == 0)
    ]

    if cases.shape[0] == 0:
        return CheckResult.OK, "", ""

    value = cases[cases[geom_col].apply(lambda x: x.is_valid)].shape[0]
    if value > len(routes_df) * threshold:
        return (
            CheckResult.WARNING,
            "Missing length",
            (
                f"WARNING: No route length saved in inspection_routes on {value} routes. "
                f"{value/routes_df.shape[0]:.2%} percent"
            ),
        )

    if value > 0:
        return (
            CheckResult.INFO,
            "Missing length",
            (
                f"INFO: No route length saved in inspection_routes on {value} routes. "
                f"{value/routes_df.shape[0]:.2%} percent"
            ),
        )

    return CheckResult.OK, "", ""


def check_missing_time(routes_df, geom_col, time_col, threshold=0.01):
    """Check that confirms that the routes with geometry have time information"""
    # checks that the geometry column is not empty and addtionally the length is 0;
    # to find cases where the time is not saved
    cases = routes_df.loc[
        (~routes_df[geom_col].apply(lambda x: x.is_empty)) & (routes_df[time_col] == 0)
    ]

    if cases.shape[0] == 0:
        return CheckResult.OK, "", ""

    value = cases[cases[geom_col].apply(lambda x: x.is_valid)].shape[0]

    if value > len(routes_df) * threshold:
        return (
            CheckResult.WARNING,
            "Missing time",
            (
                f"WARNING: No route time saved in inspection_routes on {value} routes. "
                f"{value/routes_df.shape[0]:.2%} percent"
            ),
        )

    if value > 0:
        return (
            CheckResult.INFO,
            "Missing time",
            (
                f"INFO: No route time saved in inspection_routes on {value} routes. "
                f"{value/routes_df.shape[0]:.2%} percent"
            ),
        )

    return CheckResult.OK, "", ""


def check_empty_routes(routes_df, geom_col, missing_routes_ratio):
    """Check to return the % of routes that have not been calculated and have empty geometry"""

    all_routes = routes_df.shape[0]
    empty_routes = routes_df.loc[routes_df[geom_col].apply(lambda x: x.is_empty)].shape[
        0
    ]

    try:
        ratio = empty_routes / all_routes
    except ZeroDivisionError:
        return (
            CheckResult.WARNING,
            "Empty_routes",
            "WARNING: There are no routes on dataframe",
        )

    if ratio > missing_routes_ratio:
        return (
            CheckResult.WARNING,
            "Empty_routes",
            f"WARNING: Too high number of empty routes in inspection_routes = {ratio:.2%}",
        )

    return CheckResult.OK, "", ""


def check_route_length(
    routes_df, len_col, routes_length_ceiling, routes_length_ceiling_ratio
):
    """Checks that confirms that the routes are not longer than a given threshold"""

    cases = routes_df.loc[routes_df[len_col] > routes_length_ceiling].shape[0]

    try:
        ratio = cases / routes_df.shape[0]
    except ZeroDivisionError:
        return (
            CheckResult.WARNING,
            "Long_routes_count",
            "WARNING: There are no routes on dataframe",
        )

    if ratio > routes_length_ceiling_ratio:
        return (
            CheckResult.WARNING,
            "Long_routes_percentage",
            f"WARNING: Too high percent of long routes in inspection_routes = {ratio:.2%}",
        )

    if cases > 0:
        return (
            CheckResult.INFO,
            "Long_routes_count",
            f"INFO: Too long routes in inspection_routes = {cases}",
        )

    return CheckResult.OK, "", ""


def check_provider_route_length_vs_competitor(
    provider_route_length: list, competitor_route_length: list, threshold=0.5
):
    total_provider = numpy.sum(provider_route_length)
    total_competitor = numpy.sum(competitor_route_length)
    if total_provider > (1 + threshold) * total_competitor:
        return (
            CheckResult.WARNING,
            "route_length_vs_competitor",
            f"WARNING: Extra distance provider-competitor/competitor = {(total_provider/total_competitor - 1):.2%}",
        )
    elif total_provider < (1 - threshold) * total_competitor:
        return (
            CheckResult.WARNING,
            "route_length_vs_competitor",
            f"WARNING: Less distance in provider than competitor: competitor-provider/competitor = {(1 - total_provider/total_competitor):.2%}",
        )
    return CheckResult.OK, "", ""


def check_provider_route_time_vs_competitor(
    provider_route_time: list, competitor_route_time: list, threshold=0.5
):
    total_provider = numpy.sum(provider_route_time)
    total_competitor = numpy.sum(competitor_route_time)
    if total_provider > (1 + threshold) * total_competitor:
        return (
            CheckResult.WARNING,
            "route_time_vs_competitor",
            f"WARNING: Extra time provider-competitor/competitor = {(total_provider/total_competitor - 1):.2%}",
        )
    elif total_provider < (1 - threshold) * total_competitor:
        return (
            CheckResult.WARNING,
            "route_time_vs_competitor",
            f"WARNING: Less time in provider than competitor: competitor-provider/competitor = {(1 - total_provider/total_competitor):.2%}",
        )
    return CheckResult.OK, "", ""


def check_routes_to_be_vs_calculated(routes_df, sampled_routes, missing_routes_ratio):
    """Checks the percentage of routes to be calculated, compared to the really calculated"""

    ref = routes_df.shape[0]

    try:
        ratio = 1 - ref / sampled_routes
    except ZeroDivisionError:
        ratio = 0

    if ratio > missing_routes_ratio:
        return (
            CheckResult.WARNING,
            "tb_vs_real",
            f"WARNING: Too high percentage of not calculated competitor routes = {ratio:.2%}",
        )

    return CheckResult.OK, "", ""


def check_routes_outcomes(routes_df,route_outcome,route_outcome_ratio):
    f"""Checks the percentage of {route_outcome} routes to be calculated, compared to the total routes calculated"""

    cases=routes_df[routes_df['route_outcome']==route_outcome].shape[0]

    try:
        ratio = cases / routes_df.shape[0]
    except ZeroDivisionError:
        return (
            CheckResult.WARNING,
            f"{route_outcome}_count",
            "WARNING: There are no routes on dataframe",
        )

    if ratio > route_outcome_ratio:
        return (
            CheckResult.WARNING,
            f"{route_outcome}_route_percentage",
            f"WARNING: Too high percent of {route_outcome} routes in inspection_routes = {ratio:.2%}",
        )

    return CheckResult.OK, "", ""


class SanityCheckInspection:
    """Class that stores the sanity checks for all the processes"""

    def __init__(
        self,
        routes_df: pd.DataFrame,
    ):

        # Inspection dataframe to check
        self.routes_df = routes_df

        self.warnings = {}
        self.info = {}

    def run_checks(
        self,
        sampled_routes: int,
        geom_col: str,
        len_col: str,
        time_col: str,
        routes_length_ceiling=10000,
        routes_length_ceiling_ratio=0.05,
        missing_routes_ratio=0.05,
        missing_time_length_threshold=0.1,
    ):
        """Run all the checks"""

        checks = [
            lambda routes_df: check_missing_length(
                routes_df, geom_col, len_col, threshold=missing_time_length_threshold
            ),
            lambda routes_df: check_missing_time(
                routes_df, geom_col, time_col, threshold=missing_time_length_threshold
            ),
            lambda routes_df: check_empty_routes(
                routes_df, geom_col, missing_routes_ratio
            ),
            lambda routes_df: check_route_length(
                routes_df,
                len_col,
                routes_length_ceiling,
                routes_length_ceiling_ratio,
            ),
            lambda routes_df: check_routes_to_be_vs_calculated(
                routes_df, sampled_routes, missing_routes_ratio
            ),
        ]

        for check in checks:
            result, key, value = check(self.routes_df)

            if result == CheckResult.OK:
                continue

            if result == CheckResult.WARNING:
                self.warnings[key] = value

            if result == CheckResult.INFO:
                self.info[key] = value

    def run_competitor_relative_checks(
        self,
        provider_length_col: str,
        provider_time_col: str,
        competitor_length_col: str,
        competitor_time_col: str,
    ):
        result, key, value = check_provider_route_length_vs_competitor(
            self.routes_df[provider_length_col], self.routes_df[competitor_length_col]
        )
        if result == CheckResult.WARNING:
            self.warnings[key] = value

        result, key, value = check_provider_route_time_vs_competitor(
            self.routes_df[provider_time_col], self.routes_df[competitor_time_col]
        )
        if result == CheckResult.WARNING:
            self.warnings[key] = value

    def reset_output(self):
        """Reset the dict with sanity outputs"""
        self.info = dict()
        self.warnings = dict()


class SanityCheckInspectionTBT(SanityCheckInspection):
    """Class for TbT expecific Inspection checks"""


class SanityCheckInspectionRR(SanityCheckInspection):
    """Class for TbT expecific Inspection checks"""

class SanityCheckInspectionTrunQA(SanityCheckInspection):
    """Class for TrunQA expecific Inspection checks"""

    def run_route_outcome_checks(
            self,
            route_outcome:str,
            ratio_route_outcome:float):
        
        result, key, value=check_routes_outcomes(self.routes_df,route_outcome,ratio_route_outcome)

        if result == CheckResult.WARNING:
            self.warnings[key] = value




class VMCheckUp:
    def __init__(self):
        self.GH_NDS_VM_STARTER_AZFUNCTION_URL = "https://mapanalytics-ade-vm-starter-app.azurewebsites.net/api/VMStarter"
        self.RETRY = 5  # Number of retries for a VM to be up
        self.DELAY = 15  # Delay in seconds between retries
        self.TIMEOUT = 20  # Timeout to fail a try
    

    def _is_port_open(self, ip:str, port:str):
        """Function to check if port is opne for given ip"""
        trying_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        trying_socket.settimeout(self.TIMEOUT)

        try:
            trying_socket.connect((ip, int(port)))
            trying_socket.shutdown(socket.SHUT_RDWR)
            return True
        except Exception as e:
            return False
        finally:
            trying_socket.close()


    def _check_host_status(self, ip:str, port:str):
        """Function to check if host ip:port is up"""
        for i in range(self.RETRY):
            log.info(f"Retry Attempt {i + 1} : {ip}:{port}")
            if self._is_port_open(ip, port):
                return True
            else:
                time.sleep(self.DELAY)
        return False
    

    def make_available(self, ip_port, stack_id):
        """Function to make a NDS server available. Tests if it is up if no starts it."""
        ip, port = ip_port.split(".")

        try:
            log.info(f"Checking if {ip_port} is UP and Running.")
            if self._is_port_open(ip, port):
                log.info(f"{ip_port} is UP and Running.")
                return True

            log.info(
                f"Server {ip_port} is stopped. "
                f"Calling VM starter function (stack_id={stack_id})."
            )
            req_body = {"stack-id": stack_id}
            az_function_response = requests.post(
                url=self.GH_NDS_VM_STARTER_AZFUNCTION_URL, json=req_body
            )

            if az_function_response.status_code == 200:
                log.info(
                    f"Successfully called the VM starter function (stack_id={stack_id})."
                )
                log.info(f"Waitting for VM to start.")
                time.sleep(120)
                if self._check_host_status(ip, port):
                    log.info(f"{ip_port} is UP and Running.")
                    return True
                raise Exception("Resource is still down.")
        except requests.exceptions.RequestException as e:
            log.error("Network exception Occured! ", e.response)
            return False
    

    def run(self, endpoint, stack_id):
        url = endpoint.strip()
        match = re.match(r"http://([\d\.]+):(\d+)/", url)
        if match is not None and len(match.groups()) == 2:
            ip = match.group(1)
            port = match.group(2)

            if stack_id != "":
                params = {
                    "stack_id": stack_id.strip(),
                    "ip_port": f"{ip}:{port}"
                }
                if self.make_available(**params):
                    log.info("Server is up, proceeding ahead with measurement job")
                else:
                    log.error(
                        "Please check the stop-start Github workflow and re-run this script if required"
                    )
                    raise Exception("Resource non available")
            else:
                if self._check_host_status(ip=ip, port=port):
                    log.info("Server is up, proceeding ahead with measurement job")
                else:
                    log.error(
                        "Please start the VM and run this workflow again"
                    )
                    raise Exception("Resource non available")
