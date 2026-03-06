import requests
import json
import time
import shapely
from datetime import datetime, timedelta
import pandas as pd
import logging

from tbt.utils.console_print import conditional_print, conditional_print_error

log = logging.getLogger(__name__)

def call_wms(
    geometry,
    date_start,
    date_end,
    cluster_radius=5,
    url="http://10.137.241.246/data/signs"
):
    """Function to call the SDO API

    :param geometry: geometry of the stretch
    :type geometry: shapely.geometry.LineString
    :param date_start: start date of the time window
    :type date_start: str
    :param date_end: end date of the time window
    :type date_end: str
    :param cluster_radius: radius of the cluster, defaults to 5
    :type cluster_radius: int, optional
    :param url: url of the SDO API, defaults to "http://
    :type url: str, optional
    :return: response from the SDO API
    :rtype: dict (or None in case of error)
    """

    querystring = {
        'DATA_SOURCE': 'eagle-sdo',
        'CLUSTERING':'true',
        'CLUSTER_RADIUS':str(cluster_radius),
        'IGNORE_UNKNOWN_SIGNS':'false',
        'SRS':'EPSG:4326',
        'bbox':str(geometry.bounds[:])[1:-1],
        'TIME':'{}/{}'.format(date_start,date_end)
    } 

    time.sleep(1.5)

    try: 
        r = requests.get(url, params=querystring, timeout=10)

        if r.status_code==200:
            if "Exception" in str(r.content):                
                log.info(r.content)
                conditional_print(r.content)
                return None
            else:
                sdo_json = json.loads(r.content)
                return json.dumps(sdo_json)
        else:
            log.error(f"SDO: Exception {r.status_code} when calling SDO API with querystring={str(querystring)}")
            conditional_print_error(f"SDO: Exception {r.status_code} when calling SDO API with querystring={str(querystring)}")
            return None
        
    except Exception as catched_exception:
        log.error("SDO: Timeout: %s", catched_exception)
        conditional_print_error("SDO: Timeout: %s", catched_exception)
        return None


def wrapper_sdo(
        row, 
        error_date=None,
        timewindow=4,
):
    """Function to retrieve traffic sign data from the SDO API, to be used in .apply function 

    :param row: data row from input data, containing stretch and date
    :type row: pandas.Series
    :param error_date: date of error, defaults to None
    :type error_date: datetime, optional
    :param timewindow: timewindow in which traffic signs should be checked in weeks, defaults to 4
    :type timewindow: int, optional
    :return: pandas.DataFrame with sdo response
    :rtype: pandas.DataFrame
    """
    if error_date:
        date_end = error_date
        date_start = error_date - timedelta(weeks=timewindow)
    else:
        date_end = datetime.today() - timedelta(weeks=4)
        date_start = date_end - timedelta(weeks=timewindow)
    
    # load geometry
    geometry = shapely.wkt.loads(row['stretch'])

    # transpose geometry
    geometry = shapely.geometry.LineString([(y, x) for x, y in geometry.coords])

    # call SDO API (either string or None)
    sdo_api_response = call_wms(geometry, date_start=date_start.strftime("%Y-%m-%d"), date_end=date_end.strftime("%Y-%m-%d"))

    # create pandas.Series with data
    data = pd.Series({
        'sdo_api_response': sdo_api_response,
        'sdo_data_start_date': date_start, 
        'sdo_data_end_date': date_end, 
        'sdo_api_call_date': datetime.today()
    })

    return data

