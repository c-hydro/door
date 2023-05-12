#!/usr/bin/python3

"""
door - Download ECMWF open data High Resolution single run

__date__ = '20230414'
__version__ = '1.0.0'
__author__ =
        'Andrea Libertino (andrea.libertino@cimafoundation.org',
__library__ = 'door'

General command line:
python3 hyde_downloader_satellite_gsmap_obs.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20230414 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import copy
import json
import logging
import time
import os
import xarray as xr

from argparse import ArgumentParser
from datetime import datetime
from ecmwf.opendata import Client
from requests.exceptions import HTTPError
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DOOR - ECMWF open data SINGLE RUN'
alg_version = '1.0.0'
alg_release = '2023-04-14'
# Algorithm parameter(s)
time_format = '%Y%m%d%H%M'
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Script Main
def main():
    # -------------------------------------------------------------------------------------
    # Get algorithm settings
    alg_settings, alg_time = get_args()

    # Set algorithm settings
    data_settings = read_file_json(alg_settings)

    # Set algorithm logging
    os.makedirs(data_settings['data']['log']['folder'], exist_ok=True)
    set_logging(logger_file=os.path.join(data_settings['data']['log']['folder'], data_settings['data']['log']['filename']))
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Info algorithm
    logging.info(' ============================================================================ ')
    logging.info(' ==> ' + alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logging.info(' ==> START ... ')
    logging.info(' ')

    # Time algorithm information
    start_time = time.time()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Set model timing
    time_run = datetime.strptime(alg_time, '%Y-%m-%d %H:%M')
    step_end = data_settings["data"]["dynamic"]["time"]["time_forecast_period"]
    model_freq = 3

    # Identify forecast run of interest and its availability
    logging.info(' --> TIME RUN: ' + str(time_run))
    if time_run.hour not in [0,6,12,18]:
        logging.error(" --> ERROR! ECMWF forecasts are available only for 0,6,12,18 time steps!")
        raise FileNotFoundError
    if step_end > 144:
        logging.warning(" --> WARNING! 3-hourly forecasts are available only up to 144 forecast time steps! Forecast period has been consequently limited!")
        step_end = 144
    if step_end > 90:
        if time_run.hour in [6,18]:
            logging.warning(" --> WARNING! 3-hourly hires forecast are available only up to 90 forecast time steps for 06 an 18 model issues! Forecast period has been consequently limited!")
            step_end = 90

    # Generate folder structure
    logging.info(" --> Preparing system folders ...")
    template_filled = copy.deepcopy(data_settings["algorithm"]["template"])
    for keys in data_settings["algorithm"]["template"].keys():
        template_filled[keys] = time_run.strftime(data_settings["algorithm"]["template"][keys])

    output_folder = data_settings["data"]["dynamic"]["outcome"]["folder"].format(**template_filled)
    ancillary_folder = data_settings["data"]["dynamic"]["ancillary"]["folder"].format(**template_filled)
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(ancillary_folder, exist_ok=True)
    logging.info(" --> Preparing system folders ... DONE!")

    logging.info(" --> Download forecast data from ecmwf open data server ...")
    # Setup client
    client = Client(
        source="ecmwf",
        beta=True,
        preserve_request_order=False,
        infer_stream_keyword=True,
    )
    # Perform request
    try:
        #result = client.retrieve(
        #    type = "fc",
        #    date = time_run.strftime("%Y%m%d"),
        #    time = time_run.hour,
        #    step = [i for i in np.arange(3,step_end + 1,model_freq)],
        #    param = ["10u", "10v", "2t","tp"],
        #    target = os.path.join(ancillary_folder, time_run.strftime("%Y%m%d%H") + "_fc_ecmwf_.grib2")
        #)
        #logging.info(" --> Forecast file " + result.datetime.strftime("%Y-%m-%d %H:%M") + " correctly downloaded!")
        logging.info(" --> Download forecast data from ecmwf open data server ... DONE!")
    except HTTPError:
        logging.error(" --> ERROR! File not found on the server!")
        raise FileNotFoundError

    logging.info(" --> Convert dataset to netcdf ...")
    ds = xr.load_dataset(os.path.join(ancillary_folder, time_run.strftime("%Y%m%d%H") + "_fc_ecmwf_.grib2"), engine="cfgrib",
          backend_kwargs={'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level':10}})
    logging.info(" --> Convert dataset to netcdf ... DONE")


    # -------------------------------------------------------------------------------------
    # Info algorithm
    time_elapsed = round(time.time() - start_time, 1)

    logging.info(' ')
    logging.info(' ==> ' + alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logging.info(' ==> TIME ELAPSED: ' + str(time_elapsed) + ' seconds')
    logging.info(' ==> ... END')
    logging.info(' ==> Bye, Bye')
    logging.info(' ============================================================================ ')
    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Method to read file json
def read_file_json(file_name):

    env_ws = {}
    for env_item, env_value in os.environ.items():
        env_ws[env_item] = env_value

    with open(file_name, "r") as file_handle:
        json_block = []
        for file_row in file_handle:

            for env_key, env_value in env_ws.items():
                env_tag = '$' + env_key
                if env_tag in file_row:
                    env_value = env_value.strip("'\\'")
                    file_row = file_row.replace(env_tag, env_value)
                    file_row = file_row.replace('//', '/')

            # Add the line to our JSON block
            json_block.append(file_row)

            # Check whether we closed our JSON block
            if file_row.startswith('}'):
                # Do something with the JSON dictionary
                json_dict = json.loads(''.join(json_block))
                # Start a new block
                json_block = []

    return json_dict
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Method to get script argument(s)
def get_args():
    parser_handle = ArgumentParser()
    parser_handle.add_argument('-settings_file', action="store", dest="alg_settings")
    parser_handle.add_argument('-time', action="store", dest="alg_time")
    parser_values = parser_handle.parse_args()

    if parser_values.alg_settings:
        alg_settings = parser_values.alg_settings
    else:
        alg_settings = 'configuration.json'

    if parser_values.alg_time:
        alg_time = parser_values.alg_time
    else:
        alg_time = None

    return alg_settings, alg_time
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to set logging information
def set_logging(logger_file='log.txt', logger_format=None):

    if logger_format is None:
        logger_format = '%(asctime)s %(name)-12s %(levelname)-8s ' \
                        '%(filename)s:[%(lineno)-6s - %(funcName)20s()] %(message)s'

    # Remove old logging file
    if os.path.exists(logger_file):
        os.remove(logger_file)

    # Set level of root debugger
    logging.root.setLevel(logging.INFO)

    # Open logging basic configuration
    logging.basicConfig(level=logging.INFO, format=logger_format, filename=logger_file, filemode='w')

    # Set logger handle
    logger_handle_1 = logging.FileHandler(logger_file, 'w')
    logger_handle_2 = logging.StreamHandler()
    # Set logger level
    logger_handle_1.setLevel(logging.INFO)
    logger_handle_2.setLevel(logging.INFO)
    # Set logger formatter
    logger_formatter = logging.Formatter(logger_format)
    logger_handle_1.setFormatter(logger_formatter)
    logger_handle_2.setFormatter(logger_formatter)

    # Add handle to logging
    logging.getLogger('').addHandler(logger_handle_1)
    logging.getLogger('').addHandler(logger_handle_2)

# -------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Call script from external library
if __name__ == "__main__":
    main()
# ----------------------------------------------------------------------------