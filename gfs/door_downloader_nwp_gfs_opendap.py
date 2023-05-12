#!/usr/bin/python3
"""
door - NWP GFS 0.25 OPeNDAP

__date__ = '20220512'
__version__ = '1.0.0'
__author__ =
        'Andrea Libertino (andrea.libertino@cimafoundation.org',
__library__ = 'HyDE'

General command line:
python3 door_downloader_nwp_gfs_opendap.py -settings_file configuration.json -time YYYY-MM-DD HH:MM

Version(s):
20220512 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import datetime as dt
import json
import logging
import numpy as np
import os
import pandas as pd
import time
import xarray as xr
import warnings

from argparse import ArgumentParser
from copy import deepcopy
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DOOR - NWP GFS - OPeNDAP'
alg_version = '1.0.0'
alg_release = '2022-05-12'
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
    domain = data_settings["algorithm"]["domain"]

    # Set algorithm logging
    os.makedirs(data_settings['data']['log']['folder'], exist_ok=True)
    set_logging(logger_file=os.path.join(data_settings['data']['log']['folder'], data_settings['data']['log']['filename']))

    warnings.filterwarnings(action='ignore', category=xr.SerializationWarning)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Info algorithm
    logging.info(' ============================================================================ ')
    logging.info(' ==> ' + alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logging.info(' ==> START ... ')
    logging.info(' ')

    start_time = time.time()

    time_run = dt.datetime.strptime(alg_time, '%Y-%m-%d %H:%M')
    logging.info(" --> Algorithm time: " + alg_time)

    logging.info(" --> Make folders...")
    template_filled = fill_template(data_settings["algorithm"]["template"], time_run)
    template_filled["domain"] = data_settings["algorithm"]["domain"]
    ancillary_fld = data_settings["data"]["ancillary"]["folder"].format(**template_filled)
    outcome_fld = data_settings["data"]["outcome"]["folder"].format(**template_filled)
    os.makedirs(ancillary_fld, exist_ok= True)
    os.makedirs(outcome_fld, exist_ok= True)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Algorithm setup
    logging.info(" --> Set up algorithm...")

    # Time settings
    logging.info(" ---> Time settings...")
    min_step = 1
    max_step = data_settings['data']['dynamic']["time"]['time_forecast_period'] + 1
    if max_step > 121:
        logging.error(" ERROR! Only the first 120 forecast hours are available trough OPeNDAP, use nomads or ftp for downloading further steps!")
    time_span = (min_step, max_step)
    time_range = pd.date_range(time_run + pd.Timedelta(str(min_step) + "H"),
                               time_run + pd.Timedelta(str(max_step - 1) + "H"), freq="H")
    # Spatial settings
    logging.info(" ---> Space settings...")
    lat = (data_settings['data']['static']['bounding_box']["lat_bottom"], data_settings['data']['static']['bounding_box']["lat_top"])
    lon = (data_settings['data']['static']['bounding_box']["lon_left"], data_settings['data']['static']['bounding_box']["lon_right"])
    logging.info(" --> Set up algorithm settings... Space setting DONE!")

    # Other settings
    logging.info(" ---> Model settings...")
    vars = [i for i in data_settings['data']['dynamic']["variables"].keys()]
    logging.info(" ----> Vars to be downloaded: " + (", ").join(vars))

    gfs = f"https://nomads.ncep.noaa.gov/dods/gfs_0p25_1hr"
    url = f"{gfs}/gfs{time_run.strftime('%Y%m%d')}/gfs_0p25_1hr_{str(time_run.hour).zfill(2)}z"
    logging.info(" ----> url: " + url)
    logging.info(" --> Set up algorithm... DONE")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Download
    logging.info(" --> Download forecast...")
    ancillary_file = os.path.join(ancillary_fld, data_settings["data"]["ancillary"]["filename"]).format(**template_filled)
    try:
        with xr.open_dataset(url) as ds:
            ds[vars]\
                .isel(time=slice(*time_span))\
                .sel(lat=slice(*lat), lon=slice(*lon))\
                .to_netcdf(ancillary_file)
        logging.info(" --> Download forecast... DONE")
    except:
        logging.error(" ERROR! Download failed. If you are sure the file exist, try to wait considering the 120 hit/minute limits of nomads server")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Postprocessing
    logging.info(" --> Postprocess data...")
    ds_sub = xr.open_dataset(ancillary_file).assign_coords(time = time_range)

    if "apcpsfc" in ds_sub.keys() and \
            data_settings['data']['dynamic']["vars_standards"]["decumulate_precipitation"] is True:
        logging.info(" ---> Decumulate precipitation...")
        first_step = ds_sub["apcpsfc"].values[0,:,:]
        ds_sub["apcpsfc"] = ds_sub["apcpsfc"].diff("time",1)
        ds_sub["apcpsfc"].values[0, :, :] = first_step
        ds_sub['apcpsfc'].attrs['long_name'] = 'precipitation in the time step'
        ds_sub['apcpsfc'].attrs['units'] = 'mm'
        ds_sub['apcpsfc'].attrs['standard_name'] = "precipitation"
        logging.info(" ---> Decumulate precipitation...DONE!")

    if "tmp2m" in ds_sub.keys() and \
            data_settings['data']['dynamic']["vars_standards"]["convert_temperature_to_C"] is True:
        logging.info(" ---> Convert temperature to °C...")
        ds_sub["tmp2m"] = ds_sub["tmp2m"] - 273.15
        ds_sub['tmp2m'].attrs['long_name'] = '2 metre temperature'
        ds_sub['tmp2m'].attrs['units'] = 'C'
        ds_sub['tmp2m'].attrs['standard_name'] = "air_temperature"
        logging.info(" ---> Convert temperature to °C...DONE!")

    if "ugrd10m" in ds_sub.keys() and "vgrd10m" in ds_sub.keys() and \
            data_settings['data']['dynamic']["vars_standards"]["aggregate_wind_components"] is True:
        logging.info(" ---> Aggregate wind components...")
        ds_sub["10wind"] = np.sqrt(ds_sub["ugrd10m"]**2 + ds_sub["vgrd10m"]**2)
        ds_sub['10wind'].attrs['long_name'] = '10 m wind'
        ds_sub['10wind'].attrs['units'] = 'm s**-1'
        ds_sub['10wind'].attrs['standard_name'] = "wind"
        logging.info(" ---> Aggregate wind components...DONE!")

    logging.info(" ---> Rename variables and save...")
    outcome_file = os.path.join(outcome_fld, data_settings["data"]["outcome"]["filename"]).format(**template_filled)
    ds_sub.rename(data_settings['data']['dynamic']["variables"]).to_netcdf(outcome_file)
    os.remove(ancillary_file)
    logging.info(" ---> Rename variables and save...DONE")
    # -------------------------------------------------------------------------------------

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

# -------------------------------------------------------------------------------------
# Function for fill a dictionary of templates
def fill_template(templates, time_now):
    empty_template = deepcopy(templates)
    templated_filled = {}
    for key in empty_template.keys():
        templated_filled[key] = time_now.strftime(empty_template[key])
    return templated_filled
# -------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Call script from external library
if __name__ == "__main__":
    main()
# ----------------------------------------------------------------------------