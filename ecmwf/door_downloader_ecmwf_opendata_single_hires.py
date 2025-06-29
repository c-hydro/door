#!/usr/bin/python3

"""
door - Download ECMWF open data High Resolution (0.25 degree) single run

__date__ = '20250113'
__version__ = '2.5.0'
__author__ =
        'Andrea Libertino (andrea.libertino@cimafoundation.org',
__library__ = 'door'

General command line:
python3 door_downloader_ecmwf_opendata_single_hires.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20250113 (2.5.0) --> Add support to AIFS model
                     Bug fixes in the download of forecast related to different height levels
20240313 (2.0.0) --> Update to new 0.25 resoulution
20230923 (1.1.0) --> Fix flipped latitude, removes unused dimensions from output
20230731 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import copy
import json
import logging
import time
import os
import numpy as np
import pandas as pd
import xarray as xr
from copy import deepcopy

from argparse import ArgumentParser
from datetime import datetime
from ecmwf.opendata import Client
from requests.exceptions import HTTPError

# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DOOR - ECMWF open data SINGLE RUN 0.25'
alg_version = '2.5.0'
alg_release = '2025-01-13'
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
    set_logging(
        logger_file=os.path.join(data_settings['data']['log']['folder'], data_settings['data']['log']['filename']))
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
    if "input" in data_settings["data"]["dynamic"].keys():
        model_type = data_settings["data"]["dynamic"]["input"]["model_type"]
    else:
        logging.warning(" --> WARNING! Model type not defined in the configuration file! Default is IFS!")
        model_type = "ifs"
    if model_type == "ifs":
        model_freq = 3
        rain_factor = 1000
    elif model_type == "aifs-single":
        model_freq = 6
        rain_factor = 1
        #data_settings["data"]["dynamic"]["variables"]["t2m"] = data_settings["data"]["dynamic"]["variables"]["2t"]
    else:
        logging.error(" --> ERROR! Model type not correctly defined!")
        raise NotImplementedError
    model_time_range = pd.date_range(time_run + pd.Timedelta(str(model_freq) + "H"),
                                     time_run + pd.Timedelta(str(step_end) + "H"), freq=str(model_freq) + "H")
    time_range = pd.date_range(time_run + pd.Timedelta("1H"),
                               time_run + pd.Timedelta(str(step_end - 1) + "H"), freq="H")

    # Identify forecast run of interest and its availability
    logging.info(' --> TIME RUN: ' + str(time_run))
    if time_run.hour not in [0, 6, 12, 18]:
        logging.error(" --> ERROR! ECMWF forecasts are available only for 0,6,12,18 time steps!")
        raise FileNotFoundError
    if step_end > 144:
        logging.warning(
            " --> WARNING! 3-hourly forecasts are available only up to 144 forecast time steps! Forecast period has been consequently limited!")
        step_end = 144
    if step_end > 90:
        if time_run.hour in [6, 18]:
            logging.warning(
                " --> WARNING! 3-hourly hires forecast are available only up to 90 forecast time steps for 06 an 18 model issues! Forecast period has been consequently limited!")
            step_end = 90

    # Generate folder structure
    logging.info(" --> Preparing system folders ...")
    template_filled = copy.deepcopy(data_settings["algorithm"]["template"])
    for keys in data_settings["algorithm"]["template"].keys():
        template_filled[keys] = time_run.strftime(data_settings["algorithm"]["template"][keys])
    template_filled["domain"] = data_settings["algorithm"]["domain"]

    output_folder = data_settings["data"]["dynamic"]["outcome"]["folder"].format(**template_filled)
    ancillary_folder = data_settings["data"]["dynamic"]["ancillary"]["folder"].format(**template_filled)
    ancillary_file = data_settings["data"]["dynamic"]["ancillary"]["filename"].format(**template_filled)
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(ancillary_folder, exist_ok=True)
    logging.info(" --> Preparing system folders ... DONE!")

    logging.info(" --> Download forecast data from ecmwf open data server ...")
    # Setup client
    client = Client(
        source="ecmwf",
        model=model_type,
        resol="0p25",
        preserve_request_order=False,
        infer_stream_keyword=True,
    )
    # Perform request
    try:
        result = client.retrieve(
            type="fc",
            date=time_run.strftime("%Y%m%d"),
            time=time_run.hour,
            step=[i for i in np.arange(model_freq, step_end + 1, model_freq)],
            param=[var for var in data_settings["data"]["dynamic"]["variables"].keys()],
            target=os.path.join(ancillary_folder, ancillary_file)
        )
        logging.info(" --> Forecast file " + result.datetime.strftime("%Y-%m-%d %H:%M") + " correctly downloaded!")
        logging.info(" --> Download forecast data from ecmwf open data server ... DONE!")
    except HTTPError:
        logging.error(" --> ERROR! File not found on the server!")
        raise FileNotFoundError

    logging.info(" --> Convert dataset to netcdf ...")
    rename_dict = deepcopy(data_settings["data"]["dynamic"]["variables"])
    if "10u" in rename_dict.keys():
        rename_dict["u10"] = "10u"
        del rename_dict["10u"]
    if "10v" in rename_dict.keys():
        rename_dict["v10"] = "10v"
        del rename_dict["10v"]
    if "2t" in rename_dict.keys():
        rename_dict["t2m"] = rename_dict["2t"]
        del rename_dict["2t"]
    if "2d" in rename_dict.keys():
        rename_dict["d2m"] = rename_dict["2d"]
        del rename_dict["2d"]

    file_path = os.path.join(ancillary_folder, ancillary_file)
    ds_10m = xr.load_dataset(file_path, engine="cfgrib", filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 10})
    ds_2m = xr.load_dataset(file_path, engine="cfgrib", filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 2})
    ds_surface = xr.load_dataset(file_path, engine="cfgrib", filter_by_keys={'typeOfLevel': 'surface'})

    # Merge the datasets
    frc_out = xr.merge([ds_10m, ds_2m, ds_surface], compat='override').rename_vars(rename_dict).drop_vars("time")
    frc_out = frc_out.assign_coords({"step": model_time_range}).rename({"step": "time", "latitude": "lat", "longitude": "lon"})

    frc_out = frc_out.where((frc_out.lat <= data_settings['data']['static']['bounding_box']["lat_top"]) &
                            (frc_out.lat >= data_settings['data']['static']['bounding_box']["lat_bottom"]) &
                            (frc_out.lon >= data_settings['data']['static']['bounding_box']["lon_left"]) &
                            (frc_out.lon <= data_settings['data']['static']['bounding_box']["lon_right"]), drop=True)

    # If lat is a decreasing vector, flip it and the associated variables vertically
    if frc_out.lat.values[0] > frc_out.lat.values[-1]:
        logging.warning(" --> WARNING! Latitude is decreasing, flip it and the associated variables vertically!")
        frc_out = frc_out.reindex(lat=frc_out.lat[::-1])
        for var in frc_out.data_vars:
            frc_out[var] = frc_out[var].reindex(lat=frc_out.lat[::-1])
    logging.info(" --> Convert dataset to netcdf ... DONE")

    # -------------------------------------------------------------------------------------
    # Download and compute variables
    logging.info(" --> Postprocess variables")

    if "tp" in data_settings["data"]["dynamic"]["variables"].keys():
        # Convert meters to mm
        logging.info(" ---> Convert meters to mm of rain...")
        frc_out[data_settings['data']['dynamic']["variables"]["tp"]] = frc_out[data_settings['data']['dynamic'][
            "variables"]["tp"]] * rain_factor

        if data_settings['data']['dynamic']["vars_standards"]["decumulate_precipitation"] is True:
            logging.info(" ---> Variable tp is cumulated... Performing decumulation")
            first_step = deepcopy(frc_out[data_settings['data']['dynamic']["variables"]["tp"]].values[0, :, :])
            first_time = frc_out[data_settings['data']['dynamic']["variables"]["tp"]].time[0].values
            frc_out[data_settings['data']['dynamic']["variables"]["tp"]] = frc_out[
                data_settings['data']['dynamic']["variables"]["tp"]].diff("time", 1)
            frc_out[data_settings['data']['dynamic']["variables"]["tp"]].loc[first_time, :, :] = first_step
            frc_out[data_settings['data']['dynamic']["variables"]["tp"]] = xr.where(
                frc_out[data_settings['data']['dynamic']["variables"]["tp"]] < 0, 0,
                frc_out[data_settings['data']['dynamic']["variables"]["tp"]]) / model_freq

    if "2t" in data_settings["data"]["dynamic"]["variables"].keys() and \
            data_settings['data']['dynamic']["vars_standards"]["convert_temperature_to_C"] is True:
        logging.info(" ---> Convert temperature to °C...")
        frc_out[data_settings['data']['dynamic']["variables"]["2t"]] = frc_out[data_settings['data']['dynamic'][
            "variables"]["2t"]] - 273.15
        frc_out[data_settings['data']['dynamic']["variables"]["2t"]].attrs['long_name'] = '2 metre temperature'
        frc_out[data_settings['data']['dynamic']["variables"]["2t"]].attrs['units'] = 'C'
        frc_out[data_settings['data']['dynamic']["variables"]["2t"]].attrs['standard_name'] = "air_temperature"
        logging.info(" ---> Convert temperature to °C...DONE!")
        if "2d" in data_settings["data"]["dynamic"]["variables"].keys() and \
                data_settings['data']['dynamic']["vars_standards"]["calculate_rh_ground"] is True:
                frc_out["rh"] = relative_humidity_from_dewpoint(frc_out[data_settings['data']['dynamic']["variables"]["2t"]],
                                                                frc_out[data_settings['data']['dynamic']["variables"]["2d"]] - 273.15)
                frc_out['rh'].attrs['long_name'] = 'relative humidity'
                frc_out['rh'].attrs['units'] = '%'
                frc_out['rh'].attrs['standard_name'] = "relative_humidity"

    if "10u" in data_settings["data"]["dynamic"]["variables"].keys() and "10v" in data_settings["data"]["dynamic"][
        "variables"].keys() and \
            data_settings['data']['dynamic']["vars_standards"]["aggregate_wind_components"] is True:
        logging.info(" ---> Aggregate wind components...")
        frc_out["10wind"] = np.sqrt(frc_out[data_settings['data']['dynamic']["variables"]["10u"]] ** 2 + frc_out[
            data_settings['data']['dynamic']["variables"]["10v"]] ** 2)
        frc_out['10wind'].attrs['long_name'] = '10 m wind'
        frc_out['10wind'].attrs['units'] = 'm s**-1'
        frc_out['10wind'].attrs['standard_name'] = "wind"
        logging.info(" ---> Aggregate wind components...DONE!")

    if "ssrd" in data_settings["data"]["dynamic"]["variables"].keys():
        if data_settings['data']['dynamic']["vars_standards"]["decumulate_radiation"] is True:
            logging.info(" ---> Variable ssrd is cumulated... Performing decumulation")
            first_step = deepcopy(frc_out[data_settings['data']['dynamic']["variables"]["ssrd"]].values[0, :, :])
            first_time = frc_out[data_settings['data']['dynamic']["variables"]["tp"]].time[0].values
            frc_out[data_settings['data']['dynamic']["variables"]["ssrd"]] = frc_out[
                data_settings['data']['dynamic']["variables"]["ssrd"]].diff("time", 1)
            frc_out[data_settings['data']['dynamic']["variables"]["ssrd"]].loc[first_time, :, :] = first_step
            frc_out[data_settings['data']['dynamic']["variables"]["ssrd"]] = frc_out[data_settings['data']['dynamic']["variables"]["ssrd"]] / (3600*model_freq)
            frc_out[data_settings['data']['dynamic']["variables"]["ssrd"]].attrs['units'] = 'W m**-2'
        else:
            logging.warning(" ---> WARNING! Setting to decumulate_radiation is not set by config file... Decumulation performed by default!")

    if data_settings['data']['dynamic']["vars_standards"]["aggregate_wind_components"]:
        if "10u" not in data_settings["data"]["dynamic"]["variables"].keys() or "10v" not in data_settings["data"]["dynamic"]["variables"].keys():
            logging.warning(" --> WARNING! Wind components are not available (10u, 10v), aggregated wind can not be computed!")

    if "calculate_rh_ground" in data_settings['data']['dynamic']["vars_standards"].keys() and data_settings['data']['dynamic']["vars_standards"]["calculate_rh_ground"]:
        if "2t" not in data_settings["data"]["dynamic"]["variables"].keys() or "2d" not in data_settings["data"]["dynamic"]["variables"].keys():
            logging.warning(" --> WARNING! 2m temperature and/or 2m dewpoint (2t, 2d) are missing, ground relative humidity can not be computed!")

    # frc_out = frc_out.reindex({'time': time_range}, method='nearest')
    frc_out = frc_out.drop_vars(["valid_time","surface","heightAboveGround"]).reindex({'time': time_range}, method='nearest')
    frc_out["lat"].attrs["units"] = "degrees_north"
    frc_out["lon"].attrs["units"] = "degrees_east"
    logging.info(" --> Postprocess variables..DONE")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Save output and clean system
    logging.info(" --> Save output data...")
    outcome_file = os.path.join(output_folder, data_settings["data"]["dynamic"]["outcome"]["filename"]).format(
        **template_filled)
    frc_out.to_netcdf(outcome_file)

    if data_settings["algorithm"]["flags"]["clean_ancillary"]:
        os.remove(os.path.join(ancillary_folder, ancillary_file))
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
# Function to calculate the saturation water vapor pressure
def saturation_vapor_pressure(temperature):
    """Calculate the saturation water vapor (partial) pressure.
    Instead of temperature, dewpoint may be used in order to calculate
    the actual (ambient) water vapor (partial) pressure.
    The formula used is that from [Bolton1980]_ for T in degrees Celsius:
    .. math:: 6.112 e^\frac{17.67T}{T + 243.5}
    """
    # Converted from original in terms of C to use kelvin. Using raw absolute values of C in
    # a formula plays havoc with units support.
    sat_pressure_0c = 6.112 # millibar
    return sat_pressure_0c * np.exp(17.67 * temperature / (temperature + 243.5))
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Function to calculate the relative humidity from dewpoint
def relative_humidity_from_dewpoint(temperature, dewpoint):
    """Calculate the relative humidity.
    Uses temperature and dewpoint to calculate relative humidity as the ratio of vapor
    pressure to saturation vapor pressures.
    """
    e = saturation_vapor_pressure(dewpoint)
    e_s = saturation_vapor_pressure(temperature)
    rh = e / e_s
    rh.values[rh.values > 1] = 1
    rh.values[rh.values < 0] = 0
    return rh * 100
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Call script from external library
if __name__ == "__main__":
    main()
# ----------------------------------------------------------------------------
