#!/usr/bin/python3
"""
door - NWP ICON GLOBAL

__date__ = '20230626'
__version__ = '1.0.0'
__author__ =
        'Andrea Libertino (andrea.libertino@cimafoundation.org',
__library__ = 'DOOR'

General command line:
python3 door_downloader_nwp_icon_global.py -settings_file configuration.json -time YYYY-MM-DD HH:MM

Version(s):
20230626 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import datetime as dt
import json
import logging
import shutil

import numpy as np
import os
import pandas as pd
import xarray as xr
import bz2
import tarfile

from argparse import ArgumentParser
from copy import deepcopy
from math import floor, ceil
import requests
import time
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
import glob
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DOOR - NWP ICON Global'
alg_version = '1.0.0'
alg_release = '2023-06-26'
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

    # warnings.filterwarnings(action='ignore', category=xr.SerializationWarning)
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
    logging.info(" --> Make folders...DONE")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Algorithm setup
    logging.info(" --> Set up algorithm...")

    if data_settings["algorithm"]["flags"]["downloading_mp"]:
        cpu_cores = data_settings["algorithm"]["ancillary"]["process_mp"]
        if cpu_cores is None:
            cpu_cores = cpu_count() - 1
    else:
        cpu_cores = 1

    # Model settings
    logging.info(" ---> Model settings...")
    model = data_settings["data"]["dynamic"]["input"]["model_type"]
    global model_settings
    model_settings = {}
    model_settings["cdo_path"] = data_settings["algorithm"]["ancillary"]["cdo_path"]
    logging.info(" ----> Set up model " + model)

    if model == "ICON0p125":
        url_blank = "https://opendata.dwd.de/weather/nwp/icon/grib/{run_time}/{var}/icon_global_icosahedral_single-level_{run_date}{run_time}_{step}_{VAR}.grib2.bz2"
        model_settings["grid_file"] = os.path.join(ancillary_fld,"ICON_GLOBAL2WORLD_0125_EASY","target_grid_world_0125.txt")
        model_settings["weigths_file"] = os.path.join(ancillary_fld,"ICON_GLOBAL2WORLD_0125_EASY","weights_icogl2world_0125.nc")
        url_model_data = "https://opendata.dwd.de/weather/lib/cdo/ICON_GLOBAL2WORLD_0125_EASY.tar.bz2"
    else:
        logging.error(" --> ERROR! Only global ICON 0.125 has been implemented until now!")
        raise NotImplementedError("Only ICON0p125 type has been implemented")

    if not os.path.isfile(model_settings["grid_file"]) or not os.path.isfile(model_settings["weigths_file"]):
        logging.info(" ----> Download binary decodification table")
        r = requests.get(url_model_data)
        with open(os.path.join(ancillary_fld,"binary_grids.tar.bz2"), 'wb') as f:
            f.write(r.content)
        tar = tarfile.open(os.path.join(ancillary_fld, "binary_grids.tar.bz2"), "r:bz2")
        tar.extractall(ancillary_fld)
        tar.close()
        os.remove(os.path.join(ancillary_fld, "binary_grids.tar.bz2"))

    variables = [i for i in data_settings['data']['dynamic']["variables"].keys()]

    # Time settings
    logging.info(" ---> Time settings...")
    min_step = 1
    max_step = data_settings['data']['dynamic']["time"]['time_forecast_period'] + 1
    if max_step > 181:
        logging.error(" ERROR! Only the first 180 forecast hours are available on the dwd website!")
    time_range = pd.date_range(time_run + pd.Timedelta(str(min_step) + "H"), time_run + pd.Timedelta(str(max_step - 1) + "H"), freq="H")
    if max_step > 78:
        forecast_steps = np.concatenate((np.arange(1,78,1), np.arange(78,np.min((max_step+2,180)),3)))
    else:
        forecast_steps = np.arange(1,79,1)

    # Spatial settings
    logging.info(" ---> Space settings...")
    data_settings['data']['static']['bounding_box']["lat_bottom"] = data_settings['data']['static']['bounding_box']["lat_bottom"]
    data_settings['data']['static']['bounding_box']["lon_left"] = data_settings['data']['static']['bounding_box']["lon_left"]
    data_settings['data']['static']['bounding_box']["lat_top"] = data_settings['data']['static']['bounding_box']["lat_top"]
    data_settings['data']['static']['bounding_box']["lon_right"] = data_settings['data']['static']['bounding_box']["lon_right"]

    logging.info(" --> Set up algorithm...DONE")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Download and compute variables
    first_step = True
    var_folders = []

    for var in variables:
        logging.info(" --> Compute variable: " + var)
        template_filled["var"] = var
        template_filled["VAR"] = var.upper()
        template_filled["step"] = "{step}"

        logging.info(" ---> Download forecast data...")
        ancillary_out_var = os.path.join(ancillary_fld,var,"")
        if os.path.isdir(ancillary_out_var):
            shutil.rmtree(ancillary_out_var)
        os.makedirs(ancillary_out_var, exist_ok=True)
        out_file = os.path.join(ancillary_out_var, "frc_{step}.grib2.bz2")

        var_folders += [ancillary_out_var]

        url_var = url_blank.format(**template_filled)
        urls = [url_var.format(step=str(t).zfill(3)) for t in forecast_steps]
        out_files = [out_file.format(step=str(t).zfill(3)) for t in forecast_steps]

        inputs = zip(urls, out_files)

        if data_settings["algorithm"]["flags"]["downloading_mp"]:
            logging.info(" ----> Start download in parallel mode...")
        else:
            logging.info(" ----> Start download in serial mode...")
        download_parallel(inputs,cpu_cores)
        logging.info(" ---> Download forecast data...DONE")

        out_files_unzipped = [i.replace(".bz2", "").replace("frc", "regr_frc") for i in out_files]

        if os.path.getsize(out_files_unzipped[0]) < 1000:
            logging.error(" ERROR! First file of the forecast is empty, possibly forecast file is not available yet!")
            shutil.rmtree(ancillary_out_var)
            raise FileNotFoundError(" -> Size of the downloaded forecast step is < 1000 byte! Forecast is unavailable or corrupted!")

        logging.info( "---> Open and merge forecast time steps... It can take some minutes...")
        with xr.open_mfdataset(out_files_unzipped, concat_dim='valid_time', data_vars='minimal', combine='nested', coords='minimal',
                               compat='override', engine="cfgrib") as ds:
            ds = ds.where((ds.latitude <= data_settings['data']['static']['bounding_box']["lat_top"]) &
                          (ds.latitude >= data_settings['data']['static']['bounding_box']["lat_bottom"]) &
                          (ds.longitude >= data_settings['data']['static']['bounding_box']["lon_left"]) &
                          (ds.longitude <= data_settings['data']['static']['bounding_box']["lon_right"]), drop=True)

            var_names = [vars for vars in ds.data_vars.variables.mapping]
            if len(var_names) > 1:
                logging.error("ERROR! Only one variable should be in the grib file, check file integrity!")
                raise TypeError
            var_name = var_names[0]

            ds = ds[var_name].drop("time").rename({"longitude":"lon", "latitude":"lat", "valid_time":"time"})

            if first_step is True:
                frc_out = xr.Dataset({data_settings['data']['dynamic']["variables"][var]:ds})
                first_step = False
            else:
                frc_out[data_settings['data']['dynamic']["variables"][var]] = ds

            logging.info("---> Open and merge forecast time steps...DONE")

        logging.info(" --> Compute variable: " + var + "...DONE")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Download and compute variables
    logging.info(" --> Postprocess variables")

    if "tot_prec" in variables and \
            data_settings['data']['dynamic']["vars_standards"]["decumulate_precipitation"] is True:
        logging.info(" ---> Variable tot_prec is cumulated... Performing decumulation")
        first_step = deepcopy(frc_out[data_settings['data']['dynamic']["variables"]["tot_prec"]].values[0, :, :])
        frc_out[data_settings['data']['dynamic']["variables"]["tot_prec"]] = frc_out[data_settings['data']['dynamic']["variables"]["tot_prec"]].diff("time", 1)
        frc_out[data_settings['data']['dynamic']["variables"]["tot_prec"]].loc[time_range[0], :, :] = first_step

        if len(frc_out[data_settings['data']['dynamic']["variables"]["tot_prec"]].time) >= 78:
            logging.info(
                " ---> More than 77 time steps are available. Last steps are considered with 3-hourly resolution")
            frc_out[data_settings['data']['dynamic']["variables"]["tot_prec"]][78:, :, :] = frc_out[data_settings['data']['dynamic']["variables"]["tot_prec"]][78:, :, :]  / 3
        frc_out[data_settings['data']['dynamic']["variables"]["tot_prec"]] = xr.where(frc_out[data_settings['data']['dynamic']["variables"]["tot_prec"]]<0,0,frc_out[data_settings['data']['dynamic']["variables"]["tot_prec"]])

    if "t_2m" in variables and \
            data_settings['data']['dynamic']["vars_standards"]["convert_temperature_to_C"] is True:
        logging.info(" ---> Convert temperature to °C...")
        frc_out[data_settings['data']['dynamic']["variables"]["t_2m"]] = frc_out[data_settings['data']['dynamic']["variables"]["t_2m"]] - 273.15
        frc_out[data_settings['data']['dynamic']["variables"]["t_2m"]].attrs['long_name'] = '2 metre temperature'
        frc_out[data_settings['data']['dynamic']["variables"]["t_2m"]].attrs['units'] = 'C'
        frc_out[data_settings['data']['dynamic']["variables"]["t_2m"]].attrs['standard_name'] = "air_temperature"
        logging.info(" ---> Convert temperature to °C...DONE!")

    if "u_10m" in variables and "v_10m" in variables and \
            data_settings['data']['dynamic']["vars_standards"]["aggregate_wind_components"] is True:
        logging.info(" ---> Aggregate wind components...")
        frc_out["10wind"] = np.sqrt(frc_out[data_settings['data']['dynamic']["variables"]["u_10m"]]**2 + frc_out[data_settings['data']['dynamic']["variables"]["v_10m"]]**2)
        frc_out['10wind'].attrs['long_name'] = '10 m wind'
        frc_out['10wind'].attrs['units'] = 'm s**-1'
        frc_out['10wind'].attrs['standard_name'] = "wind"
        logging.info(" ---> Aggregate wind components...DONE!")

    frc_out = frc_out.reindex({'time': time_range}, method='nearest')
    logging.info(" --> Postprocess variables..DONE")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Save output and clean system
    logging.info(" --> Save output data...")
    outcome_file = os.path.join(outcome_fld, data_settings["data"]["outcome"]["filename"]).format(**template_filled)
    frc_out.to_netcdf(outcome_file)

    if data_settings["algorithm"]["flags"]["clean_ancillary"]:
        logging.info(" ---> Clean ancillary folder for variable...")
        for fld in var_folders:
            shutil.rmtree(fld)
        logging.info(" --> Clean ancillary files...")
        for file in glob.glob(os.path.join(ancillary_fld,"*.*")):
            os.remove(file)

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

# -------------------------------------------------------------------------------------
def download_url(args):
    url, fn = args[0], args[1]
    try:
        r = requests.get(url)
        with open(fn, 'wb') as f:
            f.write(r.content)
        decompress_file(fn)
        os.remove(fn)
        os.system(model_settings["cdo_path"] + " -O remap," + model_settings["grid_file"] + ","+ model_settings["weigths_file"] +" " + fn[:-4] + " " + fn[:-4].replace("frc","regr_frc"))
        os.remove(fn[:-4])
        return fn
    except Exception as e:
        print('Exception in download_url():', e)
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
def download_parallel(in_out_files, cpu_cores):
    cpus = cpu_cores
    results = ThreadPool(cpus).imap_unordered(download_url, in_out_files)
    for result in results:
        print(' --> Compute:', result)
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
def decompress_file(filepath):
    zipfile = bz2.BZ2File(filepath)  # open the file
    data = zipfile.read()  # get the decompressed data
    newfilepath = filepath[:-4]  # assuming the filepath ends with .bz2
    open(newfilepath, 'wb').write(data)
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Call script from external library
if __name__ == "__main__":
    main()
# ----------------------------------------------------------------------------