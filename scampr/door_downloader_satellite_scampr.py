#!/usr/bin/python3

"""
door Tool - SATELLITE Self-Calibrating Multivariate Precipitation Retrieval (SCaMPR)

__date__ = '20221202'
__version__ = '1.0.0'
__author__ =
        'Andrea Libertino (andrea.libertino@cimafoundation.org',
__library__ = 'door'

General command line:
python3 door_downloader_satellite_scampr.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20211227 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import pandas as pd
import os, json, logging, time
from argparse import ArgumentParser
from bs4 import BeautifulSoup
import requests, os
import xarray as xr
from urllib.request import urlretrieve
import numpy as np
import pandas as pd
import datetime as dt
import rioxarray

# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DOOR - SATELLITE Self-Calibrating Multivariate Precipitation Retrieval (SCaMPR)'
alg_version = '1.0.0'
alg_release = '2022-12-02'
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

    start_time = time.time()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Manage paths
    logging.info(" --> Create ancillary data path...")
    downloader_settings = {}
    downloader_settings["ancillary_path"] = data_settings["data"]["dynamic"]["ancillary"]["folder"].format(domain=data_settings['algorithm']['domain'])
    os.makedirs(downloader_settings["ancillary_path"], exist_ok=True)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Get algorithm time range
    logging.info(" --> Setting algorithm time settings...")
    time_end = dt.datetime.strptime(alg_time, '%Y-%m-%d %H:%M')
    time_start = time_end - pd.Timedelta(str(data_settings["data"]["dynamic"]["time"]["time_observed_period"]) +
                                         data_settings["data"]["dynamic"]["time"]["time_observed_frequency"])
    date_to_explore = pd.date_range(time_start, time_end, freq=data_settings["data"]["dynamic"]["time"]["product_frequency"])

    downloader_settings["templates"] = data_settings["algorithm"]["template"]
    logging.info(" --> Setting algorithm time settings...DONE")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Initialise some settings
    downloader_settings["crop_map"] = data_settings["algorithm"]["flags"]["crop_map"]
    downloader_settings["clean_dynamic_data_ancillary"] = data_settings["algorithm"]["flags"]["clean_dynamic_data_ancillary"]
    downloader_settings["bounding_box"] = data_settings["data"]["static"]["bounding_box"]
    downloader_settings["url"] = "https://www.star.nesdis.noaa.gov/pub/smcd/emb/bobk/Enterprise_Global/"
    downloader_settings["ext"] = "nc"

    dict_empty = data_settings['algorithm']['template']
    dict_filled = dict_empty.copy()
    dict_filled["domain"] = data_settings['algorithm']['domain']

    # Download lat-lon file and extract geographical info
    logging.info(" --> Get geographical information...")
    urlretrieve(os.path.join(downloader_settings["url"], "rain_rate_grid_lat_lon.nc"), os.path.join(downloader_settings["ancillary_path"], "rain_rate_grid_lat_lon.nc"))
    lat_lon = xr.open_dataset(os.path.join(downloader_settings["ancillary_path"], "rain_rate_grid_lat_lon.nc"))
    lat = np.unique(lat_lon.latitude.values)
    lon = np.unique(lat_lon.longitude.values)
    os.remove(os.path.join(downloader_settings["ancillary_path"], "rain_rate_grid_lat_lon.nc"))
    logging.info(" --> Get geographical information...DONE")

    # Get file list
    logging.info(" --> Get file list from the server...")
    lista = []
    for file in list_files(downloader_settings["url"], downloader_settings["ext"]):
        lista = lista + [file]
    logging.info(" --> Get file list from the server...DONE")

    for time_now in date_to_explore:
        logging.info(" --> Compute time step " + time_now.strftime("%Y-%m-%d %H:%M"))
        lista_in = [i for i in lista if time_now.strftime("%Y%m%d%H%M") in i]

        if len(lista_in) == 0:
            logging.warning("WARNING! No map found for the time step!")
            continue
        else:
            logging.info(" ---> " + str(len(lista_in)) + " map(s) found!")

            if data_settings["algorithm"]["flags"]["download_only_first_version"]:
                lista_in = [lista_in[0]]
            for num, file in enumerate(lista_in):
                for key in dict_empty.keys():
                    dict_filled[key] = time_now.strftime(dict_empty[key])
                downloader_settings["out_folder"] = data_settings["data"]["dynamic"]["outcome"]["v" + str(num)]["folder"].format(**dict_filled)
                downloader_settings["out_name"] = data_settings["data"]["dynamic"]["outcome"]["v" + str(num)]["file_name"].format(**dict_filled)
                download_file(num, file, lat, lon, downloader_settings)

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
# List file on web site
def list_files(url, ext=''):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
def download_file(num, file, lat, lon, downloader_settings):
    logging.info(" ---> Download map v" + str(num))
    urlretrieve(file, os.path.join(downloader_settings["ancillary_path"], "file_" + str(num) + ".nc"))
    temp = xr.open_dataset(os.path.join(downloader_settings["ancillary_path"], "file_" + str(num) + ".nc"), cache=False)
    data = xr.DataArray(np.flipud(temp["RRQPE"].values), dims=["y", "x"], coords={"y": lat, "x": lon})
    if downloader_settings["crop_map"] is True:
        data = data.loc[downloader_settings["bounding_box"]["lat_bottom"]:
                        downloader_settings["bounding_box"]["lat_top"],
                        downloader_settings["bounding_box"]["lon_left"]:
                        downloader_settings["bounding_box"]["lon_right"]]
    data.rio.write_crs(4326, inplace=True)
    #data.rio.write_nodata(0, inplace=True)
    if np.isnan(data.values).any():
        logging.error("ERROR! Map cointains null values! SKIP")
        logging.info(" ---> Download map v" + str(num) + "...FAILED!")
    else:
        os.makedirs(downloader_settings["out_folder"], exist_ok=True)
        data.rio.to_raster(os.path.join(downloader_settings["out_folder"], downloader_settings["out_name"]),
            compress="DEFLATE", dtype="float32")
        logging.info(" ---> Download map v" + str(num) + "...SUCCESFUL!")
    temp.close()
    data.close()
    os.remove(os.path.join(downloader_settings["ancillary_path"], "file_" + str(num) + ".nc"))
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
# Function for fill a dictionary of templates
def fill_template(downloader_settings,time_now):
    empty_template = downloader_settings["templates"]
    template_filled = {}
    for key in empty_template.keys():
        template_filled[key] = time_now.strftime(empty_template[key])
    template_filled["domain"] = downloader_settings["domain"]
    return template_filled
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