#!/usr/bin/python3
"""
DOOR - SATELLITE H SAF h14, h141(, and h142)

__date__ = '20230504'
__version__ = '2.0.0'
__author__ =
        'Francesco Avanzi (francesco.avanzi@cimafoundation.org',
        'Fabio Delogu (fabio.delogu@cimafoundation.org',
        'Andrea Libertino (andrea.libertino@cimafoundation.org',

__library__ = 'DOOR'

General command line:
python3 door_downloader_hsaf_h14_h141.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20230420 (2.0.0) --> Moved to DOOR package, updated to also handle h14 -- including conversion from grib to nc via cdo
20210929 (1.0.0) --> First release
"""

# -------------------------------------------------------------------------------------
# Complete library
import logging
import time
import os
import netrc

import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pylab as plt

from argparse import ArgumentParser
from urllib.parse import urlparse
from ftpretty import ftpretty as ftp

from lib_door_downloader_hsaf_h14_h141_os import make_folder
from lib_door_downloader_hsaf_h14_h141_json import read_file_json
from lib_door_downloader_hsaf_h14_h141_time import set_run_time
from lib_door_downloader_hsaf_h14_h141_geo import read_file_raster
from lib_door_downloader_hsaf_h14_h141_generic import fill_tags2string
from lib_door_downloader_hsaf_h14_h141_nc import read_data_nc, h14cdoconverter
from lib_door_downloader_hsaf_h14_h141_add_variable import compute_weighted_mean
from lib_door_downloader_hsaf_h14_h141_io import write_file_tiff

logging.getLogger("rasterio").setLevel(logging.WARNING)
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DOOR - H SAF SOIL MOISTURE H14 H141'
alg_version = '2.0.0'
alg_release = '2023-05-04'
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
    make_folder(data_settings['log']['folder_name'])
    set_logging(logger_file=os.path.join(data_settings['log']['folder_name'],
                                         data_settings['log']['file_name']))
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
    # Get algorithm time range information
    time_run, time_run_range = set_run_time(alg_time, data_settings['time'])

    # Get algorithm geographical information
    geo_settings = data_settings['data']['static']
    geo_target_da, geo_target_wide, geo_target_high, geo_target_proj, \
        geo_target_transform, geo_target_bounding_box, geo_target_no_data, \
        geo_target_dim_name_x, geo_target_dim_name_y= \
        read_file_raster(os.path.join(geo_settings['folder_name'], geo_settings['file_name']))
    # Starting info
    logging.info(' --> TIME RUN: ' + str(time_run))

    # Get credentials
    info = netrc.netrc()
    urs_url = data_settings['algorithm']['ancillary']['url']
    if not urs_url.startswith('http'):
        urs_url_w_http = 'http://' + urs_url
    else:
        urs_url_w_http = urs_url
    username, account, password = info.authenticators(urlparse(urs_url_w_http).hostname)

    # Connect to server
    host = ftp(urs_url, username, password)
    logging.info(' ---> Connected to: ' + str(urs_url) + ' ... ')

    # Iterate over time steps
    for time_run_step in time_run_range:

        # time info
        time_index_step = pd.DatetimeIndex([time_run_step])

        # Starting info
        logging.info(' ---> TIME STEP: ' + str(time_run_step) + ' ... ')

        # Prepare target folder: source global
        path_source_global = data_settings['data']['dynamic']['source']['folder_name']
        path_source_global = \
            fill_tags2string(path_source_global, data_settings['algorithm']['template'],
                             {'source_sub_path_time': time_run_step})
        make_folder(path_source_global)
        logging.info(' ---> Created path: ' + path_source_global + ' ... ')

        # Prepare target folder: final outcome
        path_outcome = data_settings['data']['outcome']['folder_name']
        path_outcome = \
            fill_tags2string(path_outcome, data_settings['algorithm']['template'],
                             {'outcome_sub_path_time': time_run_step})
        make_folder(path_outcome)
        logging.info(' ---> Created path: ' + path_outcome + ' ... ')

        # Build server & local path for ftpretty
        path_server = os.path.join(data_settings['algorithm']['ancillary']['server_folder_name'],
                                   data_settings['algorithm']['ancillary']['server_file_name'])
        path_server = fill_tags2string(path_server, data_settings['algorithm']['template'],
                                       {'source_sub_path_time': time_run_step, 'source_datetime': time_run_step})

        path_source_global_w_filename = os.path.join(path_source_global,
                                                        data_settings['algorithm']['ancillary']['server_file_name'])
        path_source_global_w_filename = fill_tags2string(path_source_global_w_filename,
                                                            data_settings['algorithm']['template'],
                                                            {'source_datetime': time_run_step})
        # Download raw global data to folder
        try:
            # download
            host.get(path_server, path_source_global_w_filename)
            logging.info(' ---> Succesfully downloaded: ' + path_server + ' as ' + path_source_global_w_filename + ' ... ')
            downloaded = True

        except Exception as e:
            logging.warning(str(e))
            logging.warning(' ===> Data for timestep ' + str(time_run_step) + ' not found on server.')
            logging.warning(' ===> Data for timestep ' + str(time_run_step) + ' not processed.')
            downloaded = False

        if downloaded:

            try:

                if data_settings['data']['dynamic']['source']['decompress_bz']:
                    #decompress bz2 format
                    path_source_global_w_filename_unzipped = path_source_global_w_filename[0:-4]
                    unzipcmd = 'bzip2 -dc ' + path_source_global_w_filename + ' > ' + path_source_global_w_filename_unzipped
                    os.system(unzipcmd)
                else:
                    path_source_global_w_filename_unzipped = path_source_global_w_filename

                if data_settings['data']['dynamic']['source']['grib_info']['grib_conversion']:
                    #convert grib to nc
                    path_grib_to_nc = os.path.join(path_source_global, data_settings['data']['dynamic']['source']['grib_info']['filename_grib_to_nc'])
                    path_grib_to_nc = fill_tags2string(path_grib_to_nc, data_settings['algorithm']['template'],
                                                   {'source_datetime': time_run_step})

                    h14cdoconverter(path_source_global_w_filename_unzipped, path_source_global, path_grib_to_nc,
                                    data_settings['algorithm']['general']['path_cdo'],
                                    data_settings['data']['dynamic']['source']['variables'],
                                    dim_name_time=data_settings['data']['dynamic']['source']['var_coords']['time'],
                                    dim_name_geo_x=data_settings['data']['dynamic']['source']['var_coords']['x'],
                                    dim_name_geo_y=data_settings['data']['dynamic']['source']['var_coords']['y'],
                                    rows=data_settings['data']['dynamic']['source']['grid_remapping_info']['rows'],
                                    columns=data_settings['data']['dynamic']['source']['grid_remapping_info']['columns'])
                    path_source_global_w_filename = path_grib_to_nc

                # open nc file and load data
                var_dset = None
                for var_name in data_settings['data']['dynamic']['source']['variables']:
                    var_name_da = read_data_nc(
                        path_source_global_w_filename,
                        var_coords=data_settings['data']['dynamic']['source']['var_coords'],
                        var_name=var_name,
                        var_time=time_index_step,
                        grid_remapping_info=data_settings['data']['dynamic']['source']['grid_remapping_info'],
                        dim_name_time=data_settings['data']['dynamic']['source']['var_coords']['time'],
                        dim_name_geo_x=data_settings['data']['dynamic']['source']['var_coords']['x'],
                        dim_name_geo_y=data_settings['data']['dynamic']['source']['var_coords']['y'],
                        coord_name_time=data_settings['data']['dynamic']['source']['var_coords']['time'],
                        coord_name_geo_x=data_settings['data']['dynamic']['source']['var_coords']['x'],
                        coord_name_geo_y=data_settings['data']['dynamic']['source']['var_coords']['y'])

                    if var_dset is None:
                        var_dset = xr.Dataset(coords={"time": (["time"], time_index_step)})

                    var_dset[var_name] = var_name_da
                    logging.info(
                        ' ---> Succesfully loaded: ' + var_name + ' from ' + path_source_global_w_filename + ' ... ')

                # compute optional variable if activated
                if data_settings['data']['additional_variable']['var_mode']:
                    var_dset = compute_weighted_mean(var_dset, data_settings['data']['additional_variable'])
                    logging.info(
                        ' ---> Created additional layer: ' + data_settings['data']['additional_variable']['var_name'] + ' ... ')

                # resample over target grid
                coordinates = {data_settings['data']['dynamic']['source']['var_coords']['y']: geo_target_da["Latitude"].values,
                               data_settings['data']['dynamic']['source']['var_coords']['x']: geo_target_da["Longitude"].values}
                var_dset_clipped = var_dset.interp(coordinates, method='nearest')
                logging.info(' ---> Dataset resampled over domain ... ')

                # Export layers as geotiff
                for var_name in data_settings['data']['dynamic']['source']['variables']:
                    file_name = os.path.join(path_outcome, data_settings['data']['outcome']['file_name'])
                    file_name = \
                        fill_tags2string(file_name, data_settings['algorithm']['template'],
                                         {'domain': data_settings['algorithm']['ancillary']['domain'],
                                          'outcome_datetime': time_run_step,'layer': var_name})
                    write_file_tiff(file_name, var_dset_clipped[var_name].values,
                                    geo_target_wide, geo_target_high, geo_target_transform, geo_target_proj)
                    logging.info(' ---> Written ' + file_name + ' ... ')

                if data_settings['data']['additional_variable']['var_mode']:
                    file_name = os.path.join(path_outcome, data_settings['data']['outcome']['file_name'])
                    file_name = \
                        fill_tags2string(file_name, data_settings['algorithm']['template'],
                                         {'domain': data_settings['algorithm']['ancillary']['domain'],
                                          'outcome_datetime': time_run_step,
                                          'layer': data_settings['data']['additional_variable']['var_name']})
                    write_file_tiff(file_name, var_dset_clipped[data_settings['data']['additional_variable']['var_name']].values,
                                    geo_target_wide, geo_target_high, geo_target_transform, geo_target_proj)
                    logging.info(' ---> Written ' + file_name + ' ... ')

                # Delete source file
                if data_settings['algorithm']['flags']['cleaning_dynamic_data_source']:
                    os.remove(path_source_global_w_filename)
                    if data_settings['data']['dynamic']['source']['decompress_bz']:
                        os.remove(path_source_global_w_filename_unzipped)
                    logging.info(' ---> Deleted ' + path_source_global_w_filename + ' ... ')

            except Exception as e:
                logging.warning(str(e))
                logging.warning(' ===> Data for timestep ' + str(time_run_step) + ' not processed.')
                logging.warning(' ===> Problems with data for timestep ' + str(time_run_step) + '!')

    # close connection
    host.close()
    logging.info(' ---> CLOSED connection to ' + str(urs_url) + ' ... ')
    logging.info(' ---> TIME STEP: ' + str(time_run_step) + ' ... DONE')

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
    logging.root.setLevel(logging.DEBUG)

    # Open logging basic configuration
    logging.basicConfig(level=logging.DEBUG, format=logger_format, filename=logger_file, filemode='w')

    # Set logger handle
    logger_handle_1 = logging.FileHandler(logger_file, 'w')
    logger_handle_2 = logging.StreamHandler()
    # Set logger level
    logger_handle_1.setLevel(logging.DEBUG)
    logger_handle_2.setLevel(logging.DEBUG)
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
if __name__ == '__main__':
    main()
# ----------------------------------------------------------------------------
