#!/usr/bin/python3

"""
Door Downloading Tool - SATELLITE MODIS

__date__ = '20230331'
__version__ = '1.0.2'
__author__ = 'Fabio Delogu (fabio.delogu@cimafoundation.org'
__library__ = 'door'

General command line:
python3 door_downloader_satellite_modis.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version:
20230331 (1.0.2) --> Door package refactor
20191007 (1.0.1) --> Hyde package refactor
20180906 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import logging
import os
import time

from lib_utils_io import read_file_settings
from lib_utils_system import make_folder
from lib_utils_time import set_time

from drv_downloader_data import DriverData

from argparse import ArgumentParser
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DOOR DOWNLOADING TOOL - SATELLITE MODIS'
alg_version = '1.0.2'
alg_release = '2023-03-31'
# Algorithm parameter(s)
time_format = '%Y-%m-%d %H:%M'
zip_ext = '.gz'
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Script Main
def main():

    # -------------------------------------------------------------------------------------
    # Get algorithm settings
    alg_settings, alg_time = get_args()

    # Set algorithm settings
    data_settings = read_file_settings(alg_settings)

    # Set algorithm logging
    make_folder(data_settings['log']['folder_name'])
    set_logging(logger_file=os.path.join(data_settings['log']['folder_name'],
                                         data_settings['log']['file_name']),
                logger_format=data_settings['log']['format'])
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
    # Organize time run
    time_run, time_range = set_time(time_run_args=alg_time, time_run_file=data_settings['time']['time_now'],
                                    time_format=time_format)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Iterate over time(S)
    for time_step in time_range:

        # -------------------------------------------------------------------------------------
        # Info time
        logging.info(' ---> TIME STEP: ' + str(time_step) + ' ... ')
        # -------------------------------------------------------------------------------------

        # -------------------------------------------------------------------------------------
        # Get datasets information
        driver_data = DriverData(time_step,
                                 machine_dict=data_settings['data']['machine'],
                                 src_dict=data_settings['data']['source'],
                                 ancillary_dict=data_settings['data']['ancillary'],
                                 dst_dict=data_settings['data']['destination'],
                                 time_dict=data_settings['time'],
                                 product_dict=data_settings['product'],
                                 info_dict=data_settings['info'],
                                 template_dict=data_settings['template'],
                                 library_dict=data_settings['library'],
                                 flag_updating_source=data_settings['flags']['update_dynamic_data_source'],
                                 flag_updating_ancillary=data_settings['flags']['update_dynamic_data_ancillary'],
                                 flag_updating_destination=data_settings['flags']['update_dynamic_data_destination'],
                                 )
        # Download datasets
        file_tile_collections = driver_data.download_data()
        # Mosaic datasets
        file_mosaic_collections = driver_data.mosaic_data(file_tile_collections)
        # Resample datasets
        driver_data.resample_data(file_mosaic_collections)
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

    alg_settings, alg_time = 'configuration.json', None
    if parser_values.alg_settings:
        alg_settings = parser_values.alg_settings
    if parser_values.alg_time:
        alg_time = parser_values.alg_time
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
if __name__ == "__main__":
    main()
# ----------------------------------------------------------------------------
