#!/usr/bin/python3
"""
door - NWP CMC-GDPS

__date__ = '20231228'
__version__ = '2.0.0'
__author__ =
        'Andrea Libertino andrea.libertino@cimafoundation.org',
__library__ = 'DOOR'

General command line:
python3 door_downloader_nwp_icon_global.py -settings_file configuration.json -time YYYY-MM-DD HH:MM

Version(s):
20230626 (1.0.0) --> Beta release
20230920 (1.0.1) --> Drop useless dimensions for Continuum compatibility
20231023 (1.0.2) --> Add radiation computing
20231219 (2.0.0) --> Refactoring
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

from copy import deepcopy
from math import floor, ceil
import requests
import time
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
import glob

from ...data_sources.cmc import gdps_downloader
from ...lib.basic import read_file_json, get_args
from ...lib.log import set_logging
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DOOR - NWP CMC-GDPS Global'
alg_version = '1.0.2'
alg_release = '2023-10-23'
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
    time_run = dt.datetime.strptime(alg_time, '%Y-%m-%d %H:%M')

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
    gdps_downloader
    # Define the input variables
    icon_forecast = RemoteVariable(
        data_source = dwd_opendata.ICONDownloader(),
        variables = data_settings['data']['dynamic']["variables"],
        ancillary = os.path.join(data_settings['data']['dynamic']["ancillary"]["folder"], data_settings['data']['dynamic']["ancillary"]["filename"]),
        destination = os.path.join(data_settings['data']['dynamic']["outcome"]["folder"], data_settings['data']['dynamic']["outcome"]["filename"]),
        type = 'forecast',
        src_format = 'grib2',
        dst_format = 'netcdf'
    )