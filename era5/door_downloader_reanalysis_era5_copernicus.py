#!/usr/bin/python3

"""
door - Download ERA5 reanalysis from Copernicus

__date__ = '20211203'
__version__ = '1.0.0'
__author__ =
        'Andrea Libertino (andrea.libertino@cimafoundation.org',
__library__ = 'door'

General command line:
python3 hyde_downloader_satellite_gsmap_obs.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20211203 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import cdsapi
import pandas as pd
import xarray as xr
import numpy as np
import os, logging, json, time
from argparse import ArgumentParser
from multiprocessing import Pool, cpu_count
import datetime as dt

# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DOOR - ERA5 COPERNICUS'
alg_version = '1.0.0'
alg_release = '2021-12-03'
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

    # Setup cds api
    c = cdsapi.Client()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Get algorithm time range
    time_end = dt.datetime.strptime(alg_time,'%Y-%m-%d %H:%M')
    time_start = time_end - pd.Timedelta(str(data_settings["data"]["dynamic"]["time"]["time_observed_period"]) + data_settings["data"]["dynamic"]["time"]["time_observed_frequency"])
    time_range = pd.date_range(time_start, time_end)

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    logging.info("--> Create folders...")
    ancillary_path = data_settings["data"]["dynamic"]["ancillary"]["folder"]
    outcome_path = data_settings["data"]["dynamic"]["outcome"]["folder"]

    os.makedirs(ancillary_path, exist_ok=True)
    os.makedirs(outcome_path, exist_ok=True)
    logging.info("--> Create fodlers...DONE")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Loop trhough the years
    logging.info("--> Loop trough the years...")
    years = np.unique(time_range.year)

    for year in years:
        logging.info("---> Compute year: " + str(year))

        months = np.unique(time_range[time_range.year==year].month)

        download_info = {}
        download_info['c'] = c
        download_info['year'] = year
        download_info['time_range'] = time_range
        download_info['area'] = [data_settings["data"]["static"]["bounding_box"]["lat_top"],
                                 data_settings["data"]["static"]["bounding_box"]["lon_left"],
                                 data_settings["data"]["static"]["bounding_box"]["lat_bottom"],
                                 data_settings["data"]["static"]["bounding_box"]["lon_right"],]
        download_info['ancillary_path'] = ancillary_path
        download_info['outcome_path'] = outcome_path

        if data_settings["algorithm"]["flags"]["downloading_mp"]:
            logging.info("---> Sending download request in parallel mode...")
            process_max = data_settings["algorithm"]["ancillary"]["process_mp"]
            if process_max is None or process_max > cpu_count() - 1:
                process_max = cpu_count() - 1
        else:
            process_max = 1

        exec_pool = Pool(process_max)
        for month in months:
            exec_pool.apply_async(cds_download, args=(month, download_info))
        exec_pool.close()
        exec_pool.join()

        logging.info("---> Compute year: " + str(year) + "...DONE")

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
# Function for downloading ERA5 from Climate Data Store
def cds_download(month, download_info):
    days = np.unique(download_info['time_range'][(download_info['time_range'].year == download_info['year']) &
                                                 (download_info['time_range'].month == month)].day)

    temp_file = os.path.join(download_info['ancillary_path'],'temp_' + str(download_info['year']) + str(month).zfill(2) + '.nc')
    out_file = os.path.join(download_info['outcome_path'],'era5_obs_' + str(download_info['year']) + str(month).zfill(2) + '.nc')

    download_info['c'].retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': [
                '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
                '2m_temperature', 'surface_solar_radiation_downwards', 'total_precipitation',
            ],
            'year': str(download_info['year']),
            'month': str(month).zfill(2),
            'day': [str(i).zfill(2) for i in days],
            'time': [
                '00:00', '01:00', '02:00',
                '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00',
                '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00',
                '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00',
                '21:00', '22:00', '23:00',
            ],
            'area': download_info['area'],
        },
        temp_file)

    df_forcing = xr.open_dataset(temp_file)

    df_forcing['wind'] = np.sqrt(df_forcing['v10'] ** 2 + df_forcing['u10'] ** 2)
    df_forcing.wind.attrs["units"] = 'm s**-1'

    df_forcing['temperature'] = df_forcing['t2m'] - 273.15
    df_forcing.wind.attrs["units"] = 'C'

    df_forcing['RH'] = relative_humidity_from_dewpoint(df_forcing['temperature'], df_forcing['d2m'] - 273.15)
    df_forcing.wind.attrs["units"] = '%'

    df_forcing['rain'] = df_forcing['tp'] * 1000
    df_forcing.wind.attrs["units"] = 'mm'

    df_forcing = df_forcing.rename({"ssrd": "downward_radiation"})
    df_forcing['downward_radiation'] = df_forcing['downward_radiation']/3600
    df_forcing.downward_radiation.attrs["units"] = 'W m**-2'

    df_forcing = df_forcing.drop_vars(['v10', 'u10', 't2m', 'd2m', 'tp'])

    df_forcing.to_netcdf(out_file)
    os.remove(temp_file)

    return
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