#!/usr/bin/python3

"""
door Tool - SATELLITE IMERG

__date__ = '20211227'
__version__ = '1.0.0'
__author__ =
        'Andrea Libertino (andrea.libertino@cimafoundation.org',
__library__ = 'door'

General command line:
python3 hyde_downloader_satellite_gsmap_nowcasting.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20211227 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import pandas as pd
import os, json, logging, time
import netrc
from argparse import ArgumentParser
import requests
from multiprocessing import Pool, cpu_count, Manager
from osgeo import gdal
import datetime as dt

# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DOOR - SATELLITE IMERG'
alg_version = '1.0.0'
alg_release = '2021-12-27'
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
    domain = data_settings["algorithm"]["ancillary"]["domain"]

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
    # Setup downloader
    logging.info(" --> Setup downloader settings")

    logging.info(" ---> Multiprocessing setup...")
    if data_settings["algorithm"]["flags"]["downloading_mp"]:
        logging.info(" ----> Activate parallel mode...")
        process_max = data_settings["algorithm"]["ancillary"]["process_mp"]
        if process_max is None or process_max > cpu_count() - 1:
            process_max = cpu_count() - 1
        logging.info(" ----> Maximum parallel processes set to " + str(process_max))
    else:
        logging.info(" ----> Activate serial mode...")
        process_max = 1
    manager = Manager()
    logging.info(" ---> Multiprocessing setup...DONE")

    logging.info(" ---> Setup servers connection...")
    downloader_settings = {}
    if data_settings["algorithm"]["flags"]["download_final_imerg"]:
        logging.info(" ----> Server https://arthurhouhttps.pps.eosdis.nasa.gov/ (final runs)...")
        if not all([data_settings["algorithm"]["ancillary"]['gpm_arthurhouhttps_user'], \
                    data_settings["algorithm"]["ancillary"]['gpm_arthurhouhttps_pass']]):
            netrc_handle = netrc.netrc()
            try:
                downloader_settings['final_user'], _, downloader_settings['final_pwd'] = \
                    netrc_handle.authenticators("https://arthurhouhttps.pps.eosdis.nasa.gov")
            except:
                logging.error(
                    ' ----> Netrc authentication file or credentials not found in home directory! Generate it or provide user and password in the settings!')
                raise FileNotFoundError(
                    'Verify that your .netrc file exists in the home directory and that it includes proper credentials for https://arthurhouhttps.pps.eosdis.nasa.gov')
        else:
            downloader_settings['final_user'] = data_settings["algorithm"]["ancillary"]['gpm_arthurhouhttps_user']
            downloader_settings['final_pwd'] = data_settings["algorithm"]["ancillary"]['gpm_arthurhouhttps_pass']
        logging.info(" ----> Server https://arthurhouhttps.pps.eosdis.nasa.gov/ (final runs) setup... DONE")

    if data_settings["algorithm"]["flags"]["download_late_imerg"] or data_settings["algorithm"]["flags"]["download_early_imerg"]:
        logging.info(" ----> Server https://jsimpsonhttps.pps.eosdis.nasa.gov (final and early runs)...")
        if not all([data_settings["algorithm"]["ancillary"]['gpm_jsimpsonhttps_user'], \
                    data_settings["algorithm"]["ancillary"]['gpm_jsimpsonhttps_user']]):
            netrc_handle = netrc.netrc()
            try:
                downloader_settings['early_late_user'], _, downloader_settings['early_late_pwd'] = \
                    netrc_handle.authenticators("https://jsimpsonhttps.pps.eosdis.nasa.gov")
            except:
                logging.error(
                    ' ----> Netrc authentication file or credentials not found in home directory! Generate it or provide user and password in the settings!')
                raise FileNotFoundError(
                    'Verify that your .netrc file exists in the home directory and that it includes proper credentials for https://jsimpsonhttps.pps.eosdis.nasa.gov')
        else:
            downloader_settings['early_late_user'] = data_settings["algorithm"]["ancillary"]['gpm_jsimpsonhttps_user']
            downloader_settings['early_late_pwd'] = data_settings["algorithm"]["ancillary"]['gpm_jsimpsonhttps_pass']
        logging.info(" ----> Server https://jsimpsonhttps.pps.eosdis.nasa.gov (final and early runs) setup... DONE")
    logging.info(" ---> Setup servers connection...DONE")

    logging.info(" ---> Create ancillary data path...")
    downloader_settings["ancillary_path"] = data_settings["data"]["dynamic"]["ancillary"]["folder"].format(domain=domain)
    os.makedirs(downloader_settings["ancillary_path"], exist_ok=True)
    downloader_settings["clean_dynamic_data_ancillary"] = data_settings["algorithm"]["flags"]["clean_dynamic_data_ancillary"]
    logging.info(" ---> Create ancillary data path...DONE")

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Get algorithm time range
    logging.info(" --> Setting algorithm time settings...")
    time_end = dt.datetime.strptime(alg_time, '%Y-%m-%d %H:%M')
    time_start = time_end - pd.Timedelta(str(data_settings["data"]["dynamic"]["time"]["time_observed_period"]) +
                                         data_settings["data"]["dynamic"]["time"]["time_observed_frequency"])
    time_range = pd.date_range(time_start, time_end, freq=data_settings["data"]["dynamic"]["time"]["product_frequency"])

    downloader_settings["templates"] = data_settings["algorithm"]["template"]
    logging.info(" --> Setting algorithm time settings...DONE")

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Get spatial information
    logging.info(" --> Setting algorithm spatial settings...")

    x_min = data_settings["data"]["static"]["bounding_box"]["lon_left"]
    x_max = data_settings["data"]["static"]["bounding_box"]["lon_right"]
    y_min = data_settings["data"]["static"]["bounding_box"]["lat_bottom"]
    y_max = data_settings["data"]["static"]["bounding_box"]["lat_top"]

    if x_max is None:
        x_max = 180
    if x_min is None:
        x_min = -180
    if y_min is None or y_min<-60:
        y_min = -60
        logging.warning(" WARNING! Minimum latitude limited to -60 as it is the maximum southern latitude of IMERG")
    if y_max is None or y_min>60:
        y_max = 60
        logging.warning(" WARNING! Maximum latitude limited to +60 as it is the maximum northern latitude of IMERG")

    downloader_settings["bbox"] = [x_min,y_max,x_max,y_min]
    downloader_settings["domain"] = domain
    downloader_settings["templates"]["domain"] = domain
    logging.info(" --> Setting algorithm spatial settings...DONE")

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Process the download of the data
    if data_settings["algorithm"]["flags"]["download_final_imerg"]:
        logging.info(' --> Search and download of final imerg products...')
        global missing_steps_final
        missing_steps_final = manager.list()
        downloader_settings["outcome_path"] = os.path.join(
            data_settings["data"]["dynamic"]["outcome"]["final"]["folder"], \
            data_settings["data"]["dynamic"]["outcome"]["final"]["file_name"])
        exec_pool = Pool(process_max)
        for time_now in time_range:
            exec_pool.apply_async(dload_final_run, args=(time_now, downloader_settings))
        exec_pool.close()
        exec_pool.join()
        logging.info(' --> Search and download of final imerg products...DONE')
    else:
        missing_steps_final = time_range

    if data_settings["algorithm"]["flags"]["download_late_imerg"]:
        logging.info(' --> Search and download of late imerg products...')
        global missing_steps_late
        missing_steps_late = manager.list()
        downloader_settings["outcome_path"] = os.path.join(
            data_settings["data"]["dynamic"]["outcome"]["late"]["folder"], \
            data_settings["data"]["dynamic"]["outcome"]["late"]["file_name"])
        time_now=time_start
        dload_late_run(time_now, downloader_settings)
        exec_pool = Pool(process_max)
        for time_now in time_range:
            exec_pool.apply_async(dload_late_run, args=(time_now, downloader_settings))
        exec_pool.close()
        exec_pool.join()
        logging.info(' --> Search and download of late imerg products...DONE')
    else:
        missing_steps_late = missing_steps_final

    if data_settings["algorithm"]["flags"]["download_early_imerg"]:
        logging.info(' --> Search and download of early imerg products...')
        global missing_steps
        missing_steps = manager.list()
        downloader_settings["outcome_path"] = os.path.join(
            data_settings["data"]["dynamic"]["outcome"]["early"]["folder"], \
            data_settings["data"]["dynamic"]["outcome"]["early"]["file_name"])
        exec_pool = Pool(process_max)
        for time_now in missing_steps_late:
            exec_pool.apply_async(dload_early_run, args=(time_now, downloader_settings))
        exec_pool.close()
        exec_pool.join()
        logging.info(' --> Search and download of early imerg products...DONE')
    else:
        missing_steps = missing_steps_late

    if len(list(missing_steps))>0:
        logging.warning(' --> Some time steps are missing: ')
        for date in missing_steps:
            logging.warning(' ---> Time: ' + date.strftime("%Y-%m-%d %H:%M") + "..MISSING!")
    else:
        logging.info(' --> All data have been downloaded!')

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
# Function for download IMERG Early Run
def dload_early_run(time_now, downloader_settings):
    global missing_steps
    url = 'https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/early/' + time_now.strftime("%Y/%m") + '/' + \
          '3B-HHR-E.MS.MRG.3IMERG.' + time_now.strftime("%Y%m%d") + \
          '-S' + time_now.strftime("%H%M%S") + \
          '-E' + (time_now + pd.Timedelta("+ 29 min + 59 sec")).strftime("%H%M%S") + \
          '.' + str(int((time_now - time_now.replace(hour=0, minute=0)).total_seconds() / 60.0)).zfill(
        4) + '.V06B.30min.tif'
    ancillary_filename = os.path.join(downloader_settings["ancillary_path"], url.split('/')[-1])
    with requests.get(url, auth=(downloader_settings["early_late_user"], downloader_settings["early_late_pwd"])) as r:
        if r.status_code == 404:
            logging.warning(" WARNING! " + time_now.strftime("%Y-%m-%d %H:%M") + "... File not found! SKIP")
            missing_steps.append(time_now)
        else:
            with open(ancillary_filename, 'wb') as f:
                f.write(r.content)
            template_filled = fill_template(downloader_settings,time_now)
            local_filename_domain = downloader_settings["outcome_path"].format(**template_filled)
            os.makedirs(os.path.dirname(local_filename_domain), exist_ok = True)
            gdal.Translate(local_filename_domain, ancillary_filename, projWin = downloader_settings["bbox"], scaleParams=[[0.0,100.0,0.0,10.0]], outputType=gdal.GDT_Float32, noData=29999)
            if downloader_settings["clean_dynamic_data_ancillary"]:
                os.system('rm ' + ancillary_filename)
            logging.info(" ---> " + time_now.strftime("%Y-%m-%d %H:%M") + "... File downloaded!")

# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Function for download IMERG Late Run
def dload_late_run(time_now,downloader_settings):
    global missing_steps_late
    url = 'https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/' + time_now.strftime("%Y/%m") + '/' + \
          '3B-HHR-L.MS.MRG.3IMERG.' + time_now.strftime("%Y%m%d") + \
          '-S' + time_now.strftime("%H%M%S") + \
          '-E' + (time_now + pd.Timedelta("+ 29 min + 59 sec")).strftime("%H%M%S") + \
          '.' + str(int((time_now - time_now.replace(hour=0, minute=0)).total_seconds() / 60.0)).zfill(
        4) + '.V06B.30min.tif'
    ancillary_filename = os.path.join(downloader_settings["ancillary_path"], url.split('/')[-1])
    with requests.get(url, auth=(downloader_settings["early_late_user"], downloader_settings["early_late_pwd"])) as r:
        if r.status_code == 404:
            logging.warning(" WARNING! " + time_now.strftime("%Y-%m-%d %H:%M") + "... File not found! SKIP")
            missing_steps_late.append(time_now)
        else:
            with open(ancillary_filename, 'wb') as f:
                f.write(r.content)
            template_filled = fill_template(downloader_settings, time_now)
            local_filename_domain = downloader_settings["outcome_path"].format(**template_filled)
            os.makedirs(os.path.dirname(local_filename_domain), exist_ok=True)
            gdal.Translate(local_filename_domain, ancillary_filename, projWin = downloader_settings["bbox"], scaleParams=[[0.0,100.0,0.0,10.0]], outputType=gdal.GDT_Float32, noData=29999)
            if downloader_settings["clean_dynamic_data_ancillary"]:
                os.system('rm ' + ancillary_filename)
            logging.info(" ---> " + time_now.strftime("%Y-%m-%d %H:%M") + "... File downloaded!")

# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Function for download IMERG Final Run
def dload_final_run(time_now, downloader_settings):
    global missing_steps_final
    url = 'https://arthurhouhttps.pps.eosdis.nasa.gov/gpmdata/' + time_now.strftime("%Y/%m/%d") + '/gis/' + \
          '3B-HHR-GIS.MS.MRG.3IMERG.' + time_now.strftime("%Y%m%d") + \
          '-S' + time_now.strftime("%H%M%S") + \
          '-E' + (time_now + pd.Timedelta("+ 29 min + 59 sec")).strftime("%H%M%S") + \
          '.' + str(int((time_now - time_now.replace(hour=0, minute=0)).total_seconds() / 60.0)).zfill(4) + '.V06B.tif'
    ancillary_filename = os.path.join(downloader_settings["ancillary_path"], url.split('/')[-1])
    with requests.get(url, auth=(downloader_settings["final_user"], downloader_settings["final_pwd"])) as r:
        if r.status_code == 404:
            logging.warning(" WARNING! " + time_now.strftime("%Y-%m-%d %H:%M") + "... File not found! SKIP")
            missing_steps_final.append(time_now)
        else:
            with open(ancillary_filename, 'wb') as f:
                f.write(r.content)
            template_filled = fill_template(downloader_settings, time_now)
            local_filename_domain = downloader_settings["outcome_path"].format(**template_filled)
            os.makedirs(os.path.dirname(local_filename_domain), exist_ok=True)
            gdal.Translate(local_filename_domain, ancillary_filename, projWin = downloader_settings["bbox"], scaleParams=[[0.0,100.0,0.0,10.0]], outputType=gdal.GDT_Float32, noData=29999)
            if downloader_settings["clean_dynamic_data_ancillary"]:
                os.system('rm ' + ancillary_filename)
            logging.info(" ---> " + time_now.strftime("%Y-%m-%d %H:%M") + "... File downloaded!")

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