#!/usr/bin/python3

"""
door Tool - SATELLITE GSMAP GAUGE historical

__date__ = '20220319'
__version__ = '1.0.0'
__author__ =
        'Andrea Libertino (andrea.libertino@cimafoundation.org',
__library__ = 'door'

General command line:
python3 door_downloader_satellite_gsmap_gauge_historical.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20220319 (1.0.0) --> Beta release
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
import gzip
import shutil
from cdo import Cdo
import ftplib

# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DOOR - SATELLITE GSMAP GAUGE historical'
alg_version = '1.0.0'
alg_release = '2022-03-19'
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

    downloader_settings["cdo"] = data_settings["algorithm"]["ancillary"]["cdo_bin"]

    logging.info(" ----> ftp hokusai.eorc.jaxa.jp setup...")
    if not all([data_settings["algorithm"]["ancillary"]['gsmap_ftp_user'], \
                data_settings["algorithm"]["ancillary"]['gsmap_ftp_pass']]):
        netrc_handle = netrc.netrc()
        try:
            downloader_settings['gsmap_user'], _, downloader_settings['gsmap_pwd'] = \
                netrc_handle.authenticators("hokusai.eorc.jaxa.jp")
        except:
            logging.error(
                ' ----> Netrc authentication file or credentials not found in home directory! Generate it or provide user and password in the settings!')
            raise FileNotFoundError(
                'Verify that your .netrc file exists in the home directory and that it includes proper credentials for https://arthurhouhttps.pps.eosdis.nasa.gov')
    else:
        downloader_settings['gsmap_user'] = data_settings["algorithm"]["ancillary"]['gsmap_ftp_user']
        downloader_settings['gsmap_pwd'] = data_settings["algorithm"]["ancillary"]['gsmap_ftp_pass']
    logging.info(" ----> ftp hokusai.eorc.jaxa.jp setup... DONE")

    logging.info(" ---> Create ancillary data paths...")
    downloader_settings["ancillary_path"] = data_settings["data"]["dynamic"]["ancillary"]["folder"].format(domain=domain)
    os.makedirs(downloader_settings["ancillary_path"], exist_ok=True)
    downloader_settings["clean_dynamic_data_ancillary"] = data_settings["algorithm"]["flags"]["clean_dynamic_data_ancillary"]
    downloader_settings["ctl_template"] = data_settings["data"]["dynamic"]["ancillary"]["ctl_file_settings"]
    logging.info(" ---> Create ancillary data path...DONE")

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Get algorithm time range
    logging.info(" --> Setting algorithm time settings...")
    time_end = dt.datetime.strptime(alg_time, '%Y-%m-%d %H:%M')
    time_start = time_end - pd.Timedelta(str(data_settings["data"]["dynamic"]["time"]["time_observed_period"]) +
                                         data_settings["data"]["dynamic"]["time"]["time_observed_frequency"])
    if data_settings["algorithm"]["flags"]["download_full_days"]:
        data_settings["data"]["dynamic"]["time"]["time_observed_frequency"] = "1D"
    time_range = pd.date_range(time_start, time_end, freq=data_settings["data"]["dynamic"]["time"]["time_observed_frequency"])

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
        logging.warning(" WARNING! Minimum latitude limited to -60 as it is the maximum southern latitude of GSMAP")
    if y_max is None or y_min>60:
        y_max = 60
        logging.warning(" WARNING! Maximum latitude limited to +60 as it is the maximum northern latitude of GSMAP")

    downloader_settings["bbox"] = [x_min,x_max,y_min,y_max]
    downloader_settings["domain"] = domain
    downloader_settings["templates"]["domain"] = domain
    logging.info(" --> Setting algorithm spatial settings...DONE")

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Process the download of the data
    logging.info(' --> Search and download of gsmap gauge products...')
    global missing_steps
    missing_steps = manager.list()
    downloader_settings["outcome_path"] = os.path.join(
        data_settings["data"]["dynamic"]["outcome"]["folder"], \
        data_settings["data"]["dynamic"]["outcome"]["file_name"])
    exec_pool = Pool(process_max)
    if data_settings["algorithm"]["flags"]["download_full_days"]:
        for time_now in time_range:
            exec_pool.apply_async(dload_gsmap_gauge_full_days, args=(time_now, downloader_settings))
    else:
        for time_now in time_range:
            exec_pool.apply_async(dload_gsmap_gauge, args=(time_now, downloader_settings))
    exec_pool.close()
    exec_pool.join()
    logging.info(' --> Search and download of gsmap gauge products...DONE')

    if len(list(missing_steps))>0:
        logging.warning(' --> Some time steps are missing: ')
        for date in missing_steps:
            logging.warning(' ---> Time: ' + date.strftime("%Y-%m-%d %H:%M") + "..MISSING!")
    else:
        logging.info(' --> All data have been downloaded!')

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
# Function for download IMERG Early Run
def dload_gsmap_gauge(time_now, downloader_settings):
    ftp = ftplib.FTP("hokusai.eorc.jaxa.jp")
    ftp.login(downloader_settings['gsmap_user'], downloader_settings['gsmap_pwd'])
    logging.info(" ---> " + time_now.strftime("%Y-%m-%d %H:%M") + "... ")
    global missing_steps
    try:
        ftp.cwd("/realtime/hourly_G/" + time_now.strftime("%Y/%m/%d") + "/")
        remote_filename = 'gsmap_gauge.' + time_now.strftime("%Y%m%d") + '.' + time_now.strftime("%H%M") + '.dat.gz'
        ancillary_filename = os.path.join(downloader_settings["ancillary_path"], remote_filename)
        ftp.retrbinary("RETR " + remote_filename, open(ancillary_filename, 'wb').write)

        template_filled = fill_template(downloader_settings,time_now)
        local_filename_domain = downloader_settings["outcome_path"].format(**template_filled)
        os.makedirs(os.path.dirname(local_filename_domain), exist_ok = True)

        filename_ctl = ancillary_filename.replace(".dat.gz", ".ctl")
        ancillary_filename_out = ancillary_filename.replace(".dat.gz", ".dat")
        with gzip.open(ancillary_filename, 'rb') as f_in:
            with open(ancillary_filename_out, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Compile ctl file
        hour_step = time_now.strftime('%H:00')
        day_step = time_now.strftime('%-d')
        year_step = time_now.strftime('%Y')
        month_step = time_now.strftime('%b').lower()

        tdef_ctl_step = hour_step + 'Z' + day_step + month_step + year_step
        dset_ctl_step = ancillary_filename_out
        tags_ctl_step = {'dset': dset_ctl_step, 'tdef': tdef_ctl_step}

        ctl_template_step = {}
        for template_ctl_key, template_ctl_content_raw in downloader_settings["ctl_template"].items():
            template_ctl_content_fill = template_ctl_content_raw.format(**tags_ctl_step)
            ctl_template_step[template_ctl_key] = template_ctl_content_fill

        with open(filename_ctl, "w") as ctl_handle:
            for line_key, line_content in ctl_template_step.items():
                ctl_handle.write(line_content)
                ctl_handle.write("\n")
            ctl_handle.close()
        logging.info(" ---> Write ctl file...DONE")

        # Set cdo
        os.environ['PATH'] = os.environ['PATH'] + ':' + downloader_settings["cdo"]
        cdo = Cdo()
        bbox_cdo = ','.join(str(i) for i in downloader_settings["bbox"])
        cdo.import_binary(input=filename_ctl, output=ancillary_filename_out + ".nc", options='-f nc')
        cdo.sellonlatbox(bbox_cdo, input=ancillary_filename_out + ".nc", output=local_filename_domain)

        if downloader_settings["clean_dynamic_data_ancillary"]:
            for ancillary_files in [ancillary_filename, ancillary_filename_out]:
                try:
                    os.remove(ancillary_files)
                except:
                    continue
        logging.info(" ---> " + time_now.strftime("%Y-%m-%d %H:%M") + "... File downloaded!")

    except ftplib.error_perm as reason:
        if str(reason)[:3] == '550':
            logging.warning(" WARNING! " + time_now.strftime("%Y-%m-%d %H:%M") + "... File not found! SKIP")
            missing_steps.append(time_now)
    ftp.quit()

# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Function for download IMERG Early Run
def dload_gsmap_gauge_full_days(time_now, downloader_settings):
    ftp = ftplib.FTP("hokusai.eorc.jaxa.jp")
    ftp.login(downloader_settings['gsmap_user'], downloader_settings['gsmap_pwd'])
    logging.info(" ---> Download day " + time_now.strftime("%Y-%m-%d") + "... ")
    global missing_steps
    try:
        ftp.cwd("/realtime/hourly_G/" + time_now.strftime("%Y/%m/%d") + "/")
        filenames = ftp.nlst()
        for remote_filename in filenames:
            logging.info(" ---> Download file " + remote_filename + "... ")
            ancillary_filename = os.path.join(downloader_settings["ancillary_path"], remote_filename)
            with open(ancillary_filename, 'wb') as file:
                ftp.retrbinary('RETR ' + remote_filename, file.write)

            template_filled = fill_template(downloader_settings,time_now)
            local_filename_domain = downloader_settings["outcome_path"].format(**template_filled)
            os.makedirs(os.path.dirname(local_filename_domain), exist_ok = True)

            filename_ctl = ancillary_filename.replace(".dat.gz", ".ctl")
            ancillary_filename_out = ancillary_filename.replace(".dat.gz", ".dat")
            with gzip.open(ancillary_filename, 'rb') as f_in:
                with open(ancillary_filename_out, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Compile ctl file
            logging.info(" ----> Write ctl file...")
            hour_step = time_now.strftime('%H:00')
            day_step = time_now.strftime('%-d')
            year_step = time_now.strftime('%Y')
            month_step = time_now.strftime('%b').lower()

            tdef_ctl_step = hour_step + 'Z' + day_step + month_step + year_step
            dset_ctl_step = ancillary_filename_out
            tags_ctl_step = {'dset': dset_ctl_step, 'tdef': tdef_ctl_step}

            ctl_template_step = {}
            for template_ctl_key, template_ctl_content_raw in downloader_settings["ctl_template"].items():
                template_ctl_content_fill = template_ctl_content_raw.format(**tags_ctl_step)
                ctl_template_step[template_ctl_key] = template_ctl_content_fill

            with open(filename_ctl, "w") as ctl_handle:
                for line_key, line_content in ctl_template_step.items():
                    ctl_handle.write(line_content)
                    ctl_handle.write("\n")
                ctl_handle.close()

            # Set cdo
            logging.info(" ----> Crop and save...")
            os.environ['PATH'] = os.environ['PATH'] + ':' + downloader_settings["cdo"]
            cdo = Cdo()
            bbox_cdo = ','.join(str(i) for i in downloader_settings["bbox"])
            cdo.import_binary(input=filename_ctl, output=ancillary_filename_out + ".nc", options='-f nc')
            cdo.sellonlatbox(bbox_cdo, input=ancillary_filename_out + ".nc", output=local_filename_domain)

            if downloader_settings["clean_dynamic_data_ancillary"]:
                for ancillary_files in [ancillary_filename, ancillary_filename_out]:
                    try:
                        os.remove(ancillary_files)
                    except:
                        continue
        logging.info(" ---> All files for day " + time_now.strftime("%Y-%m-%d") + " downloaded!")

    except ftplib.error_perm as reason:
        if str(reason)[:3] == '550':
            logging.warning(" WARNING! " + time_now.strftime("%Y-%m-%d") + "... Folder not found! SKIP")
            missing_steps.append(time_now)
    ftp.quit()

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