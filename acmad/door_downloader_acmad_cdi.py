#!/usr/bin/python3

"""
door Tool - ACFRICA CDI ACMAD

__date__ = '20220705'
__version__ = '1.0.2'
__author__ =
        'Andrea Libertino (andrea.libertino@cimafoundation.org',
        'Alessandro Masoero (alessandro.masoero@cimafoundation.org'
__library__ = 'door'

General command line:
python3 door_downloader_satellite_chirps.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20230307 (1.0.2) --> Fix download of data when effectively available
20230214 (1.0.1) --> Fix download of data without FAPAR
20220705 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import requests
import datetime
import netrc
import os, json, logging, time

from argparse import ArgumentParser
from requests.auth import HTTPBasicAuth

# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DOOR - SATELLITE CHIRPS'
alg_version = '1.0.0'
alg_release = '2022-07-05'
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
    data_settings["algorithm"]["template"]["domain"] = domain

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
    product = 'cdi'

    # Check server availability
    base_url = "https://ada.acmad.org/"
    logging.info(" --> Checking " + base_url + " availability")
    try:
        available_datasets = requests.get(f"{base_url}data_api/rs_data/get_available_datasets/").json()[product]
        logging.info(" --> Server seems ok and responding")
    except ConnectionError:
        logging.error(" --> ERROR! Server not available")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Set reference time
    # Real-time mode
    if alg_time is None:
        logging.info(" --> Reference time not provided, check last vailable map")
        date_ref = datetime.datetime(available_datasets["latest_year"], available_datasets["latest_month"],
                                     available_datasets["latest_dekad"])
    # Historical mode
    else:
        date_run = datetime.datetime.strptime(alg_time, "%Y-%m-%d %H:%M")
        logging.info(" --> Historical mode active, delayed time is " + alg_time)
        if date_run.day < 11:
            day_ref = 1
        elif date_run.day < 21:
            day_ref = 11
        else:
            day_ref = 21
        date_ref = datetime.datetime(date_run.year, date_run.month, day_ref)

    if date_ref.day == 1:
        dekad = "1st"
    elif date_ref.day == 11:
        dekad = "2nd"
    elif date_ref.day == 21:
        dekad = "3rd"
    else:
        raise NotImplementedError("Only 3 maps per month are supported")

    logging.info(" --> Reference time is " + date_ref.strftime("%Y-%m-%d") + " - " + dekad + " dekad")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Setup output folder
    template_filled = fill_template(data_settings["algorithm"]["template"], date_ref)
    output_folder = data_settings["data"]["dynamic"]["outcome"]["folder"].format(**template_filled)
    os.makedirs(output_folder, exist_ok=True)
    output_filename = os.path.join(output_folder, data_settings["data"]["dynamic"]["outcome"]["filename"].format(**template_filled))

    # Check if file already exists
    if os.path.exists(output_filename):
        if data_settings["algorithm"]["flags"]["overwrite_output"]:
            logging.info(" --> Overwriting existing file")
        else:
            logging.info(" --> File already exists, exit without errors")
            exit(0)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Download
    if str(date_ref.year) in available_datasets.keys():
        # If it's last month, I check if the reference dekad has been computed
        if date_ref.strftime("%m") == available_datasets[str(date_ref.year)]["months"][-1]:
            if str(date_ref.day) in available_datasets[str(date_ref.year)]["dekads"]:
                logging.info(" --> File available on server")
            else:
                logging.error(" --> File not available yet on server")
                raise FileNotFoundError()
        # If I am not in the last month all the dekads should be available
        elif date_ref.strftime("%m") in available_datasets[str(date_ref.year)]["months"]:
            logging.info(" --> File available on server")
        else:
             logging.error(" --> File not available on server")
             raise FileNotFoundError()
    else:
        logging.error(" --> File not available on server")
        raise FileNotFoundError()

    logging.info(" --> Download the data from remote server")

    logging.info(" --> Setup connection")
    if data_settings["algorithm"]["ancillary"]["credentials"]["username"] is None or data_settings["algorithm"]["ancillary"]["credentials"]["password"] is None:
        netrc_handle = netrc.netrc()
        username, _, password = netrc_handle.authenticators('http://ada.acmad.org:5000/')
    else:
        username = data_settings["algorithm"]["ancillary"]["credentials"]["username"]
        password = data_settings["algorithm"]["ancillary"]["credentials"]["password"]
    logging.info(" --> Connection established")

    
    # FastAPI endpoint details
    year = date_ref.year
    month = date_ref.strftime("%b")  # Example month ['Jan' ,'Feb', Mar, ... ]
    download_url = "http://ada.acmad.org:5000/"
    url = f'{download_url}download/{product}/{year}/{month}/{dekad}'

    # Format the URL with the parameters
    formatted_url = url.format(product=product, year=year, month=month, dekad=dekad)
    logging.info(" --> Downloading file " + formatted_url)

    # Make the GET request with authentication
    response = requests.get(formatted_url, auth=HTTPBasicAuth(username, password))

    # Check if the request was successful
    if response.ok:
        # You can handle the file response here, e.g., save it to a file
        with open(f'{output_filename}', 'wb') as f:
            f.write(response.content)
        logging.info("File downloaded successfully and saved to " + output_filename)
    else:
        logging.error("Error:", response.status_code, response.text)
        raise FileNotFoundError()

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
# Function for fill a dictionary of templates
def fill_template(templates,time_now):
    empty_template = templates
    template_filled = {}
    for key in empty_template.keys():
        template_filled[key] = time_now.strftime(empty_template[key])
    template_filled["domain"] = templates["domain"]
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