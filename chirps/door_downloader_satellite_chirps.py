#!/usr/bin/python3

"""
door Tool - SATELLITE CHIRPS

__date__ = '20220705'
__version__ = '1.0.0'
__author__ =
        'Andrea Libertino (andrea.libertino@cimafoundation.org',
        'Alessandro Masoero (alessandro.masoero@cimafoundation.org'
__library__ = 'door'

General command line:
python3 door_downloader_satellite_chirps.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20220705 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
from osgeo import gdal, gdalconst, osr
import pandas as pd
import os, json, logging, time
import netrc
import xarray as xr
import rioxarray as rxr
from argparse import ArgumentParser
import requests
from multiprocessing import Pool, cpu_count, Manager
from osgeo import gdal
import datetime as dt
from multiprocessing.pool import ThreadPool
import gzip
import shutil

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
    # Check validity of the settings
    if data_settings["algorithm"]["flags"]["crop_with_bounding_box"] is True and data_settings["algorithm"]["flags"]["regrid_with_map"] is True:
        logging.error(" ERROR! crop_with_bounding_box and regrid_with_map are mutually exclusive settings! Check your settings file!")
        raise KeyError("Choose if regrid with a provided raster or crop with a bounding box!")

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
    logging.info(" ---> Multiprocessing setup...DONE")

    logging.info(" ---> Create ancillary data path...")

    global downloader_settings
    downloader_settings = {}
    downloader_settings["ancillary_path"] = data_settings["data"]["dynamic"]["ancillary"]["folder"].format(domain=domain)
    os.makedirs(downloader_settings["ancillary_path"], exist_ok=True)
    downloader_settings["clean_dynamic_data_ancillary"] = data_settings["algorithm"]["flags"]["clean_dynamic_data_ancillary"]
    logging.info(" ---> Create ancillary data path...DONE")

    logging.info(" --> Setup downloader settings...DONE")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Get algorithm time range
    logging.info(" --> Setting algorithm time settings...")
    time_end = dt.datetime.strptime(alg_time, '%Y-%m-%d %H:%M')
    time_start = time_end - pd.Timedelta(str(data_settings["data"]["dynamic"]["time"]["time_observed_period"]) +
                                         data_settings["data"]["dynamic"]["time"]["time_observed_frequency"])

    spat_res = data_settings["data"]["dynamic"]["product"]["spatial_resolution"]

    if data_settings["data"]["dynamic"]["time"]["product_resolution"] == "daily":
        freq = "D"
        url_blank = os.path.join("https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs", spat_res, "{data_daily_year}/chirps-v2.0.{data_daily_time}.tif.gz")
        if spat_res == "p25":
            url_prelim_blank = os.path.join("https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/tifs",
                                            spat_res, "{data_daily_year}/chirps-v2.0.{data_daily_time}.tif")
        elif spat_res == "p05":
            url_prelim_blank = os.path.join("https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/tifs",
                                            spat_res, "{data_daily_year}/chirps-v2.0.{data_daily_time}.tif.gz")
        else:
            logging.error(" ERROR! Only p05 and p25 spatial resolution have been implemented! Check your settings file!")
    elif data_settings["data"]["dynamic"]["time"]["product_resolution"] == "monthly":
        freq = "MS"
        if spat_res != "p25":
            logging.error(" ERROR! Only p25 spatial resolution for monthly products! Check your settings file!")
        url_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_monthly/tifs/chirps-v2.0.{data_monthly_time}.tif.gz"
        url_prelim_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_monthly/tifs/chirps-v2.0.{data_monthly_time}.tif"
    else:
        logging.error(" ERROR! Only daily and monthly products have been implemented! Check your settings file!")
        raise NotImplementedError
    time_range = pd.date_range(time_start, time_end, freq=freq)

    downloader_settings["templates"] = data_settings["algorithm"]["template"]
    logging.info(" --> Setting algorithm time settings...DONE")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Get spatial information
    logging.info(" --> Setting algorithm spatial settings...")

    downloader_settings["crop_with_bounding_box"] = data_settings["algorithm"]["flags"]["crop_with_bounding_box"]
    if downloader_settings["crop_with_bounding_box"]:
        x_min = data_settings["data"]["static"]["bounding_box"]["lon_left"]
        x_max = data_settings["data"]["static"]["bounding_box"]["lon_right"]
        y_min = data_settings["data"]["static"]["bounding_box"]["lat_bottom"]
        y_max = data_settings["data"]["static"]["bounding_box"]["lat_top"]

        if x_max is None:
            x_max = 180
        if x_min is None:
            x_min = -180
        if y_min is None or y_min<-50:
            y_min = -50
            logging.warning(" WARNING! Minimum latitude limited to -60 as it is the maximum southern latitude of IMERG")
        if y_max is None or y_min>50:
            y_max = 50
            logging.warning(" WARNING! Maximum latitude limited to +60 as it is the maximum northern latitude of IMERG")
        downloader_settings["bbox"] = (x_min,y_max,x_max,y_min)

    downloader_settings["regrid_with_map"] = data_settings["algorithm"]["flags"]["regrid_with_map"]
    if downloader_settings["regrid_with_map"]:
        downloader_settings["ref_map"] = data_settings["data"]["static"]["grid_raster"]

    downloader_settings["domain"] = domain
    downloader_settings["templates"]["domain"] = domain
    out_file_template = os.path.join(data_settings["data"]["dynamic"]["outcome"]["final"]["folder"], data_settings["data"]["dynamic"]["outcome"]["final"]["file_name"])
    preliminar_file_template = os.path.join(data_settings["data"]["dynamic"]["outcome"]["preliminar"]["folder"], data_settings["data"]["dynamic"]["outcome"]["preliminar"]["file_name"])
    logging.info(" --> Setting algorithm spatial settings...DONE")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    logging.info(" ---> Download final CHIRPS data...")
    ancillary_file_template = os.path.join(downloader_settings["ancillary_path"], data_settings["data"]["dynamic"]["outcome"]["final"]["file_name"] + ".gz")
    urls = [url_blank.format(**fill_template(downloader_settings, t)) for t in time_range]
    out_files = [out_file_template.format(**fill_template(downloader_settings, t)) for t in time_range]
    ancillary_files = [ancillary_file_template.format(**fill_template(downloader_settings, t)) for t in time_range]

    in_out_files = zip(urls, out_files, ancillary_files)
    if data_settings["algorithm"]["flags"]["downloading_mp"]:
        logging.info(" ----> Start download in parallel mode...")
    else:
        logging.info(" ----> Start download in serial mode...")

    results = ThreadPool(process_max).imap_unordered(download_url, in_out_files)
    for result in results:
        print(result)
    logging.info(" ---> Download CHIRPS data...DONE")
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    if data_settings["algorithm"]["flags"]["fill_with_preliminary_version"]:
        logging.info(" ---> Download preliminar CHIRPS data...")
        ancillary_file_template = os.path.join(downloader_settings["ancillary_path"],data_settings["data"]["dynamic"]["outcome"]["final"]["file_name"])
        if spat_res == "p05":
            ancillary_file_template = os.path.join(downloader_settings["ancillary_path"],data_settings["data"]["dynamic"]["outcome"]["final"]["file_name"] + ".gz")
        missing_time = [t for t in time_range if not os.path.isfile(out_file_template.format(**fill_template(downloader_settings, t)))]
        urls = [url_prelim_blank.format(**fill_template(downloader_settings, t)) for t in missing_time]
        out_files = [preliminar_file_template.format(**fill_template(downloader_settings, t)) for t in missing_time]
        ancillary_files = [ancillary_file_template.format(**fill_template(downloader_settings, t)) for t in missing_time]

        in_out_files = zip(urls, out_files, ancillary_files)
        if data_settings["algorithm"]["flags"]["downloading_mp"]:
            logging.info(" ----> Start download in parallel mode...")
        else:
            logging.info(" ----> Start download in serial mode...")

        results = ThreadPool(process_max).imap_unordered(download_url, in_out_files)
        for result in results:
            print(result)
        logging.info(" ---> Download preliminar CHIRPS data...DONE")

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

# -------------------------------------------------------------------------------------
def download_url(args):
    url, out, fn = args[0], args[1], args[2]
    # Download file
    try:
        r = requests.get(url)
        with open(fn, 'wb') as f:
            f.write(r.content)
        if os.path.getsize(fn) < 200:
            raise FileNotFoundError("ERROR! Data not available: ")
        os.makedirs(os.path.dirname(fn), exist_ok=True)
        os.makedirs(os.path.dirname(out), exist_ok=True)
    except Exception as e:
        os.remove(fn)
        return ("---> ERROR! Data not available : " + os.path.basename(fn))
    # Postprocess data
    if os.path.isfile(fn):
        if fn[-2:] == "gz":
            # If file is gzipped it means that it a final version, and should be read with virtualization
            pre_string = "/vsigzip/"
            nodata = -9999
        else:
            # Preliminary files are not zipped. Common reader can be used.
            pre_string = ""
            # Daily preliminary has nodata = -1 (and p25 in url name)
            if "p25" in url:
                nodata = -1
            else:
                # Monthly preliminary has nodata = -9999
                nodata = -9999
        if downloader_settings["crop_with_bounding_box"]:
            gdal.Translate(out, pre_string + fn, projWin = downloader_settings["bbox"], **{"noData": nodata,"creationOptions":['COMPRESS=DEFLATE']})
        if downloader_settings["regrid_with_map"]:
            data = gdal.Open(pre_string + fn, gdalconst.GA_ReadOnly)
            data.GetRasterBand(1).SetNoDataValue(-9999)
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(4326)
            src_proj = srs.ExportToWkt()
            match_ds = gdal.Open(downloader_settings["ref_map"], gdalconst.GA_ReadOnly)
            match_proj = srs.ExportToWkt()
            match_geotrans = match_ds.GetGeoTransform()
            wide = match_ds.RasterXSize
            high = match_ds.RasterYSize

            dst = gdal.GetDriverByName('GTiff').Create(out, wide, high, 1, gdalconst.GDT_Float32, options=['COMPRESS=DEFLATE'])
            dst.SetGeoTransform(match_geotrans)
            dst.SetProjection(match_proj)
            dst.GetRasterBand(1).SetNoDataValue(nodata)

            gdal.ReprojectImage(data, dst, src_proj, match_proj, gdalconst.GRA_NearestNeighbour)
            del dst
        else:
            gdal.Translate(out, pre_string + fn, **{"noData": nodata, "creationOptions":"COMPRESS=DEFLATE"})
        if downloader_settings["clean_dynamic_data_ancillary"]:
            os.remove(fn)
        return ("---> Download succesful: " + os.path.basename(fn))
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Call script from external library
if __name__ == "__main__":
    main()
# ----------------------------------------------------------------------------