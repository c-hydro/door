#!/usr/bin/python3

"""
DRYES - Tool MODIS downloader

Operative script for MODIS product downloading

__date__ = '20210104'
__version__ = '1.0.0'
__author__ =
        'Alessandro Masoero' (alessandro.masoero@cimafoundation.org',
        'Michel Isabellon' (michel.isabellon@cimafoundation.org',

__library__ = 'dryes'

General command line:
python get_data_MODIS_op.py -settings_file configuration.json -product=MOD16A2 -time=20210104 -dayback=30

Configuration file required (conf_{MODISPRODUCT}.json)
3 arguments required:
- Product (e.g. MOD13A2)
- TIME (e.g. 20210104)
- DAYBACK (e.g. 30)

Version(s):
20210909 (1.0.1) (clean tmp folder before downloading)
20230208 (1.0.2) (version 061)
20230727 (2.0.0) --> New release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import logging, time
import glob
import datetime
import shutil
# import pymodis
# import urllib.request
from argparse import ArgumentParser
from lib.lib_data_io_json import read_file_json
from lib.lib_rasterRegrid import rasterRegrid
from lib.modisDownloaderWget import *
from lib.lib_data_io_tiff import *
from lib.lib_utils_logging import set_logging
from lib.lib_convertmodis_gdal import createMosaicGDAL


# -------------------------------------------------------------------------------------
# Algorithm information
alg_name = 'DRYES - SATELLITE MODIS'
alg_version = '1.0.0'
alg_release = '2022-07-05'
# Algorithm parameter(s)
time_format = '%Y%m%d%H%M'
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Script Main
def main():

    # -------------------------------------------------------------------------------------
    # Define and import Arguments
    parser = ArgumentParser()

    parser.add_argument("-s", "-settings_file", type=str, default=None,
                    help="json configuration file")

    parser.add_argument("-t", "-time", type=str, default=None,
                    help="End Date in YYYYMMDD format")

    parser.add_argument("-d", "-dayback", type=int, default=15,
                    help="Number of days back in the past")

    args = parser.parse_args()

    alg_settings = args.s


    data_settings = read_file_json(alg_settings)

    # -------------------------------------------------------------------------------------
    # Set algorithm logging
    os.makedirs(data_settings['log']['folder'], exist_ok=True)
    set_logging(logger_file=os.path.join(data_settings['log']['folder'], data_settings['log']['filename']))

    alg_time_start = time.time()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Info algorithm
    logging.info(' ============================================================================ ')
    logging.info(' ==> ' + alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logging.info(' ==> START ... ')
    logging.info(' ')
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Check validity of the settings
    if data_settings["algorithm"]["flags"]["crop_with_bounding_box"] is True and data_settings["algorithm"]["flags"]["regrid_with_map"] is True:
        logging.error(" ERROR! crop_with_bounding_box and regrid_with_map are mutually exclusive settings! Check your settings file!")
        raise KeyError("Choose if regrid with a provided raster or crop with a bounding box!")

    # Import settings from json
    logging.info(" ==> Importing settings from settings_file")

    productName = data_settings['settings']['product']

    if(productName not in data_settings['settings']['tested_products']):
        logging.error(' ==> Product "' + productName + '" is not not implemented yet')
        raise NotImplementedError('Case not implemented yet')

    output_dir = data_settings["data"]["dynamic"]["output"]["folder"]

    output_prefix = data_settings["data"]["dynamic"]["output"]["output_prefix"]

    subset = data_settings["settings"]["subset"]
    valid_range = data_settings["settings"]["valid_range"]
    scale_factor = data_settings["settings"]["scale_factor"]
    provider = data_settings["settings"]["provider"]
    user_name = data_settings["settings"]["usern"]
    password = data_settings["settings"]["passw"]
    interpmethod = data_settings["settings"]["interpmethod"]

    geotarget = data_settings["data"]["static"]["grid_raster"]
    mask = data_settings["data"]["static"]["mask"]

    # -------------------------------------------------------------------------------------
    # Set dates
    if args.t == None:
        log_stream.error('No date provided from command line, use json info')
        end = data_settings["time"]["end_date"]
        if(end == None):
            log_stream.error(' ==> End date needs to be set in arguments or in json file')
            raise IOError('Variable time not valid')

        oDateTo = datetime.datetime.strptime(str(end),'%Y%m%d')
        start = data_settings["time"]["start_date"]
        if(start ==None):
            oDateFrom = datetime.datetime.strptime(str(start),'%Y%m%d')
        else:
            dayback = data_settings["time"]["dayback"]
            log_stream.error(' ==> Start date or dayback needs to be set in arguments or in json file')
            raise IOError('Variable time not valid')
    else:
        oDateTo = datetime.datetime.strptime(str(args.t),'%Y%m%d')
        oDateFrom = oDateTo - datetime.timedelta(days=int(args.d))

    logging.info(" ==> Downloading data from " + str(oDateFrom) + " to " + str(oDateTo))
    oDate = oDateFrom

    # -------------------------------------------------------------------------------------
    # Start download across the dates
    while (oDate <= oDateTo):

        logging.info(' ==> Try to downloading ' + oDate.strftime('%Y-%m-%d'))

        # Downloader, generate URL
        CMR_URL = 'https://cmr.earthdata.nasa.gov'
        URS_URL = 'https://urs.earthdata.nasa.gov'
        CMR_PAGE_SIZE = 2000
        CMR_FILE_URL = ('{0}/search/granules.json?version=2.0provider={1}'
                        '&sort_key[]=start_date&sort_key[]=producer_granule_id'
                        '&scroll=true&page_size={2}'.format(CMR_URL, provider, CMR_PAGE_SIZE))

        # Generate Date for downloader
        date_st = oDate.strftime('%Y') + '-' + oDate.strftime('%m') + '-' + oDate.strftime('%d')
        time_start = date_st + 'T00:00:00Z'
        time_end = date_st + 'T23:59:59Z'
        filename_filter = '*A' + oDate.strftime('%Y') + oDate.strftime('%j') + '*' # julian day

        # Get Bounding Box (MODIS tiles selection)
        bounding_box = getBoundingBox(geotarget)

        # Set URLs DOWNLOAD HDF5 for selected day
        url_list = []
        version = data_settings['settings']['version']
        polygon = ''

        # Quering URL files
        url_list = cmr_search(productName, version, time_start, time_end, cmr_url=CMR_FILE_URL,
                              page_size=CMR_PAGE_SIZE, bounding_box=bounding_box,
                              polygon=polygon, filename_filter=filename_filter)
        logging.info(" ==> Querying for data: " + str(oDate.strftime("%Y-%m-%d")))

        # Testing if downloading files exists, otherwise skip to next date
        if(len(url_list)==0):
            logging.info(" ==> No MODIS data for day: " + str(oDate.strftime("%Y-%m-%d")) + " --> MOVE TO NEXT DAY")
            oDate = oDate + datetime.timedelta(days=1)
            continue

        # Setting ancillary folder for downloading
        dwn_folder = data_settings["data"]["dynamic"]["ancillary"]["folder"] + oDate.strftime('%Y') + '/' + oDate.strftime('%m') + '/' + oDate.strftime('%d')
        if not os.path.exists(dwn_folder):
            os.makedirs(dwn_folder)
        else:
            shutil.rmtree(dwn_folder)
            logging.info(' ==> Removing existing tmp folder content')
            os.makedirs(dwn_folder)

        # Download hdf files 2 options, CMR or WGET download
        logging.info(" ==> Downloading: " + str(oDate.strftime("%Y-%m-%d")) + ' ' + str(len(url_list)) + " files")
        cmr_download(url_list, user_name, password, URS_URL, dwn_folder)
        #wget_download(url_list, user_name, password, URS_URL, dwn_folder)

        # Get HDF5 list
        hdf5list_file = os.path.join(dwn_folder,'hdf5names.json')
        if os.path.exists(hdf5list_file):

            destination = output_dir + oDate.strftime('%Y') + '/' + oDate.strftime('%m') + '/' + oDate.strftime('%d')

            # Create destination folder
            if os.path.exists(destination):
                logging.info(' ==> Destination folder exists: ' + destination)
            else:
                os.system('mkdir -p ' + destination)
                logging.info(' ==> Destination folder created: ' + destination)

            with open(hdf5list_file) as hl:
                hdf5_ls = json.load(hl)

            for ii in range (0,len(subset)):

                subset_mask = [0] * subset[ii]
                subset_mask[subset[ii]-1]=1

                # Filenames of temporary and final files
                output_tif_name = output_prefix[ii]  + "_" + oDate.strftime('%Y%m%d') + ".tif"
                output_tif = os.path.join(dwn_folder,output_tif_name)
                tmp_mosaic = os.path.join(dwn_folder,'tmp_mosaic.tif')
                tmp_validrange = os.path.join(dwn_folder,'tmp_validrange.tif')
                tmp_regrid = os.path.join(dwn_folder,'tmp_regrid.tif')
                tmp_masked = os.path.join(dwn_folder,'tmp_masked.tif')

                # Create Mosaic
                mosaic = createMosaicGDAL(hdf5_ls, subset_mask, 'GTiff')
                mosaic.run(tmp_mosaic)

                # Apply fill value
                ValidRange = valid_range[ii]
                logging.info(' ==> Filter valid range values')
                keepValidRange (tmp_mosaic, tmp_validrange, ValidRange)

                # Regrid to Reference
                logging.info(' ==> Regridding to geotarget')
                rasterRegrid(tmp_validrange, geotarget, tmp_regrid , interpmethod)

                # MASK
                # check if mask file is needed/present
                if mask:
                    logging.info(' ==> Masking files')
                    maskMap(tmp_regrid, mask, tmp_masked)
                else:
                    logging.info(' ==> No mask file present')
                    os.rename(tmp_regrid, tmp_masked)

                # Rescale and save
                if  scale_factor[ii]!=1:
                    logging.info(' ==> Rescaling to scale factor ' + str(scale_factor[ii]))
                    rescale (tmp_masked, output_tif, scale_factor[ii])
                else:
                    logging.info(' ==> No rescaling needed')
                    os.rename(tmp_masked, output_tif)

                # Move to destination
                logging.info(' ==> Saving file to destination folder ' + destination)
                destinationFile = os.path.join(destination, output_tif_name)
                os.rename(output_tif, destinationFile)

            # Remove temporary
            if(data_settings['algorithm']['flags']['clean_data_ancillary_mosaic']):
                logging.info(' ==> Cleaning download temporary files')
                fileList_tmp = glob.glob(dwn_folder + '/*tmp_*')
                for filePath in fileList_tmp:
                    try:
                        os.remove(filePath)
                    except:
                        print("Error while deleting file : ", filePath)
            else:
                logging.info(" ==> Temporary files Maintained")

            # Remove hdf files
            if (data_settings['algorithm']['flags']['clean_data_ancillary_hdf']):
                logging.info(' ==> Cleaning download temporary files')
                fileList_hdf = glob.glob(dwn_folder + '/*hdf*') + \
                               glob.glob(dwn_folder + '/*xml*') + \
                               glob.glob(dwn_folder + '/*credentials*')
                for filePath in fileList_hdf:
                    try:
                        os.remove(filePath)
                    except:
                        logging.info(" ==> Error while deleting file : ", filePath)
            else:
                logging.info(' ==> HDF files Maintained')

        oDate = oDate + datetime.timedelta(days=1)
        logging.info(" ==> Downloading and resampling happened with success ==> MOVE TO NEXT DAY " + str(oDate))

    # -------------------------------------------------------------------------------------
    # Info algorithm
    alg_time_elapsed = round(time.time() - alg_time_start, 1)

    logging.info(' ')
    logging.info('[' + alg_name + ' (Version ' + alg_version + ' - Release ' + alg_release + ')]')
    logging.info(' ==> TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    logging.info(' ==> ... END')
    logging.info(' ==> Bye, Bye')
    logging.info(' ============================================================================ ')
    # -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Call script from external library
if __name__ == "__main__":
    main()
# -------------------------------------------------------------------------------------