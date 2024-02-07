import os
from typing import Optional
import tempfile

import gzip
import shutil

from ...base_downloaders import URLDownloader
from ...utils.time import TimeRange
from ...utils.space import BoundingBox

import logging
logger = logging.getLogger(__name__)

class CHIRPSDownloader(URLDownloader):
    
    name = "CHIRPS"
    default_options = {
        'get_prelim' : True, # if True, will also download preliminary data if available
    }

    protocol = "http"
    
    def __init__(self, product: str) -> None:
        self.product = product
        if self.product == "CHIRPSp25-daily":
            self.url_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p25/{time:%Y}/chirps-v2.0.{time:%Y.%m.%d}.tif.gz"
            self.url_prelim_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/tifs/p25/{time:%Y}/chirps-v2.0.{time:%Y.%m.%d}.tif"
            self.ts_per_year = 365 # daily
            self.prelim_nodata = -1
        elif self.product == "CHIRPSp05-daily":
            self.url_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05/{time:%Y}/chirps-v2.0.{time:%Y.%m.%d}.tif.gz"
            self.url_prelim_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/tifs/p05/{time:%Y}/chirps-v2.0.{time:%Y.%m.%d}.tif"
            self.ts_per_year  = 365 # daily
            self.prelim_nodata = -9999
        elif self.product == "CHIRPSp25-monthly":
            self.url_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_monthly/tifs/chirps-v2.0.{time:%Y.%m}.tif.gz"
            self.url_prelim_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_monthly/tifs/chirps-v2.0.{time:%Y.%m}.tif"
            self.ts_per_year  = 12 # monthly
            self.prelim_nodata = -9999
        else:
            logger.error(" --> ERROR! Only CHIRPSp25-daily, CHIRPSp05-daily and CHIRPSp25-monthly has been implemented until now!")
            raise NotImplementedError()
        
        self.nodata = -9999
            
    def get_data(self,
                 time_range: TimeRange,
                 space_bounds: BoundingBox,
                 destination: str,
                 options: Optional[dict] = None) -> None:

        # Check options
        options = self.check_options(options)

        logger.info(f'------------------------------------------')
        logger.info(f'Starting download of {self.product} data')
        logger.info(f'Data requested between {time_range.start:%Y-%m-%d} and {time_range.end:%Y-%m-%d}')
        logger.info(f'Bounding box: {space_bounds.bbox}')
        logger.info(f'------------------------------------------')

        # Get the timesteps to download
        timesteps = time_range.get_timesteps_from_tsnumber(self.ts_per_year)
        missing_times = []
        logger.info(f'Found {len(timesteps)} timesteps to download.')
        # Do all of this inside a temporary folder
        with tempfile.TemporaryDirectory() as tmp_path:
            # Download the data for the specified times
            for i, time_now in enumerate(timesteps):
                logger.info(f' - Timestep {i+1}/{len(timesteps)}: {time_now:%Y-%m-%d}')

                tmp_filename = f'temp_{self.product}{time_now:%Y%m%d}.tif.gz'
                tmp_destination = os.path.join(tmp_path, tmp_filename)
                # Download the data
                if options['get_prelim']:
                    success = self.download(tmp_destination, min_size = 200, missing_action = 'ignore', time = time_now)
                    if not success:
                        logger.info(f'  -> Could not find data for {time_now:%Y-%m-%d}, will check preliminary folder later')
                        missing_times.append(time_now)
                else:
                    success = self.download(tmp_destination, min_size = 200, missing_action = 'warn', time = time_now)

                if success:
                    # Unzip the data
                    self.extract(tmp_destination)
                    # Regrid the data
                    destination_now = time_now.strftime(destination)
                    space_bounds.crop_raster(tmp_destination[:-3], destination_now)
                    logger.info(f'  -> SUCCESS! Data for {time_now:%Y-%m-%d} dowloaded and cropped to bounds')

            # Fill with prelimnary data
            if len(missing_times) > 0 and options['get_prelim']:
                logger.info(f'Checking preliminary folder for missing data for {len(missing_times)} timesteps.')
                for i, time_now in enumerate(missing_times):
                    logger.info(f' - Timestep {i+1}/{len(timesteps)}: {time_now:%Y-%m-%d}')

                    tmp_filename = f'temp_{self.product}{time_now:%Y%m%d}.tif'
                    tmp_destination = os.path.join(tmp_path, tmp_filename)                
                    self.url_blank = self.url_prelim_blank
                    success = self.download(tmp_destination, min_size = 200, missing_action = 'warn', time = time_now)
                    if success:
                        # Regrid the data
                        destination_now = time_now.strftime(destination)
                        space_bounds.crop_raster(tmp_destination, destination_now)
                        logger.info(f'  -> SUCCESS! Data for {time_now:%Y-%m-%d} dowloaded and cropped to bounds')
        
        logger.info(f'------------------------------------------')

    def extract(self, filename: str):
        """
        extracts from a .gz file
        """
        file_out = filename[:-3]
        with gzip.open(filename, 'rb') as f_in:
            with open(file_out, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)