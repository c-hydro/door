import os
from typing import Optional
import tempfile
import rasterio
import numpy as np

import gzip
import shutil

from ...base_downloaders import URLDownloader
from ...utils.space import BoundingBox
from ...utils.geotiff import crop_raster
from ...tools import timestepping as ts

class CHIRPSDownloader(URLDownloader):
    
    name = "CHIRPS_downloader"
    default_options = {
        'get_prelim' : True, # if True, will also download preliminary data if available
    }
    
    def __init__(self, product: str) -> None:
        self.product = product
        if self.product == "CHIRPSp25-daily":
            url_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p25/{timestep.start:%Y}/chirps-v2.0.{timestep.start:%Y.%m.%d}.tif.gz"
            self.url_prelim_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/tifs/p25/{timestep.start:%Y}/chirps-v2.0.{timestep.start:%Y.%m.%d}.tif"
            self.ts_per_year = 365 # daily
            self.prelim_nodata = -1
        elif self.product == "CHIRPSp05-daily":
            url_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05/{timestep.start:%Y}/chirps-v2.0.{timestep.start:%Y.%m.%d}.tif.gz"
            self.url_prelim_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/tifs/p05/{timestep.start:%Y}/chirps-v2.0.{timestep.start:%Y.%m.%d}.tif.gz"
            self.ts_per_year  = 365 # daily
            self.prelim_nodata = -9999
        elif self.product == "CHIRPSp25-monthly":
            url_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_monthly/tifs/chirps-v2.0.{timestep.start:%Y.%m}.tif.gz"
            self.url_prelim_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_monthly/tifs/chirps-v2.0.{timestep.start:%Y.%m}.tif"
            self.ts_per_year  = 12 # monthly
            self.prelim_nodata = -9999
        elif self.product == "CHIRPSp05-dekads":
            url_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_dekad/tifs/chirps-v2.0.{timestep.start:%Y.%m}.{timestep.dekad_of_month}.tif.gz"
            self.url_prelim_blank = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_dekad/tifs/chirps-v2.0.{timestep.start:%Y.%m}.{timestep.dekad_of_month}.tif"
            self.ts_per_year  = 36 # dekadly
            self.prelim_nodata = -9999
        else:
            url_blank = None

        super().__init__(url_blank, protocol = 'http')
        if url_blank is None:
            self.log.error(" --> ERROR! Only CHIRPSp25-daily, CHIRPSp05-daily and CHIRPSp25-monthly has been implemented until now!")
            raise NotImplementedError()
        
        self.nodata = -9999
            
    def get_data(self,
                 time_range: ts.TimeRange,
                 space_bounds: BoundingBox,
                 destination: str,
                 options: Optional[dict] = None) -> None:

        # Check options
        options = self.check_options(options)

        self.log.info(f'------------------------------------------')
        self.log.info(f'Starting download of {self.product} data')
        self.log.info(f'Data requested between {time_range.start:%Y-%m-%d} and {time_range.end:%Y-%m-%d}')
        self.log.info(f'Bounding box: {space_bounds.bbox}')
        self.log.info(f'------------------------------------------')

        # Get the timesteps to download
        timesteps = time_range.get_timesteps_from_tsnumber(self.ts_per_year)
        missing_times = []
        self.log.info(f'Found {len(timesteps)} timesteps to download.')

        # Download the data for the specified times
        for i, this_ts in enumerate(timesteps):
            time_now = this_ts.start
            self.log.info(f' - Timestep {i+1}/{len(timesteps)}: {this_ts}')

            # Do all of this inside a temporary folder
            tmpdirs = os.path.join(os.getenv('HOME'), 'tmp')
            os.makedirs(tmpdirs, exist_ok=True)
            with tempfile.TemporaryDirectory(dir = tmpdirs) as tmp_path:
                tmp_filename = f'temp_{self.product}{time_now:%Y%m%d}.tif.gz'
                tmp_destination = os.path.join(tmp_path, tmp_filename)

                # Download the data
                if options['get_prelim']:
                    success = self.download(tmp_destination, min_size = 200, missing_action = 'ignore', timestep = this_ts)
                    if not success:
                        self.log.info(f'  -> Could not find data for {time_now:%Y-%m-%d}, will check preliminary folder later')
                        missing_times.append(this_ts)
                else:
                    success = self.download(tmp_destination, min_size = 200, missing_action = 'warn', timestep = this_ts)

                if success:
                    # Unzip the data
                    self.extract(tmp_destination)
                    # Regrid the data
                    destination_now = time_now.strftime(destination)
                    crop_raster(tmp_destination[:-3], space_bounds, destination_now)

                    # change the nodata value to np.nan
                    with rasterio.open(destination_now, 'r+') as ds:
                        data = ds.read(1)
                        data = np.where(np.isclose(data, self.nodata, equal_nan=True), np.nan, data)
                        ds.write(data, 1)
                        ds.nodata = np.nan

                    self.log.info(f'  -> SUCCESS! Data for {time_now:%Y-%m-%d} dowloaded and cropped to bounds')

        # Fill with prelimnary data
        if len(missing_times) > 0 and options['get_prelim']:
            self.log.info(f'Checking preliminary folder for missing data for {len(missing_times)} timesteps.')
            
            for i, this_ts in enumerate(missing_times):
                self.log.info(f' - Timestep {i+1}/{len(timesteps)}: {this_ts}')

                time_now = this_ts.start
                # Do all of this inside a temporary folder
                with tempfile.TemporaryDirectory(dir = tmpdirs) as tmp_path:

                    if self.url_prelim_blank.endswith('.gz'):
                        tmp_filename = f'temp_{self.product}{time_now:%Y%m%d}.tif.gz'
                    else:
                        tmp_filename = f'temp_{self.product}{time_now:%Y%m%d}.tif'
                    tmp_destination = os.path.join(tmp_path, tmp_filename) 
                                   
                    self.url_blank = self.url_prelim_blank
                    success = self.download(tmp_destination, min_size = 200, missing_action = 'warn', timestep = this_ts)
                    if success:
                        if tmp_destination.endswith('.gz'):
                            self.extract(tmp_destination)
                            tmp_destination = tmp_destination[:-3]
                            
                        # Regrid the data
                        destination_now = time_now.strftime(destination)
                        crop_raster(tmp_destination, space_bounds, destination_now)

                        # Add a metadata field to specify it is preliminary
                        with rasterio.open(destination_now, 'r+') as ds:
                            ds.update_tags(PRELIMINARY = 'True')
                            # change the nodata value to np.nan
                            #set the raster value to np.nan where it is equal to the preliminary nodata value
                            data = ds.read(1)
                            data = np.where(np.isclose(data, self.prelim_nodata, equal_nan=True), np.nan, data)
                            ds.write(data, 1)
                            ds.nodata = np.nan
                        
                        self.log.info(f'  -> SUCCESS! Data for {time_now:%Y-%m-%d} dowloaded and cropped to bounds')
        
        self.log.info(f'------------------------------------------')

    def extract(self, filename: str):
        """
        extracts from a .gz file
        """
        file_out = filename[:-3]
        with gzip.open(filename, 'rb') as f_in:
            with open(file_out, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)