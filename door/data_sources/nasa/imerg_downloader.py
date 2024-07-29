import os
from typing import Optional
from osgeo import gdal
import tempfile
import rioxarray

import gzip
import netrc
import shutil
import datetime as dt

from ...base_downloaders import URLDownloader
from ...utils.space import BoundingBox, crop_to_bb
from ...tools.timestepping import TimeRange

class IMERGDownloader(URLDownloader):
    
    name = "IMERG_downloader"
    default_options = {
        'credentials_arthurhouhttps' : {'username': None, 'password': None},
        'credentials_jsimpsonhttps': {'username': None, 'password': None},
        'results_in_mmh' : True, # if True, will also download preliminary data if available
    }
    def __init__(self, product: str) -> None:
        self.product = product
        if self.product == "IMERG-final":
            url_blank = "https://arthurhouhttps.pps.eosdis.nasa.gov/gpmdata/{time_now:%Y/%m/%d}/gis/3B-HHR-GIS.MS.MRG.3IMERG.{time_now:%Y%m%d}-S{time_now:%H%M%S}-E{time_end:%H%M%S}.{step:%04d}.{version}.tif'"
            self.ts_per_year = 17520 # 30 mins
            self.prelim_nodata = -1
        elif self.product == "IMERG-late":
            url_blank = "https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/{time_now:%Y/%m}/3B-HHR-L.MS.MRG.3IMERG.{time_now:%Y%m%d}-S{time_now:%H%M%S}-E{time_end:%H%M%S}.{step:%04d}.{version}.30min.tif'"
            self.ts_per_year = 17520 # 30 mins
            self.prelim_nodata = -1
        elif self.product == "IMERG-early":
            url_blank = "https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/early/{time_now:%Y/%m}/3B-HHR-E.MS.MRG.3IMERG.{time_now:%Y%m%d}-S{time_now:%H%M%S}-E{time_end:%H%M%S}.{step:%04d}.{version}.30min.tif'"
            self.ts_per_year = 17520 # 30 mins
            self.prelim_nodata = -1
        else:
            url_blank = None
        
        super().__init__(url_blank, protocol = 'http')
        if url_blank is None:
            self.log.error(" --> ERROR! Only IMERG-final, IMERG-late and IMERG-early has been implemented until now!")
            raise NotImplementedError()
        
        self.nodata = -9999
            
    def get_data(self,
                 time_range: TimeRange,
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
        # Do all of this inside a temporary folder
        with tempfile.TemporaryDirectory() as tmp_path:
            # Download the data for the specified times
            for i, timestep in enumerate(timesteps):
                self.log.info(f' - Timestep {i+1}/{len(timesteps)}: {timestep}')

                time_now = timestep.start
                tmp_filename = f'temp_{self.product}{time_now:%Y%m%d}.tif.gz'
                tmp_destination = os.path.join(tmp_path, tmp_filename)
                # Download the data
                success = self.download(tmp_destination, min_size = 200, missing_action = 'warn', time = time_now, auth = self.setup_credentials(options))
                if not success:
                    self.log.info(f'  -> Could not find data for {time_now:%Y-%m-%d}')
                    missing_times.append(time_now)
                if success:
                    # Unzip the data
                    self.extract(tmp_destination)
                    # Regrid the data
                    destination_now = time_now.strftime(destination)
                    cropped = crop_to_bb(tmp_destination[:-3], space_bounds)
                    cropped.rio.to_raster(destination_now)
                    self.log.info(f'  -> SUCCESS! Data for {time_now:%Y-%m-%d} dowloaded and cropped to bounds')

            # Fill with prelimnary data
            if len(missing_times) > 0 and options['get_prelim']:
                self.log.info(f'Checking preliminary folder for missing data for {len(missing_times)} timesteps.')
                for i, time_now in enumerate(missing_times):
                    self.log.info(f' - Timestep {i+1}/{len(timesteps)}: {time_now:%Y-%m-%d}')

                    tmp_filename = f'temp_{self.product}{time_now:%Y%m%d}.tif'
                    tmp_destination = os.path.join(tmp_path, tmp_filename)                
                    self.url_blank = self.url_prelim_blank
                    success = self.download(tmp_destination, min_size = 200, missing_action = 'warn', time = time_now)
                    if success:
                        # Regrid the data
                        destination_now = time_now.strftime(destination)
                        cropped = crop_to_bb(tmp_destination[:-3], space_bounds)
                        cropped.rio.to_raster(destination_now)
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

    def setup_credentials(self, options):
        if self.product == "IMERG-final":
            ref_site = "arthurhouhttps"
        else:
            ref_site = "jsimpsonhttps"
        if options["credentials_" + ref_site]['username'] is None or options["credentials_" + ref_site]['password'] is None:
            self.log.warning(f' --> Credentials {ref_site} are None, try to read them from .netrc file!')
            try:
                netrc_handle = netrc.netrc()
                user, _, password = netrc_handle.authenticators(ref_site)
                credentials = (user, password)
            except FileNotFoundError:
                self.log.error(f' --> .netrc file not found in the home directory, please provide credentials for {ref_site} site!')
                raise FileNotFoundError()
        else:
            credentials = (options["credentials_" + ref_site]["username"], options["credentials_" + ref_site]["password"])
        return credentials

def compute_name_features(time_now, type):
    if type == "late":
        # versioning of 06 imerg late
        if time_now <= dt.datetime(2022, 5, 8, 15, 30, 0):
            vers = "B"
        elif time_now <= dt.datetime(2023, 7, 1, 13, 30, 0):
            vers = "C"
        elif time_now <= dt.datetime(2023, 11, 8, 1, 30, 0):
            vers = "D"
        else:
            vers = "E"
    if type == "early":
        if time_now <= dt.datetime(2022, 5, 9, 1, 30, 0):
            vers = "B"
        elif time_now <= dt.datetime(2023, 7, 1, 23, 30, 0):
            vers = "C"
        elif time_now <= dt.datetime(2023, 11, 8, 12, 30, 0):
            vers = "D"
        else:
            vers = "E"
    vers_code = "V07A" if type == "final" else "V06" + vers
    time_end = time_now + dt.timedelta(minutes=29, seconds=59)
    step = int((time_now - time_now.replace(hour=0, minute=0)).total_seconds() / 60.0)

    return vers_code, time_end, step

import pandas as pd
import datetime as dt

date_range = pd.date_range(dt.datetime(2012,1,1,0,0,0), dt.datetime(2023,12,31,23,0,0), freq = "30min")
for now in date_range:
    file_now = f'/home/idrologia/share/CAMBOGIA_LAOS/CALIBRATION/data/data_dynamic/imerg/late/{now:%Y/%m/%d}/cambolaos_late_imerg_{now:%Y%m%d%H%M}_mm_30min.tif'
    if os.path.isfile(file_now):
        pass
    else:
        print(now.strftime("%Y%m%d%H%M\n"))

