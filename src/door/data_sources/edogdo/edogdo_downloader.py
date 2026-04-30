import os
from typing import Generator, Optional, Sequence
import xarray as xr
import rioxarray as rxr
import datetime as dt
import requests
import zipfile
import numpy as np
import re

from ...base_downloaders import URLDownloader

from d3tools import timestepping as ts
from d3tools.timestepping.timestep import TimeStep
from d3tools.timestepping.fixed_num_timestep import FixedNTimeStep
from d3tools.spatial import BoundingBox, crop_to_bb

class EDOGDODownloader(URLDownloader):
    source = "EDOGDO"
    name = "EDOGDODownloader"

    single_temp_folder = True

    default_options = {}

    home = "https://drought.emergency.copernicus.eu/data/Drought_Observatories_datasets/"

    available_products: dict = {
        "smang": "GDO_Soil_Moisture_Index_Anomaly",
        "smian": "EDO_Soil_Moisture_Index_Anomaly",
    }

    def __init__(self, product: str) -> None:
        self.set_product(product)
        super().__init__(self.url_blank, protocol = 'http')

    def set_product(self, product: str) -> None:
        self.product = product.lower()
        if self.product not in self.available_products:
            raise ValueError(f'Product {product} not available. Choose one of {self.available_products.keys()}')
        self.path = f'{self.home}{self.available_products[self.product]}/'
        self.url_blank = self.path + '{ver_path}/{filename}.zip'
        self.set_versions()
        last_file = self.get_last_file()
        file_info = self.parse_filename(last_file)
        self.type = file_info['type']
        self.observatory = file_info['observatory']
        self.freq = file_info['frequency']

    def get_last_file(self, pattern = None, version = None) -> str:

        last_version = self.versions[0] if version is None else version
        ver_path = f'ver{"-".join(last_version)}'
        file_folder = f'{self.path}/{ver_path}/'

        response = requests.get(file_folder)
        if response.status_code != 200:
            raise ValueError(f'Could not access {file_folder}. Check the URL or your internet connection.')

        # parse the file name
        if pattern is None:
            pattern = rf'href="({self.product}[^"]+).zip"'
        
        file_name_pattern = re.compile(pattern)
        file_name_matches = file_name_pattern.findall(response.text)
        if not file_name_matches:
            return None
        
        last_file = sorted(file_name_matches)[-1]
        return last_file

    def set_versions(self) -> None:
        """
        Set the versions for the downloader based on the available products.
        """
        # check what is in self.path
        response = requests.get(self.path)
        if response.status_code != 200:
            raise ValueError(f'Could not access {self.path}. Check the URL or your internet connection.')
        
        # parse the versions (they are in the form of verx-y-z)
        version_pattern = re.compile(r'href="ver(\d+-\d+-\d+)/"')
        version_matches = version_pattern.findall(response.text)
        self.versions   = [v.replace('-', '') for v in sorted(version_matches, reverse = True)]

    @staticmethod
    def parse_filename(filename: str) -> dict:
        """
        Parse the filename to extract relevant information.
        """
        basename = filename.split('.')[0]
        product, type, observatory, t0, t1, frequency = basename.split('_')
        timestep = ts.TimeStep.from_unit(frequency)
        t0 = timestep.from_date(dt.datetime.strptime(t0, '%Y%m%d'))
        t1 = timestep.from_date(dt.datetime.strptime(t1, '%Y%m%d'))
        return {
            'type': type,
            'observatory': observatory,
            't0': t0,
            't1': t1,
            'frequency': frequency,
            'timestep': timestep
        }

    def get_last_published_ts(self, **kwargs) -> ts.TimeRange:
        
        """
        Get the last published timestamp for the dataset.
        """

        last_file = self.get_last_file()

        # parse the filename to get the start and end date
        file_info = self.parse_filename(last_file)
        return file_info['t1']

    def get_last_published_date(self, **kwargs) -> dt.datetime:

        """
        Get the last published date for the dataset.
        """

        return self.get_last_published_ts().end

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
        
        for filename in os.listdir(tmp_path):
            if timestep.start.strftime('%Y%m%d') in filename and filename.endswith('.tif'):
                data = rxr.open_rasterio(os.path.join(tmp_path, filename))
                break
        else:
            year = timestep.year
            tmp_file_zip = f'temp_{self.product}{year}.zip'

            # check if the file is not already downloaded in the tmp_path
            tmp_destination = os.path.join(tmp_path, tmp_file_zip)
            if not os.path.exists(tmp_destination):
                pattern = rf'href="({self.product}_{self.type}_{self.observatory}_{year}\d{{4}}_{year}\d{{4}}_{self.freq})\.zip"'
                for ver in self.versions:
                    filename = self.get_last_file(pattern = pattern, version = ver)
                    file_info = self.parse_filename(filename)
                    if timestep <= file_info['t1']:
                        # download the file
                        self.download(tmp_destination, min_size = 2000, missing_action = 'warning', ver_path = f'ver{"-".join(ver)}', filename = filename)
                        # unzip the file inside the tmp_path
                        with zipfile.ZipFile(tmp_destination, 'r') as zip_ref:
                            zip_ref.extractall(tmp_path)
                        break

                # open the data relevant to the timestep
                for filename in os.listdir(tmp_path):
                    if timestep.start.strftime('%Y%m%d') in filename and filename.endswith('.tif'):
                        data = rxr.open_rasterio(os.path.join(tmp_path, filename))
                        break

        # crop the data
        cropped = crop_to_bb(data, space_bounds)

        nanvalue = cropped.attrs.get('_FillValue', np.nan)
        cropped = cropped.where(~np.isclose(cropped, nanvalue, equal_nan = True), np.nan)
        cropped.attrs['_FillValue'] = np.nan

        # remove the file after processing (to clear the tmp_path)
        os.remove(os.path.join(tmp_path, filename))

        yield cropped, {}