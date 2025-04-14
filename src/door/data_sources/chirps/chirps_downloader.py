import os
from typing import Generator
import numpy as np
import xarray as xr
import rioxarray as rxr
import datetime as dt
import requests

from ...base_downloaders import URLDownloader

from d3tools import timestepping as ts
from d3tools.timestepping.timestep import TimeStep
from d3tools.timestepping.fixed_num_timestep import FixedNTimeStep
from d3tools.spatial import BoundingBox, crop_to_bb

from ...utils.io import decompress_gz

class CHIRPSDownloader(URLDownloader):
    source = "CHIRPS"
    name = "CHIRPS_downloader"
    
    default_options = {
        'get_prelim' : True, # if True, will also download preliminary data if available
    }

    homev2 = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/"
    homev3 = "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/"
    available_products: dict = {
        "CHIRPSp25-daily": {
            "ts_per_year": 365,
            "url" : homev2 + 'global_daily/tifs/p25/{timestep.start:%Y}/chirps-v2.0.{timestep.start:%Y.%m.%d}.tif.gz',
            "nodata" : -9999,
            "prelim_url" : homev2 + "prelim/" + 'global_daily/tifs/p25/{timestep.start:%Y}/chirps-v2.0.{timestep.start:%Y.%m.%d}.tif',
            "prelim_nodata": -1
        },
        "CHIRPSp05-daily": {
            "ts_per_year": 365,
            "url" : homev2 + 'global_daily/tifs/p05/{timestep.start:%Y}/chirps-v2.0.{timestep.start:%Y.%m.%d}.tif.gz',
            "nodata" : -9999,
            "prelim_url" : homev2 + "prelim/" + 'global_daily/tifs/p05/{timestep.start:%Y}/chirps-v2.0.{timestep.start:%Y.%m.%d}.tif.gz',
            "prelim_nodata": -9999
        },
        "CHIRPSp05-dekads": {
            "ts_per_year": 36,
            "url" : homev2 + 'global_dekad/tifs/chirps-v2.0.{timestep.start:%Y.%m}.{timestep.dekad_of_month}.tif.gz',
            "nodata" : -9999,
            "prelim_url" : homev2 + "prelim/" + 'global_dekad/tifs/chirps-v2.0.{timestep.start:%Y.%m}.{timestep.dekad_of_month}.tif',
            "prelim_nodata": -9999
        },
        "CHIRPSp25-monthly": {
            "ts_per_year": 12,
            "url" : homev2 + 'global_monthly/tifs/chirps-v2.0.{timestep.start:%Y.%m}.tif.gz',
            "nodata" : -9999,
            "prelim_url" : homev2 + "prelim/" + 'global_monthly/tifs/chirps-v2.0.{timestep.start:%Y.%m}.tif',
            "prelim_nodata": -9999
        },
        "CHIRPSv3-dekads": {
            "ts_per_year": 36,
            "url" : homev3 + 'dekads/global/tifs/chirps-v3.0.{timestep.start:%Y.%m}.{timestep.dekad_of_month}.tif',
            "nodata" : -9999,
            "prelim_url" : homev3 + "prelim/" + 'pentads/global/tifs/chirps-v3.0.{timestep.start:%Y.%m}.{pentad_of_month}.tif',
            "prelim_nodata": -9999
        },
        "CHIRPSv3-monthly": {
            "ts_per_year": 12,
            "url" : homev3 + 'monthly/global/tifs/chirps-v3.0.{timestep.start:%Y.%m}.tif',
            "nodata" : -9999,
            "prelim_url" : homev3 + "prelim/" + 'pentads/global/tifs/chirps-v3.0.{timestep.start:%Y.%m}.{pentad_of_month}.tif',
            "prelim_nodata": -9999
        }
    }

    def __init__(self, product: str) -> None:
        self.set_product(product)
        super().__init__(self.url_blank, protocol = 'http')

    def set_product(self, product: str) -> None:
        self.product = product
        if product not in self.available_products:
            raise ValueError(f'Product {product} not available. Choose one of {self.available_products.keys()}')
        self.ts_per_year = self.available_products[product]["ts_per_year"]
        self.url_blank = self.available_products[product]["url"]
        self.url_prelim_blank = self.available_products[product]["prelim_url"]
        self.nodata = self.available_products[product]["nodata"]
        self.prelim_nodata = self.available_products[product]["prelim_nodata"]

    def get_last_published_ts(self, prelim = None, product = None, **kwargs) -> ts.TimeRange:
        
        """
        Get the last published date for the dataset.
        """
        if prelim is None:
            prelim = self.get_prelim
        
        if product is None:
            product = self.product

        ts_per_year = self.available_products[product]["ts_per_year"]
        url = self.available_products[product]["url"] if not prelim else self.available_products[product]["prelim_url"]

        if ts_per_year == 365:
            TimeStep = ts.Day
        else:
            TimeStep = FixedNTimeStep.get_subclass(ts_per_year)

        current_timestep = TimeStep.from_date(dt.datetime.now())
        while True:
            if "pentad_of_month" in url:
                pentad_of_month = 6 if ts_per_year == 12 else current_timestep.dekad_of_month*2
                current_url = url.format(timestep = current_timestep, pentad_of_month = pentad_of_month)
            else:   
                current_url = url.format(timestep = current_timestep)
            
            # send a request to the url
            response = requests.head(current_url)

            # if the request is successful, the last published timestep is the current timestep
            if response.status_code is requests.codes.ok:
                return current_timestep

            # if the request is not successful, move to the previous timestep
            current_timestep -= 1

    def get_last_published_date(self, **kwargs) -> dt.datetime:
        return self.get_last_published_ts(**kwargs).end

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
        
        ts_end = timestep.end
        tmp_filename_raw = f'temp_{self.product}{ts_end:%Y%m%d}'
        tmp_filename = f'{tmp_filename_raw}.tif.gz' if self.url_blank.endswith('.gz') else f'{tmp_filename_raw}.tif'
        tmp_destination = os.path.join(tmp_path, tmp_filename)
        success = self.download(tmp_destination, min_size = 200, missing_action = 'ignore', timestep = timestep)
        nodata = self.nodata
        isprelim = False
        if not success and self.get_prelim:
            if "pentad_of_month" not in self.url_prelim_blank:
                tmp_filename = f'{tmp_filename_raw}.tif.gz' if self.url_prelim_blank.endswith('.gz') else f'{tmp_filename_raw}.tif'
                tmp_destination = os.path.join(tmp_path, tmp_filename)
                success = self.download(tmp_destination, min_size = 200, missing_action = 'ignore', timestep = timestep, prelim = True)
                
            else:
                if isinstance(timestep, ts.Month):
                    pentads = [1, 2, 3, 4, 5, 6]
                elif isinstance(timestep, ts.Dekad):
                    pentads = [timestep.dekad_of_month*2-1, timestep.dekad_of_month*2]

                tmp_filenames = []
                for pentad in pentads:
                    tmp_filename = f'{tmp_filename_raw}_{pentad}.tif'
                    tmp_destination = os.path.join(tmp_path, tmp_filename)
                    success = self.download(tmp_destination, min_size = 200, missing_action = 'ignore', timestep = timestep, pentad_of_month = pentad, prelim = True)
                    tmp_filenames.append(tmp_filename)
                    if not success:
                        break
                else:

                    data_list  = [rxr.open_rasterio(os.path.join(tmp_path, filename)) for filename in tmp_filenames]
                    data_stack = np.stack([d.values for d in data_list], axis = 0)
                    data_sum  = np.sum(data_stack, axis = 0)

                    data_sum[np.any(data_stack == self.prelim_nodata, axis = 0)] = self.prelim_nodata
                    tmp_destination = os.path.join(tmp_path, tmp_filename_raw + '.tif')
                    data_list[0].copy(data = data_sum).rio.to_raster(tmp_destination, compress = 'lzw')
                    success = True

            nodata = self.prelim_nodata
            isprelim = True
        
        if success:
            # Unzip the data
            unzipped = decompress_gz(tmp_destination)
            
            # crop the data
            cropped = crop_to_bb(unzipped, space_bounds)

            # change the nodata value to np.nan and return the data
            cropped = cropped.where(~np.isclose(cropped, nodata, equal_nan=True), np.nan)
            cropped.rio.no_data = np.nan

            if isprelim:
                cropped.attrs['PRELIMINARY'] = 'True'

            yield cropped, {}
    
    def format_url(self, prelim = False, **kwargs):
        """
        Format the url for the download
        """
        if prelim:
            url = self.url_prelim_blank.format(**kwargs)
        else:
            url = self.url_blank.format(**kwargs)
        return url