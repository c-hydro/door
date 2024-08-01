import os
from typing import Generator
import numpy as np
import xarray as xr

from ...base_downloaders import URLDownloader
from ...tools.timestepping.timestep import TimeStep

from ...utils.space import BoundingBox, crop_to_bb
from ...utils.io import decompress_gz

class CHIRPSDownloader(URLDownloader):
    name = "CHIRPS_downloader"
    default_options = {
        'get_prelim' : True, # if True, will also download preliminary data if available
    }

    home = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/"
    prelim_home = home + "prelim/"
    available_products: dict = {
        "CHIRPSp25-daily": {
            "ts_per_year": 365,
            "url" : home + 'global_daily/tifs/p25/{timestep.start:%Y}/chirps-v2.0.{timestep.start:%Y.%m.%d}.tif.gz',
            "nodata" : -9999,
            "prelim_url" : prelim_home + 'global_daily/tifs/p25/{timestep.start:%Y}/chirps-v2.0.{timestep.start:%Y.%m.%d}.tif',
            "prelim_nodata": -1
        },
        "CHIRPSp05-daily": {
            "ts_per_year": 365,
            "url" : home + 'global_daily/tifs/p05/{timestep.start:%Y}/chirps-v2.0.{timestep.start:%Y.%m.%d}.tif.gz',
            "nodata" : -9999,
            "prelim_url" : prelim_home + 'global_daily/tifs/p05/{timestep.start:%Y}/chirps-v2.0.{timestep.start:%Y.%m.%d}.tif.gz',
            "prelim_nodata": -9999
        },
        "CHIRPSp05-dekads": {
            "ts_per_year": 36,
            "url" : home + 'global_dekad/tifs/chirps-v2.0.{timestep.start:%Y.%m}.{timestep.dekad_of_month}.tif.gz',
            "nodata" : -9999,
            "prelim_url" : prelim_home + 'global_dekad/tifs/chirps-v2.0.{timestep.start:%Y.%m}.{timestep.dekad_of_month}.tif',
            "prelim_nodata": -9999
        },
        "CHIRPSp25-monthly": {
            "ts_per_year": 12,
            "url" : home + 'global_monthly/tifs/chirps-v2.0.{timestep.start:%Y.%m}.tif.gz',
            "nodata" : -9999,
            "prelim_url" : prelim_home + 'global_monthly/tifs/chirps-v2.0.{timestep.start:%Y.%m}.tif',
            "prelim_nodata": -9999
        },
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

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
        
        ts_end = timestep.end
        tmp_filename_raw = f'temp_{self.product}{ts_end:%Y%m%d}'
        tmp_filename = f'{tmp_filename_raw}.tif.gz'
        tmp_destination = os.path.join(tmp_path, tmp_filename)
        success = self.download(tmp_destination, min_size = 200, missing_action = 'ignore', timestep = timestep)
        nodata = self.nodata
        isprelim = False
        if not success and self.get_prelim:
            tmp_filename = f'{tmp_filename_raw}.tif.gz' if self.url_prelim_blank.endswith('.gz') else f'{tmp_filename_raw}.tif'
            tmp_destination = os.path.join(tmp_path, tmp_filename)
            success = self.download(tmp_destination, min_size = 200, missing_action = 'ignore', timestep = timestep, prelim = True)
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