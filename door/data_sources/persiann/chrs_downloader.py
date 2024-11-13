import os
from typing import Generator
import numpy as np
import xarray as xr
import datetime as dt
import requests
import rioxarray

from ...tools import timestepping as ts
from ...tools.timestepping.timestep import TimeStep
from ...tools.timestepping.fixed_num_timestep import FixedNTimeStep
from ...base_downloaders import URLDownloader

from ...utils.space import BoundingBox, crop_to_bb
from ...utils.io import decompress_gz

class CHRSDownloader(URLDownloader):
    source = "CHRS"
    name = "CHRSDownloader"

    default_options = {
    }

    home = "https://persiann.eng.uci.edu/CHRSdata/"
    available_products: dict = {
        "PDIRNow-1hourly": {
            "ts_per_year": 8760,
            "url": home + 'PDIRNow/PDIRNow1hourly/{timestep.start:%Y}/pdirnow1h{timestep.start:%y%m%d%H}.bin.gz',
            "nodata": -9999,
            "rows_cols": (3000, 9000),
            "lat_lon_box": [59.98, -59.98, .02, 359.98],
            "lat_lon_steps": [-0.04, 0.04],
            "scale_factor": 100,
            "dtype": '<i2'  # little-endian int16
        }
    }

    def __init__(self, product: str) -> None:
        self.set_product(product)
        super().__init__(self.url_blank, protocol='http')

    def set_product(self, product: str) -> None:
        self.product = product
        if product not in self.available_products:
            raise ValueError(f'Product {product} not available. Choose one of {self.available_products.keys()}')
        self.ts_per_year = self.available_products[product]["ts_per_year"]
        self.url_blank = self.available_products[product]["url"]
        self.nodata = self.available_products[product]["nodata"]
        self.rows_cols = self.available_products[product]["rows_cols"]
        self.lat_lon_box = self.available_products[product]["lat_lon_box"]
        self.lat_lon_steps = self.available_products[product]["lat_lon_steps"]
        self.dtype = self.available_products[product]["dtype"]
        self.scale_factor = self.available_products[product]["scale_factor"]

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:

        ts_end = timestep.end
        tmp_filename_raw = f'temp_{self.product}{ts_end:%Y%m%d}'
        tmp_filename = f'{tmp_filename_raw}.bin.gz'
        tmp_destination = os.path.join(tmp_path, tmp_filename)
        print(" --> Download " + str(timestep))
        success = self.download(tmp_destination, min_size=200, missing_action='ignore', timestep=timestep)
        if success:
            # Unzip the data
            unzipped = decompress_gz(tmp_destination)

            data = np.fromfile(unzipped, dtype=np.dtype(self.dtype))
            data = data.reshape(self.rows_cols)
            data_in_mm_hr = data.astype(np.float32) / self.scale_factor
            data_in_mm_hr[data == self.nodata] = np.nan

            # Create latitude and longitude arrays
            lons = np.arange(self.lat_lon_box[2], self.lat_lon_box[3] + self.lat_lon_steps[1]/2, self.lat_lon_steps[1])
            lats = np.arange(self.lat_lon_box[0], self.lat_lon_box[1] + self.lat_lon_steps[0]/2, self.lat_lon_steps[0])

            # Create the xarray DataArray compliant with the rio extension
            data_array = xr.DataArray(
                data_in_mm_hr,
                dims=["y", "x"],
                coords={
                    "y": lats,
                    "x": lons
                },
                name="precipitation_rate",
                attrs={
                    "units": "mm/hr"}
            )
            # Set the CRS to WGS84 (EPSG:4326) and geotransform attributes
            data_array = data_array.rio.write_crs("EPSG:4326")
            data_array = data_array.rio.write_nodata(self.nodata)

            # crop the data
            cropped = crop_to_bb(data_array, space_bounds)

            yield cropped, {}

    def format_url(self, prelim=False, **kwargs):
        """
        Format the url for the download
        """
        if prelim:
            url = self.url_prelim_blank.format(**kwargs)
        else:
            url = self.url_blank.format(**kwargs)
        return url

