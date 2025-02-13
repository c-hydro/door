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
from ...utils.auth import get_credentials_from_netrc

class GSMAPDownloader(URLDownloader):
    source = "JAXA"
    name = "GSMAPDownloader"

    default_options = {
        'overwite_existing': False,    #still need to be impelmented
        'overwrite_prelim': True,       #still need to be impelmented
        'get_prelim': True,  # if True, will also download preliminary data if available
    }

    host = "sftp://rainmap@hokusai.eorc.jaxa.jp"
    prelim_home = host + "prelim/"
    available_products: dict = {
        "gsmap-gauge": {
            "ts_per_year": 8760,
            "url": '/realtime/hourly_G/{timestep.start:%Y/%m/%d}/gsmap_gauge.{timestep.start:%Y%m%d.%H%M}.dat.gz',
            "nodata": -99,
            "rows_cols": (1200, 3600),
            "lat_lon_box": [60.0, -59.95, 0.05, 360.0],
            "lat_lon_steps": [-0.1, 0.1],
            "scale_factor": 1,
            "dtype": 'float32'  # little-endian int16
        }
    }

    def __init__(self, product: str) -> None:
        self.set_product(product)
        self.read_netrc_credentials()
        super().__init__(self.url_blank, protocol='sftp', host = self.host)

    def read_netrc_credentials(self) -> None:
        credentials = get_credentials_from_netrc(self.host)
        self.username, self.password = credentials.split(":")

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
        tmp_filename = f'{tmp_filename_raw}.dat.gz'
        tmp_destination = os.path.join(tmp_path, tmp_filename)

        # Check if file already exists and handle overwrite_existing flag
        if os.path.exists(tmp_destination) and not self.default_options['overwrite_existing']:
            # Load the existing file to check for preliminary flag
            existing_data = xr.open_dataarray(tmp_destination)
            if 'PRELIMINARY' in existing_data.attrs and existing_data.attrs['PRELIMINARY'] == 'True' and not self.default_options['overwrite_prelim']:
                print(f" --> Skipping download for {timestep} as file is preliminary and overwrite_prelim is False")
                return
            print(f" --> Skipping download for {timestep} as file already exists and overwrite_existing is False")
            return

        # Downlaod the file
        print(" --> Download " + str(timestep))
        success = self.download(tmp_destination, min_size=200, missing_action='ignore', timestep=timestep, auth= (self.username, self.password))
        if success:
            # Unzip the data
            unzipped = decompress_gz(tmp_destination)

            data = np.fromfile(unzipped, dtype=np.dtype(self.dtype))
            data = data.reshape(self.rows_cols)
            data_in_mm_hr = data / self.scale_factor
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
            # add an attribute to specify that data are not preliminary like PRELIMINARY=NO
            data_array.attrs['PRELIMINARY'] = 'False'

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