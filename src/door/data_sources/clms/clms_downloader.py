import os
import requests
import numpy as np
import xarray as xr
from typing import Optional, Iterable, Sequence
import datetime as dt

from ...utils.io import download_http, handle_missing
from ...base_downloaders import URLDownloader

from d3tools.spatial import BoundingBox, crop_to_bb
from d3tools import timestepping as ts
from d3tools.timestepping.timestep import TimeStep
from d3tools.timestepping.fixed_num_timestep import FixedNTimeStep
from d3tools.data import Dataset

class CLMSDownloader(URLDownloader):
    source = "CLMS"
    name = "CLMS_downloader"

    clms_url = "https://globalland.vito.be/download/"

    default_options = {
        'variables': '020',
        'crop_to_bounds': True,
        'ts_per_year': 36
    }

    available_products = {
        'swi': {
            'versions': ["3.1.1", "3.2.1"],
            'url': clms_url + 'geotiff/soil_water_index/swi_12.5km_v3_{ts_str}daily/{timestep.start:%Y}/{timestep.start:%Y%m%d}/c_gls_SWI{ts_str}-SWI{var}_{timestep.start:%Y%m%d}1200_GLOBE_ASCAT_V{version}.tiff',
            'nodata': 255,
            'scale_factor': 0.5
        }
    }

    available_variables = ["001", "005", "010", "020", "040", "060", "100"]

    def __init__(self, product: str) -> None:
        self.set_product(product)

    def set_product(self, product: str) -> None:
        self.product = product.lower()
        if self.product not in self.available_products:
            raise ValueError(f'Product {product} not available. Choose one of {self.available_products.keys()}')
        self.nodata = self.available_products[self.product]["nodata"]
        self.scale_factor = self.available_products[self.product]["scale_factor"]
        self.versions = self.available_products[self.product]["versions"]
        self.url_blank =  self.available_products[self.product]["url"]

    def set_variables(self, variables: list) -> None:
        self.variables = []
        for var in variables:
            this_var = var.lower()
            if this_var not in self.available_variables:
                msg = f'Variable {var} not available. Choose one of {self.available_variables}'
            else:
                self.variables.append(this_var)
        if len(self.variables) == 0:
            raise ValueError('No valid variables selected')

    def get_last_published_ts(self, **kwargs) -> ts.TimeRange:

        """
        Get the last published date for the dataset.
        """

        ts_per_year = self.ts_per_year

        # Set ts_str based on the ts_per_year
        if ts_per_year == 36:
            ts_str = '10'
            TimeStep = FixedNTimeStep.get_subclass(ts_per_year)
        elif ts_per_year == 365:
            ts_str = ''
            TimeStep = ts.Day
        else:
            raise ValueError(f"ts_per_year {self.ts_per_year} not supported")

        current_timestep = TimeStep.from_date(dt.datetime.now())
        # Get the URL without version

        while True:
            for v in self.versions:
                current_url_v = self.url_blank.format(
                    ts_str=ts_str,
                    timestep=current_timestep,
                    var=self.variables[0],
                    version=v
                )

                # send a request to the url
                response = requests.head(current_url_v)

                # if the request is successful, the last published timestep is the current timestep
                if response.status_code is requests.codes.ok:
                    return current_timestep

            # if the request is not successful, move to the previous timestep
            current_timestep -= 1

    def get_last_published_date(self, **kwargs) -> dt.datetime:
        return self.get_last_published_ts(**kwargs).end

    def _get_data_ts(self,
                     time_range: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str,
                     **kwargs) -> Iterable[tuple[xr.DataArray, dict]]:
        
        """
        Get the data for a specific timestep.
        """
        for variable in self.variables:
            yield from self._get_data_ts_singlevar(time_range, space_bounds, tmp_path, variable, **kwargs)

    def _get_data_ts_singlevar(
                        self,
                        time_range: TimeStep,
                        space_bounds: BoundingBox,
                        tmp_path: str,
                        variable: str,
                        **kwargs) -> Iterable[tuple[xr.DataArray, dict]]:
        ''' Get the data for a specific timestep and variable. '''

        # Get the URL without version
        url_blank = self.url_blank.format(
            ts_str=self.ts_str,
            timestep=time_range,
            var=variable,
            version="{version}"
        )

        # Download the file
        ts_end = time_range.end
        tmp_filename_raw = f'temp_{self.product}{variable}_{ts_end:%Y%m%d}.tif'
        tmp_destination = os.path.join(tmp_path, tmp_filename_raw)

        # try to download the file in both versions
        success = False
        for version in self.versions:
            url_v = url_blank.format(version=version)

            response = requests.head(url_v)

            if response.status_code is requests.codes.ok:
                url = url_v
                success = True
                break

        if success:
            download_http(url, tmp_destination)

            # Crop the data
            cropped = crop_to_bb(tmp_destination, space_bounds)

            # Change the nodata value to np.nan and return the data
            cropped = cropped.where(~np.isclose(cropped, self.nodata, equal_nan=True), np.nan)
            cropped.rio.no_data = np.nan

            # Apply the scale factor
            cropped *= self.scale_factor

            yield cropped, {'variable': variable}

        else:
            # If the loop ends without breaking, the data is missing
            handle_missing('warning', {'timestep': time_range, 'variable': variable})

    def get_data(self,
                 time_range: ts.TimeRange|Sequence[dt.datetime],
                 space_bounds:  Optional[BoundingBox] = None,
                 destination: Optional[Dataset|dict|str] = None,
                 options:  Optional[dict] = None) -> None:
        """
        Get data from this downloader and saves it to a file
        """

        # get options and check them against the default options
        if options is not None: 
            self.set_options(options)
        
        # Set ts_str based on the ts_per_year
        if self.ts_per_year == 36:
            self.ts_str = '10'
        elif self.ts_per_year == 365:
            self.ts_str = ''
        else:
            raise ValueError(f"ts_per_year {self.ts_per_year} not supported")
        
        super().get_data(time_range, space_bounds, destination)