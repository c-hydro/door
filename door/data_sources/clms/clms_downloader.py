import os
import requests
import numpy as np
import xarray as xr
import tempfile
from typing import Optional, Iterable

from ...utils.space import BoundingBox, crop_to_bb
from ...utils.io import download_http, handle_missing
from ...tools import timestepping as ts
from ...tools.timestepping.timestep import TimeStep
from ...tools.data import Dataset
from ...base_downloaders import URLDownloader

class CLMSDownloader(URLDownloader):
    source = "CLMS"
    name = "CLMS_downloader"

    clms_url = "https://globalland.vito.be/download/"

    default_options = {
        'layers': None,
        'crop_to_bounds': True,
        'ts_per_year': 36
    }

    available_products = {
        'swi': {
            'versions': ["3.1.1", "3.2.1"],
            'url': clms_url + 'geotiff/soil_water_index/swi_12.5km_v3_{ts_str}daily/{timestep.start:%Y}/{timestep.start:%Y%m%d}/c_gls_SWI{ts_str}-SWI{layer}_{timestep.start:%Y%m%d}1200_GLOBE_ASCAT_V{version}.tiff',
            'nodata': 255,
            'scale_factor': 0.5,
            'available_layers': ["001", "005", "010", "020", "040", "060", "100"],
        }
    }

    def __init__(self, product: str) -> None:
        self.set_product(product)

    def set_product(self, product: str) -> None:
        self.product = product.lower()
        if self.product not in self.available_products:
            raise ValueError(f'Product {product} not available. Choose one of {self.available_products.keys()}')
        self.nodata = self.available_products[self.product]["nodata"]
        self.scale_factor = self.available_products[self.product]["scale_factor"]
        self.versions = self.available_products[self.product]["versions"]
        self.available_layers = self.available_products[self.product]["available_layers"]
        self.url_blank =  self.available_products[self.product]["url"]

    def _get_data_ts(self,
                     time_range: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str,
                     layer: str,
                     missing_action: str = 'warning',
                     **kwargs) -> Iterable[tuple[xr.DataArray, dict]]:
        ''' Get the data for a specific timestep. '''

        # Get the URL without version
        url_blank = self.url_blank.format(
            ts_str=self.ts_str,
            timestep=time_range,
            layer=layer,
            version="{version}"
        )

        # Download the file
        ts_end = time_range.end
        tmp_filename_raw = f'temp_{self.product}{layer}_{ts_end:%Y%m%d}.tif'
        tmp_destination = os.path.join(tmp_path, tmp_filename_raw)

        # try to download the file in both versions
        for version in self.versions:
            url = url_blank.format(version=version)
            try:
                download_http(url, tmp_destination)

                # Crop the data
                cropped = crop_to_bb(tmp_destination, space_bounds)

                # Change the nodata value to np.nan and return the data
                cropped = cropped.where(~np.isclose(cropped, self.nodata, equal_nan=True), np.nan)
                cropped.rio.no_data = np.nan

                # Apply the scale factor
                cropped *= self.scale_factor

                yield cropped, {'layer': layer}
                break

            except Exception as e:
                continue

        # If the loop ends without breaking, the data is missing
        handle_missing(missing_action, {'timestep': time_range, 'layer': layer})

    def get_data(self,
                 time_range: ts.TimeRange,
                 space_bounds:  Optional[BoundingBox] = None,
                 destination: Optional[Dataset|dict|str] = None,
                 options:  Optional[dict] = None) -> None:
        """
        Get data from this downloader and saves it to a file
        """
        # get options and check them against the default options
        if options is not None:
            self.set_options(options)

        # Check if the layers are available
        if self.layers is None:
            self.layers = self.available_layers
        else:
            for layer in self.layers:
                if layer not in self.available_layers:
                    raise ValueError(f'Layer {layer} not available. Choose one of {self.available_layers}')

        # Set ts_str based on the ts_per_year
        if self.ts_per_year == 36:
            self.ts_str = '10'
        elif self.ts_per_year == 365:
            self.ts_str = ''
        else:
            raise ValueError(f"ts_per_year {self.ts_per_year} not supported")

        # set the space bounds
        if space_bounds is None:
            if hasattr(self, 'bounds'):
                space_bounds = self.bounds
            else:
                raise ValueError('No space bounds specified')

        if destination is not None:
            self.set_destination(destination)

        if hasattr(self, 'destination'):
            destination = self.destination
        else:
            raise ValueError('No destination specified')

        # get the timesteps to download
        timesteps = self._get_timesteps(time_range)

        for timestep in timesteps:
            with tempfile.TemporaryDirectory() as tmp_path:
                for layer in self.layers:
                    data_struct = self._get_data_ts(timestep, space_bounds, tmp_path, layer)
                    if not data_struct:
                        self.log.warning(f'No data found for timestep {timestep}')
                        continue
                    for data, tags in data_struct:
                        if 'timestep' in tags:
                            timestep = tags.pop('timestep')
                        destination.write_data(data, timestep, **tags)