import os
from typing import Generator, Optional, Sequence
import xarray as xr
import datetime as dt
import requests
import tempfile

from ...base_downloaders import URLDownloader

from d3tools import timestepping as ts
from d3tools.timestepping.timestep import TimeStep
from d3tools.timestepping.fixed_num_timestep import FixedNTimeStep
from d3tools.spatial import BoundingBox, crop_to_bb

class NOAADownloader(URLDownloader):
    source = "NOAA"
    name = "NOAA_downloader"

    single_temp_folder = True

    default_options = {
        "ts_per_year": 365
    }

    home = "https://psl.noaa.gov/thredds/fileServer/Datasets/"

    available_products: dict = {
        "cpc_global_precip": {
            "url_blank" : home + 'cpc_global_precip/precip.{year}.nc',
            "nodata" : -9999,
            "varname" : "precip",
            "agg_method" : "sum",
            "metadata" : "https://psl.noaa.gov/thredds/iso/Datasets/cpc_global_precip/precip.{year}.nc?catalog=http://psl.noaa.gov/thredds/catalog/Datasets/cpc_global_precip/catalog.html&dataset=Datasets/cpc_global_precip/precip.{year}.nc"
        }
    }

    def __init__(self, product: str) -> None:
        self.set_product(product)
        super().__init__(self.url_blank, protocol = 'http')

    def set_product(self, product: str) -> None:
        self.product = product.lower()
        if self.product not in self.available_products:
            raise ValueError(f'Product {product} not available. Choose one of {self.available_products.keys()}')
        for key in self.available_products[self.product]:
            setattr(self, key, self.available_products[self.product][key])

    def get_last_published_ts(self, **kwargs) -> ts.TimeRange:
        
        """
        Get the last published date for the dataset.
        """

        last_date = self.get_last_published_date(**kwargs)

        # get the timestep of the last date
        ts_per_year = self.ts_per_year if hasattr(self, 'ts_per_year') else 365
        last_date_timestep = FixedNTimeStep(last_date, ts_per_year)

        # if the last date is the last day of its timestep, return the last timestep
        if last_date == last_date_timestep.end:
            return last_date_timestep
        # else, return the timestep before the one of the last date
        else:
            return last_date_timestep - 1

    def get_last_published_date(self, **kwargs) -> dt.datetime:

        """
        Get the last published date for the dataset.
        """
        import xml.etree.ElementTree as ET

        year = dt.datetime.now().year
        with requests.get(self.metadata.format(year = year)) as response:
            root = ET.fromstring(response.content)

        # Find the gml:endPosition element
        end_position = root.find('.//gml:endPosition', namespaces={'gml': 'http://www.opengis.net/gml/3.2'})
        if end_position is not None:
            end_date = end_position.text

        # Convert to datetime object if needed
        end_date_dt = dt.datetime.fromisoformat(end_date.replace('Z', '+00:00')).date()
        return end_date_dt

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
        

        year = timestep.year
        tmp_file_nc = f'temp_{self.product}{year}.nc'

        # check if the file is not already downloaded in the tmp_path
        tmp_destination = os.path.join(tmp_path, tmp_file_nc)
        if not os.path.exists(tmp_destination):
            # download the file
            self.download(tmp_destination, min_size = 2000, missing_action = 'warning', year = year)
        
        # open the file
        raw_data = xr.open_dataset(tmp_destination, engine = 'netcdf4')
        vardata = raw_data[self.varname]

        # only select the relevant time range
        inrange = (vardata.time.dt.date >= timestep.start.date()) & (vardata.time.dt.date <= timestep.end.date())
        vardata = vardata.sel(time = inrange)

        # crop the data
        cropped = crop_to_bb(vardata, space_bounds)

        # aggregate the data
        if self.agg_method == 'sum':
            aggregated = cropped.sum(dim = 'time')
        elif self.agg_method == 'mean':
            aggregated = cropped.mean(dim = 'time')
        else:
            raise ValueError(f'Aggregation method {self.agg_method} not recognized')

        yield aggregated, {}