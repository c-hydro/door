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

    home = "https://psl.noaa.gov/thredds/"

    available_products: dict = {
        "cpc_global_precip": {
            "url_blank" : home + 'dodsC/Datasets/cpc_global_precip/precip.{year}.nc',
            "nodata" : -9999,
            "varname" : "precip",
            "agg_method" : "sum"        }
    }

    cached_data = None

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
        if hasattr(self, 'ts_per_year'):
            last_date_timestep = FixedNTimeStep.get_subclass(self.ts_per_year).from_date(last_date)
        else:
            last_date_timestep = ts.Day.from_date(last_date)

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

        this_year = dt.datetime.now().year
        while this_year >= 2025:
            # open the yearly file
            url = self.format_url(year = this_year)
            if requests.head(url + ('.html')).status_code == 200:
                raw_data = xr.open_dataset(url, engine = 'netcdf4')
                self.cached_data = {this_year: raw_data}
                break
            this_year -= 1
        
        vardata = raw_data[self.varname]
        end_date = vardata.time.values[-1]

        # Convert to datetime object if needed
        end_date_dt = dt.datetime.fromtimestamp(end_date.astype('datetime64[s]').astype(int), tz=dt.timezone.utc)
        return end_date_dt

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
        

        this_year = timestep.year

        if self.cached_data is not None and this_year in self.cached_data:
            raw_data = self.cached_data[this_year]
        else:
            # open the yearly file
            url = self.format_url(year = this_year)
            raw_data = xr.open_dataset(url, engine = 'netcdf4')
            self.cached_data = {this_year: raw_data}
        
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