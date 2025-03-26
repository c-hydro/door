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

class JRADownloader(URLDownloader):
    source = "JRA"
    name = "JRA_downloader"

    single_temp_folder = True
    separate_vars = True

    default_options = {
        "resolution": 0.375,
        "freq" : 'd',
        'variables'   : 'precipitation',
        'agg_method'  : None
    }

    grid_codes = {
        0.375 : 'gauss',
        1.25  : 'll125'
    }

    home = "https://thredds.rda.ucar.edu/thredds/fileServer/files/g/d640000/"
    

    available_agg_methods = ['mean', 'max', 'min', 'sum']

    available_products: dict = {
        "jra-3q": {
            "url_blank" : home + "{dataset}/{month.start:%Y%m}/jra3q..{var_code}.{var_name}-{grid_code}.{month.start:%Y%m%d}00_{month.end:%Y%m%d}23.nc",
            "data_list" : "https://thredds.rda.ucar.edu/thredds/catalog/files/g/d640000/{dataset}/catalog.html"
        }
    }

    available_variables: dict = {
        "jra-3q": {
            "precipitation": {
                "dataset" : 'fcst_phy2m',
                "var_code" : '0_1_52',
                "var_name" : "tprate1have-sfc-fc", # this is a rate in mm/s, will need to multiply by 3600 to get mm/h and then sum to get total precipitation
                "agg_method" : 'sum'
            }
        }
    }

    def __init__(self, product: str) -> None:
        self.set_product(product)
        super().__init__(self.url_blank, protocol = 'http')

    def set_variables(self, variables: str|list[str]) -> None:
        """
        Set the variables to download.
        """
        if isinstance(variables, str):
            variables = [variables]
        super().set_variables(variables)
 
        agg_options = self.agg_method
        if not isinstance(agg_options, list):
            agg_options = [agg_options]

        if len(agg_options) != len(variables):
            msg = 'The number of aggregation methods must be the same as the number of variables'
            self.log.error(msg)
            raise ValueError(msg)
        
        for agg, var in zip(agg_options, variables):
            agg = self.check_agg(agg)
            self.variables[var].update({'agg_method': agg})

    def check_agg(self, agg):
        if not isinstance(agg, list): agg = [agg]
        for a in agg:
            if a not in self.available_agg_methods:
                msg = f'Aggregation method {a} not available'
                self.log.error(msg)
                raise ValueError(msg)
        return agg

    def get_last_published_ts(self, **kwargs) -> ts.TimeRange:
        
        """
        Get the last published date for the dataset.
        """

        last_date = self.get_last_published_date(**kwargs)

        # get the timestep of the last date
        freq = self.freq if hasattr(self, 'freq') else 'd'
        last_date_timestep = ts.TimeStep.from_unit(freq).from_date(last_date)

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
        import re
        last_month = None
        for variable in self.variables:
            if 'dataset' not in self.variables[variable]:
                raise ValueError(f'Dataset not defined for variable {variable}')
            
            url = self.data_list.format(dataset = self.variables[variable]['dataset'])
            with requests.get(url) as response:
                # this is 100% not the best way to do this, but it works for now
                matches = re.findall(r'href="(\d{4})(\d{2})/catalog.html"', response.text)
            
            this_last_month = ts.Month(int(matches[-1][0]), int(matches[-1][1]))
            last_month = this_last_month if last_month is None else min(last_month, this_last_month)

        return last_month.end

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
        
        this_var = self.variables[self.variable]
        this_month = ts.Month(timestep.year, timestep.month)
        tmp_file_nc = f'temp_{self.product}{this_month.year}{this_month.month}.nc'

        # check if the file is not already downloaded in the tmp_path
        tmp_destination = os.path.join(tmp_path, tmp_file_nc)
        if not os.path.exists(tmp_destination):
            tags = {
                'dataset' : this_var['dataset'],
                'var_code' : this_var['var_code'],
                'var_name' : this_var['var_name'],
                'grid_code' : self.grid_codes[self.resolution],
                'month' : this_month
            }
            # download the file
            self.download(tmp_destination, min_size = 2000, missing_action = 'warning', **tags)
        
        # open the file
        raw_data = xr.open_dataset(tmp_destination, engine = 'netcdf4')
        vardata = raw_data[f"{this_var['var_name']}-{self.grid_codes[self.resolution]}"]

        # only select the relevant time range
        inrange = (vardata.time.dt.date >= timestep.start.date()) & (vardata.time.dt.date <= timestep.end.date())
        vardata = vardata.sel(time = inrange)

        # crop the data
        cropped = crop_to_bb(vardata, space_bounds)

        # if this is precipitation data, we need to transform it to mm/h
        if this_var['var_name'] == 'tprate1have-sfc-fc':
            cropped *= 3600

        # aggregate the data
        for agg_method in this_var['agg_method']:
            if agg_method == 'sum':
                aggregated = cropped.sum(dim = 'time')
            elif agg_method == 'mean':
                aggregated = cropped.mean(dim = 'time')
            elif agg_method == 'max':
                aggregated = cropped.max(dim = 'time')
            elif agg_method == 'min':
                aggregated = cropped.min(dim = 'time')
            else:
                raise ValueError(f'Aggregation method {self.agg_method} not recognized')

            yield aggregated, {'agg_method': agg_method, 'variable': self.variable, 'resolution': str(self.resolution).replace('.', '')}