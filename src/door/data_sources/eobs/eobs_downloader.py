import os
from typing import Generator, Optional, Sequence
import numpy as np
import xarray as xr
import datetime as dt
import requests
import tempfile

from ...base_downloaders import URLDownloader

from d3tools import timestepping as ts
from d3tools.timestepping.timestep import TimeStep
from d3tools.timestepping.fixed_num_timestep import FixedNTimeStep
from d3tools.spatial import BoundingBox, crop_to_bb
from d3tools.data import Dataset

class EOBSDownloader(URLDownloader):
    source = "E-OBS"
    name   = "E-OBS_downloader"

    default_options = {
        "resolution"  : 0.1,
        "variables"   : ['rr'],
    }

    single_temp_folder = True
    separate_vars      = True

    # daily data, monthly files
    #    months/ens/rr_0.1deg_day_2024_12_grid_ensmean.nc

    # daily data, 15-year files
    #    Grid_0.1deg_reg_ensemble/rr_ens_mean_0.1deg_reg_2011-2024_v30.0e.nc
    #    Grid_0.1deg_reg_ensemble/tg_ens_mean_0.1deg_reg_1995-2010_v30.0e.nc
    #    Grid_0.25deg_reg_ensemble/tn_ens_mean_0.25deg_reg_1980-1994_v30.0e.nc

    home = "https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/"

    # data for the current year can be downloaded in monthly chunks from here
    month_url    = home + 'months/ens/{variable}_{resolution}deg_day_{year}_{month:02}_grid_ensmean.nc'

    # for previous years, the data is stored in 15-year chunks
    longterm_url = home + 'Grid_{resolution}deg_reg_ensemble/{variable}_ens_mean_{resolution}deg_reg_{tr.start.year}-{tr.end.year}_{version}.nc'
    longterm_timeranges = [
        ts.TimeRange(dt.datetime(1950, 1, 1), dt.datetime(1964, 12, 31)),
        ts.TimeRange(dt.datetime(1965, 1, 1), dt.datetime(1979, 12, 31)),
        ts.TimeRange(dt.datetime(1980, 1, 1), dt.datetime(1994, 12, 31)),
        ts.TimeRange(dt.datetime(1995, 1, 1), dt.datetime(2010, 12, 31)),
        ts.TimeRange(dt.datetime(2011, 1, 1), dt.datetime(2024,  6, 30))
    ]

    # the current version of the dataset is v30.0e, this seems to be updated every 6 months
    version = 'v30.0e'

    # available variables
    available_variables = ['rr', # precipitation
                           'tg', # temperature mean
                           'tn', # temperature min
                           'tx', # temperature max
                          ]

    # frequency is daily
    ts_per_year = 365

    #available_resolutions
    available_resolutions = [0.1, 0.25]

    def __init__(self) -> None:
        self.url_blank = self.month_url
        super().__init__(self.url_blank, protocol = 'http')

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

        last_date = self.get_last_published_date(**kwargs)

        # get the timestep of the last date
        last_date_timestep = ts.Day.from_date(last_date)

        return last_date_timestep

    def get_last_published_date(self, **kwargs) -> dt.datetime:

        """
        Get the last published date for the dataset.
        """

        this_month = ts.Month.from_date(dt.datetime.now())
        has_data = False
        while not has_data:
            try:
                url = self.month_url.format(variable = self.variables[0], resolution = self.resolution, year = this_month.year, month = this_month.month)
                r = requests.head(url)
                r.raise_for_status()
                has_data = True
            except requests.exceptions.HTTPError:
                this_month -= 1

        return this_month.end

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
        
        while True:

            # first check if either the month nc or the longterm nc file is already downloaded
            relevant_month_url = self.month_url.format(variable = self.variable, resolution = self.resolution, year = timestep.year, month = timestep.month)
            relevant_month_nc  = os.path.basename(relevant_month_url)
            month_tmp_file     = os.path.join(tmp_path, relevant_month_nc)

            if os.path.exists(month_tmp_file):
                raw_data = xr.open_dataset(month_tmp_file, engine = 'netcdf4')
                break

            # if the month file is not downloaded, check if the longterm file is
            relevant_longterm_tr = [tr for tr in self.longterm_timeranges if tr.contains(timestep.start)]

            if len(relevant_longterm_tr) > 0:
                relevant_longterm_url = self.longterm_url.format(variable = self.variable,
                                                                 resolution = self.resolution,
                                                                 tr = relevant_longterm_tr[0],
                                                                 version = self.version)
                relevant_longterm_nc  = os.path.basename(relevant_longterm_url)
                longterm_tmp_file     = os.path.join(tmp_path, relevant_longterm_nc)

                if os.path.exists(longterm_tmp_file):
                    raw_data = xr.open_dataset(longterm_tmp_file, engine = 'netcdf4')
                    break
            
            # if neither the month nor the longterm file is downloaded, try downloading the month file
            self.url_blank = relevant_month_url
            success = self.download(month_tmp_file, min_size = 2000, missing_action = 'ignore')
            if success:
                raw_data = xr.open_dataset(month_tmp_file, engine = 'netcdf4')
                break

            # if the month file is not available, try downloading the longterm file
            if len(relevant_longterm_tr) > 0:
                self.url_blank = relevant_longterm_url
                success = self.download(longterm_tmp_file, min_size = 2000, missing_action = 'warning')
                if success:
                    raw_data = xr.open_dataset(longterm_tmp_file, engine = 'netcdf4')
                    break

            # if neither the month nor the longterm file is available, raise an error
            raise FileNotFoundError(f'Neither the month nor the longterm file is available for timestep {timestep}')

        # breakpoint()
        vardata = raw_data[self.variable]

        # only select the relevant time range
        inrange = (vardata.time.dt.date >= timestep.start.date()) & (vardata.time.dt.date <= timestep.end.date())
        vardata = vardata.sel(time = inrange)

        # # aggregate the data
        # if self.agg_method == 'sum':
        #     vardata = vardata.sum(dim = 'time')
        # elif self.agg_method == 'mean':
        #     vardata = vardata.mean(dim = 'time')
        # else:
        #     raise ValueError(f'Aggregation method {self.agg_method} not recognized')

        # crop the data
        cropped = crop_to_bb(vardata, space_bounds)

        yield cropped, {"variable" : self.variable, "resolution" : self.resolution}