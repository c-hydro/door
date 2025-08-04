import cdsapi
import datetime as dt
from typing import Generator
import xarray as xr
import cfgrib

import d3tools.timestepping as ts
from d3tools.spatial import BoundingBox

from ...base_downloaders import APIDownloader

import os

class CDSDownloader(APIDownloader):

    name = "CDS_downloader"
    apikey_env_vars = 'CDSAPI_KEY' # this should be in the form UID:API_KEY already

    def __init__(self, dataset) -> None:

        # if key is None, this will automatically look for the .cdsapirc file
        key = os.getenv(self.apikey_env_vars, None)
        if isinstance(key, str):
            if key.startswith("'" or '"') and key.endswith("'" or '"'):
                key = key[1:-1]
        client = cdsapi.Client(url=self.cds_url, key=key)
        
        super().__init__(client)
        self.dataset = dataset

    def download(self, request: dict, destination: str,
                 min_size: float = None, missing_action: str = 'error') -> None:
        """
        Downloads data from the CDS API based on the request.
        dataset: the name of the dataset to download from
        request: a dictionary with the request parameters
        output: the name of the output file
        """
        return super().download(destination, min_size, missing_action, name = self.dataset, request = request, target = destination)

    def get_last_published_ts(self, ts_per_year = None, **kwargs) -> ts.TimeRange:
        
        """
        Get the last published date for the dataset.
        """
        if ts_per_year is None:
            ts_per_year = self.ts_per_year

        # get the last published timestep
        last_published = self.get_last_published_date()
        if ts_per_year == 365:
            TimeStep = ts.Day
        else:
            TimeStep = ts.fixed_num_timestep.FixedNTimeStep.get_subclass(ts_per_year)
        return TimeStep.from_date(last_published + dt.timedelta(days=1)) - 1
    
    def build_request(self,
                      time:ts.TimeRange,
                      space_bounds:BoundingBox) -> dict:
        """
        Make a request for the CDS API.
        """
        variables = [var for var in self.variables.keys()]

        # get the correct timesteps
        start = time.start
        end = time.end

        years = set()
        months = set()
        days = set()

        this_time = start
        while this_time <= end:
            years.add(this_time.year)
            months.add(this_time.month)
            days.add(this_time.day)
            this_time += dt.timedelta(days=1)

        years_str = [str(y) for y in years]
        months_str = [str(m).zfill(2) for m in months]
        days_str = [str(d).zfill(2) for d in days]

        # Get the bounding box in the correct order
        W, S, E, N = space_bounds.bbox

        request = {
            'data_format': 'grib', # we always want grib, it's smaller, then we convert
            'download_format' : 'unarchived', #TODO: change this to "zip" and handle unzipping before opening the data!
            'variable': variables,
            'year' : years_str,
            'month': months_str,
            'day'  : days_str,
            'area': [N, W, S, E],
        }

        return request
    
    def _get_data_ts(self,
                     timestep: ts.TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:

        

        timestep_start = timestep.start
        timestep_end   = timestep.end

        tmp_filename = f'temp_{self.dataset}_{timestep_start:%Y%m%d}-{timestep_end:%Y%m%d}.grib2'
        tmp_destination = os.path.join(tmp_path, tmp_filename)


        request = self.build_request(timestep, space_bounds)
        success = self.download(request, tmp_destination, min_size = 100,  missing_action = 'e')

        # this will create a list of xarray datasets, one for each "well-formed" cube in the grib file,
        # this is needed because requesting multiple variables at once will return a single grib file that might contain multiple cubes
        # (if the variable have different dimensions)
        return cfgrib.open_datasets(tmp_destination)
    
    def _aggregate_variable(self, vardata, timestep, varopts):
        
        agg_timesteps = timestep.get_timesteps_from_tsnumber(self.ts_per_year_agg)

        for agg_timestep in agg_timesteps:

            timestep_start = agg_timestep.start
            timestep_end   = agg_timestep.end

            # filter data to the aggregation timestep
            inrange = (vardata.time.dt.date >= timestep_start.date()) & (vardata.time.dt.date <= timestep_end.date())
            vardata_ts = vardata.sel(time = inrange)

            # add start and end time as attributes
            vardata_ts.attrs['start_time'] = timestep_start
            vardata_ts.attrs['end_time']   = timestep_end

            # do the necessary aggregations:
            for agg in varopts['agg_method']:

                vardata_ts.attrs['agg_function'] = agg
                if agg == 'mean':
                    aggdata = vardata_ts.mean(dim='time', skipna = False)
                elif agg == 'max':
                    aggdata = vardata_ts.max(dim='time', skipna = False)
                elif agg == 'min':
                    aggdata = vardata_ts.min(dim='time', skipna = False)
                elif agg == 'sum':
                    aggdata = vardata_ts.sum(dim='time', skipna = False)

                aggdata = aggdata.rio.set_spatial_dims('longitude', 'latitude')
                aggdata = aggdata.rio.write_crs(self.spatial_ref)

                yield aggdata, {'variable': varopts.get('var'), 'agg_method': agg, 'timestep': agg_timestep}