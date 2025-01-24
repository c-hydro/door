import datetime as dt
import os
from typing import Generator
import xarray as xr
import numpy as np

from .cds_downloader import CDSDownloader

from d3tools.spatial import BoundingBox
from d3tools import timestepping as ts
from d3tools.timestepping.timestep import TimeStep
from d3tools.timestepping.fixed_num_timestep import FixedNTimeStep

class ERA5Downloader(CDSDownloader):

    source = "ERA5"
    name = "ERA5_downloader"

    available_products = ['reanalysis-era5-single-levels', 'reanalysis-era5-land']

    available_variables = {'total_precipitation': {'varname': 'tp',  'agg_method': 'sum'},
                           '2m_temperature':      {'varname': 't2m', 'agg_method': 'mean'},
                           'volumetric_soil_water_layer_1': {'varname': 'swvl1', 'agg_method': 'mean'},
                           'volumetric_soil_water_layer_2': {'varname': 'swvl2', 'agg_method': 'mean'},
                           'volumetric_soil_water_layer_3': {'varname': 'swvl3', 'agg_method': 'mean'},
                           'volumetric_soil_water_layer_4': {'varname': 'swvl4', 'agg_method': 'mean'}}
    
    available_agg_methods = ['mean', 'max', 'min', 'sum']
    
    default_options = {
        'variables'   : 'total_precipitation',
        'agg_method'  : None,
        'ts_per_year' : 12, # the number of timesteps per year to split the download over #365=daily, 12=monthly, 36=10-daily
        'ts_per_year_agg' : 365
    }

    spatial_ref =  'GEOGCRS["WGS 84",\
                    ENSEMBLE["World Geodetic System 1984 ensemble",\
                        MEMBER["World Geodetic System 1984 (Transit)"],\
                        MEMBER["World Geodetic System 1984 (G730)"],\
                        MEMBER["World Geodetic System 1984 (G873)"],\
                        MEMBER["World Geodetic System 1984 (G1150)"],\
                        MEMBER["World Geodetic System 1984 (G1674)"],\
                        MEMBER["World Geodetic System 1984 (G1762)"],\
                        MEMBER["World Geodetic System 1984 (G2139)"],\
                        ELLIPSOID["WGS 84",6378137,298.257223563,\
                            LENGTHUNIT["metre",1]],\
                        ENSEMBLEACCURACY[2.0]],\
                    PRIMEM["Greenwich",0,\
                        ANGLEUNIT["degree",0.0174532925199433]],\
                    CS[ellipsoidal,2],\
                        AXIS["geodetic latitude (Lat)",north,\
                            ORDER[1],\
                            ANGLEUNIT["degree",0.0174532925199433]],\
                        AXIS["geodetic longitude (Lon)",east,\
                            ORDER[2],\
                            ANGLEUNIT["degree",0.0174532925199433]],\
                    USAGE[\
                        SCOPE["Horizontal component of 3D system."],\
                        AREA["World."],\
                        BBOX[-90,-180,90,180]],\
                    ID["EPSG",4326]]'

    def __init__(self, product = 'reanalysis-era5-single-levels') -> None:
        super().__init__(product)

        if product not in self.available_products:
            msg = f'Product {product} not available for ERA5'
            self.log.error(msg)
            raise ValueError(msg)

    def set_variables(self, variables: list[str]) -> None:
        """
        Set the variables to download.
        """

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

        # If in the variable list we have total precipitation, we need to download the data for the next day as well
        if 'total_precipitation' in self.variables:
            end += dt.timedelta(days=1)
        
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
            'product_type': 'reanalysis',
            'format': 'grib', # we always want grib, it's smaller, then we convert
            'variable': variables,
            'year' : years_str,
            'month': months_str,
            'day'  : days_str,
            'time': [ # we always want all times in a day
                '00:00', '01:00', '02:00',
                '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00',
                '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00',
                '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00',
                '21:00', '22:00', '23:00',
            ],
        'area': [N, W, S, E],
        }

        return request
    
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
            TimeStep = FixedNTimeStep.get_subclass(ts_per_year)
        return TimeStep.from_date(last_published + dt.timedelta(days=1)) - 1

    def get_last_published_date(self, **kwargs) -> dt.datetime:
        now = dt.datetime.now()
        now = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return now - dt.timedelta(days=6)
         
    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:

        import cfgrib

        timestep_start = timestep.start
        timestep_end   = timestep.end

        tmp_filename = f'temp_{self.dataset}_{timestep_start:%Y%m%d}-{timestep_end:%Y%m%d}.grib2'
        tmp_destination = os.path.join(tmp_path, tmp_filename)


        request = self.build_request(timestep, space_bounds)
        success = self.download(request, tmp_destination, min_size = 100,  missing_action = 'e')

        # this will create a list of xarray datasets, one for each "well-formed" cube in the grib file,
        # this is needed because requesting multiple variables at once will return a single grib file that might contain multiple cubes
        # (if the variable have different dimensions)
        all_data = cfgrib.open_datasets(tmp_destination)

        # loop over the variables
        for var, varopts in self.variables.items():
            varname = varopts['varname']

            # find the data for the variable
            for this_data in all_data:
                if varname in this_data:
                    data = this_data
                    break

            # check if we are using any preliminary data, or if it is all final
            if 'expver' in data.dims:
                self.log.warning('  -> Some of the data is preliminary, we will use the final version where available')
                data_final  = data.sel(expver=1)
                data_prelim = data.sel(expver=5)
                data = xr.where(np.isnan(data_final), data_prelim, data_final)

            vardata = data[varname]

            #Handle the time dimension:
            # Valid times, is the value that we want to use for the time dimension.
            if varname == 'tp':
                # For precipitation, we remove 1 hour to make it easier to filter for the day later
                # the original time is the end time of the step so a step that ends at midnight is actally from the previous day
                valid_times = vardata.valid_time.values.flatten() - np.timedelta64(1, 'h')
            else:
                valid_times = vardata.valid_time.values.flatten() 

            # for some products we have a time and a step dimension, we need to combine them, for others we don't and we only have time
            if 'step' in vardata.dims:
                vardata = vardata.rename({'time': 'time_orig'})
                vardata = vardata.stack(time=('time_orig', 'step'))
                vardata = vardata.drop_vars(['time', 'time_orig', 'step'])

            vardata = vardata.assign_coords(time=valid_times)

            # filter data to the selected days (we have to do this because the API returns data for longer periods than we actually need)
            inrange = (vardata.time.dt.date >= timestep_start.date()) & (vardata.time.dt.date <= timestep_end.date())
            vardata = vardata.sel(time = inrange)

            # Convert Kelvin to Celsius if we are dealing with temperatures
            if varname == 't2m':
                vardata = vardata - 273.15

            # finally, remove non needed dimensions
            vardata = vardata.squeeze()

            # verify that we have all the data we need (i.e. no timesteps of complete nans)!
            time_to_check = timestep_start
            while time_to_check <= timestep_end:
                istoday = vardata.time.dt.date == time_to_check.date()
                this_data = vardata.sel(time = istoday)
                for time in this_data.time:
                    if this_data.sel(time = time).isnull().all():
                        self.log.error(f'  -> Missing data for {var} at time {time:%Y-%m-%d %H:%M}')
                        raise ValueError(f'Missing data for {var} at time {time:%Y-%m-%d %H:%M}')

                time_to_check += dt.timedelta(days=1)

            # remove all GRIB attributes
            for attr in vardata.attrs.copy():
                if attr.startswith('GRIB'):
                    del vardata.attrs[attr]

            ts_as_tr = ts.TimeRange(start = timestep_start, end = timestep_end)
            agg_timesteps = ts_as_tr.get_timesteps_from_tsnumber(self.ts_per_year_agg)

            for agg_timestep in agg_timesteps:
                timestep_start = agg_timestep.start
                timestep_end   = agg_timestep.end

                # filter data to the aggregation timestep
                inrange = (vardata.time.dt.date >= timestep_start.date()) & (vardata.time.dt.date <= timestep_end.date())
                vardata_ts = vardata.sel(time = inrange)

                # add start and end time as attributes
                vardata_ts.attrs['start_time'] = timestep_start
                vardata_ts.attrs['end_time'] = timestep_end

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

                    yield aggdata, {'variable': var, 'agg_method': agg, 'timestep': agg_timestep}