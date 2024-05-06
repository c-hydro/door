import datetime as dt
from typing import Optional
import tempfile
import os
import xarray as xr
import numpy as np
import cfgrib

from .cds_downloader import CDSDownloader
from ...utils.space import BoundingBox
from ...utils.time import TimeRange
from ...utils.netcdf import save_netcdf
from ...utils.geotiff import save_array_to_tiff
from ...utils.parse import format_string

import logging
logger = logging.getLogger(__name__)

class ERA5Downloader(CDSDownloader):

    available_products = ['reanalysis-era5-single-levels', 'reanalysis-era5-land']
    available_variables = {'total_precipitation': 'tp',
                           '2m_temperature': 't2m',
                           'volumetric_soil_water_layer_1': 'swvl1',
                           'volumetric_soil_water_layer_2': 'swvl2',
                           'volumetric_soil_water_layer_3': 'swvl3',
                           'volumetric_soil_water_layer_4': 'swvl4',}
    
    default_options = {
        'variables':         'total_precipitation',
        'output_format':     'netcdf',   # one of netcdf, GeoTIFF
        'aggregate_in_time':  None,      # one of 'mean', 'max', 'min', 'sum'
        'timesteps_per_year': 365,       # the number of timesteps per year to split the download over #365=daily, 12=monthly, 36=10-daily
        'n_processes':        1,         # number of parallel processes to use
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
        if product not in self.available_products:
            msg = f'Product {product} not available for ERA5'
            logger.error(msg)
            raise ValueError(msg)

        super().__init__(product)

    def check_options(self, options: dict) -> dict:
        options = super().check_options(options)

        if 'necdf' in options['output_format'].lower():
            options['output_format'] = 'netcdf'
        elif 'tif' in options['output_format'].lower():
            options['output_format'] = 'GeoTIFF'
        else:
            logger.warning(f'Output format {options["output_format"]} not supported. Using netcdf')
            options['output_format'] = 'netcdf'

        if options['aggregate_in_time'] is not None and not isinstance(options['aggregate_in_time'], list):
            options['aggregate_in_time'] = [options['aggregate_in_time']]

        for aggregation in options['aggregate_in_time']:
            if aggregation not in ['mean', 'max', 'min', 'sum']:
                logger.warning(f'Unknown method {aggregation}, won\'t aggregate')
                options['aggregate_in_time'].remove(aggregation)

        for variable in options['variables']:
            if variable not in self.available_variables:
                logger.warning(f'Variable {variable} not available for ERA5 or not implemented/tested, removing from list')
                options['variables'].remove(variable)

        return options

    def build_request(self,
                      variables:list[str]|str,
                      time:TimeRange,
                      space_bounds:BoundingBox) -> dict:
        """
        Make a request for the CDS API.
        """
        if isinstance(variables, str):
            variables = [variables]

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

    def get_singledata(self,
                       step_range: TimeRange,
                       space_bounds: BoundingBox,
                       destination: str,
                       options: Optional[dict] = None) -> None:

        # Check options
        options = self.check_options(options)
        self.variables = options['variables']

        self.aggregations = options['aggregate_in_time']

        timestep_start = step_range.start #timesteps[i]
        timestep_end   = step_range.end #timesteps[i+1] - dt.timedelta(days=1)
        #logger.info(f' - Block {i+1}/{ntimesteps}: starting at {timestep_start:%Y-%m-%d}')

        # Do all of this inside a temporary folder
        tmpdirs = os.path.join(os.getenv('HOME'), 'tmp')
        os.makedirs(tmpdirs, exist_ok=True)
        with tempfile.TemporaryDirectory(dir = tmpdirs) as tmp_path:
            self.working_path = tmp_path

            logger.debug(f' ----> Downloading data')

            tmp_filename = f'temp_{self.dataset}_{timestep_start:%Y%m%d}-{timestep_end:%Y%m%d}.grib2'
            tmp_destination = os.path.join(tmp_path, tmp_filename)

            # If in the variable list we have total precipitation, we need to download the data for the next day as well
            # we need to add 1 day to the end time to get all the data for the last day (23h-00h is included in the next day's data)
            if 'total_precipitation' in self.variables:
                request_end = timestep_end + dt.timedelta(days=1)
            else:
                request_end = timestep_end

            request = self.build_request(self.variables, TimeRange(timestep_start, request_end), space_bounds)
            self.download(request, tmp_destination, min_size = 200,  missing_action = 'w')

            # if the download fail interrupt
            if not os.path.exists(tmp_destination):
                logger.error(f'  -> Download failed, skipping block')
                return

            # this will create a list of xarray datasets, one for each "well-formed" cube in the grib file,
            # this is needed because requesting multiple variables at once will return a single grib file that might contain multiple cubes
            # (if the variable have different dimensions)
            all_data = cfgrib.open_datasets(tmp_destination)

            for var in self.variables:
                logger.debug(f' --> Variable {var}')
                varname = self.available_variables[var]

                # get the data for the variable from the list of datasets
                for this_data in all_data:
                    if varname in this_data:
                        data = this_data
                        break

                # check if we are using any preliminary data, or if it is all final
                if 'expver' in data.dims:
                    logger.warning('  -> Some of the data is preliminary, we will use the final version where available')
                    data_final  = data.sel(expver=1)
                    data_prelim = data.sel(expver=5)

                    data = xr.where(np.isnan(data_final), data_prelim, data_final)

                vardata = data[varname]

                # We need to handle the time dimention:

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

                vardata = vardata.assign_coords(time=valid_times)

                # filter data to the selected days (we have to do this because the API returns data for 36 hours)
                inrange = (vardata.time.dt.date >= timestep_start.date()) & (vardata.time.dt.date <= timestep_end.date())
                vardata = vardata.sel(time = inrange)

                if varname == 't2m':
                    # Convert Kelvin to Celsius
                    vardata = vardata - 273.15

                # remove non needed dimensions
                vardata = vardata.squeeze()

                # verify that we have all the data we need (i.e. no timesteps of complete nans)!
                time_to_check = timestep_start
                while time_to_check <= timestep_end:
                    istoday = vardata.time.dt.date == time_to_check.date()
                    this_data = vardata.sel(time = istoday)
                    for time in this_data.time:
                        if this_data.sel(time = time).isnull().all():
                            logger.error(f'  -> Missing data for {var} at time {time:%Y-%m-%d %H:%M}')
                            raise ValueError(f'Missing data for {var} at time {time:%Y-%m-%d %H:%M}')

                    time_to_check += dt.timedelta(days=1)

                # add start and end time as attributes
                vardata.attrs['start_time'] = timestep_start
                vardata.attrs['end_time'] = timestep_end

                for agg in self.aggregations:
                    vardata.attrs['agg_function'] = agg
                    if agg == 'mean':
                        logger.debug(f' ----> Aggregating data to mean')
                        aggdata = vardata.mean(dim='time')
                    elif agg == 'max':
                        logger.debug(f' ----> Aggregating data to max')
                        aggdata = vardata.max(dim='time')
                    elif agg == 'min':
                        logger.debug(f' ----> Aggregating data to min')
                        aggdata = vardata.min(dim='time')
                    elif agg == 'sum':
                        logger.debug(f' ----> Aggregating data to sum')
                        aggdata = vardata.sum(dim='time')
                    else:
                        logger.debug(f' ----> No aggregation needed')
                        aggdata = vardata

                    aggdata = aggdata.rio.set_spatial_dims('longitude', 'latitude')
                    aggdata = aggdata.rio.write_crs(self.spatial_ref)

                    out_name = format_string(destination, {'variable': var})
                    out_name = format_string(out_name, {'aggregation': agg})
                    out_name = timestep_end.strftime(out_name)

                    if options['output_format'] == 'GeoTIFF':
                        logger.debug(f' ----> Saving to GeoTIFF')
                        save_array_to_tiff(aggdata, out_name)
                    else:
                        logger.debug(f' ----> Saving to netcdf')
                        save_netcdf(aggdata, out_name)

    def get_data(self,
                 time_range: TimeRange,
                 space_bounds: BoundingBox,
                 destination: str,
                 options: Optional[dict] = None) -> None:

        # Check options
        options = self.check_options(options)

        # set the bounding box to EPSG:4326
        space_bounds.transform('EPSG:4326')

        logger.info(f'------------------------------------------')
        logger.info(f'Starting download of {self.dataset} data from {self.name}')
        logger.info(f'Data requested between {time_range.start:%Y-%m-%d} and {time_range.end:%Y-%m-%d}')
        logger.info(f'Bounding box: {space_bounds.bbox}')
        logger.info(f'------------------------------------------')

        # Get the timesteps to download
        timesteps = time_range.get_timesteps_from_tsnumber(options['timesteps_per_year'], get_end = True)
        ntimesteps = len(timesteps) - 1

        # Download the data for the specified issue times
        logger.info(f'Found {ntimesteps} blocks of data to download.')

        if options['n_processes'] > 1:
            import multiprocessing
            ds = self.dataset
            step_ranges = [TimeRange(timesteps[i], timesteps[i+1] - dt.timedelta(days=1)) for i in range(0, ntimesteps)]
            with multiprocessing.Pool(options['n_processes']) as p:
                p.starmap(get_singledata, [(ds,step_ranges[i],space_bounds, destination, options) for i in range(0, ntimesteps)])

        else:
            for i in range(0, ntimesteps):
                step_range = TimeRange(timesteps[i], timesteps[i+1] - dt.timedelta(days=1))
                self.get_singledata(step_range, space_bounds, destination, options)
                
            logger.info(f'  -> SUCCESS! Data for {len(self.variables)} variables downloaded.')
        logger.info(f'------------------------------------------')

def get_singledata(dataset, step_range, space_bounds, destination, options):
    downloader = ERA5Downloader(dataset)
    downloader.get_singledata(step_range, space_bounds, destination, options)