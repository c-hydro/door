import datetime as dt
from typing import Optional
import tempfile
import os
import xarray as xr
import numpy as np

from .cds_downloader import CDSDownloader
from ...utils.space import BoundingBox
from ...utils.time import TimeRange
from ...utils.netcdf import save_netcdf
from ...utils.geotiff import save_array_to_tiff
from ...utils.parse import format_string

import logging
logger = logging.getLogger(__name__)

class ERA5Downloader(CDSDownloader):

    available_products = ['reanalysis-era5-single-levels']
    available_variables = {'total_precipitation': 'tp'}
    default_options = {
        'variables':         'total_precipitation',
        'output_format':     'netcdf', # one of netcdf, GeoTIFF
        'aggregate_in_time':  None,    # one of 'mean', 'max', 'min', 'sum'
        'timesteps_per_year': 365,     # the number of timesteps per year to split the download over #365=daily, 12=monthly, 36=10-daily
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

        if 'tif' in options['output_format'].lower():
            options['output_format'] = 'GeoTIFF'
        else:
            logger.warning(f'Output format {options["output_format"]} not supported. Using netcdf')
            options['output_format'] = 'netcdf'
        
        if options['aggregate_in_time'] is not None and options['aggregate_in_time'].lower() not in ['mean', 'max', 'min', 'sum']:
            logger.warning(f'Unknown method {options["aggregate_in_time"]}, won\'t aggregate')
            options['aggregate_in_time'] = None

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

        if start.year != end.year:
            msg = f'Request is split between multiple years, this should not happen'
            logger.debug(msg)
            raise ValueError(msg)
        elif start.month != end.month:
            msg = f'Request is split between multiple months, this should not happen'
            logger.debug(msg)
            raise ValueError(msg)
        
        year = str(start.year)
        month = str(start.month).zfill(2)
        days = [start.day + i for i in range((end-start).days + 1)]
        days = [str(d).zfill(2) for d in days]

        # Get the bounding box in the correct order
        W, S, E, N = space_bounds.bbox

        request = {
            'product_type': 'reanalysis',
            'format': 'grib', # we always want grib, it's smaller, then we convert
            'variable': variables,
            'year' : year,
            'month': month,
            'day'  : days,
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
    
    def get_data(self,
                 time_range: TimeRange,
                 space_bounds: BoundingBox,
                 destination: str,
                 options: Optional[dict] = None) -> None:

        # Check options
        options = self.check_options(options)

        # set the bounding box to EPSG:4326
        space_bounds.transform('EPSG:4326')

        self.variables = options['variables']
        
        logger.info(f'------------------------------------------')
        logger.info(f'Starting download of {self.dataset} data from {self.name}')
        logger.info(f'Data requested between {time_range.start:%Y-%m-%d %H:%M} and {time_range.end:%Y-%m-%d %H:%m}')
        logger.info(f'Bounding box: {space_bounds.bbox}')
        logger.info(f'------------------------------------------')

        # Get the timesteps to download
        timesteps = time_range.get_timesteps_from_tsnumber(options['timesteps_per_year'], get_end = True)
        ntimesteps = len(timesteps) - 1

        # Download the data for the specified issue times
        logger.info(f'Found {ntimesteps} blocks of data to download.')
        for i  in range(ntimesteps):
            timestep_start = timesteps[i]
            timestep_end   = timesteps[i+1] - dt.timedelta(days=1)
            logger.info(f' - Block {i+1}/{ntimesteps}: starting at {timestep_start:%Y-%m-%d}')

            # Do all of this inside a temporary folder
            tmpdirs = os.path.join(os.getenv('HOME'), 'tmp')
            os.makedirs(tmpdirs, exist_ok=True)
            with tempfile.TemporaryDirectory(dir = tmpdirs) as tmp_path:
                self.working_path = tmp_path

                logger.debug(f' ----> Downloading data')
                
                tmp_filename = f'temp_{self.dataset}_{timestep_start:%Y%m%d}-{timestep_end:%Y%m%d}.grib2'
                tmp_destination = os.path.join(tmp_path, tmp_filename)
                request = self.build_request(self.variables, TimeRange(timestep_start, timestep_end), space_bounds)
                self.download(request, tmp_destination, min_size = 200,  missing_action = 'w')
                
                data = xr.open_dataset(tmp_destination, engine='cfgrib')
                #data = xr.open_dataset('/home/luca/Downloads/adaptor.mars.internal-1708037793.2312179-18856-1-d2512f5e-7708-46a5-8418-e52847aa208a.grib', engine='cfgrib')

                # We need to handle the time dimention
                # Valid times, contains the time + step combined (times, step) array.
                # first we linearise it, we remove 1 hour to make it easier to filter for the day later
                # the original time is the end time of the step so a step that ends at midnight is actally from the previous day
                valid_times = data.valid_time.values.flatten() - np.timedelta64(1, 'h')

                # We need to create a new time dimension that is the combination of the time and step dimensions
                data = data.stack(valid_time=('time', 'step'))

                # and asign it the new time values
                data = data.drop_vars(['valid_time', 'time', 'step'])
                data = data.assign_coords(valid_time=valid_times)

                # filter data to the selected days (we have to do this because the API returns data for 36 hours)
                inrange = (data.valid_time.dt.date >= timestep_start.date()) & (data.valid_time.dt.date <= timestep_end.date())
                data = data.sel(valid_time = inrange)

                # check if we are using any preliminary data, or if it is all final
                if 'expver' in data.dims:
                    logger.warning('  -> Some of the data is preliminary, we will use the final version where available')
                    data_final  = data.sel(expver=1)
                    data_prelim = data.sel(expver=5)

                    data = xr.where(np.isnan(data_final), data_prelim, data_final)

                # rename the time dimension to time
                data = data.rename_dims({'valid_time': 'time'})

                # remove non needed dimensions
                data = data.squeeze()

                for var in self.variables:
                    logger.debug(f' --> Variable {var}')
                    varname = self.available_variables[var]

                    vardata = data[varname]

                    # add start and end time as attributes
                    vardata.attrs['start_time'] = timestep_start
                    vardata.attrs['end_time'] = timestep_end

                    if options['aggregate_in_time'] is not None:
                        logger.debug(f' ----> Aggregating data')
                        vardata.attrs['agg_function'] = options['aggregate_in_time']
                        if options['aggregate_in_time'] == 'mean':
                            vardata = vardata.mean(dim='time')
                        elif options['aggregate_in_time'] == 'max':
                            vardata = vardata.max(dim='time')
                        elif options['aggregate_in_time'] == 'min':
                            vardata = vardata.min(dim='time')
                        elif options['aggregate_in_time'] == 'sum':
                            vardata = vardata.sum(dim='time')

                    vardata = vardata.rio.set_spatial_dims('longitude', 'latitude')
                    vardata = vardata.rio.write_crs(self.spatial_ref)

                    out_name = format_string(destination, {'variable': var})
                    out_name = timestep_end.strftime(out_name)

                    if options['output_format'] == 'GeoTIFF':
                        logger.debug(f' ----> Saving to GeoTIFF')
                        save_array_to_tiff(vardata, out_name)
                    else:
                        logger.debug(f' ----> Saving to netcdf')
                        save_netcdf(vardata, out_name)
                
            logger.info(f'  -> SUCCESS! Data for {len(self.variables)} variables downloaded.')
        logger.info(f'------------------------------------------')
