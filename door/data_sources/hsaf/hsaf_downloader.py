import os
import tempfile
import numpy as np
import xarray as xr
from typing import Optional

from ...base_downloaders import URLDownloader
from ...utils.time import TimeRange
from ...utils.space import BoundingBox
from ...utils.netcdf import crop_netcdf
from ...utils.geotiff import save_array_to_tiff
from ...utils.auth import get_credentials
from ...utils.io import decompress_bz2 

# from dam.utils.io_geotiff import read_geotiff_asXarray, write_geotiff_fromXarray

import logging
logger = logging.getLogger(__name__)

class HSAFDownloader(URLDownloader):

    name = "HSAF"
    default_options = {
        "variables": ["var40", "var41", "var42", "var43"],
        "add_variable": None,
        "cdo_path": "/usr/bin/cdo"
    }
    add_variable_options = {
        "variable": "var28",
        "input_variables": ["var40", "var41"],
        "weights": [0.75, 0.25],
    }
    variable_mapping = {'var40': 'swi1', 'var41': 'swi2', 'var42': 'swi3', 'var43': 'swi4'}

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

    def __init__(self, product: str) -> None:
        self.product = product
        url_host = "ftp://ftphsaf.meteoam.it"
        if self.product == "HSAF-h141":
            url_blank = "/products/h141/h141/netCDF4/{time:%Y}/h141_{time:%Y%m%d}00_R01.nc"
            self.ts_per_year = 365 # daily
            self.format = 'nc'
        elif self.product == "HSAF-h14":
            url_blank = "/hsaf_archive/h14/{time:%Y/%m/%d}/h14_{time:%Y%m%d}_0000.grib.bz2"
            self.ts_per_year  = 365 # daily
            self.format = 'bz2'
        else:
            logger.error(" --> ERROR! Only HSAF-h141-daily and HSAF-h14-daily has been implemented until now!")
            raise NotImplementedError()

        super().__init__(url_blank, protocol = 'ftp', host = url_host)
        self.nodata = -9999

    def get_data(self,
                 time_range: TimeRange,
                 space_bounds: BoundingBox,
                 destination: str,
                 options: Optional[dict] = None) -> None:

        # Check options
        options = self.check_options(options)
        self.cdo_path = options['cdo_path']
        self.variables = options['variables']

        logger.info(f'------------------------------------------')
        logger.info(f'Starting download of {self.product} data')
        logger.info(f'Data requested between {time_range.start:%Y-%m-%d} and {time_range.end:%Y-%m-%d}')
        logger.info(f'Bounding box: {space_bounds.bbox}')
        logger.info(f'------------------------------------------')

        # Get the timesteps to download
        timesteps = time_range.get_timesteps_from_tsnumber(self.ts_per_year)
        logger.info(f'Found {len(timesteps)} timesteps to download.')

        credentials = get_credentials(self.host)

        # Download the data for the specified times
        for i, time_now in enumerate(timesteps):
            logger.info(f' - Timestep {i+1}/{len(timesteps)}: {time_now:%Y-%m-%d}')

            # Do all of this inside a temporary folder
            with tempfile.TemporaryDirectory() as tmp_path:
                tmp_filename = f'temp_{self.product}{time_now:%Y%m%d}.nc' if self.format == 'nc' else f'temp_{self.product}{time_now:%Y%m%d}.grib.bz2'
                tmp_file = os.path.join(tmp_path, tmp_filename)

                # Download the data
                success = self.download(tmp_file, time = time_now, auth = credentials, missing_action = 'ignore', min_size = 500000)

                # if not succes
                if not success:
                    logger.info(f'  -> Could not find data for {time_now:%Y-%m-%d}')
                    continue

                elif success:
                    # Unzip the data
                    if self.format == 'bz2':
                        decompress_bz2(tmp_file)
                        tmp_file = self.remapgrib(tmp_file[:-4])

                    try:
                        file_handle = xr.open_dataset(tmp_file, engine='netcdf4')
                    except Exception as e:
                        logger.error(f' --> ERROR! The file {tmp_file} is corrupted')
                        continue

                    # if present change names swi1, swi2, swi3, swi4 to var40, var41, var42, var43
                    for var_name in self.variables:
                        # get the variable name from the mapping
                        alternative_name = self.variable_mapping[var_name]
                        # if both var_name and alternative_name are not present in the file then the file is corrupted
                        if alternative_name in file_handle:
                            file_handle = file_handle.rename_vars({alternative_name: var_name})
                        elif var_name not in file_handle:
                            logger.error(f' --> ERROR! The variable {var_name} is not present in the file {tmp_file}')
                            continue
                    
                    for var_name in self.variables:

                        destination_now = time_now.strftime(destination).replace('{variable}', var_name).replace('{var}', var_name)
                        #tmp_var_name = f'temp_{var_name}_{time_now:%Y%m%d}.tif'
                        #tmp_var_file = os.path.join(tmp_path, tmp_var_name)

                        var_data = file_handle[var_name]

                        # turn self.nodata into np.nan
                        var_data = var_data.where(var_data != self.nodata, np.nan)
                        var_data = var_data.rio.write_nodata(np.nan)

                        # assign geoprojection to var_data from space_bounds
                        var_data = var_data.rio.write_crs(self.spatial_ref)

                        cropped = crop_netcdf(src=var_data, BBox=space_bounds)
                        save_array_to_tiff(cropped.squeeze(), destination_now)

                    logger.info(f'  -> SUCCESS! Data for {time_now:%Y-%m-%d} saved to destination folder')

                # # If add_variable is True, add the variable to the downloaded file
                # if options['add_variable'] is not None:
                #     # calculate the new variable

                #     self.add_variable(destination, time_now, self.add_variable_options)

        logger.info(f'------------------------------------------')

    def remapgrib(self, file_path: str) -> str:
        '''
        Remap the grib file to a regular grid.
        '''

        cdo_path = self.cdo_path
        
        # if last 3 characters are .bz2 then decompress the file
        if file_path[-4:] == '.bz2':
            os.system(f'bunzip2 {file_path}')
            file_path = file_path[:-4]
        file_out = file_path[:-5] + '_remap.grib'
        nc_file = file_out[:-4] + 'nc'
        os.system(f'{cdo_path} -R remapcon,r1600x800 -setgridtype,regular {file_path} {file_out}')
        os.system(f'{cdo_path} -f nc copy {file_out} {nc_file}')
        return nc_file
    
    # def add_variable(self, destination: str, time: datetime, variable_options: dict) -> None:
    #     '''
    #     Add a variable to the downloaded file.
    #     '''
    #     variable_name = variable_options['variable']
    #     input_variables = variable_options['input_variables']
    #     weights = variable_options['weights']

    #     # Open the input files
    #     input_files = [time.strftime(destination).replace('{var}', var) for var in input_variables]
    #     input_data = [rio.open(file).read(1) for file in input_files]

    #     # create a new xarray variable empty from input files
    #     new_var = read_geotiff_asXarray(input_files[0])
    #     # fill all values with np.nan
    #     new_var.values = np.full(new_var.shape, np.nan)

    #     # Calculate the new variable
    #     weighted_sum = sum([data * weight for data, weight in zip(input_data, weights)])
    #     weighted_sum = np.expand_dims(weighted_sum, axis=0)
    #     new_var.values = weighted_sum

    #     new_var = new_var.fillna(self.nodata)

    #     # Save the new variable as geotiff
    #     new_var_file = time.strftime(destination).replace('{var}', variable_name)
    #     write_geotiff_fromXarray(new_var, new_var_file)

    #     logger.info(f'  -> SUCCESS! New variable {variable_name} added to the downloaded file')

    #     return