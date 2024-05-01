import os
import tempfile
import netrc
import datetime
import numpy as np
import xarray as xr
import rasterio as rio
from typing import Optional
from ftpretty import ftpretty as ftp
from osgeo import gdal, gdalconst

from ...base_downloaders import URLDownloader
from ...utils.time import TimeRange
from ...utils.space import BoundingBox
from ...utils.geotiff import save_array_to_tiff, transform_longitude
from ...utils.io import decompress_bz2

from dam.utils.io_geotiff import read_geotiff_asXarray, write_geotiff_fromXarray

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
    # dims_order = ['time', 'lat', 'lon']

    def __init__(self, product: str) -> None:
        self.product = product
        if self.product == "HSAF-h141":
            self.url_host = "ftphsaf.meteoam.it"
            self.url_blank = "/products/h141/h141/netCDF4/{time:%Y}/h141_{time:%Y%m%d}00_R01.nc"
            self.ts_per_year = 365 # daily
            self.format = 'nc'
        elif self.product == "HSAF-h14":
            self.url_host = "ftphsaf.meteoam.it"
            self.url_blank = "/hsaf_archive/h14/{time:%Y/%m/%d}/h14_{time:%Y%m%d}_0000.grib.bz2"
            self.ts_per_year  = 365 # daily
            self.format = 'bz2'
        else:
            logger.error(" --> ERROR! Only HSAF-h141-daily and HSAF-h14-daily has been implemented until now!")
            raise NotImplementedError()

        super().__init__(self.url_blank, protocol = 'http')
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

        url_host = self.url_host

        self.host = ftp(url_host, get_credentials(url_host)[0], get_credentials(url_host)[1])

        # Download the data for the specified times
        for i, time_now in enumerate(timesteps):
            logger.info(f' - Timestep {i+1}/{len(timesteps)}: {time_now:%Y-%m-%d}')

            # Do all of this inside a temporary folder
            with tempfile.TemporaryDirectory() as tmp_path:
                tmp_filename = f'temp_{self.product}{time_now:%Y%m%d}.nc' if self.format == 'nc' else f'temp_{self.product}{time_now:%Y%m%d}.grib.bz2'
                tmp_file = os.path.join(tmp_path, tmp_filename)

                # Download the data
                success = self.download(tmp_file, time = time_now)

                if not success:
                    logger.info(f'  -> Could not find data for {time_now:%Y-%m-%d}')
                elif success:
                    # Unzip the data
                    if self.format == 'bz2':

                        tmp_file = decompress_bz2(tmp_file)
                        tmp_file = remapgrib(tmp_file, cdo_path=self.cdo_path)

                    file_handle = xr.open_dataset(tmp_file, engine='netcdf4')

                    for var_name in self.variables:

                        destination_now = time_now.strftime(destination).replace('{var}', var_name)
                        tmp_var_name = f'temp_{var_name}_{time_now:%Y%m%d}.tif'
                        tmp_var_file = os.path.join(tmp_path, tmp_var_name)

                        var_data = file_handle[var_name]

                        # turn np.nan into nodata value
                        var_data = var_data.fillna(self.nodata)

                        # assign geoprojection to var_data from space_bounds
                        var_data.rio.write_crs(space_bounds.proj, inplace=True)

                        save_array_to_tiff(var_data, tmp_var_file)
                        transform_longitude(tmp_var_file)

                        crop_raster(src=tmp_var_file, BBox=space_bounds, output_file=destination_now)

                        logger.info(f'  -> SUCCESS! Data for {time_now:%Y-%m-%d} - {var_name} dowloaded and cropped to bounds')

                    logger.info(f'  -> SUCCESS! Data for {time_now:%Y-%m-%d} saved to destination folder')

                # If add_variable is True, add the variable to the downloaded file
                if options['add_variable'] is not None:
                    # calculate the new variable

                    self.add_variable(destination, time_now, self.add_variable_options)

        logger.info(f'------------------------------------------')

    def download(self, destination: str, min_size: float = None, missing_action: str = 'error', auth: dict = None, **kwargs) -> bool:
        """
        Download the data from the URL to the destination.
        :param destination:
        :param min_size:
        :param missing_action:
        :param auth:
        :param kwargs:
        :return:
        """
        # Download the data from the URL
        url = self.format_url(**kwargs)
        host = self.host

        try:
            host.get(url, destination)
            success = True
        except Exception as e:
            logging.warning(' ===> Data for timestep ' + str(time_run_step) + ' not found on server.')
            success = False

        return success

    def add_variable(self, destination: str, time: datetime, variable_options: dict) -> None:
        '''
        Add a variable to the downloaded file.
        '''
        variable_name = variable_options['variable']
        input_variables = variable_options['input_variables']
        weights = variable_options['weights']

        # Open the input files
        input_files = [time.strftime(destination).replace('{var}', var) for var in input_variables]
        input_data = [rio.open(file).read(1) for file in input_files]

        # create a new xarray variable empty from input files
        new_var = read_geotiff_asXarray(input_files[0])
        # fill all values with np.nan
        new_var.values = np.full(new_var.shape, np.nan)

        # Calculate the new variable
        weighted_sum = sum([data * weight for data, weight in zip(input_data, weights)])
        weighted_sum = np.expand_dims(weighted_sum, axis=0)
        new_var.values = weighted_sum

        new_var = new_var.fillna(self.nodata)

        # Save the new variable as geotiff
        new_var_file = time.strftime(destination).replace('{var}', variable_name)
        write_geotiff_fromXarray(new_var, new_var_file)

        logger.info(f'  -> SUCCESS! New variable {variable_name} added to the downloaded file')

        return

def remapgrib(file_path: str, cdo_path: str="/usr/bin/cdo") -> str:
    '''
    Remap the grib file to a regular grid.
    '''
    file_out = file_path[:-5] + '_remap.grib'
    nc_file = file_out[:-4] + 'nc'
    os.system(f'{cdo_path} -R remapcon,r1600x800 -setgridtype,regular {file_path} {file_out}')
    os.system(f'{cdo_path} -f nc copy {file_out} {nc_file}')
    # return file_out
    return nc_file

def crop_raster(src: str|gdal.Dataset, BBox: BoundingBox, output_file: str) -> None:
    """
    Cut a geotiff to a bounding box.
    """

    if isinstance(src, str):
        src_ds = gdal.Open(src, gdalconst.GA_ReadOnly)
    else:
        src_ds = src

    geoprojection = src_ds.GetProjection()

    # transform the bounding box to the geotiff projection dealing with -180 to 180 longitude
    BBox_trans = BBox.transform(geoprojection, inplace = False)

    min_x, min_y, max_x, max_y = BBox_trans.bbox

    # Create the output file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    gdal.Warp(output_file, src_ds, outputBounds=(min_x, min_y, max_x, max_y),
            outputType = src_ds.GetRasterBand(1).DataType, creationOptions = ['COMPRESS=LZW'])

    # Close the datasets
    src_ds = None

def get_credentials(product):
    '''
    Get the credentials from the .netrc file.
    '''
    logger.warning(f' --> Try to read credentials from .netrc file!')
    try:
        netrc_handle = netrc.netrc()
        user, _, password = netrc_handle.authenticators(product)
        credentials = (user, password)
    except FileNotFoundError:
        logger.error(f' --> .netrc file not found in the home directory, please provide credentials for {product} site!')
        raise FileNotFoundError()

    return credentials