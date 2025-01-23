import os
import numpy as np
import xarray as xr
from typing import Generator

from ...base_downloaders import URLDownloader

from ...utils.auth import get_credentials
from ...utils.io import decompress_bz2

import datetime as dt
import requests

from d3tools.spatial import BoundingBox, crop_to_bb
from d3tools import timestepping as ts
from d3tools.timestepping.timestep import TimeStep
from d3tools.timestepping.fixed_num_timestep import FixedNTimeStep

# from dam.utils.io_geotiff import read_geotiff_asXarray, write_geotiff_fromXarray

class HSAFDownloader(URLDownloader):

    source = "HSAF"
    name = "HSAF_downloader"
    
    default_options = {
        "variables": [], #["var40", "var41", "var42", "var43"],
        "custom_variables": None,
        "cdo_path": "/usr/bin/cdo"
    }

    available_products: dict = {
        "HSAF-h141": {
            "ts_per_year": 365,
            "url" : "/products/h141/h141/netCDF4/{timestep.start:%Y}/h141_{timestep.start:%Y%m%d}00_R01.nc",
            "nodata" : -9999,
            "format" : 'nc'
        },
        "HSAF-h14": {
            "ts_per_year": 365,
            "url" : "/hsaf_archive/h14/{timestep.start:%Y/%m/%d}/h14_{timestep.start:%Y%m%d}_0000.grib.bz2",
            "nodata" : -9999,
            "format" : 'bz2'
        },
    }

    available_variables: dict = {
        "var40": {'alt_name' : "swi1", 'depth': [0   , 0.07]}, # 0-7 cm
        "var41": {'alt_name' : "swi2", 'depth': [0.07, 0.28]}, # 7-28 cm
        "var42": {'alt_name' : "swi3", 'depth': [0.28, 0.72]}, # 28-72 cm
        "var43": {'alt_name' : "swi4", 'depth': [0.72, 1.89]}, # 72-189 cm
    }

    credential_env_vars = {'username' : 'HSAF_LOGIN', 'password' : 'HSAF_PWD'}

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
        self.set_product(product)
        url_host = "ftp://ftphsaf.meteoam.it"

        super().__init__(self.url_blank, protocol = 'ftp', host = url_host)
        self.credentials = get_credentials(env_variables=self.credential_env_vars, url = url_host)

    def set_product(self, product: str) -> None:
        self.product = product
        if product not in self.available_products:
            raise ValueError(f'Product {product} not available. Choose one of {self.available_products.keys()}')
        self.ts_per_year = self.available_products[product]["ts_per_year"]
        self.url_blank = self.available_products[product]["url"]
        self.nodata = self.available_products[product]["nodata"]
        self.format = self.available_products[product]["format"]

    def set_variables(self, variables: list) -> None:
        if self.custom_variables:
            self.custom_variables = self.find_parents_of_custom_variables(self.custom_variables)
            parent_variables = []
            for var in self.custom_variables:
                parent_variables.extend(self.custom_variables[var]['variables'])
        else:
            self.custom_variables = None
            parent_variables = []

        self.original_variables = variables.copy()
        variables.extend(parent_variables)
        variables = np.unique(variables)

        super().set_variables(variables)

    def find_parents_of_custom_variables(self, custom_variables: dict) -> dict:
        '''
        Find the parent variables of the custom variables.
        custom_variables are in the form {'var_name': [depth_start, depth_end]}
        '''
        parents = {}
        for var in custom_variables:
            parents[var] = {'variables': [], 'weights': []}
            depth_start, depth_end = custom_variables[var]
            size = depth_end - depth_start
            for parent_var in self.available_variables:
                parent_depth_start, parent_depth_end = self.available_variables[parent_var]['depth']
                overlap = min(depth_end, parent_depth_end) - max(depth_start, parent_depth_start)
                if overlap > 0:
                    parents[var]['variables'].append(parent_var)
                    parents[var]['weights'].append(overlap / size)
                
        return parents
    
    def get_last_published_ts(self, product = None, **kwargs) -> ts.TimeRange:
        
        """
        Get the last published date for the dataset.
        """
        
        if product is None:
            product = self.product

        ts_per_year = self.available_products[product]["ts_per_year"]
        url = self.available_products[product]["url"]

        if ts_per_year == 365:
            TimeStep = ts.Day
        else:
            TimeStep = FixedNTimeStep.get_subclass(ts_per_year)

        current_timestep = TimeStep.from_date(dt.datetime.now())
        while True:
            current_url = url.format(timestep = current_timestep)
            
            # send a request to the url
            response = requests.head(current_url)

            # if the request is successful, the last published timestep is the current timestep
            if response.status_code is requests.codes.ok:
                return current_timestep
            
            # if the request is not successful, move to the previous timestep
            current_timestep -= 1

    def get_last_published_date(self, **kwargs) -> dt.datetime:
        return self.get_last_published_ts(**kwargs).end

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
       
        tmp_filename = f'temp_{self.product}.nc' if self.format == 'nc' else f'temp_{self.product}.grib.bz2'
        tmp_file = os.path.join(tmp_path, tmp_filename)

        # Download the data
        success = self.download(tmp_file, timestep = timestep, auth = self.credentials, missing_action = 'ignore', min_size = 500000)

        # if not success
        if not success:
            return
        
        # otherwise
        if self.format == 'bz2':
            decompress_bz2(tmp_file)
            tmp_file = self.remapgrib(tmp_file[:-4])

        file_handle = xr.open_dataset(tmp_file, engine='netcdf4')

        all_vars = {}
        for varname, variable in self.variables.items():
            if variable['alt_name'] in file_handle:
                var_data = file_handle[variable['alt_name']]
            elif varname in file_handle:
                var_data = file_handle[varname]
            else:
                return
            
            var_data = var_data.where(~np.isclose(var_data, self.nodata, equal_nan=True), np.nan)
            var_data = var_data.rio.write_nodata(np.nan)

            var_data = var_data.rio.write_crs(self.spatial_ref)

            cropped = crop_to_bb(src=var_data, BBox=space_bounds)
            
            if varname in self.original_variables:
                yield cropped.squeeze(), {'variable': varname}
            
            all_vars[varname] = cropped

        if self.custom_variables is not None:
            for custom_variable in self.custom_variables:
                variables, weights = self.custom_variables[custom_variable]['variables'], self.custom_variables[custom_variable]['weights']
                new_var = sum([all_vars[var] * weight for var, weight in zip(variables, weights)])
                yield new_var.squeeze(), {'variable': custom_variable}

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