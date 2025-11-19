import os
import rioxarray as rxr
import xarray as xr
from typing import Generator
from affine import Affine

import datetime as dt

from .cmr_downloader import CMRDownloader
from d3tools.spatial import BoundingBox, crop_to_bb

from d3tools.timestepping.timestep import TimeStep

class MERRA2Downloader(CMRDownloader):

    source = 'MERRA2'
    name = 'MERRA2_downloader'

    available_products = {
        'tavg1_2d' : { # time-averaged (hourly), single level
            'provider'   : 'GES_DISC',
            'freq'       : 'daily', # files are daily, data is hourly
            'version'    : '5.12.4'
        }
    }

    available_variables = {
        'tavg1_2d' : { 
            'precipitation'  : {'product_id' : 'M2T1NXFLX', 'varname' : 'PRECTOT', 'agg_method' : 'sum'},
            'temperature'    : {'product_id' : 'M2T1NXSLV', 'varname' : 'T2M',     'agg_method' : 'mean'}
        }
    }

    available_agg_methods = ['mean', 'max', 'min', 'sum']

    default_options = {
        'variables'  : ['precipitation'],
        'agg_method' : ['sum']
    }

    file_ext = ['.nc4']

    @property
    def start(self):
        return dt.datetime(1980,1,1)

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

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
        """
        Get data from the CMR.
        """

        for var, varopts in self.variables.items():
            self.product_id = varopts['product_id']

            # Check the data from the CMR
            url_list = self.cmr_search(timestep, space_bounds)

            if not url_list:
                return None
            
            # download the data (only one file)
            file = os.path.join(tmp_path, os.path.basename(url_list[0]))
            if not os.path.exists(file):
                self.download(url_list, tmp_path)[0]

            # open the file with rasterio
            all_data = xr.open_dataset(file)

            # ensure the latitude is descending
            all_data = all_data.sortby('lat', ascending=False)

            # picke the single variable we need
            data = all_data[varopts['varname']]

            # set spatial reference
            data = data.rio.write_crs("EPSG:4326")
            data = data.rio.set_spatial_dims(x_dim="lon", y_dim="lat")

            # crop to the bounding box
            cropped_data = crop_to_bb(data, space_bounds)

            # set the missing value
            cropped_data = cropped_data.where(cropped_data < 9.9e14, other = float('nan'))
            cropped_data.attrs = {'_FillValue': float('nan')}

            # set and convert the unit if needed
            if var == 'precipitation':
                # from kg/m2/s to mm (1 kg/m2 = 1 mm of water; multiply by 3600 to get hourly total)
                cropped_data = cropped_data * 3600.0
                cropped_data.attrs['units'] = 'mm'
            elif var == 'temperature':
                # from K to °C
                cropped_data = cropped_data - 273.15
                cropped_data.attrs['units'] = '°C'

            # Aggregate if needed
            agg_methods = varopts['agg_method']
            for agg_method in agg_methods:
                if agg_method == 'mean':
                    agg_data = cropped_data.mean(dim='time')
                elif agg_method == 'max':
                    agg_data = cropped_data.max(dim='time')
                elif agg_method == 'min':
                    agg_data = cropped_data.min(dim='time')
                elif agg_method == 'sum':
                    agg_data = cropped_data.sum(dim='time')
                else:
                    msg = f'Aggregation method {agg_method} not recognized'
                    self.log.error(msg)
                    raise ValueError(msg)

                yield agg_data, {'variable': var, 'agg_method': agg_method}
            