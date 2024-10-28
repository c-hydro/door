import rioxarray as rxr
import xarray as xr
from typing import Generator

import datetime as dt

from .cmr_downloader import CMRDownloader
from ...utils.space import BoundingBox, crop_to_bb

from ...tools.timestepping.timestep import TimeStep

class GRACEDownloader(CMRDownloader):

    source = 'GRACE'
    name = 'GRACE_downloader'

    available_products = {
        'tws' : {
            'provider'   : 'POCLOUD',
            'freq'       : 'monthly',
            'version'    : {'post2018': 'RL06.3v04', 'pre2018': 'RL06v04'},
            'product_id' : {'post2018': 'TELLUS_GRFO_L3_JPL_RL06.3_LND_v04', 'pre2018':'TELLUS_GRAC_L3_JPL_RL06_LND_v04'}
        }
    }

    available_variables = {
        'tws' : { #file:///home/luca/Downloads/GRACE-FO_L3_Handbook_JPL.pdf
            'TWS' :             {'id': 0, 'no_data': -99999},
            'TWA_uncertainty' : {'id': 1, 'no_data': -99999},
        }
    }

    file_ext = ['.tif', '.tiff']
    
    def __init__(self, product: str): 	
        super().__init__(product)

        # assume GRACE-FO data
        self.product_id = self.available_products[self.product]['product_id']['post2018']
        self.version    = self.available_products[self.product]['version']['post2018']

    @property
    def start(self):
        return dt.datetime(2002,4,1)

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
        """
        Get data from the CMR.
        """

        # set the variable and version based on the timestep (pre/post 2018 = GRACE/GRACE-FO)
        if timestep.year < 2018:
            key = 'pre2018'
        else:
            key = 'post2018'

        self.product_id = self.available_products[self.product]['product_id'][key]
        self.version    = self.available_products[self.product]['version'][key]

        # Check the data from the CMR
        url_list = self.cmr_search(timestep, space_bounds)

        if not url_list:
            return None
        
        # download the data (only one file)
        file = self.download(url_list, tmp_path)[0]

        # open the file with rasterio
        all_data = rxr.open_rasterio(file)   
        for varname, variable in self.variables.items():

            # create a new dataset with only the current layer (band)
            data = all_data.sel(band=variable['id'] + 1).drop('band')

            # Crop this band and save it
            cropped_data = crop_to_bb(data, space_bounds)
            yield cropped_data, {'variable': varname}
            