# input standard python (xarray, np ecc)
import xarray as xr
from typing import Iterable

# d3tools e other cima
from d3tools import timestepping as ts
from d3tools import spatial as sp

# internal imports
from ...base_downloaders import DOORDownloader





class CDSEDownloader(DOORDownloader):
    """CDSE Downloader class."""

    source = "CDSE"
    name = "CDSE_Downloader"

    default_options = {
        'accept_RTs' : ['RT6' , 'RT0'],
        'variables'  : None
    }

    available_products = {
        'fapar' : {
            'product_name'   : ['fapar_global_300m_10daily_v2'],
            'file_catalogue' : ['https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/bio-geophysical/vegetation_properties/fapar_global_300m_10daily_v2/fapar_global_300m_10daily_v2_nc.csv'],
            'freq' : 't'
        }
    }

    available_variables = {
        'fapar' : {'FAPAR' : {'dtype' : 'float32'},
                   'NOBS'  : {'dtype' : 'int8'},
                   'QFLAG' : {'dtype' : 'int8'},
                   'RMSE'  : {'dtype' : 'float32'},
                   'LENGTH_BEFORE' : {'dtype' : 'int8'},
                   'LENGTH_AFTER'  : {'dtype' : 'int8'}}
    }

    def __init__(self, product: str)  -> None:
        super().__init__()
        self.set_product(product)

    def _get_data_ts(self,
                     time_step: ts.TimeStep,
                     space_bounds: sp.BoundingBox,
                     tmp_path: str,
                     **kwargs) -> Iterable[tuple[xr.DataArray, dict]]:

        """
        Get the data for a specific timestep.
        """
        raise NotImplementedError



