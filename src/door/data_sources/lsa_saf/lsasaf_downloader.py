import os
from typing import Generator, Optional, Sequence
import xarray as xr
import datetime as dt
import requests
import tempfile

from ...base_downloaders import URLDownloader
from ...utils.auth import get_credentials

from d3tools import timestepping as ts
from d3tools.timestepping.timestep import TimeStep
from d3tools.spatial import BoundingBox, crop_to_bb

class LSASAFDownloader(URLDownloader):
    source = "LSA-SAF"
    name = "LSASAF_downloader"

    single_temp_folder = True

    default_options = {
        "ts_per_year": 365
    }

    credential_env_vars = {'username' : 'LSASAF_LOGIN', 'password' : 'LSASAF_PWD'}

    home = "https://datalsasaf.lsasvcs.ipma.pt"
    url_blank = home + '/PRODUCTS/{satellite}/{product_name}/NETCDF/%Y/%m/%d/{filename}'

    available_products: dict = {
        "et0": {
            "product_name" : "METREF",
            "satellite" : "MSG",
            "filename" : 'NETCDF4_LSASAF_MSG_METREF_MSG-Disk_%Y%m%d0000.nc',
            "freq" : 'd',
            "nodata" : -9999,
            "varname" : "precip",
        },
        "et": {
            "product_name" : "MDMETv3",
            "satellite" : "MSG",
            "filename" : 'NETCDF4_LSASAF_MSG_DMETv3_MSG-Disk_%Y%m%d0000.nc',
            "freq" : 'd',
            "nodata" : -9999,
            "varname" : "precip",
        }
    }

    available_variables: dict = {
        "et0": ["METREF", "quality_flag"],
        "et" : ["ET", "max_nsolts_missing", "missing_values_percent"]
    }

    def __init__(self, product: str) -> None:
        self.set_product(product)
        super().__init__(self.url_blank, protocol = 'http')

    def set_product(self, product: str) -> None:
        self.product = product.lower()
        if self.product not in self.available_products:
            raise ValueError(f'Product {product} not available. Choose one of {self.available_products.keys()}')
        for key in self.available_products[self.product]:
            setattr(self, key, self.available_products[self.product][key])

    def get_last_published_ts(self, **kwargs) -> ts.TimeRange:
        
        """
        Get the last published date for the dataset.
        """

        credentials = self.get_credentials()

        this_ts = ts.TimeStep.from_unit(self.freq).from_date(dt.datetime.now())
        while True:
            this_url = this_ts.end.strftime(self.url_blank.format(
                product_name = self.product_name,
                satellite = self.satellite,
                filename = self.filename
            ))
            response = requests.head(this_url, auth = tuple(credentials.split(':')))
            if response.status_code is requests.codes.ok:
                break
            this_ts = this_ts - 1

        return this_ts

    def get_last_published_date(self, **kwargs) -> dt.datetime:

        """
        Get the last published date for the dataset.
        """        

        return self.get_last_published_ts(**kwargs).end

    def get_credentials(self) -> str:

        # credentials will be looked for in the environment variables
        # username = 'EARTHDATA_LOGIN', password = 'EARTHDATA_PWD'
        # should be saved in a .netrc file in the user's home directory
        # with the following line:
        # machine urs.earthdata.nasa.gov login <username> password <password>
        if not hasattr(self, 'credentials') or not isinstance(self.credentials, str):
            self.credentials = get_credentials(env_variables=self.credential_env_vars,
                                               url=self.home, encode = False)
        
        return self.credentials

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
        

        year = timestep.year
        tmp_file_nc = f'temp_{self.product}{year}.nc'

        # check if the file is not already downloaded in the tmp_path
        tmp_destination = os.path.join(tmp_path, tmp_file_nc)
        if not os.path.exists(tmp_destination):
            # download the file
            self.download(tmp_destination, min_size = 2000, missing_action = 'warning', year = year)
        
        # open the file
        raw_data = xr.open_dataset(tmp_destination, engine = 'netcdf4')
        vardata = raw_data[self.varname]

        # only select the relevant time range
        inrange = (vardata.time.dt.date >= timestep.start.date()) & (vardata.time.dt.date <= timestep.end.date())
        vardata = vardata.sel(time = inrange)

        # crop the data
        cropped = crop_to_bb(vardata, space_bounds)

        # aggregate the data
        if self.agg_method == 'sum':
            aggregated = cropped.sum(dim = 'time')
        elif self.agg_method == 'mean':
            aggregated = cropped.mean(dim = 'time')
        else:
            raise ValueError(f'Aggregation method {self.agg_method} not recognized')

        yield aggregated, {}