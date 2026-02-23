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

    single_temp_folder = False

    retries = 3
    retry_delay = 10  # seconds

    default_options = {
        "ts_per_year": 365,
        "variables" : None,  # all variables
    }

    credential_env_vars = {'username' : 'LSASAF_LOGIN', 'password' : 'LSASAF_PWD'}

    home = "https://datalsasaf.lsasvcs.ipma.pt"
    url_blank = home + '/PRODUCTS/{satellite}/{product_name}/NETCDF/{time:%Y/%m/%d}/{filename}'

    available_products: dict = {
        "et0": {
            "product_name" : "METREF",
            "satellite" : "MSG",
            "filename" : 'NETCDF4_LSASAF_MSG_METREF_MSG-Disk_{time:%Y%m%d}0000.nc',
            "freq" : 'd'
        },
        "et": {
            "product_name" : "MDMETv3",
            "satellite" : "MSG",
            "filename" : 'NETCDF4_LSASAF_MSG_DMETv3_MSG-Disk_{time:%Y%m%d}0000.nc',
            "freq" : 'd'
        }
    }

    available_variables: dict = {
        "et0": {"METREF":{'nodata' : float('nan')}, "quality_flag":{'nodata' : float('nan')}},
        "et" : {"ET":{'nodata' : float('nan')}, "max_nslots_missing":{'nodata' : float('nan')}, "missing_values_percent":{'nodata' : float('nan')}}
    }

    def __init__(self, product: str) -> None:
        self.set_product(product)
        super().__init__(self.url_blank, protocol = 'http')

    # def set_product(self, product: str) -> None:
    #     self.product = product.lower()
    #     if self.product not in self.available_products:
    #         raise ValueError(f'Product {product} not available. Choose one of {self.available_products.keys()}')
    #     for key in self.available_products[self.product]:
    #         setattr(self, key, self.available_products[self.product][key])

    def set_variables(self, variables: list[str] | None) -> None:
            if variables is None:
                variables = self.available_variables[self.product].keys()
            super().set_variables(variables)

    def get_last_published_ts(self, **kwargs) -> ts.TimeRange:
        
        """
        Get the last published date for the dataset.
        """

        credentials = self.get_credentials()

        this_ts = ts.TimeStep.from_unit(self.freq).from_date(dt.datetime.now())
        while True:
            this_filename = self.filename.format(time = this_ts.start)
            this_url = self.url_blank.format(
                product_name = self.product_name,
                satellite = self.satellite,
                filename = this_filename,
                time = this_ts.start
            )
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
        

        credentials = self.get_credentials()
        tmp_file_nc = f'temp_{self.product}{timestep.start:%Y%m%d}.nc'

        # check if the file is not already downloaded in the tmp_path
        tmp_destination = os.path.join(tmp_path, tmp_file_nc)
        this_filename = self.filename.format(time = timestep.start)

        success = False
        while not success and self.retries > 0:

            success = self.download(tmp_destination, min_size = 2000, missing_action = 'warning', auth = tuple(credentials.split(':')),
                        time = timestep.start, product_name = self.product_name, satellite = self.satellite, filename = this_filename)

            if not success:
                self.retries -= 1
                if self.retries > 0:
                    print(f'Retrying download in {self.retry_delay} seconds... ({self.retries} retries left)')
                    import time
                    time.sleep(self.retry_delay)
                    continue
                else:
                    print('Max retries reached. Giving up.')
                    break

            # open the file
            raw_data = xr.open_dataset(tmp_destination, engine = 'h5netcdf')
            raw_data.close()
            for var, varopts in self.variables.items():
                vardata = raw_data[var].isel(time = 0, drop = True)  # remove the time dimension if present

                # crop to the bounding box
                vardata = crop_to_bb(vardata, space_bounds)

                # set the metadata
                vardata = vardata.rio.write_crs('EPSG:4326')
                vardata = vardata.rio.set_spatial_dims(x_dim = 'lon', y_dim = 'lat')

                yield vardata, {'variable' : var}