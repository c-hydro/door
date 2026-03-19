import json
import os
import requests
import numpy as np
import pandas as pd
import xarray as xr
import zipfile
from typing import Iterable
import datetime as dt
import re

from ...utils.io import handle_missing
from ...base_downloaders import URLDownloader
from ...utils.auth import get_credentials

from d3tools.spatial import BoundingBox, crop_to_bb
from d3tools import timestepping as ts
from d3tools.timestepping.timestep import TimeStep

class CLMSDownloader(URLDownloader):
    source = "CLMS"
    name = "CLMS_downloader"

    credential_env_vars = {'username' : 'EARTHDATA_LOGIN', 'password' : 'EARTHDATA_PWD'}

    odata_base_url     = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    auth_server_url    = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    odata_download_url = "https://download.dataspace.copernicus.eu/odata/v1/Products"

    default_options = {
        'accept_RTs' : ['RT6' , 'RT0'],
        'variables'  : None
    }

    available_products = {
        # 'swi': {
        #     'versions': ["3.1.1", "3.2.1", "4.0.1"],
        #     'url': clms_url + 'netcdf/soil_water_index/swi_12.5km_v{version[0]}_{ts_str}daily/{timestep.start:%Y}/{timestep.start:%Y%m%d}/c_gls_SWI{ts_str}_{timestep.start:%Y%m%d}1200_GLOBE_ASCAT_V{version}.nc',
        #     'nodata': 255,
        #     'scale_factor': 0.5
        # },
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
    #available_variables = ["001", "005", "010", "020", "040", "060", "100"]

    def __init__(self, product: str) -> None:
        self.set_product(product)

        catalogues = [pd.read_csv(c, sep =';', parse_dates=['content_date_start', 'content_date_end']) for c in self.file_catalogue]
        self.catalogue = pd.concat(catalogues, ignore_index=True)

    # def set_variables(self, variables: list) -> None:
    #     self.variables = []
    #     for var in variables:
    #         this_var = var.lower()
    #         if this_var not in self.available_variables:
    #             msg = f'Variable {var} not available. Choose one of {self.available_variables}'
    #         else:
    #             self.variables.append(this_var)
    #     if len(self.variables) == 0:
    #         raise ValueError('No valid variables selected')

    def get_credentials(self) -> str:

        # credentials will be looked for in the environment variables
        # username = 'EARTHDATA_LOGIN', password = 'EARTHDATA_PWD'
        # should be saved in a .netrc file in the user's home directory
        # with the following line:
        # machine urs.earthdata.nasa.gov login <username> password <password>
        if not hasattr(self, 'credentials') or not isinstance(self.credentials, str):
            self.credentials = get_credentials(env_variables=self.credential_env_vars,
                                               url=self.odata_base_url, encode = False)
        
        return self.credentials

    def get_access_token(self, username, password):
        """
        Retrieve an access token from the authentication server.
        This token is used for subsequent API calls.
        """
        auth_data = {
            "client_id": "cdse-public",
            "grant_type": "password",
            "username": username,
            "password": password,
        }
        response = requests.post(self.auth_server_url, data=auth_data, verify=True, allow_redirects=False)
        if response.status_code == 200:
            return json.loads(response.text)["access_token"]
        else:
            print(f"Failed to retrieve access token. Status code: {response.status_code}")
            exit(1)

    def get_last_published_ts(self, RTs = None, **kwargs) -> ts.TimeRange:

        """
        Get the last published timestep for the dataset.
        """

        ts = TimeStep.from_unit(self.freq)
        last_date = self.get_last_published_date(RTs=RTs, **kwargs)
        return ts.from_date(last_date)

    def get_last_published_date(self, RTs = None, **kwargs) -> dt.datetime:

        """
        Get the last published date for the dataset.
        """

        if RTs is None:
            RTs = self.accept_RTs

        # filter the catalogue based on the accepted RTs
        all_names = self.catalogue['name'].tolist()
        keep = [any(rt in name for rt in RTs) for name in all_names]
        filtered_catalogue = self.catalogue[keep]

        # get the biggest end date (convert to a datetime.date)
        end_dates = filtered_catalogue['content_date_end'].tolist()
        last_date = max(end_dates).to_pydatetime()
        #last_date = last_date.replace(hour=0, minute=0, second=0, microsecond=0)
        return last_date

    def _get_data_ts(self,
                     time_step: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str,
                     **kwargs) -> Iterable[tuple[xr.DataArray, dict]]:

        """
        Get the data for a specific timestep.
        """
        # # filter the catalogue based on the accepted RTs
        # all_names = self.catalogue['name'].tolist()
        # keep = [any(rt in name for rt in self.accept_RTs) for name in all_names]
        # filtered_catalogue = self.catalogue[keep]

        # # filter the catalogue based on the time_step
        # start_dates = filtered_catalogue['content_date_start'].tolist()
        # matching_indices = [i for i, date in enumerate(start_dates) if date == pd.Timestamp(time_step.start)]
        # if len(matching_indices) == 0:
        #     handle_missing('warning', {'timestep': time_step})
        #     return
        # elif len(matching_indices) > 1:
        #     product_names = filtered_catalogue['name'][matching_indices].tolist()
        #     # use regular expression to find the version "vX.Y.Z" and the RT "RTX"
        #     versions = [re.search(r'v\d+\.\d+\.\d+', name).group(0) for name in product_names]
        #     RTs      = [re.search(r'RT\d+', name).group(0) for name in product_names]
            
        #     # choose the highest version and prefer higher RTs (higher number)
        #     version_numbers = [tuple(map(int, v[1:].split('.'))) for v in versions]
        #     max_version = max(version_numbers)
        #     max_version_str = f'v{max_version[0]}.{max_version[1]}.{max_version[2]}'
        #     # filter again based on the max version
        #     final_indices = [i for i in matching_indices if versions[matching_indices.index(i)] == max_version_str]
        #     if len(final_indices) > 1:
        #         # choose the highest RT
        #         rt_numbers = [int(re.search(r'RT(\d+)', RT).group(1)) for RT in RTs if RTs[matching_indices.index(i)] == RT]
        #         max_rt_number = max(rt_numbers)
        #         final_indices = [i for i in final_indices if int(re.search(r'RT(\d+)', RTs[matching_indices.index(i)]).group(1)) == max_rt_number]
        #         if len(final_indices) > 1:
        #             raise ValueError(f"Multiple files found for timestep {time_step} with same version and RT. Indices: {final_indices}")
            
        #     selected_index = final_indices[0]
        # else:
        #     selected_index = matching_indices[0]

        # id_to_download = filtered_catalogue['id'].tolist()[selected_index]

        # # Set the destination
        # ts_end = time_step.end
        # tmp_filename_raw = f'temp_{self.product}_{ts_end:%Y%m%d}.zip'
        # tmp_destination = os.path.join(tmp_path, tmp_filename_raw)

        # # Get credentials and access token
        # username, password = self.get_credentials().split(':')
        # access_token = self.get_access_token(username, password)

        # # Create a session and update headers
        # session = requests.Session()
        # session.headers.update({"Authorization": f"Bearer {access_token}"})

        # # Perform the GET request with manual redirect handling to preserve auth header
        # download_url = f"{self.odata_download_url}({id_to_download})/$value"
        # response = session.get(download_url, stream=True, allow_redirects=False)

        # # Check if the request was successful
        # if response.status_code == 200:
        #     with open(tmp_destination, "wb") as file:
        #         for chunk in response.iter_content(chunk_size=8192):
        #             if chunk:  # filter out keep-alive new chunks
        #                 file.write(chunk)
        # else:
        #     print(f"Failed to download file. Status code: {response.status_code}")
        #     print(response.text)

        # # open it
        # with zipfile.ZipFile(tmp_destination, 'r') as zip_ref:
        #     zip_ref.extractall(tmp_path)
        # extracted_files = zip_ref.namelist()
        # netcdf_file = [f for f in extracted_files if f.endswith('.nc')][0]
        # netcdf_path = os.path.join(tmp_path, netcdf_file)
        # data = xr.open_dataset(netcdf_path, engine = 'h5netcdf', decode_timedelta = False, chunks={},
        #                        drop_variables = [var for var in self.available_variables[self.product] if var not in self.variables])

        data = xr.open_dataset('/home/luca/Desktop/c_gls_FAPAR300-RT0_202512100000_GLOBE_OLCI_V2.0.1.nc',
                               engine = 'h5netcdf', decode_timedelta = False, chunks={})
        
        # crop the data as first thing, to reduce memory usage
        data = crop_to_bb(data, space_bounds)

        # remove the time dimension (there is only one timestep per file)
        data = data.isel(time = 0)

        # extract the variables from the file, crop them and yield them
        for variable, varopts in self.variables.items():
            this_data = data[variable]
            dtype = varopts['dtype']
            if dtype.startswith('int'):
                this_data = this_data.where(~np.isnan(this_data), other = 255.)
            this_data = this_data.astype(dtype)
            
            # remove dtype and valid_range attributes since they are actually wrong currently
            this_data.attrs.pop('dtype', None)
            this_data.attrs.pop('valid_range', None)

            # ensure the crs is set
            this_data = this_data.rio.write_crs("EPSG:4326")

            yield this_data, {'variable': variable}

    # def get_data(self,
    #              time_range: ts.TimeRange|Sequence[dt.datetime],
    #              space_bounds:  Optional[BoundingBox] = None,
    #              destination: Optional[Dataset|dict|str] = None,
    #              options:  Optional[dict] = None) -> None:
    #     """
    #     Get data from this downloader and saves it to a file
    #     """

    #     # get options and check them against the default options
    #     if options is not None: 
    #         self.set_options(options)
        
    #     # Set ts_str based on the ts_per_year
    #     if self.ts_per_year == 36:
    #         self.ts_str = '10'
    #     elif self.ts_per_year == 365:
    #         self.ts_str = ''
    #     else:
    #         raise ValueError(f"ts_per_year {self.ts_per_year} not supported")
        
    #     super().get_data(time_range, space_bounds, destination)