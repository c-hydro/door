import os
import requests
import numpy as np
import pandas as pd
import xarray as xr
import rioxarray as rxr
from typing import Iterable
import datetime as dt
import re
import boto3
from botocore.exceptions import ClientError

from ...base_downloaders import DOORDownloader
from ...utils.auth import get_credentials

from d3tools.spatial import BoundingBox, crop_to_bb
from d3tools.timestepping import TimeStep, TimeRange

class CDSES3Downloader(DOORDownloader):
    source = "CDSE-S3"
    name = "CDSE-S3_downloader"

    # same as the CDSE downloader, but with the S3 path to the files instead of the OData API.
    # the odata API is better for smaller AOIs, but it uses a lot of credits and is rather inefficient for larger domains
    # the S3 always downloads global files, but it is more efficient and does not use credits, so it is better for larger AOIs.

    # authentication is via AWS credentials
    credential_env_vars = {'username' : 'CDSE_AWS_ACCESS_KEY', 'password' : 'CDSE_AWS_SECRET_KEY'}
    bucket = 'eodata'
    s3_endpoint = 'https://eodata.dataspace.copernicus.eu/'

    # odata_base_url     = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    # auth_server_url    = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    # odata_download_url = "https://download.dataspace.copernicus.eu/odata/v1/Products"

    default_options = {
        "product": "fapar",
        "consolidation": [0,6], # will take the highest available for a timestep
        "variables": None,      # None means all available variables for the product
    }

    available_products = {
        'fapar' : {
            'product_name'   : ['fapar_global_300m_10daily_v2'],
            'file_catalogue' : ['https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/bio-geophysical/vegetation_properties/fapar_global_300m_10daily_v2/fapar_global_300m_10daily_v2_cog.csv'],
            "frequency": "dekad",
            "data_type" : "UINT8",
            "resolution": 1/336,
            "available_bounds": (-180, -60, 180, 80),
        }
    }
    available_variables = {
        "fapar": {
            "FAPAR":         {"scale_factor": 1/250,"fill_value": 255},
            "NOBS":          {"scale_factor": 1,    "fill_value": 255},
            "QFLAG":         {"scale_factor": 1,    "fill_value": 255},
            "RMSE":          {"scale_factor": 1/250,"fill_value": 255},
            "LENGTH_BEFORE": {"scale_factor": 1,    "fill_value": 255},
            "LENGTH_AFTER":  {"scale_factor": 1,    "fill_value": 255},
        }
    }

    def __init__(self, product: str) -> None:
        super().__init__()
        self.set_product(product)

        self.catalogue = self._make_catalogue()
        self.s3_client = self._make_client()
    
    def _make_catalogue(self):
        catalogues = [pd.read_csv(c, sep =';', parse_dates=['content_date_start', 'content_date_end']) for c in self.file_catalogue]
        full_catalogue = pd.concat(catalogues, ignore_index=True).copy()

        # add version and RT information to the catalogue
        full_catalogue['version'] = full_catalogue['name'].apply(lambda name: re.search(r'[Vv]\d+\.\d+\.\d+', name).group(0) if re.search(r'[Vv]\d+\.\d+\.\d+', name) else None)
        full_catalogue['consolidation'] = full_catalogue['name'].apply(lambda name: int(re.search(r'RT(\d+)', name).group(1)) if re.search(r'RT\d+', name) else None)
        return full_catalogue

    def _make_client(self):
        aws_access_key, aws_secret_key = self.get_credentials().split(':')
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
                                 config=boto3.session.Config(signature_version='s3v4'),
                                 endpoint_url=self.s3_endpoint)

        # test the credentials by listing the buckets (this will raise an error if the credentials are invalid)
        try:
            s3_client.list_buckets()
            return s3_client
        except ClientError:
            msg = f"AWS credentials are invalid - must be set in the environment variables: {self.credential_env_vars['username']}, {self.credential_env_vars['password']}"
            self.log.error(msg)
            raise ValueError(msg)

    def _filter_catalogue(self, timestep = None, consolidation = None):
        if consolidation is None:
            consolidation = self.consolidation
        
        filtered_catalogue = self.catalogue.copy()

        if consolidation is not None:
            filtered_catalogue = filtered_catalogue[filtered_catalogue['consolidation'].isin(consolidation)].copy()

        if timestep is not None:
            filtered_catalogue = filtered_catalogue[(filtered_catalogue['content_date_start'] == timestep.start)].copy()

        return filtered_catalogue

    def get_credentials(self) -> str:
        if not hasattr(self, 'credentials') or not isinstance(self.credentials, str):
            self.credentials = get_credentials(env_variables=self.credential_env_vars, encode = False)
        return self.credentials

    def get_last_published_ts(self, consolidation = None, **kwargs) -> TimeRange:

        """
        Get the last published timestep for the dataset.
        """

        ts = TimeStep.from_unit(self.frequency)
        last_date = self.get_last_published_date(consolidation=consolidation)
        return ts.from_date(last_date)

    def get_last_published_date(self, consolidation = None, **kwargs) -> dt.datetime:

        """
        Get the last published date for the dataset.
        """

        if consolidation is None:
            consolidation = self.consolidation

        # filter the catalogue based on the accepted RTs
        filtered_catalogue = self._filter_catalogue(consolidation = consolidation)

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
        # filter the catalogue based on the accepted RTs
        filtered_catalogue = self._filter_catalogue(timestep = time_step)

        if len(filtered_catalogue) == 0:
            msg = f"No file found for timestep {time_step} and RTs {self.consolidation}"
            self.log.error(msg)
            raise ValueError(msg)

        # arrange the catalogue based on the consolidation and versions (newest version first and highest consolidation first)
        filtered_catalogue.sort_values(by=['consolidation', 'version'], ascending=[False, False], inplace=True)

        S3_path = filtered_catalogue.iloc[0]['s3_path']
        key_prefix = S3_path.replace('s3://eodata/', '')
        for variable, varoptions in self.variables.items():
            filename = filtered_catalogue.iloc[0]['name'].replace('_cog', '.tiff').replace('-RT', f'-{variable}-RT')
            key = f'{key_prefix}/{filename}'

            # download the file from S3 to the temporary path
            tmp_destination = os.path.join(tmp_path, filename)
            self.log.info(f"Downloading file from S3: {filename}")
            self.s3_client.download_file(self.bucket, key, tmp_destination)

            # open the file using dask for efficient processing
            data = rxr.open_rasterio(tmp_destination, chunks={'x': 1024, 'y': 1024})

            # crop to the bounding box
            data = crop_to_bb(data, space_bounds)

            # all of the metadata in the file is actually already correct...
            yield data, {'variable': variable}