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
        # Note: "product" is not included here because it's set via __init__, not set_options
        "consolidation": [0,6], # will take the highest available for a timestep (FAPAR only)
        "variables": None,      # None means all available variables for the product
    }

    available_products = {
        'fapar': {
            'product_name': ['fapar_global_300m_10daily_v2'],
            'file_catalogue': [
                'https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/'
                'bio-geophysical/vegetation_properties/fapar_global_300m_10daily_v2/'
                'fapar_global_300m_10daily_v2_cog.csv'
            ],
            "frequency": "dekad",
            "data_type": "UINT8",
            "resolution": 1 / 336,
            "available_bounds": (-180, -60, 180, 80),
        },

        'swi': {
            'product_name': ['swi_global_12.5km_10daily_v4'],
            'file_catalogue': [
                'https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/'
                'bio-geophysical/soil_water_index/swi_global_12.5km_10daily_v4/'
                'swi_global_12.5km_10daily_v4_cog.csv'
            ],
            "frequency": "dekad",
            "data_type": "UINT8",
            "resolution": 0.1,  # docs describe this as 0.1 degree / ~12.5 km
            "available_bounds": (-180, -90, 180, 90),
        }
    }

    available_variables = {
        "fapar": {
            "FAPAR":         {"scale_factor": 1 / 250, "fill_value": 255},
            "NOBS":          {"scale_factor": 1,       "fill_value": 255},
            "QFLAG":         {"scale_factor": 1,       "fill_value": 255},
            "RMSE":          {"scale_factor": 1 / 250, "fill_value": 255},
            "LENGTH_BEFORE": {"scale_factor": 1,       "fill_value": 255},
            "LENGTH_AFTER":  {"scale_factor": 1,       "fill_value": 255},
        },

        "swi": {
            # 10-daily Soil Water Index at different characteristic time lengths
            "SWI001": {"scale_factor": 0.05, "fill_value": 255},
            "SWI005": {"scale_factor": 0.05, "fill_value": 255},
            "SWI010": {"scale_factor": 0.05, "fill_value": 255},
            "SWI015": {"scale_factor": 0.05, "fill_value": 255},
            "SWI020": {"scale_factor": 0.05, "fill_value": 255},
            "SWI040": {"scale_factor": 0.05, "fill_value": 255},
            "SWI060": {"scale_factor": 0.05, "fill_value": 255},
            "SWI100": {"scale_factor": 0.05, "fill_value": 255},

            # Quality flags
            "QFLAG001": {"scale_factor": 0.05, "fill_value": 255},
            "QFLAG005": {"scale_factor": 0.05, "fill_value": 255},
            "QFLAG010": {"scale_factor": 0.05, "fill_value": 255},
            "QFLAG015": {"scale_factor": 0.05, "fill_value": 255},
            "QFLAG020": {"scale_factor": 0.05, "fill_value": 255},
            "QFLAG040": {"scale_factor": 0.05, "fill_value": 255},
            "QFLAG060": {"scale_factor": 0.05, "fill_value": 255},
            "QFLAG100": {"scale_factor": 0.05, "fill_value": 255},

            # Percentage of valid observations in the 10-day synthesis period
            "VOBS001": {"scale_factor": 0.1, "fill_value": 255},
            "VOBS005": {"scale_factor": 0.1, "fill_value": 255},
            "VOBS010": {"scale_factor": 0.1, "fill_value": 255},
            "VOBS015": {"scale_factor": 0.1, "fill_value": 255},
            "VOBS020": {"scale_factor": 0.1, "fill_value": 255},
            "VOBS040": {"scale_factor": 0.1, "fill_value": 255},
            "VOBS060": {"scale_factor": 0.1, "fill_value": 255},
            "VOBS100": {"scale_factor": 0.1, "fill_value": 255},
        },
    }
    
    def __init__(self, product: str, **kwargs) -> None:
        super().__init__()
        self.log.debug(f"CDSES3Downloader.__init__ called with product='{product}' (type: {type(product).__name__})")
        
        # Check if product is a template string that wasn't replaced
        if isinstance(product, str) and '{' in product:
            self.log.error(f"Product is a template string: '{product}' - it appears the workflow didn't substitute template variables!")
            raise ValueError(f"Product contains template variable: {product}")
        
        self.set_product(product)
        self.log.debug(f"After set_product: self.product='{self.product}'")

        # Initialize catalogue and S3 client for both products
        self.catalogue = self._make_catalogue()
        self.s3_client = self._make_client()
    
    def set_options(self, options: dict) -> None:
        """
        Override to ensure SWI products ignore consolidation in options.
        - SWI does not have consolidation levels, so consolidation must be None
        - Product is set via __init__, not via options, so preserve it
        """

        self.log.debug(f"set_options called: product={self.product}, options={options}")
        
        super().set_options(options)
        
        self.log.debug(f"After super().set_options(): consolidation={getattr(self, 'consolidation', 'not set')}, product={self.product}")
        
        # Ensure SWI products don't use consolidation
        if self.product == "swi":
            if self.consolidation is not None:
                self.log.info(f"Consolidation option will be ignored for product '{self.product}'")
                self.consolidation = None
    
    def _make_catalogue(self):
        catalogues = [pd.read_csv(c, sep =';', parse_dates=['content_date_start', 'content_date_end']) for c in self.file_catalogue]
        full_catalogue = pd.concat(catalogues, ignore_index=True).copy()

        # add version to the catalogue
        full_catalogue['version'] = full_catalogue['name'].apply(lambda name: re.search(r'[Vv]\d+\.\d+\.\d+', name).group(0) if re.search(r'[Vv]\d+\.\d+\.\d+', name) else None)
        
        # if the product is FAPAR, add consolidation (RT) to the catalogue
        if self.product == "fapar":
            full_catalogue['consolidation'] = full_catalogue['name'].apply(lambda name: int(re.search(r'RT(\d+)', name).group(1)) if re.search(r'RT\d+', name) else None)

        # add 1 day to the content_date_start for SWI products to align with the actual date of the data
        if self.product == "swi":
            full_catalogue['content_date_start'] = full_catalogue['content_date_start'] + pd.Timedelta(days=1)
            
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

    def _filter_catalogue(self, timestep=None, consolidation=None):
        if consolidation is None:
            consolidation = self.consolidation

        filtered_catalogue = self.catalogue.copy()

        # Filter by consolidation if specified and if the column exists
        if consolidation is not None and "consolidation" in filtered_catalogue.columns:
            filtered_catalogue = filtered_catalogue[filtered_catalogue["consolidation"].isin(consolidation)].copy()

        if timestep is not None:
            # SWI catalogue dates are commonly at 12:00, while TimeStep starts
            # are often midnight. Match by calendar date to avoid missing files.
            target_date = pd.Timestamp(timestep.start).date()
            filtered_catalogue = filtered_catalogue[filtered_catalogue["content_date_start"].dt.date == target_date].copy()

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
    
    def _make_cog_filename(self, catalogue_name: str, variable: str) -> str:
        """
        Convert the catalogue product name into the per-variable COG filename.
        """

        base = catalogue_name.replace("_cog", "")

        if self.product == "fapar":
            return base.replace("-RT", f"-{variable}-RT") + ".tiff"

        elif self.product == "swi":           
            return base.replace("c_gls_SWI10_", f"c_gls_SWI10-{variable}_", 1) + ".tiff"
        
        else:
            raise ValueError(f"No COG filename rule implemented for product: {self.product}")
    
    def _get_data_ts(self,
                     time_step: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str,
                     **kwargs) -> Iterable[tuple[xr.DataArray, dict]]:

        """
        Get the data for a specific timestep.
        """
        # Ensure consolidation is None for SWI products
        self.log.debug(f"_get_data_ts START: product={self.product}, consolidation={self.consolidation}, timestep={time_step}")
        
        self.log.debug(f"_get_data_ts after check: product={self.product}, consolidation={self.consolidation}")
        filtered_catalogue = self._filter_catalogue(timestep = time_step)
        self.log.debug(f"_get_data_ts after filter: got {len(filtered_catalogue)} matching entries")

        if len(filtered_catalogue) == 0:
            if self.product == "swi" or self.consolidation is None:
                msg = f"No file found for timestep {time_step}"
            else:
                msg = f"No file found for timestep {time_step} and RTs {self.consolidation}"
            
            # Debug info
            available_dates = sorted(self.catalogue['content_date_start'].unique()) if 'content_date_start' in self.catalogue.columns else []
            self.log.error(f"{msg} [product={self.product}, consolidation={self.consolidation}, catalogue_size={len(self.catalogue)}, available_dates={available_dates[:5] if available_dates else 'none'}]")
            raise ValueError(msg)

        # arrange the catalogue based on the consolidation and versions (newest version first and highest consolidation first)
        sort_columns = []
        ascending = []

        if "consolidation" in filtered_catalogue.columns and filtered_catalogue["consolidation"].notna().any():
            sort_columns.append("consolidation")
            ascending.append(False)

        if filtered_catalogue["version"].notna().any():
            sort_columns.append("version")
            ascending.append(False)

        if sort_columns:
            filtered_catalogue.sort_values(by=sort_columns,ascending=ascending,inplace=True)

        S3_path = filtered_catalogue.iloc[0]['s3_path']
        key_prefix = S3_path.replace('s3://eodata/', '')
        for variable, varoptions in self.variables.items():
            filename = self._make_cog_filename(filtered_catalogue.iloc[0]["name"],variable)

            key = f"{key_prefix}/{filename}"
            tmp_destination = os.path.join(tmp_path, filename)

            self.log.info(f"Downloading file from S3: {filename}")
            self.s3_client.download_file(self.bucket, key, tmp_destination)

            # open the file using dask for efficient processing
            data = rxr.open_rasterio(tmp_destination, chunks={"x": 1024, "y": 1024})
            data = crop_to_bb(data, space_bounds)

            fill_value   = varoptions.get("fill_value")
            scale_factor = varoptions.get("scale_factor", 1)

            data.attrs.update({"scale_factor": scale_factor,"fill_value": fill_value})

            yield data, {"variable": variable}