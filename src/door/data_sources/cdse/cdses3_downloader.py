import os
import xarray as xr
import rioxarray as rxr
from typing import Iterable, Iterator
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
        "consolidation": None, # FAPAR only
        "tvalue"       : None, # SWI only
        "variables"    : None, # None means all available variables for the product
    }

    available_products = {
        'fapar': {
            'product_name': ['fapar_global_300m_10daily_v2'],

            's3_key_root'     : 'CLMS/bio-geophysical/vegetation_properties/fapar_global_300m_10daily_v2',
            's3_path_pattern' : '%Y/%m/%d/c_gls_FAPAR300-RT{consolidation}_%Y%m%d0000_GLOBE_OLCI_{version}_cog',
            's3_file_pattern' : 'c_gls_FAPAR300-{variable}-RT{consolidation}_%Y%m%d0000_GLOBE_OLCI_{version}_cog.tiff',

            "default_consolidation": [0,6], # RT0 and RT6 are the most commonly used consolidation levels for FAPAR, but we can include more if needed
            "frequency": "dekad",
            "data_type": "UINT8",
            "fill_value": 255,
            "resolution": 1 / 336,
            "available_bounds": (-180, -60, 180, 80),
        },

        'swi': {
            'product_name': ['swi_global_12.5km_10daily_v4'],
            's3_key_root'     : 'CLMS/bio-geophysical/soil_water_index/swi_global_12.5km_10daily_v4',
            's3_path_pattern' : '%Y/%m/%d/c_gls_SWI10_%Y%m%d1200_GLOBE_ASCAT_{version}_cog',
            's3_file_pattern' : 'c_gls_SWI10-{variable}{t_value:03d}_%Y%m%d1200_GLOBE_ASCAT_{version}.tiff',

            "default_t-value" : [1,5,10,15,20,40,60,100], # all available t-values
            "frequency": "dekad",
            "data_type": "UINT8",
            "fill_value": 255,
            "resolution": 0.1,  # docs describe this as 0.1 degree / ~12.5 km
            "available_bounds": (-180, -90, 180, 90),
        }
    }

    available_variables = {
        "fapar": {
            "FAPAR":         {"scale_factor": 1 / 250},
            "NOBS":          {"scale_factor": 1      },
            "QFLAG":         {"scale_factor": 1      },
            "RMSE":          {"scale_factor": 1 / 250},
            "LENGTH_BEFORE": {"scale_factor": 1      },
            "LENGTH_AFTER":  {"scale_factor": 1      },
        },

        "swi": {
            "SWI":   {"scale_factor": 1 / 200}, # 10-daily Soil Water Index
            "QFLAG": {"scale_factor": 1 / 200}, # Quality flags
            "VOBS":  {"scale_factor": 1 / 100}, # Percentage of valid observations in the 10-day synthesis period
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
            if self.tvalue is None:
                self.tvalue = self.available_products[self.product]["default_t-value"]
            elif not isinstance(self.tvalue, list):
                self.tvalue = [self.tvalue]
        elif self.product == "fapar":
            if hasattr(self, 'tvalues') and self.tvalues is not None:
                self.log.info(f"tvalues option will be ignored for product '{self.product}'")
            self.tvalue = [None]
            if self.consolidation is None:
                self.consolidation = self.available_products[self.product]["default_consolidation"]
            elif not isinstance(self.consolidation, list):
                self.consolidation = [self.consolidation]
    
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

    def _list_subdirs(self, prefix: str) -> list[str]:
        """Return the names of the folders directly under an S3 key prefix."""
        paginator = self.s3_client.get_paginator('list_objects_v2')
        names = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter='/'):
            for common_prefix in page.get('CommonPrefixes', []):
                names.append(common_prefix['Prefix'][len(prefix):].rstrip('/'))
        return names

    def _iter_dates_desc(self) -> Iterator[dt.date]:
        """Yield every date folder (YYYY/MM/DD) under the product root, newest first."""
        root = f"{self.s3_key_root}/"
        for year in sorted(self._list_subdirs(root), reverse=True):
            for month in sorted(self._list_subdirs(f"{root}{year}/"), reverse=True):
                for day in sorted(self._list_subdirs(f"{root}{year}/{month}/"), reverse=True):
                    yield dt.date(int(year), int(month), int(day))

    def _product_folder_regex(self, date: dt.date) -> re.Pattern:
        """Regex matching the product folder names expected on a date, capturing consolidation and version."""
        leaf = date.strftime(self.s3_path_pattern.rsplit('/', 1)[-1])
        regex = re.escape(leaf)
        regex = regex.replace(re.escape('{consolidation}'), r'(?P<consolidation>\d+)')
        regex = regex.replace(re.escape('{version}'), r'(?P<version>[Vv]\d+\.\d+\.\d+)')
        return re.compile(regex)

    def _list_products_on_date(self, date: dt.date) -> list[dict]:
        """List the product folders available on S3 for a date, parsed into consolidation and version."""
        regex = self._product_folder_regex(date)
        prefix = f"{self.s3_key_root}/{date.strftime('%Y/%m/%d')}/"
        products = []
        for name in self._list_subdirs(prefix):
            match = regex.fullmatch(name)
            if match is None:
                continue
            groups = match.groupdict()
            products.append({
                'name': name,
                'consolidation': int(groups['consolidation']) if groups.get('consolidation') else None,
                'version': groups.get('version'),
                'key_prefix': f"{prefix}{name}",
            })
        return products

    @staticmethod
    def _filter_products(products: list[dict], consolidation=None) -> list[dict]:
        """Keep only the products whose consolidation is in the accepted list (no filter if None)."""
        if consolidation is None:
            return products
        accepted = consolidation if isinstance(consolidation, list) else [consolidation]
        return [p for p in products if p['consolidation'] in accepted]

    def get_last_published_date(self, consolidation = None, **kwargs) -> dt.datetime:

        """
        Get the last published date for the dataset, by searching the S3 date folders newest first.
        """

        if consolidation is None:
            consolidation = self.consolidation

        for date in self._iter_dates_desc():
            if self._filter_products(self._list_products_on_date(date), consolidation):
                return dt.datetime.combine(date, dt.time())

        raise ValueError(f"No products found on S3 for product '{self.product}' and consolidation {consolidation}")

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
        target_date = time_step.end.date()
        products = self._filter_products(self._list_products_on_date(target_date), consolidation=self.consolidation)
        self.log.debug(f"_get_data_ts after S3 listing for {target_date}: got {len(products)} matching entries")

        if len(products) == 0:
            if self.product == "swi" or self.consolidation is None:
                msg = f"No file found for timestep {time_step}"
            else:
                msg = f"No file found for timestep {time_step} and RTs {self.consolidation}"

            self.log.error(f"{msg} [product={self.product}, consolidation={self.consolidation}, s3_root={self.s3_key_root}, date={target_date}]")
            raise ValueError(msg)

        # newest version first, then highest consolidation first
        products.sort(
            key=lambda p: (
                p['consolidation'] if p['consolidation'] is not None else -1,
                tuple(map(int, re.findall(r'\d+', p['version'] or ''))),
            ),
            reverse=True,
        )

        product = products[0]
        this_consolidation = product['consolidation']
        key_prefix = product['key_prefix']
        for variable, varoptions in self.variables.items():
            for t in self.tvalue:
                if t is not None:
                    t = int(t)
                    varname = f"{variable}{t:03d}"
                else:
                    varname = variable

                filename = self._make_cog_filename(product["name"], varname)

                key = f"{key_prefix}/{filename}"
                tmp_destination = os.path.join(tmp_path, filename)

                self.log.info(f"Downloading file from S3: {filename}")
                self.s3_client.download_file(self.bucket, key, tmp_destination)

                # open the file using dask for efficient processing
                data = rxr.open_rasterio(tmp_destination, chunks={"x": 1024, "y": 1024})
                data = crop_to_bb(data, space_bounds)

                fill_value   = self.fill_value
                scale_factor = varoptions.get("scale_factor", 1)

                attrs = {
                    "scale_factor": scale_factor,
                    "fill_value": fill_value,
                }
                data.attrs.update(attrs)

                if t is not None:
                    data.attrs["tvalue"] = t
                    yield data, {"variable": variable, "tvalue": t}
                if this_consolidation is not None:
                    data.attrs["consolidation"] = this_consolidation
                    yield data, {"variable": variable, "consolidation": this_consolidation}
