from typing import Optional
import datetime as dt
import os

import pandas as pd
import numpy as np
import xarray as xr
import requests

from .utils.time import TimeRange
from .utils.space import BoundingBox
from .utils.io import download_http, check_download, handle_missing

from .utils.netcdf import crop_netcdf

import logging
logger = logging.getLogger(__name__)

class DOORDownloader():
    """
    Base class for all DOOR downloaders.
    """

    name = "DOOR Downloader"
    default_options = {}

    def __init__(self) -> None:
        pass

    def get_data(self,
                 time_range: TimeRange,
                 space_bounds:  BoundingBox,
                 destination: str,
                 options:  Optional[dict] = None) -> xr.Dataset:
        """
        Get data from this downloader as an xarray.Dataset.
        The output dataset will have 3 dimensions: time, latitude (or y), longitude (or x),
        and 1 or more variables as specified in the variables argument.
        """
        raise NotImplementedError()

    def check_options(self, options: dict) -> dict:
        """
        Check options and set defaults.
        """
        if options is None:
            return self.default_options
        for key, value in self.default_options.items():
            if key not in options:
                options[key] = value

        keys_to_delete = []
        for key in options:
            if key not in self.default_options:
                logging.warning(f'Unknown option {key} will be ignored')
                keys_to_delete.append(key)
        for key in keys_to_delete:
            del options[key]
    
        return options
    
    #TODO: this is a bit of an akward spot to put this, but it is used by all forecast downloaders, so it makes some sense to have it here
    def postprocess_forecast(self, ds: xr.Dataset, space_bounds: BoundingBox) -> None:
        """
        Postprocess the forecast data.
        """
        # Drop existing time dimension (it refers to issue forecast time)
        ds = ds.drop_vars("time", errors='ignore')

        # Assign new time dimension and rename spatial coordinates
        ds = ds.assign_coords({self.frc_dims["time"]: self.frc_time_range}).rename({v: k for k, v in self.frc_dims.items()})

        # Crop with bounding box
        ds = crop_netcdf(ds, space_bounds)

        # If lat is a decreasing vector, flip it and the associated variables vertically
        if ds.lat.values[0] > ds.lat.values[-1]:
            print(" --> WARNING! Latitude is decreasing, flip it and the associated variables vertically!")
            ds = ds.reindex(lat=ds.lat[::-1])
            for var in ds.data_vars:
                ds[var] = ds[var].reindex(lat=ds.lat[::-1])

        # Drop unused variables
        ds = ds.drop(["valid_time", "step", "surface", "heightAboveGround"], errors='ignore')
        ds["lat"].attrs["units"] = "degrees_north"
        ds["lon"].attrs["units"] = "degrees_east"

        return ds

class URLDownloader(DOORDownloader):
    """
    Downloader for data from a URL.
    This typer of downloader is useful for data that can be downloaded from a URL.
    It allows to specify a URL template with placeholders for various parameters (as keyword arguments).
    """

    def __init__(self, url_blank: str, protocol: str = 'http') -> None:
        
        self.url_blank = url_blank
        if protocol.lower() != 'http':
            raise ValueError(f'Protocol {protocol} not supported')
        else:
            self.protocol = protocol

    def format_url(self, **kwargs) -> str:
        """
        Format the URL with the specified parameters.
        """
        return self.url_blank.format(**kwargs)

    def download(self, destination: str, min_size: float = None, missing_action: str = 'error',
                 **kwargs) -> bool:
        """
        Downloads data from url
        Eventually check file size to avoid empty files
        """

        url = self.format_url(**kwargs)
        if self.protocol == 'http':
            try:
                download_http(url, destination)
            except Exception as e:
                handle_missing(missing_action, kwargs)
                logger.debug(f'Error downloading {url}: {e}')
                return False

        success_flag, success_msg = check_download(destination, min_size, missing_action)
        if success_flag > 0:
            handle_missing(missing_action, kwargs)
            logger.debug(f'Error downloading file from {url}: {success_msg}')
            return False

        return True

class APIDownloader(DOORDownloader):
    """
    Downloader for data from an API.
    This typer of downloader is useful for data that can be downloaded from an API.
    Once and API client is specified, it uses a dict to send a request.
    """

    def __init__(self, client) -> None:
        self.client = client
        pass
        #self.cds = cdsapi.Client(progress=False)#, quiet=True)

    def download(self, destination: str, min_size: float = None, missing_action: str = 'error', **kwargs) -> bool:
        """
        Downloads data from the CDS API based on the request.
        dataset: the name of the dataset to download from
        request: a dictionary with the request parameters
        output: the name of the output file
        """
        # send request to the client (this works for ecmwf and cdsapi, not sure how generalisable it is)
        try:
            output = self.client.retrieve(**kwargs)
            logger.debug(f'Output: {output}')
        except Exception as e:
            handle_missing(missing_action, kwargs)
            logger.debug(f'Error downloading data: {e}')
            return False

        success_flag, success_msg = check_download(destination, min_size, missing_action)
        if success_flag > 0:
            handle_missing(missing_action, kwargs)
            logger.debug(f'Error downloading data: {success_msg}')
            return False

        return True
