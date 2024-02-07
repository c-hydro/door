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

class FRCdownloader(DOORDownloader):

    def __init__(self, product: str, max_steps: int) -> None:
        self.product = product
        self.max_steps = max_steps
        self.freq_hours = None

    def compute_model_steps(self, start: dt.datetime) -> (list, list):
        """
        Compute the forecast steps for the model with regular n-hourly time frequency
        """
        step_h = self.freq_hours
        max_step = self.max_steps + step_h
        forecast_steps = np.arange(step_h, max_step, step_h)
        time_range = [start + pd.Timedelta(str(i) + "h") for i in forecast_steps]
        return time_range, forecast_steps

    def check_max_steps(self, max_steps_model: int) -> None:
        """
        Check if selected max_steps is available for the model
        """
        if self.max_steps >= max_steps_model:
            print(f'ERROR! Only the first {max_steps_model} forecast hours are available!')
            self.max_steps = max_steps_model

    def postprocess_forecast(self, frc_out: xr.Dataset, space_bounds: BoundingBox) -> None:
        """
        Postprocess the forecast data.
        """
        # Drop existing time dimension (it refers to issue forecast time)
        frc_out = frc_out.drop_vars("time", errors='ignore')

        # Assign new time dimension and rename spatial coordinates
        frc_out = frc_out.assign_coords({self.frc_dims["time"]: self.frc_time_range}).rename({v: k for k, v in self.frc_dims.items()})

        # Crop with bounding box
        frc_out = frc_out.where((frc_out.lat <= space_bounds.bbox[3]) &
                 (frc_out.lat >= space_bounds.bbox[1]) &
                 (frc_out.lon >= space_bounds.bbox[0]) &
                 (frc_out.lon <= space_bounds.bbox[2]), drop=True)

        # If lat is a decreasing vector, flip it and the associated variables vertically
        if frc_out.lat.values[0] > frc_out.lat.values[-1]:
            print(" --> WARNING! Latitude is decreasing, flip it and the associated variables vertically!")
            frc_out = frc_out.reindex(lat=frc_out.lat[::-1])
            for var in frc_out.data_vars:
                frc_out[var] = frc_out[var].reindex(lat=frc_out.lat[::-1])

        # Drop unused variables
        frc_out = frc_out.drop(["valid_time", "step", "surface", "heightAboveGround"], errors='ignore')
        frc_out["lat"].attrs["units"] = "degrees_north"
        frc_out["lon"].attrs["units"] = "degrees_east"

        return frc_out
