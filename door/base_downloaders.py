from typing import Optional
import datetime as dt
import os

import pandas as pd
import numpy as np
import xarray as xr
import requests

from .utils.parse import format_dict
from .utils.time import TimeRange
from .utils.space import BoundingBox
from .utils.io import download_http

import logging
logger = logging.getLogger(__name__)

class DOORDownloader():
    """
    Base class for all DOOR downloaders.
    """

    name = "DOOR Downloader"
    default_options = {}
    url_blank = None

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

    def handle_missing(self, level: str, specs: dict = {}):
        """
        Handle missing data.
        Level can be 'error', 'warn' or 'ignore'.
        """
        options = format_dict(specs)
        if level.lower() in ['e', 'error']:
            raise FileNotFoundError(f'ERROR! {self.name} data not available: {options}')
        elif level.lower() in ['w', 'warn', 'warning']:
            logger.warning(f'{self.name} data not available: {options}')
        elif level.lower() in ['i', 'ignore']:
            pass
        else:
            raise ValueError(f'Invalid missing data error level: {level}')
    def download(self, destination: str, min_size: float = None, missing_action: str = 'error',
                 protocol: str = 'http', **kwargs) -> bool:
        """
        Downloads data from url
        Eventually check file size to avoid empty files
        """
        if protocol.lower() == 'http':
            url = self.url_blank.format(**kwargs)
            try:
                download_http(url, destination)
            except Exception as e:
                self.handle_missing(missing_action, kwargs)
                return False
        else:
            raise ValueError(f'Protocol {protocol} not supported')

        # check if file has been actually downloaded
        if not os.path.isfile(destination):
            self.handle_missing(missing_action, kwargs)
            return False

        # check if file is empty
        if min_size is not None and os.path.getsize(destination) < min_size:
            self.handle_missing(missing_action, kwargs)
            return False

        return True

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
        time_range = [start + pd.Timedelta(str(i) + "H") for i in forecast_steps]
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
