from typing import Optional
import datetime as dt
import logging
import os

import xarray as xr
import requests

from .utils.parse import format_dict
from .utils.time import TimeRange
from .utils.space import SpatialReference

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
                 space_ref:  SpatialReference,
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
            logging.warning(f'{self.name} data not available: {options}')
        elif level.lower() in ['i', 'ignore']:
            pass
        else:
            raise ValueError(f'Invalid missing data error level: {level}')

class HTTPDownloader(DOORDownloader):
    """
    Base class for all DOOR downloaders that download data from HTTP.
    """

    def __init__(self, url_blank) -> None:
        self.url_blank = url_blank

    def download(self,
                 destination: str,
                 min_size: float = None,
                 missing_action: str = 'error',
                 **kwargs) -> str:
        """
        Downloads data from http or https url
        Eventually check file size to avoid empty files
        """
        url = self.url_blank.format(**kwargs)
        try:
            r = requests.get(url)
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            with open(destination, 'wb') as f:
                f.write(r.content)
        except Exception as e:
            self.handle_missing(missing_action, kwargs)
            return False

        # check if file has been actually dowloaded
        if not os.path.isfile(destination):
            self.handle_missing(missing_action, kwargs)
            return False
        
        # check if file is empty
        if min_size is not None and os.path.getsize(destination) < min_size:
            self.handle_missing(missing_action, kwargs)
            return False
        
        return True

