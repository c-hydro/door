from typing import Optional
import logging
from abc import ABC, abstractmethod

import xarray as xr
import os

from .utils.space import BoundingBox
from .utils.io import download_http, check_download, handle_missing, download_ftp
from .utils.netcdf import crop_netcdf

from .tools import timestepping as ts
from .tools.timestepping.timestep import TimeStep
from .tools.data import Dataset

class DOORDownloader(ABC):
    """
    Base class for all DOOR downloaders.
    """

    name = "DOOR_Downloader"
    default_options = {}

    def __init__(self) -> None:
        self.log = logging.getLogger(self.name)

    def get_data(self,
                 time_range: ts.TimeRange,
                 space_bounds:  BoundingBox,
                 destination: Dataset|dict|str,
                 options:  Optional[dict] = None) -> None:
        """
        Get data from this downloader and saves it to a file
        """
        # get options and check them against the default options
        self.set_options(options)

        # ensure destination is a Dataset
        if isinstance(destination, str):
            path = os.path.dirname(destination)
            filename = os.path.basename(destination)
            destination = Dataset.from_options({'path': path, 'filename': filename})
        elif isinstance(destination, dict):
            destination = Dataset.from_options(destination)
        
        # get the timesteps to download
        timesteps = self._get_timesteps(time_range)

        for timestep in timesteps:
            data_struct = self._get_data_ts(timestep, space_bounds)
            if not data_struct:
                self.log.warning(f'No data found for timestep {timestep}')
                continue
            for data, tags in data_struct:
                destination.write_data(data, timestep, **tags)

    @abstractmethod
    def _get_data_ts(self, time_range: TimeStep, space_bounds: BoundingBox) -> list[tuple[xr.DataArray, dict]]:
        """
        Get data from this downloader as xr.Dataset.
        The return structure is a list of tuples, where each tuple contains the data and a dictionary of tags related to that data.
        """
        raise NotImplementedError

    def _get_timesteps(self, time_range: ts.TimeRange) -> list[TimeStep]:
        """
        Get the timesteps to download, assuming.
        """
        if hasattr(self, 'ts_per_year'):
            return time_range.get_timesteps_from_tsnumber(self.ts_per_year)
        else:
            raise NotImplementedError

    def check_options(self, options: Optional[dict] = None) -> dict:
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

    def set_options(self, options: dict) -> None:
        options = self.check_options(options)
        for key, value in options.items():
            setattr(self, key, value)

        if 'variables' in options:
            varopts = options['variables']
            self.set_variables(varopts)

    def check_variables(self, varopts: dict) -> dict:
        if not isinstance(varopts, list): 
            varopts = [varopts]
        if not hasattr(self, 'available_variables'):
            return varopts
        for variable in varopts:
            if variable not in self.available_variables:
                self.log.warning(f'Variable {variable} not available or not implemented/tested, removing from list')
                varopts.remove(variable)
        return varopts
    
    def set_variables(self, varopts: list) -> None:
        varopts = self.check_variables(varopts)
        self.variables = {}
        for var in varopts:
            self.variables[var] = self.available_variables[var]
            
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

    name = "URL_Downloader"

    def __init__(self, url_blank: str, protocol: str = 'http', host: str|None = None) -> None:

        self.url_blank = url_blank
        if protocol.lower() not in ['http', 'ftp']:
            raise ValueError(f'Protocol {protocol} not supported')
        else:
            self.protocol = protocol.lower()

        if self.protocol == 'ftp':
            if host is None:
                raise ValueError(f'FTP host must be specified')
            else:
                self.host = host

        super().__init__()

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
        if not "auth" in kwargs:
            kwargs["auth"] = None

        url = self.format_url(**kwargs)
        if self.protocol == 'http':
            try:
                download_http(url, destination, kwargs["auth"])
            except Exception as e:
                handle_missing(missing_action, kwargs)
                self.log.debug(f'Error downloading {url}: {e}')
                return False
        elif self.protocol == 'ftp':
            try:
                download_ftp(self.host, url, destination, kwargs["auth"])
            except Exception as e:
                handle_missing(missing_action, kwargs)
                self.log.debug(f'Error downloading {url}: {e}')
                return False

        success_flag, success_msg = check_download(destination, min_size, missing_action)
        if success_flag > 0:
            handle_missing(missing_action, kwargs)
            self.log.debug(f'Error downloading file from {url}: {success_msg}')
            return False

        return True

class APIDownloader(DOORDownloader):
    """
    Downloader for data from an API.
    This typer of downloader is useful for data that can be downloaded from an API.
    Once and API client is specified, it uses a dict to send a request.
    """

    name = "API_Downloader"

    def __init__(self, client) -> None:
        self.client = client
        super().__init__()

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
            self.log.debug(f'Output: {output}')
        except Exception as e:
            handle_missing(missing_action, kwargs)
            self.log.debug(f'Error downloading data: {e}')
            return False

        success_flag, success_msg = check_download(destination, min_size, missing_action)
        if success_flag > 0:
            handle_missing(missing_action, kwargs)
            self.log.debug(f'Error downloading data: {success_msg}')
            return False

        return True
    
    def retrieve(self, **kwargs):
        return self.client.retrieve(**kwargs)






