from typing import Optional, Iterable, Sequence, Any
from copy import deepcopy
import logging
from abc import ABC, ABCMeta, abstractmethod
import datetime as dt

import tempfile
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
import xarray as xr
import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.transform import Affine
import os

import paramiko
import ftplib
import requests

from .utils.io import check_download, handle_missing
from .utils.time import HalfHourTimeStep
from .utils.exceptions import (
    ConfigurationError,
    DataUnavailableError,
    DataValidationError,
    OperationalError,
    ProcessingError,
)

from d3tools import spatial as sp
from d3tools import timestepping as ts
from d3tools.data import Dataset
from d3tools.spatial import BoundingBox
from d3tools.timestepping.timestep import TimeStep
from d3tools.exit import rm_at_exit

class MetaDOORDownloader(ABCMeta):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        elif 'source' in attrs:
            sources = [attrs['source'], *attrs.get('source_aliases', [])]
            for source in sources:
                cls.subclasses[source] = cls
                if isinstance(source, str):
                    cls.subclasses[source.lower()] = cls

class DOORDownloader(ABC, metaclass=MetaDOORDownloader):
    """
    Base class for all DOOR downloaders.
    """

    name = "DOOR_Downloader"
    default_options = {}

    single_temp_folder = False
    separate_vars = False
    def __init__(self) -> None:
        self.log = logging.getLogger("door." + self.name)

    # Factory ---------
    @classmethod
    def from_options(cls, source: dict|str|None, *args, **kwargs) -> 'DOORDownloader':
        if isinstance(source, dict):
            init_options = source.copy()
            init_options.update(kwargs)
            source = init_options.pop('source', None)
        elif isinstance(source, str) or source is None:
            init_options = kwargs.copy()
        else:
            raise TypeError("'source' must be a mapping, a string, or None")

        source = cls.get_source(source)
        if source is None:
            raise ValueError("No data source specified in downloader options")

        Subclass: 'DOORDownloader' = cls.get_subclass(source)

        bdo = {}
        bdo['bounds'] = init_options.pop('bounds', None)
        bdo['destination'] = init_options.pop('destination', None)
        bdo['options'] = init_options.pop('options', None)
        
        if bdo['options'] is None:
            bdo['options'] = {}
        if not isinstance(bdo['options'], dict):
            raise TypeError("'options' must be a mapping")

        downloader = Subclass(*args, **init_options)
        downloader.set_bounds(bdo['bounds'])
        downloader.set_destination(bdo['destination'])
        downloader.set_options(bdo['options'])

        return downloader

    @classmethod
    def get_subclass(cls, source: str):
        source = cls.get_source(source)
        Subclass: 'Dataset'|None = cls.subclasses.get(source)
        if Subclass is None and isinstance(source, str):
            Subclass = cls.subclasses.get(source.lower())
        if Subclass is None:
            raise ValueError(f"Invalid data source: {source}")
        return Subclass
    
    @classmethod
    def get_source(cls, source: Optional[str] = None):
        if source is not None:
            return source
        elif hasattr(cls, 'source'):
            return cls.source

    def set_bounds(self, bounds: None|sp.BoundingBox|list[float]|tuple[float]|Dataset) -> None:
        """
        Set the bounds of the data to download.
        """
        if bounds is None:
            return
        elif isinstance(bounds, sp.BoundingBox):
            _bounds = bounds
        elif isinstance(bounds, (list, tuple)):
            _bounds = sp.BoundingBox(*bounds)
        elif isinstance(bounds, str):
            _bounds = sp.BoundingBox.from_file(bounds)
        else:
            try:
                _bounds = sp.BoundingBox.from_dataset(bounds)
            except Exception as error:
                raise ValueError('Invalid bounds') from error

        self.bounds = _bounds

    def set_destination(self, destination: Dataset|dict|str|None) -> None:
        """
        Set the destination of the data to download.
        """
        if destination is None:
            return
        elif isinstance(destination, str):
            path = os.path.dirname(destination)
            filename = os.path.basename(destination)
            destination = Dataset.from_options({'path': path, 'filename': filename})
        elif isinstance(destination, dict):
            destination = Dataset.from_options(destination)
        
        self.destination = destination

    def get_data(self,
                 time_range: ts.TimeRange|Sequence[dt.datetime],
                 space_bounds:  Optional[sp.BoundingBox] = None,
                 destination: Optional[Dataset|dict|str] = None,
                 options:  Optional[dict] = None) -> None:
        """
        Get data from this downloader and saves it to a file
        """
        # get options and check them against the default options
        time_range = self._check_get_data_args(time_range, space_bounds, destination, options)

        timesteps = self._get_timesteps(time_range)
        if not timesteps:
            self.log.warning(f"No valid timesteps found between {time_range.start} and {time_range.end}")
            return

        self.log.info(
            " --> Download window: %s to %s (%s %s)",
            time_range.start.strftime("%Y-%m-%d %H:%M"),
            time_range.end.strftime("%Y-%m-%d %H:%M"),
            len(timesteps),
            timesteps[0].unit,
        )
        self.log.info(" --> Space bounds: %s", self.bounds.bbox)

        # sometimes it is convenient to download each variable separately, others it is better to download all the data at once
        if self.separate_vars:
            for variable in self.variables:
                self.variable = variable
                self._loop_timesteps_and_save_data(timesteps)
        else:
            self._loop_timesteps_and_save_data(timesteps)

    def _check_get_data_args(self,
                             time_range: ts.TimeRange|Sequence[dt.datetime],
                             space_bounds:  Optional[sp.BoundingBox] = None,
                             destination: Optional[Dataset|dict|str] = None,
                             options:  Optional[dict] = None) -> tuple[ts.TimeRange, sp.BoundingBox, Dataset]:
        
        # get options and check them against the default options
        if options is not None: 
            self.set_options(options)

        # set the space bounds
        self.set_bounds(space_bounds)
        # check if the space bounds are set
        if not hasattr(self, 'bounds'):
            raise ValueError('No space bounds specified')
        
        # set the destination
        self.set_destination(destination)
        # check if the destination is set
        if not hasattr(self, 'destination'):
            raise ValueError('No destination specified')
        
        # get the timesteps to download
        if isinstance(time_range, Sequence):
            time_range = list(time_range)
            time_range.sort()
            time_range = ts.TimeRange(time_range[0], time_range[-1])
        
        return time_range
        
    def _loop_timesteps_and_save_data(self, timesteps: list[ts.TimeStep]) -> None:
        # download the data, either in a single temp folder or in separate temp folders (one per timestep)
        # the latter is more space efficient, but at times you have to download the data for several timesteps at once
        # so it is better to have the option to download all the data in a single folder
        if self.single_temp_folder:
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True, dir=os.getenv('TMP')) as tmp_path:
                for timestep in timesteps:
                    self._get_and_save_data_ts(timestep, tmp_path)
                rm_at_exit(tmp_path)
        else:
            for timestep in timesteps:
                with tempfile.TemporaryDirectory(ignore_cleanup_errors=True, dir=os.getenv('TMP')) as tmp_path:
                    self._get_and_save_data_ts(timestep, tmp_path)
                rm_at_exit(tmp_path)

    def _get_and_save_data_ts(self,
                              timestep: ts.TimeStep,
                              tmp_path: str) -> None:
        
        data_struct = self._get_data_ts(timestep, self.bounds, tmp_path)
        if data_struct is None:
            self.log.warning(' --> No data found for timestep %s', timestep)
            return

        data_written = False
        for data, item_tags in data_struct:
            data_written = True
            tags = dict(item_tags or {})
            write_timestep = tags.pop('timestep', timestep)
            try:
                self.destination.write_data(data, write_timestep, **tags)
            finally:
                data.close()

            tags_str = ', '.join(f'{k}={v}' for k, v in tags.items())
            msg0 = f"Data for {write_timestep}"
            msg2 = f" saved to {self.destination.get_key(write_timestep, **tags)}"
            msg1 = f" [{tags_str}]" if tags_str else ""
            self.log.info(' --> ' + msg0 + msg1 + msg2)

        if not data_written:
            self.log.warning(' --> No data found for timestep %s', timestep)

    @abstractmethod
    def _get_data_ts(self, time_range: ts.TimeStep, space_bounds: sp.BoundingBox, tmp_path: str) -> Iterable[tuple[xr.DataArray, dict]]:
        """
        Get data from this downloader as xr.Dataset.
        The return structure is a list of tuples, where each tuple contains the data and a dictionary of tags related to that data.
        """
        raise NotImplementedError

    def get_last_published_ts(self) -> ts.TimeStep:
        """
        Get the last timestep available in the downloader.
        """
        raise NotImplementedError

    def _get_timesteps(self, time_range: ts.TimeRange) -> list[ts.TimeStep]:
        """
        Get the timesteps to download, assuming.
        """
        if hasattr(self, 'ts_per_year'):
            return time_range.get_timesteps_from_tsnumber(self.ts_per_year)
        elif hasattr(self, 'frequency') or hasattr(self, 'freq'):
            self.freq = getattr(self, 'frequency', None) or self.freq
        else:
            raise ValueError('No frequency or ts_per_year attribute found')

        freq = self.freq.lower()
        if freq in ['d', 'days', 'day', 'daily']:
            return time_range.days
        elif freq in ['t', 'dekads', 'dekad', 'dekadly', '10-day', '10-days']:
            return time_range.dekads
        elif freq in ['m', 'months', 'month', 'monthly']:
            return time_range.months
        elif freq in ['y', 'years', 'year', 'yearly', 'a', 'annual']:
            return time_range.years
        elif freq in ['8-days', '8day', '8dayly', '8-day', 'viirs']:
            return time_range.viirstimes
        elif freq in ['h', 'hours', 'hour', 'hourly']:
            return time_range.hours
        else:
            raise ValueError(f'Frequency {freq} not supported')

    @staticmethod
    def _copy_option_value(value):
        """Copy option containers while retaining built workflow objects."""
        if hasattr(value, 'get_key'):
            return value
        if isinstance(value, dict):
            return {
                key: DOORDownloader._copy_option_value(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [DOORDownloader._copy_option_value(item) for item in value]
        if isinstance(value, tuple):
            return tuple(DOORDownloader._copy_option_value(item) for item in value)
        return deepcopy(value)

    def check_options(self, options: Optional[dict] = None) -> dict:
        """Validate options and merge them with independent defaults."""
        checked = {
            key: self._copy_option_value(value)
            for key, value in self.default_options.items()
        }
        if options is None:
            return checked
        if not isinstance(options, dict):
            raise TypeError("Downloader options must be a mapping")

        for key, value in options.items():
            if key not in self.default_options:
                self.log.warning(f'Unknown option {key} will be ignored')
                continue
            checked[key] = self._copy_option_value(value)
        return checked

    def set_options(self, options: dict) -> None:
        options = self.check_options(options)
        for key, value in options.items():
            setattr(self, key, value)
        
        if 'variables' in options:
            variables = options['variables']
            self.set_variables(variables)

    def set_product(self, product: str) -> None:
        self.product = product.lower()
        if self.product not in self.available_products:
            raise ValueError(f'Product {product} not available. Choose one of {self.available_products.keys()}')
        for key in self.available_products[self.product]:
            setattr(self, key, self.available_products[self.product][key])

    def set_variables(self, variables: list|str|None) -> None:
        available_variables = self.available_variables
        if hasattr(self, 'product') and self.product in available_variables:
            available_variables = available_variables[self.product]
        self.variables = {}
        if variables is None:
            variables = available_variables.keys()
        elif isinstance(variables, str):
            variables = [variables]
        for var in variables:
            if var in available_variables:
                self.variables[var] = available_variables[var] if isinstance(available_variables, dict) else var

    def get_last_ts(self, **kwargs) -> tuple[ts.TimeStep]:
        """
        Get the last timestep available in the destination and the last timestep available in the downloader.
        """
        
        last_ts_output = None

        if hasattr(self, 'variables'):
            if isinstance(self.variables, list):
                variables = self.variables
            elif isinstance(self.variables, dict):
                variables = list(self.variables.keys())
        else:
            variables = ['__var__']

        tiles = self.destination.tile_names
        if tiles is None:
            tiles = ['__tile__']

        for i, variable in enumerate(variables):
            if variable == '__var__':
                agg_methods = self.agg_method if hasattr(self, 'agg_method') else ['__agg_method__']
            else:
                agg_methods = self.agg_method[i] if hasattr(self, 'agg_method') else ['__agg_method__']
            if not isinstance(agg_methods, list):
                agg_methods = [agg_methods]

            for agg_method in agg_methods:
                for tile in tiles:
                    case = {'variable': variable, 'tile': tile, 'agg_method': agg_method}
                    now = None if last_ts_output is None else last_ts_output.end + dt.timedelta(days = 1)
                    output = self.destination.get_last_ts(now = now, **case, **kwargs)
                    if output is not None:
                        last_ts_output = output if last_ts_output is None else min(output, last_ts_output)
                    else:
                        last_ts_output = None
                        break
                         
        last_ts_input  = self.get_last_published_ts()
        return last_ts_input, last_ts_output

    #TODO: this is a bit of an akward spot to put this, but it is used by all forecast downloaders, so it makes some sense to have it here
    def postprocess_forecast(self, ds: xr.Dataset, space_bounds: sp.BoundingBox) -> None:
        """
        Postprocess the forecast data.
        """
        # Drop existing time dimension (it refers to issue forecast time)
        ds = ds.drop_vars("time", errors='ignore')

        # Assign new time dimension and rename spatial coordinates
        ds = ds.assign_coords({self.frc_dims["time"]: self.frc_time_range}).rename({v: k for k, v in self.frc_dims.items()})

        # Crop with bounding box
        ds = sp.crop_to_bb(ds, space_bounds)

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

    def __init__(self, url_blank: str, protocol: str = 'http') -> None:

        self.url_blank = url_blank
        if protocol.lower() not in ['http', 'https']:
            raise ValueError(f'Protocol {protocol} not supported')
        else:
            self.protocol = protocol.lower()

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
        self.log.info(f"Downloading file from {url}")
        try:
            r = requests.get(url, auth = kwargs["auth"])
            if r.status_code != 200:
                raise FileNotFoundError(r.text)
            
            os.makedirs(os.path.dirname(destination), exist_ok=True)

            with open(destination, 'wb') as f:
                f.write(r.content)

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

class FTPDownloader(DOORDownloader):
    """
    Downloader for data from an FTP server via FTP or SFTP.
    This typer of downloader is useful for data that can be downloaded from an FTP server.
    It allows to specify a URL template with placeholders for various parameters (as keyword arguments).
    """

    name = "FTP_Downloader"

    def __init__(self, host: str, port: int = 21, protocol: str = 'ftp', user: str = 'anonymous', password: str = 'anonymous') -> None:
        if protocol.lower() not in ['ftp', 'sftp']:
            raise ValueError(f'Protocol {protocol} not supported')
        self.protocol = protocol.lower()
        
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        super().__init__()

        if self.protocol == 'sftp':
            self.transport = paramiko.Transport((host, port))
            self.transport.connect(username=user, password=password)
            self.client = paramiko.SFTPClient.from_transport(self.transport)
        elif self.protocol == 'ftp':
            self.client = ftplib.FTP()
            self.client.connect(host, port)
            self.client.login(user, password)

    def __del__(self):
        """
        Close the FTP or SFTP client connection when the downloader is deleted.
        """
        if hasattr(self, 'client'):
            try:
                self.client.close()
            except Exception as e:
                self.log.debug(f'Error closing {self.protocol} client: {e}')

        if hasattr(self, 'transport'):
            try:
                self.transport.close()
            except Exception as e:
                self.log.debug(f'Error closing {self.protocol} transport: {e}')
        
    def download(self, blank_path, destination: str, min_size: float = None, missing_action: str = 'error', **kwargs) -> bool:
        """
        Downloads data from FTP or SFTP server.
        Eventually check file size to avoid empty files.
        """

        url = blank_path.format(**kwargs)

        try:
            if self.protocol == 'sftp':
                self.client.get(url, destination)
            elif self.protocol == 'ftp':
                with open(destination, 'wb') as f:
                    self.client.retrbinary(f'RETR {url}', f.write)
        except Exception as e:
            handle_missing(missing_action, kwargs)
            self.log.debug(f'Error downloading {url} via {self.protocol}: {e}')
            return False

        success_flag, success_msg = check_download(destination, min_size, missing_action)
        if success_flag > 0:
            handle_missing(missing_action, kwargs)
            self.log.debug(f'Error downloading file from {url}: {success_msg}')
            return False

        return True

    def check_data(self, blank_path,  **kwargs) -> bool:
        """
        Check if the data is available on the FTP or SFTP server.
        This method can be used to check if the data is available before downloading it.
        """
        url = blank_path.format(**kwargs)
        if self.protocol == 'ftp':
            # For FTP, we can use the 'nlst' command to check if the file exists
            if len(self.client.nlst(url)) > 0:
                return True
        elif self.protocol == 'sftp':
            try:
                self.client.stat(url)
                return True
            except Exception:
                pass
        return False
                
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


# Generic raster downloader ---------
@dataclass
class RasterPayload:
    """In-memory raster plus metadata and the downloaded raw file, if any."""

    data: np.ndarray
    transform: Affine
    crs: CRS | str = "EPSG:4326"
    nodata: float | int | None = None
    dtype: str = "float32"
    tags: dict[str, Any] = field(default_factory=dict)
    raw_path: str | None = None


@dataclass(frozen=True)
class TimestepResult:
    timestep: TimeStep
    status: str
    output_path: str | None = None
    message: str | None = None


class RasterDownloader(DOORDownloader):
    """Base class for independent raster products written as GeoTIFFs.

    Raster files are I/O-bound and independent by timestep. This class uses
    a thread pool, writes each output atomically, and keeps expected missing
    products separate from download, validation, and processing failures.
    """

    name = "RasterDownloader"
    frequency = "hourly"
    publication_delay_minutes = 240

    default_options = {
        "variables": {"precipitation": "precipitation"},
        "download_workers": 1,
        "download_attempts": 4,
        "retry_seconds": 5.0,
        "timeout_seconds": 120,
        "sleep_between_requests": 0.0,
        "minimum_file_size": 200,
        "overwrite_existing": False,
        "validate_existing": True,
        "fail_on_missing": False,
        "fail_if_all_missing": True,
        "retry_not_found": False,
        "output_dtype": "float32",
        "output_nodata": -9999.0,
        "compression": "deflate",
        "compression_level": 6,
        "tiled": True,
        "block_size": 256,
        "raw_destination": None,
    }

    def check_options(self, options=None) -> dict:
        checked = super().check_options(options)
        try:
            checked["download_workers"] = max(1, int(checked["download_workers"]))
            checked["download_attempts"] = max(1, int(checked["download_attempts"]))
            checked["retry_seconds"] = max(0.0, float(checked["retry_seconds"]))
            checked["timeout_seconds"] = max(1, int(checked["timeout_seconds"]))
            checked["sleep_between_requests"] = max(
                0.0, float(checked["sleep_between_requests"])
            )
            checked["minimum_file_size"] = max(1, int(checked["minimum_file_size"]))
            checked["compression_level"] = int(checked["compression_level"])
            checked["block_size"] = max(16, int(checked["block_size"]))
        except (TypeError, ValueError) as error:
            raise ConfigurationError(
                "Invalid raster download setting.", [str(error)]
            ) from error

        if checked["compression_level"] not in range(1, 10):
            raise ConfigurationError(
                "Invalid GeoTIFF compression level.",
                ["compression_level must be between 1 and 9."],
            )
        if str(checked["output_dtype"]).lower() not in {
            "float32",
            "float64",
            "int16",
            "uint16",
            "int32",
            "uint32",
        }:
            raise ConfigurationError(
                "Unsupported GeoTIFF output dtype.",
                [f"Value: {checked['output_dtype']}"],
            )
        return checked

    # Time management ---------
    def _get_timesteps(self, time_range) -> list[TimeStep]:
        if getattr(self, "ts_per_year", None) != 17520:
            return super()._get_timesteps(time_range)

        timestep = HalfHourTimeStep.from_date(time_range.start)
        timesteps = []
        while timestep.start <= time_range.end:
            timesteps.append(timestep)
            timestep = timestep + 1
        return timesteps

    def get_last_published_ts(self, **kwargs) -> TimeStep:
        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        available = now - dt.timedelta(minutes=self.publication_delay_minutes)

        if getattr(self, "ts_per_year", None) == 17520:
            minute = 30 if available.minute >= 30 else 0
            available = available.replace(
                minute=minute,
                second=0,
                microsecond=0,
            )
            return HalfHourTimeStep.from_date(available)

        available = available.replace(minute=0, second=0, microsecond=0)
        return ts.Hour.from_date(available)

    def get_last_ts(self, **kwargs) -> tuple[TimeStep, TimeStep | None]:
        if getattr(self, "ts_per_year", None) != 17520:
            return super().get_last_ts(**kwargs)

        last_date = self.destination.get_last_date(**kwargs)
        last_output = (
            HalfHourTimeStep.from_date(last_date)
            if last_date is not None
            else None
        )
        return self.get_last_published_ts(), last_output

    # Public run ---------
    def get_data(self, time_range, space_bounds=None, destination=None, options=None):
        checked_range = self._check_get_data_args(
            time_range, space_bounds, destination, options
        )
        timesteps = self._get_timesteps(checked_range)
        if not timesteps:
            self.log.warning(
                " --> No valid raster timesteps found between %s and %s",
                checked_range.start,
                checked_range.end,
            )
            return

        self.log.info(
            " --> Download window: %s to %s (%d timesteps)",
            checked_range.start.strftime("%Y-%m-%d %H:%M"),
            checked_range.end.strftime("%Y-%m-%d %H:%M"),
            len(timesteps),
        )
        self.log.info(" --> Space bounds: %s", self.bounds.bbox)
        self.log.info(" --> Parallel workers: %d", self.download_workers)

        started = time.monotonic()
        results: list[TimestepResult] = []
        if self.download_workers <= 1 or len(timesteps) == 1:
            for index, timestep in enumerate(timesteps, start=1):
                self.log.info(
                    " --> Timestep %d/%d: %s", index, len(timesteps), timestep
                )
                results.append(self._process_timestep(timestep))
        else:
            with ThreadPoolExecutor(
                max_workers=min(self.download_workers, len(timesteps)),
                thread_name_prefix=self.source.lower(),
            ) as executor:
                futures = {
                    executor.submit(self._process_timestep, timestep): timestep
                    for timestep in timesteps
                }
                completed = 0
                for future in as_completed(futures):
                    completed += 1
                    timestep = futures[future]
                    try:
                        result = future.result()
                    except OperationalError:
                        for pending in futures:
                            pending.cancel()
                        raise
                    except Exception as error:
                        for pending in futures:
                            pending.cancel()
                        raise ProcessingError(
                            "Unexpected raster timestep failure.",
                            [f"Timestep: {timestep}", str(error)],
                        ) from error
                    results.append(result)
                    self.log.info(
                        " --> Completed %d/%d timesteps", completed, len(timesteps)
                    )

        missing = [item for item in results if item.status == "missing"]
        written = [item for item in results if item.status == "written"]
        skipped = [item for item in results if item.status == "skipped"]

        self.log.info(
            " --> Raster run summary: written=%d, skipped=%d, missing=%d (%.1f seconds)",
            len(written),
            len(skipped),
            len(missing),
            time.monotonic() - started,
        )
        for item in sorted(missing, key=lambda value: value.timestep.start):
            self.log.warning(
                " ---> Missing %s: %s",
                item.timestep.start.strftime("%Y-%m-%d %H:%M"),
                item.message or "remote product unavailable",
            )

        if missing and self.fail_on_missing:
            raise DataUnavailableError(
                "One or more requested raster products are unavailable.",
                [
                    f"Missing timesteps: {len(missing)}/{len(results)}",
                    *[
                        item.timestep.start.strftime("%Y-%m-%d %H:%M")
                        for item in missing[:20]
                    ],
                ],
            )
        if missing and not written and not skipped and self.fail_if_all_missing:
            raise DataUnavailableError(
                "No requested raster product is available.",
                [
                    f"Missing timesteps: {len(missing)}",
                    f"Window: {checked_range.start} to {checked_range.end}",
                ],
            )

    # Per-timestep processing ---------
    def _process_timestep(self, timestep: TimeStep) -> TimestepResult:
        output_path = self._get_dataset_key(self.destination, timestep)
        self._validate_output_path(output_path)

        action, message = self._existing_output_policy(
            output_path, timestep, incoming_tags=None
        )
        if action == "skip":
            self.log.info(
                " ---> %s already exists; skip%s",
                timestep.start.strftime("%Y-%m-%d %H:%M"),
                f" ({message})" if message else "",
            )
            return TimestepResult(timestep, "skipped", output_path)
        if message == "existing GeoTIFF is invalid":
            self.log.warning(
                " ---> Existing GeoTIFF is invalid and will be replaced: %s",
                output_path,
            )

        try:
            with tempfile.TemporaryDirectory(
                ignore_cleanup_errors=True, dir=os.getenv("TMP")
            ) as tmp_path:
                payload = self._download_and_prepare(timestep, self.bounds, tmp_path)
                if payload is None:
                    return TimestepResult(
                        timestep, "missing", message="empty downloader result"
                    )
                self._validate_payload(payload, timestep)

                action, message = self._existing_output_policy(
                    output_path, timestep, incoming_tags=payload.tags
                )
                if action == "skip":
                    self.log.info(
                        " ---> %s downloaded but existing output is kept%s",
                        timestep.start.strftime("%Y-%m-%d %H:%M"),
                        f" ({message})" if message else "",
                    )
                    return TimestepResult(timestep, "skipped", output_path)
                if os.path.isfile(output_path) and message:
                    self.log.info(" ---> %s", message)

                self._write_geotiff_atomic(payload, output_path)
                if payload.raw_path and self.raw_destination is not None:
                    self._preserve_raw_file(payload.raw_path, timestep)
        except DataUnavailableError as error:
            return TimestepResult(timestep, "missing", message=str(error))

        self.log.info(
            " ---> %s saved to %s",
            timestep.start.strftime("%Y-%m-%d %H:%M"),
            output_path,
        )
        if self.sleep_between_requests:
            time.sleep(self.sleep_between_requests)
        return TimestepResult(timestep, "written", output_path)

    def _download_and_prepare(
        self,
        timestep: TimeStep,
        space_bounds: BoundingBox,
        tmp_path: str,
    ) -> RasterPayload:
        raise NotImplementedError

    def _get_data_ts(self, timestep, space_bounds, tmp_path) -> Iterable:
        """Raster downloader classes use direct atomic GeoTIFF writing in ``get_data``."""
        raise NotImplementedError(
            "RasterDownloader writes GeoTIFFs directly and does not use _get_data_ts."
        )

    # Retry ---------
    def _run_with_retry(self, operation, description: str):
        last_error: Exception | None = None
        for attempt in range(1, self.download_attempts + 1):
            try:
                return operation()
            except ConfigurationError:
                raise
            except DataUnavailableError as error:
                last_error = error
                if not self.retry_not_found or attempt == self.download_attempts:
                    raise
            except OperationalError as error:
                last_error = error
                if attempt == self.download_attempts:
                    raise
            except Exception as error:
                last_error = error
                if attempt == self.download_attempts:
                    raise ProcessingError(
                        "Raster operation failed.",
                        [description, f"Attempt: {attempt}", str(error)],
                    ) from error

            wait_seconds = self.retry_seconds * attempt
            self.log.warning(
                " ---> %s failed (attempt %d/%d): %s",
                description,
                attempt,
                self.download_attempts,
                last_error,
            )
            if wait_seconds:
                time.sleep(wait_seconds)

        raise ProcessingError(
            "Raster retry loop ended unexpectedly.", [description, str(last_error)]
        )

    # Output ---------
    def _get_dataset_key(self, dataset, timestep: TimeStep, **tags: Any) -> str:
        if isinstance(dataset, str):
            context = {
                "domain": getattr(self, "domain", "domain"),
                "product": getattr(self, "product", "product"),
                "source": getattr(self, "source", "source"),
                **tags,
            }
            try:
                path = dataset.format(**context)
            except KeyError as error:
                raise ConfigurationError(
                    "Unable to resolve a raster output path.",
                    [f"Missing tag: {error.args[0]}", dataset],
                ) from error
            path = timestep.start.strftime(path)
            return os.path.expanduser(os.path.expandvars(path))
        if isinstance(dataset, dict):
            dataset = Dataset.from_options(dataset)
        try:
            path = dataset.get_key(timestep, **tags)
        except (KeyError, TypeError):
            path = dataset.get_key(
                timestep,
                domain=getattr(self, "domain", "domain"),
                product=getattr(self, "product", "product"),
                source=getattr(self, "source", "source"),
                **tags,
            )
        try:
            path = os.fspath(path)
        except TypeError as error:
            raise ConfigurationError(
                "Unable to resolve a raster output path.",
                [f"Timestep: {timestep}", f"Value: {path!r}"],
            ) from error
        if not isinstance(path, str) or not path:
            raise ConfigurationError(
                "Unable to resolve a raster output path.",
                [f"Timestep: {timestep}"],
            )
        return path

    @staticmethod
    def _validate_output_path(output_path: str) -> None:
        lowered = output_path.lower()
        if not lowered.endswith((".tif", ".tiff")):
            raise ConfigurationError(
                "Raster destination must be a GeoTIFF.", [output_path]
            )
        if "://" in output_path:
            raise ConfigurationError(
                "Direct raster GeoTIFF writing currently requires a local destination.",
                [output_path],
            )

    def _existing_output_policy(
        self,
        output_path: str,
        timestep: TimeStep,
        incoming_tags: dict[str, Any] | None = None,
    ) -> tuple[str, str | None]:
        """Decide whether an existing raster should be kept or replaced.

        Subclasses can override this hook to implement source-specific
        precedence rules while preserving the generic overwrite behaviour.
        """
        if not os.path.isfile(output_path):
            return "write", None
        if self.validate_existing and not self._existing_geotiff_is_valid(output_path):
            return "write", "existing GeoTIFF is invalid"
        if self.overwrite_existing:
            return "write", "overwrite_existing is enabled"
        return "skip", None

    @staticmethod
    def _existing_geotiff_is_valid(output_path: str) -> bool:
        try:
            with rasterio.open(output_path) as source:
                return (
                    source.count >= 1
                    and source.width > 0
                    and source.height > 0
                    and source.crs is not None
                    and source.transform is not None
                )
        except Exception:
            return False

    def _write_geotiff_atomic(self, payload: RasterPayload, output_path: str) -> None:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        data = np.asarray(payload.data)
        if data.ndim != 2:
            raise DataValidationError(
                "Satellite output raster must be two-dimensional.",
                [f"Shape: {data.shape}"],
            )

        output_dtype = np.dtype(self.output_dtype)
        data = data.astype(output_dtype, copy=False)
        block_size = min(self.block_size, data.shape[0], data.shape[1])
        # GeoTIFF tile sizes must be multiples of 16. Tiny test rasters are
        # written striped instead of forcing an invalid tile size.
        use_tiles = bool(self.tiled and block_size >= 16)
        if use_tiles:
            block_size = max(16, (block_size // 16) * 16)

        temp_output = output_path + f".part-{os.getpid()}-{time.time_ns()}.tif"
        profile = {
            "driver": "GTiff",
            "height": data.shape[0],
            "width": data.shape[1],
            "count": 1,
            "dtype": output_dtype.name,
            "crs": CRS.from_user_input(payload.crs),
            "transform": payload.transform,
            "nodata": payload.nodata,
            "compress": self.compression,
            "BIGTIFF": "IF_SAFER",
            "tiled": use_tiles,
        }
        if str(self.compression).lower() == "deflate":
            profile["zlevel"] = self.compression_level
            if np.issubdtype(output_dtype, np.floating):
                profile["predictor"] = 3
        if use_tiles:
            profile["blockxsize"] = block_size
            profile["blockysize"] = block_size

        try:
            with rasterio.open(temp_output, "w", **profile) as destination:
                destination.write(data, 1)
                if payload.tags:
                    destination.update_tags(
                        **{key: str(value) for key, value in payload.tags.items()}
                    )
            os.replace(temp_output, output_path)
        except Exception as error:
            try:
                os.remove(temp_output)
            except OSError:
                pass
            raise ProcessingError(
                "Unable to write the raster GeoTIFF.",
                [output_path, str(error)],
            ) from error

    def _preserve_raw_file(self, raw_path: str, timestep: TimeStep) -> None:
        if self.raw_destination is None:
            raise ConfigurationError(
                "Raw raster preservation is enabled but raw_destination is missing."
            )
        target_path = self._get_dataset_key(self.raw_destination, timestep)
        if os.path.abspath(raw_path) == os.path.abspath(target_path):
            return
        os.makedirs(os.path.dirname(target_path) or ".", exist_ok=True)
        shutil.copy2(raw_path, target_path)
        self.log.info(" ---> Raw source saved to %s", target_path)

    @staticmethod
    def _validate_payload(payload: RasterPayload, timestep: TimeStep) -> None:
        data = np.asarray(payload.data)
        if data.ndim != 2 or data.size == 0:
            raise DataValidationError(
                "Downloaded raster is empty or malformed.",
                [f"Timestep: {timestep}", f"Shape: {data.shape}"],
            )
        if not isinstance(payload.transform, Affine):
            raise DataValidationError(
                "Downloaded raster has no valid affine transform.",
                [f"Timestep: {timestep}"],
            )


Downloader = DOORDownloader
