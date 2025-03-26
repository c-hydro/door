from typing import Optional, Iterable, Sequence
import logging
from abc import ABC, ABCMeta, abstractmethod
import datetime as dt

import tempfile
import xarray as xr
import os

from .utils.io import download_http, check_download, handle_missing, download_ftp, download_sftp

from d3tools import spatial as sp
from d3tools import timestepping as ts
from d3tools.data import Dataset
from d3tools.exit import rm_at_exit

class MetaDOORDownloader(ABCMeta):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        elif 'source' in attrs:
            cls.subclasses[attrs['source']] = cls

class DOORDownloader(ABC, metaclass=MetaDOORDownloader):
    """
    Base class for all DOOR downloaders.
    """

    name = "DOOR_Downloader"
    default_options = {}

    single_temp_folder = False
    separate_vars = False

    def __init__(self) -> None:
        self.log = logging.getLogger(self.name)

    ## CLASS METHODS FOR FACTORY
    @classmethod
    def from_options(cls, source: dict|str, *args, **kwargs) -> 'Dataset':
        if isinstance(source, dict):
            init_options = source
            init_options.update(kwargs)
            source = init_options.pop('source', None)
        else:
            init_options = kwargs
        source = cls.get_source(source)
        Subclass: 'Dataset' = cls.get_subclass(source)

        bdo = {}
        bdo['bounds'] = init_options.pop('bounds', None)
        bdo['destination'] = init_options.pop('destination', None)
        bdo['options'] = init_options.pop('options', {})
        
        downloader = Subclass(*args, **init_options)
        downloader.set_bounds(bdo['bounds'])
        downloader.set_destination(bdo['destination'])
        downloader.set_options(bdo['options'])

        return downloader

    @classmethod
    def get_subclass(cls, source: str):
        source = cls.get_source(source)
        Subclass: 'Dataset'|None = cls.subclasses.get(source)
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
        elif isinstance(bounds, (list, tuple)):
            _bounds = sp.BoundingBox(*bounds)
        elif isinstance(bounds, str):
            _bounds = sp.BoundingBox.from_file(bounds)
        else:
            try:
                _bounds = sp.BoundingBox.from_dataset(bounds)
            except:
                raise ValueError('Invalid bounds')

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
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_path:
                for timestep in timesteps:
                    self._get_and_save_data_ts(timestep, tmp_path)
                    rm_at_exit(tmp_path)
        else:
            for timestep in timesteps:
                with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_path:
                    self._get_and_save_data_ts(timestep, tmp_path)
                    rm_at_exit(tmp_path)

    def _get_and_save_data_ts(self,
                              timestep: ts.TimeStep,
                              tmp_path: str) -> None:
        
        data_struct = self._get_data_ts(timestep, self.bounds, tmp_path)
        if not data_struct:
            self.log.warning(f'No data found for timestep {timestep}')
            return
        for data, tags in data_struct:
            if 'timestep' in tags:
                timestep = tags.pop('timestep')
            self.destination.write_data(data, timestep, **tags)

    @abstractmethod
    def _get_data_ts(self, time_range: ts.TimeStep, space_bounds: sp.BoundingBox) -> Iterable[tuple[xr.DataArray, dict]]:
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
            variables = options['variables']
            self.set_variables(variables)

    def set_product(self, product: str) -> None:
        self.product = product.lower()
        if self.product not in self.available_products:
            raise ValueError(f'Product {product} not available. Choose one of {self.available_products.keys()}')
        for key in self.available_products[self.product]:
            setattr(self, key, self.available_products[self.product][key])

    def set_variables(self, variables: list) -> None:
        available_variables = self.available_variables
        if hasattr(self, 'product') and self.product in available_variables:
            available_variables = available_variables[self.product]
        self.variables = {}
        for var in variables:
            if var in available_variables:
                self.variables[var] = available_variables[var]

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

    def __init__(self, url_blank: str, protocol: str = 'http', host: str|None = None) -> None:

        self.url_blank = url_blank
        if protocol.lower() not in ['http', 'ftp', 'sftp', 'https']:
            raise ValueError(f'Protocol {protocol} not supported')
        else:
            self.protocol = protocol.lower()

        if self.protocol == 'ftp' or self.protocol == 'sftp':
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
        try:
            if self.protocol == 'http' or self.protocol == 'https':
                download_http(url, destination, kwargs["auth"])
            elif self.protocol == 'ftp':
                download_ftp(self.host, url, destination, kwargs["auth"])
            elif self.protocol == 'sftp':
                download_sftp(self.host, url, destination, kwargs["auth"])
            else:
                raise ValueError(f'Protocol {self.protocol} not supported')
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

Downloader = DOORDownloader