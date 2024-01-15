from typing import Optional
import datetime as dt
import logging
import os

import xarray as xr
import requests

class DOORDownloader():
    """
    Base class for all DOOR downloaders.
    """
    def __init__(self) -> None:
        pass

    # def setup_product_time(self, start_time: dt.datetime, end: dt.datetime) -> pd.date_range:
    #     """
    #     Set the product time features for the data source.
    #     This is a mandatory method for all subclasses.
    #     """
    #     if self.freq is None:
    #         logging.error(" --> ERROR! Frequency for the product must be defined!")
    #     return [i for i in pd.date_range(start=start_time, end=end, freq=self.freq)]

    # def setup_io(self, time_range: list, template: dict) -> dict:
    #     """
    #     For each time step define the output name and the url to download the data.
    #     """

    def _get_data(self, timesteps: list[dt.datetime], bbox: tuple[float], variables = Optional[list]) -> xr.Dataset:
        """
        Get data from this downloader as an xarray.Dataset.
        Returns all the data in the specified boundinx box at the timesteps specified.
        Does not carry out any transformation or checks on the data.
        """
        raise NotImplementedError()

class HTTPDownloader(DOORDownloader):
    """
    Base class for all DOOR downloaders that download data from HTTP.
    """

    def __init__(self, url: str):
        self.url = url

    def download(self, destination: str, min_size = None) -> str:
        """
        Downloads data from http or https url
        Eventually check file size to avoid empty files
        """
        url = self.url
        try:
            r = requests.get(url)
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            with open(destination, 'wb') as f:
                f.write(r.content)
                if min_size is not None:
                    if os.path.getsize(destination) < min_size:
                        os.remove(destination)
                        raise FileNotFoundError("ERROR! Data not available")
            return destination
        except Exception as e:
            logging.warning(f'Exception in download url ({url}):', e)
            return None