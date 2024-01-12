import datetime

import pandas as pd
import xarray as xr
import logging
import datetime as dt

from typing import Optional
from ..lib.time import TimeRange

class DOORDownloader():
    def __init__(self,
                 product: str,
                 start_time: dt.datetime,
                 end_time: dt.datetime,
                 output = str,
                 time_template = dict,
                 bbox= Optional[list],    # [x_min,y_max,x_max,y_min]
                 tmp_path = Optional[str],
                 variables = Optional[list]) -> None:
        self.product = product
        self.start_time = start_time
        self.end_time = end_time
        self.variables = variables
        self.freq = None

    def setup_product_time(self, start_time: dt.datetime, end: dt.datetime) -> pd.date_range:
        """
        Set the product time features for the data source.
        This is a mandatory method for all subclasses.
        """
        if self.freq is None:
            logging.error(" --> ERROR! Frequency for the product must be defined!")
        return [i for i in pd.date_range(start=start_time, end=end, freq=self.freq)]

    def setup_io(self, time_range: list, template: dict) -> dict:
        """
        For each time step define the output name and the url to download the data.
        """

    def get_data(self, start_time: dt.datetime, end: dt.datetime, bbox: Optional[list], frc_length: Optional[float], variables: Optional[list]):
        """
        Get data from the data source as an xarray.Dataset.
        for a single time. This is a mandatory method for all subclasses.
        """





