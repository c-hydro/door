import datetime
import xarray as xr

from ..lib.time import TimeRange

class DOORDownloader():

    def get_data(self, space: list, time: list) -> xr.Dataset:
        """
        Get data from the data source as an xarray.Dataset.
        for a single time. This is a mandatory method for all subclasses.
        """
    def get_times(time_start: datetime.datetime, time_end: datetime.datetime) -> datetime.datetime:
        """
        Get a list of times between two dates.
        This is a mandatory method for all subclasses.
        """

    def get_start(self) -> datetime.datetime:
        """
        Get the start of the data source.
        This is a mandatory method for all subclasses.
        """

    def make(self, bbox: list, time_range: TimeRange)  -> None:
        """
        Gathers all the data from the remote source in the TimeRange,
        also checks that the data is not available yet before gathering it
        """

        source = self.data_source
        source_name = source.__class__.__name__