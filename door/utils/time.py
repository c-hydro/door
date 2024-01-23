from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Optional
import logging

class TimeRange():

    def __init__(self, 
                 start: datetime|str,
                 end: datetime|str):
        """
        Object useful to define a time range to download data.
        """
        self.check_inputs(start, end)

    def check_inputs(self, start, end):
        """
        Check inputs
        """

        # make sure only one of start/end or timesteps is specified

        if start is None or end is None:
            raise ValueError('Both start and end time must be specified')
        else:
            if not isinstance(start, datetime):
                if isinstance(start, str):
                    start = get_time_from_str(start, 'start')
                else:
                    raise ValueError('Invalid start time')
            if not isinstance(end, datetime):
                if isinstance(end, str):
                    end = get_time_from_str(end, 'end')
                else:
                    raise ValueError('Invalid end time')
            if start < end:
                self.start = start
                self.end = end
            else:
                logging.warning('Start time given is after end time, switching start and end')
                self.start = end
                self.end = start

    def get_regular_timesteps(self, timesteps_per_year: int) -> datetime:
        """
        This will yield the timesteps to download. on a regular frequency
        frquency is expressed as an integer indicating the number of times per year (e.g. 12 for monthly data, 365 for daily data, etc.).
        Allows hourly, daily, monthly and yearly data.
        """
        now = self.start
        while now <= self.end:
            yield now
            if timesteps_per_year == 8760:
                now += timedelta(hours = 1)
            elif timesteps_per_year == 365:
                now += timedelta(days = 1)
            elif timesteps_per_year == 12:
                now += relativedelta(months = 1)
            elif timesteps_per_year == 1:
                now += relativedelta(years = 1)
            else:
                # dekads are not implemented here, because no data is available at this frequency to download
                raise ValueError(f'Invalid data frequency: {timesteps_per_year} times per year is not supported')            

def get_time_from_str(string: str, name = None) -> datetime:
    available_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S']
    for format in available_formats:
        try:                                                           
            return datetime.strptime(string, format)
        except ValueError:                                                        
            pass
    if name is None:
        raise ValueError(f'Invalid format for time: {string}, expected one of' + '; '.join(available_formats))
    else:
        raise ValueError(f'Invalid format for {name} time: {string}, expected one of' + '; '.join(available_formats))