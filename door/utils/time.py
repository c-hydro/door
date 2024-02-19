import datetime as dt
import dateutil.relativedelta as dtr
import numpy as np
import pandas as pd

from typing import Generator
import logging

class TimeRange():

    def __init__(self, 
                 start: dt.datetime|str,
                 end: dt.datetime|str):
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
            if not isinstance(start, dt.datetime):
                if isinstance(start, str):
                    start = get_time_from_str(start, 'start')
                else:
                    raise ValueError('Invalid start time')
            if not isinstance(end, dt.datetime):
                if isinstance(end, str):
                    end = get_time_from_str(end, 'end')
                else:
                    raise ValueError('Invalid end time')
            if start <= end:
                self.start = start
                self.end = end
            else:
                logging.warning('Start time given is after end time, switching start and end')
                self.start = end
                self.end = start

    def get_timesteps_from_tsnumber(self, timesteps_per_year: int, get_end = False) -> list[dt.datetime]:
        return list(self.gen_timesteps_from_tsnumber(timesteps_per_year, get_end = get_end))
    
    def gen_timesteps_from_tsnumber(self, timesteps_per_year: int, get_end = False) -> Generator[dt.datetime, None, None]:
        """
        This will yield the timesteps to download on a regular frequency by the number of timesteps per year.
        timesteps_per_year is expressed as an integer indicating the number of times per year (e.g. 12 for monthly data, 365 for daily data, etc.).
        Allows hourly, daily, dekadly, monthly and yearly data.
        """
        if timesteps_per_year == 8760:
            now = self.start.replace(minute = 0, second = 0)
        elif timesteps_per_year == 12:
            now = self.start.replace(day = 1, hour = 0, minute = 0, second = 0)
        elif timesteps_per_year == 365:
            now = self.start.replace(hour = 0, minute = 0, second = 0)
        elif timesteps_per_year == 36:
            start_day = self.start.day
            if start_day <= 10:
                now = self.start.replace(day = 1, hour = 0, minute = 0, second = 0)
            elif start_day <= 20:
                now = self.start.replace(day = 11, hour = 0, minute = 0, second = 0)
            else:
                now = self.start.replace(day = 21, hour = 0, minute = 0, second = 0)
        elif timesteps_per_year == 1:
            now = self.start.replace(month = 1, day = 1, hour = 0, minute = 0, second = 0)
        else:
            raise ValueError(f'Invalid data frequency: {timesteps_per_year} times per year is not supported')

        while True:
            yield now
            if timesteps_per_year == 8760:
                now += dt.timedelta(hours = 1)
            elif timesteps_per_year == 365:
                now += dt.timedelta(days = 1)
            elif timesteps_per_year == 36:
                now = add_dekad(now)
            elif timesteps_per_year == 12:
                now += dtr.relativedelta(months = 1)
            elif timesteps_per_year == 1:
                now += dtr.relativedelta(years = 1)
            else:
                # dekads are not implemented here, because no data is available at this frequency to download
                raise ValueError(f'Invalid data frequency: {timesteps_per_year} times per year is not supported')
            if now > self.end:
                if get_end:
                    yield now
                break

    def get_timesteps_from_DOY(self, doy_list: list[int]) -> list[dt.datetime]:
        return list(self.gen_timesteps_from_DOY(doy_list))
    
    def gen_timesteps_from_DOY(self, doy_list: list[int]) -> Generator[dt.datetime, None, None]:
        """
        This will yield the timesteps to download on a given list of days of the year.
        This is useful for MODIS and VIIRS data that are available at preset DOYs.
        """
        start_year = self.start.year
        end_year = self.end.year

        for year in range(start_year, end_year+1):
            for doy in doy_list:
                date = dt.datetime(year, 1, 1) + dt.timedelta(days=doy-1)
                if date >= self.start and date <= self.end:
                    yield date

    def get_timesteps_from_issue_hour(self, issue_hours: list[int]) -> list[dt.datetime]:
        return list(self.gen_timesteps_from_issue_hour(issue_hours))
    
    def gen_timesteps_from_issue_hour(self, issue_hours: list) -> Generator[dt.datetime, None, None]:
        """
        This will yield the timesteps to download oa product issued daily at given hours
        """
        now = self.start
        while now <= self.end:
            for issue_hour in issue_hours:
                issue_time = dt.datetime(now.year, now.month, now.day, issue_hour)
                if now <= issue_time <= self.end:
                    now = issue_time
                    yield now
            day_after = now + dt.timedelta(days=1)
            now = dt.datetime(day_after.year, day_after.month, day_after.day, issue_hours[0])

def get_time_from_str(string: str, name = None) -> dt.datetime:
    available_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S']
    for format in available_formats:
        try:                                                           
            return dt.datetime.strptime(string, format)
        except ValueError:                                                        
            pass
    if name is None:
        raise ValueError(f'Invalid format for time: {string}, expected one of' + '; '.join(available_formats))
    else:
        raise ValueError(f'Invalid format for {name} time: {string}, expected one of' + '; '.join(available_formats))
    
def get_regular_steps(start: dt.datetime, step_hrs: int, max_steps: int) -> tuple[list[int], list[dt.datetime]]:
    """
    Compute the forecast steps for the model with regular n-hourly time frequency
    """
    max_step = (max_steps + 1) * step_hrs
    forecast_steps = np.arange(step_hrs, max_step, step_hrs)
    time_range = [start + dt.timedelta(hours=float(i)) for i in forecast_steps]
    return time_range, forecast_steps

def add_dekad(time: dt.datetime) -> dt.datetime:
    """
    Add the dekad to the time
    """
    day = time.day
    if day == 1:
        return time.replace(day = 11)
    elif day == 11:
        return time.replace(day = 21)
    elif day == 21:
        next_month = time + dtr.relativedelta(months = 1)
        return next_month.replace(day = 1)
    else:
        raise ValueError('Invalid day for dekad')

# def check_max_steps(self, max_steps_model: int) -> None:
#     """
#     Check if selected max_steps is available for the model
#     """
#     if self.max_steps >= max_steps_model:
#         print(f'ERROR! Only the first {max_steps_model} forecast hours are available!')
#         self.max_steps = max_steps_model