import datetime as dt
import dateutil.relativedelta as dtr
import numpy as np

from ..tools import timestepping as ts

#TODO: deprecate this module

class TimeRange():

    def __init__(self, 
                 start: dt.datetime|str,
                 end: dt.datetime|str):
        """
        Use tools.timestepping.TimeRange instead
        """
        return ts.TimeRange(start, end)

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

def subtract_dekad(time: dt.datetime) -> dt.datetime:
    """
    Subtract the dekad to the time
    """
    day = time.day
    if day == 1:
        previous_month = time - dtr.relativedelta(months = 1)
        return previous_month.replace(day = 21)
    elif day == 11:
        return time.replace(day = 1)
    elif day == 21:
        return time.replace(day = 11)
    else:
        raise ValueError('Invalid day for dekad')

def get_decade_days(time=dt.datetime, first:bool=False, last:bool=False) -> list[dt.datetime]:
    """
    Get the days of the dekad of the given time
    """
    days = [time + dt.timedelta(days=i) for i in range(10)]
    if time.day == 21:
        days = [time + dt.timedelta(days=i) for i in range(11)]
        days = [d for d in days if d.month == time.month]

    if first:
        return days[0]
    elif last:
        return days[-1]

    return days

# def check_max_steps(self, max_steps_model: int) -> None:
#     """
#     Check if selected max_steps is available for the model
#     """
#     if self.max_steps >= max_steps_model:
#         print(f'ERROR! Only the first {max_steps_model} forecast hours are available!')
#         self.max_steps = max_steps_model
