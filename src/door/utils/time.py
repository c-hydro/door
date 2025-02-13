import datetime as dt
import numpy as np

def get_regular_steps(start: dt.datetime, step_hrs: int, max_steps: int) -> tuple[list[int], list[dt.datetime]]:
    """
    Compute the forecast steps for the model with regular n-hourly time frequency
    """
    max_step = (max_steps + 1) * step_hrs
    forecast_steps = np.arange(step_hrs, max_step, step_hrs)
    time_range = [start + dt.timedelta(hours=float(i)) for i in forecast_steps]
    return time_range, forecast_steps
