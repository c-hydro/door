import datetime as dt
import numpy as np

from d3tools.timestepping import FixedLenTimeStep

def get_regular_steps(start: dt.datetime, step_hrs: int, max_steps: int) -> tuple[list[int], list[dt.datetime]]:
    """
    Compute the forecast steps for the model with regular n-hourly time frequency
    """
    max_step = (max_steps + 1) * step_hrs
    forecast_steps = np.arange(step_hrs, max_step, step_hrs)
    time_range = [start + dt.timedelta(hours=float(i)) for i in forecast_steps]
    return time_range, forecast_steps


# Sub-hourly timesteps ---------
class HalfHourTimeStep(FixedLenTimeStep):
    """A 30-minute timestep used by IMERG products."""

    length = 1 / 48
    unit = "30min"

    def __init__(self, year: int, step: int) -> None:
        super().__init__(year, step, self.length)

    @staticmethod
    def get_step_from_date(date: dt.datetime) -> int:
        half_hour = 1 if date.minute < 30 else 2
        return (date.timetuple().tm_yday - 1) * 48 + date.hour * 2 + half_hour

    def get_start(self) -> dt.datetime:
        return dt.datetime(self.year, 1, 1) + dt.timedelta(minutes=30 * (self.step - 1))

    def get_end(self) -> dt.datetime:
        return self.get_start() + dt.timedelta(minutes=30, seconds=-1)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__} "
            f"({self.start:%Y%m%d %H%M}-{self.end:%H%M})"
        )
