import datetime
import logging
from typing import Optional

import pandas as pd
import xarray as xr

from ..downloader_http import downloaderHTTP

class CMC_GDPS_Downloader(downloaderHTTP):
    def __init__(self, product: str, time_range: list, ref_time, variables) -> None:
        super().__init__(product, time_range, ref_time, variables)
        if self.product == "GDPS0p15":
            self.url_blank = "https://dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/{run_time}/{step}/CMC_glb_{VAR}_latlon.15x.15_{run_date}{run_time}_P{step}.grib2"
            self.frc_length = "240H"
            self.frc_freq = "3H"
            self.out_type = "netcdf"
        else:
            logging.error(" --> ERROR! Only Global Deterministic Forecast System 0.15 has been implemented until now!")
            raise NotImplementedError("Only GDPS0p15 type has been implemented")

    def make_3dfile(self, template: dict, output: str):
        for var in self.variables:


