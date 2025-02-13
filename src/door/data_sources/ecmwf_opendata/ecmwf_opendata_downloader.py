
from typing import Generator
import xarray as xr
import os
from ecmwf.opendata import Client

from d3tools.timestepping.timestep import TimeStep
from d3toos.spatial import BoundingBox
from d3tools.timestepping import TimeRange

from ...base_downloaders import APIDownloader
from ...utils.time import get_regular_steps

class ECMWFOpenDataDownloader(APIDownloader):
    source = "ecmwf-opendata"
    name   = "ECMWF-OpenData_downloader"

    default_options = {
        'frc_max_step': 144,
        'variables': ["10u", "10v"]
    }

    available_products = {
        "HRES": {
            'prod_code': "fc",
            'freq_hours': 3,
            'issue_hours': [0, 6, 12, 18],
            'frc_dims': {"time": "step", "lat": "latitude", "lon": "longitude"}
            }
    }

    available_variables = ["10u", "10v"]

    def set_product(self, product: str) -> None:
        self.product = product.upper()
        if self.product not in self.available_products:
            raise ValueError(f'Product {product} not available. Choose one of {self.available_products.keys()}')
        for key in self.available_products[self.product]:
            setattr(self, key, self.available_products[self.product][key])

    def set_variables(self, variables: list) -> None:
        self.variables = []
        for var in variables:
            this_var = var.lower()
            if this_var not in self.available_variables:
                msg = f'Variable {var} not available. Choose one of {self.available_variables}'
            else:
                self.variables.append(this_var)
        if len(self.variables) == 0:
            raise ValueError('No valid variables selected')

    def __init__(self, product: str) -> None:
        client = Client(
            source="ecmwf",
            beta=True,
            preserve_request_order=False,
            infer_stream_keyword=True)
        super().__init__(client)

        self.set_product(product)

    def _get_timesteps(self, time_range: TimeRange) -> list[TimeStep]:
        return time_range.get_timesteps_from_issue_hour(self.issue_hours)

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
        
        run_time = timestep.start
        
        # Set forecast steps

        # at 0 and 12, we have 144 steps max, at 6 and 18, we have 90 steps max
        if run_time.hour == 0 or run_time.hour == 12:
            max_steps = min(self.frc_max_step, 144)
        else:
            max_steps = min(self.frc_max_step, 90)
        self.frc_time_range, self.frc_steps = get_regular_steps(run_time, self.freq_hours, max_steps)

        tmp_filename = f'temp_frc{self.product}_{run_time:%Y%m%d%H}.grib2'
        tmp_destination = os.path.join(tmp_path, tmp_filename)
        request = self.build_request(run_time, tmp_destination)
        success = self.download(tmp_destination, min_size = 200,  missing_action = 'w', request = request, target = tmp_destination)
        
        if not success:
            return
        self.log.debug(' ----> SUCCESS! Downloaded forecast data')
        #self.log.debug(f' ----> Postprocess data')
        frc_raw = xr.load_dataset(tmp_destination, engine="cfgrib")
        frc_out = self.postprocess_forecast(frc_raw, space_bounds)
        #self.log.debug(' ----> SUCCESS! Postprocessed forecast data')

        yield frc_out, {}


    def build_request(self, run_time, destination):
        """
        Build the request to download the data
        """
        request = dict(
            type=self.prod_code,
            date=run_time.strftime("%Y%m%d"),
            time=run_time.hour,
            step=[i for i in self.frc_steps],
            param=self.variables,
            target=destination
        )
        return request