import os
from typing import Optional
import tempfile
import xarray as xr
from ecmwf.opendata import Client
from requests.exceptions import HTTPError

from ...base_downloaders import APIDownloader
from ...utils.time import TimeRange, get_regular_steps
from ...utils.space import BoundingBox
from ...utils.netcdf import save_netcdf

class ECMWFOpenDataDownloader(APIDownloader):
    name = "ECMWF-OpenData_downloader"
    default_options = {
        'frc_max_step': 144,
        'variables': ["10u", "10v"]
    }

    def __init__(self, product: str) -> None:
        client = Client(
            source="ecmwf",
            beta=True,
            preserve_request_order=False,
            infer_stream_keyword=True)
        super().__init__(client)

        self.product = product
        if self.product == "HRES":
            self.prod_code = "fc"
            self.freq_hours = 3
            self.issue_hours = [0, 6, 12, 18]
            self.frc_dims = {"time": "step", "lat": "latitude", "lon": "longitude"}
        else:
            self.log.error(" --> ERROR! Only HRES has been implemented until now!")
            raise NotImplementedError()

        self.frc_steps = None
        self.frc_time_range = None              ##### QUESTI E' BENE DICHIARARLI QUA ANCHE SE SONO COSE CHE COMPILO DOPO?
        self.variables = None

        client = Client(
            source="ecmwf",
            beta=True,
            preserve_request_order=False,
            infer_stream_keyword=True)
        super().__init__(client)

    def get_data(self,
                 time_range: TimeRange,
                 space_bounds: BoundingBox,
                 destination: str,
                 options: Optional[dict] = None) -> None:

        # Check options
        options = self.check_options(options)

        self.variables = options['variables']
        
        self.log.info(f'------------------------------------------')
        self.log.info(f'Starting download of {self.product} data from {self.name}')
        self.log.info(f'Data requested between {time_range.start:%Y-%m-%d %H:%M} and {time_range.end:%Y-%m-%d %H:%m}')
        self.log.info(f'Bounding box: {space_bounds.bbox}')
        self.log.info(f'------------------------------------------')

        # Get the timesteps to download
        timesteps = time_range.get_timesteps_from_issue_hour(self.issue_hours)

        # Download the data for the specified issue times
        self.log.info(f'Found {len(timesteps)} model issues to download.')
        for i, run_time in enumerate(timesteps):
            self.log.info(f' - Model issue {i+1}/{len(timesteps)}: {run_time:%Y-%m-%d_%H}')

            

            
            # Set forecast steps
            self.log.debug(" ----> Set forecast steps")

            # at 0 and 12, we have 144 steps max, at 6 and 18, we have 90 steps max
            if run_time.hour == 0 or run_time.hour == 12:
                max_steps = min(options['frc_max_step'], 144)
            else:
                max_steps = min(options['frc_max_step'], 90)
            self.frc_time_range, self.frc_steps = get_regular_steps(run_time, self.freq_hours, max_steps)

            # Do all of this inside a temporary folder
            tmpdirs = os.path.join(os.getenv('HOME'), 'tmp')
            os.makedirs(tmpdirs, exist_ok=True)
            with tempfile.TemporaryDirectory(dir = tmpdirs) as tmp_path:
                self.working_path = tmp_path

                self.log.debug(f' ----> Downloading data')
                
                tmp_filename = f'temp_frc{self.product}_{run_time:%Y%m%d%H}.grib2'
                tmp_destination = os.path.join(tmp_path, tmp_filename)
                request = self.build_request(run_time, tmp_destination)
                self.download(tmp_destination, min_size = 200,  missing_action = 'w', **request)
            
                self.log.debug(' ----> SUCCESS! Downloaded forecast data')

                self.log.debug(f' ----> Postprocess data')
                frc_out = xr.load_dataset(tmp_destination, engine="cfgrib")
                frc_out = self.postprocess_forecast(frc_out, space_bounds)
                self.log.debug(' ----> SUCCESS! Postprocessed forecast data')

                out_name = run_time.strftime(destination)
                save_netcdf(frc_out, out_name)
                #os.makedirs(os.path.dirname(out_name), exist_ok=True)
                #frc_out.to_netcdf(out_name)
                self.log.info(f'  -> SUCCESS! Data for {len(self.variables)} variables dowloaded and cropped to bounds.')

        self.log.info(f'------------------------------------------')

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