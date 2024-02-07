import os
from typing import Optional
import tempfile
import xarray as xr
from ecmwf.opendata import Client
from requests.exceptions import HTTPError

from ...base_downloaders import FRCdownloader
from ...utils.time import TimeRange
from ...utils.space import BoundingBox

import logging
logger = logging.getLogger(__name__)

class ECMWFOpenDataDownloader(FRCdownloader):
    name = "ECMWF-OpenData"
    default_options = {
        'frc_max_step': 144,
        'variables': ["10u", "10v"],
    }

    def __init__(self, product: str) -> None:
        self.product = product
        if self.product == "HRES":
            self.prod_code = "fc"
            self.freq_hours = 3
            self.issue_hours = [0, 6, 12, 18]
            self.frc_dims = {"time": "step", "lat": "latitude", "lon": "longitude"}
        else:
            logger.error(" --> ERROR! Only HRES has been implemented until now!")
            raise NotImplementedError()

        self.frc_steps = None
        self.frc_time_range = None              ##### QUESTI E' BENE DICHIARARLI QUA ANCHE SE SONO COSE CHE COMPILO DOPO?
        self.variables = None

    def get_data(self,
                 time_range: TimeRange,
                 space_bounds: BoundingBox,
                 destination: str,
                 options: Optional[dict] = None) -> None:

        # Check options
        options = self.check_options(options)
        
        logger.info(f'------------------------------------------')
        logger.info(f'Starting download of {self.product} data from {self.name}')
        logger.info(f'Data requested between {time_range.start:%Y-%m-%d %H:%M} and {time_range.end:%Y-%m-%d %H:%m}')
        logger.info(f'Bounding box: {space_bounds.bbox}')
        logger.info(f'------------------------------------------')

        # Get the timesteps to download
        timesteps = time_range.get_timesteps_from_issue_hour(self.issue_hours)
        missing_times = []

        # Do all of this inside a temporary folder
        with tempfile.TemporaryDirectory() as tmp_path:

            self.working_path = tmp_path

            # Download the data for the specified issue times
            logger.info(f'Found {len(timesteps)} model issues to download.')
            for i, run_time in enumerate(timesteps):

                logger.info(f' - Model issue {i+1}/{len(timesteps)}: {run_time:%Y-%m-%d_%H}')
                # Set forecast steps
                logger.debug(" ----> Set forecast steps")
                self.max_steps = options['frc_max_step']
                if run_time.hour == 0 or run_time.hour == 12:
                    self.check_max_steps(144)
                else:
                    self.check_max_steps(90)
                self.frc_time_range, self.frc_steps = self.compute_model_steps(time_range.start)
                tmp_destination = os.path.join(tmp_path, "")
                os.makedirs(tmp_destination, exist_ok=True)

                logger.debug(f' ----> Downloading data')
                self.variables = options['variables']
                tmp_filename = f'temp_frc{self.product}_{run_time:%Y%m%d%H}.grib2'
                tmp_destination = os.path.join(tmp_path, tmp_filename)
                self.download(tmp_destination, min_size=200, missing_action='warn', run_time=run_time)
                logger.debug(' ----> SUCCESS! Downloaded forecast data')

                logger.debug(f' ----> Postprocess data')
                frc_out = xr.load_dataset(tmp_destination, engine="cfgrib")
                frc_out = self.postprocess_forecast(frc_out, space_bounds)                  #### QUESTA COSA PUO ESSERE FATTA IN MANIERA PIU PULITA?
                logger.debug(' ----> SUCCESS! Postprocessed forecast data')

                out_name = run_time.strftime(destination)
                os.makedirs(os.path.dirname(out_name), exist_ok=True)
                frc_out.to_netcdf(out_name)
                logger.info(f'  -> SUCCESS! Data for {len(self.variables)} variables dowloaded and cropped to bounds.')

        logger.info(f'------------------------------------------')


    def download(self, destination: str, min_size: float = None, missing_action: str = 'error',
                 protocol: str = 'http', **kwargs) -> bool:
        """
        Downloads data with ecmwf-opendata client
        """
        client: Client = Client(
            source="ecmwf",
            beta=True,
            preserve_request_order=False,
            infer_stream_keyword=True,
        )
        # Perform request
        try:
            result = client.retrieve(
                type=self.prod_code,
                date=kwargs["run_time"].strftime("%Y%m%d"),
                time=kwargs["run_time"].hour,
                step=[i for i in self.frc_steps],
                param=self.variables,
                target=destination
            )
            logger.debug(" --> Forecast file " + result.datetime.strftime("%Y-%m-%d %H:%M") + " correctly downloaded!")
        except HTTPError:
            self.handle_missing(missing_action, kwargs)

        # check if file has been actually downloaded
        if not os.path.isfile(destination):
            self.handle_missing(missing_action, kwargs)
            return False

        # check if file is empty
        if min_size is not None and os.path.getsize(destination) < min_size:
            self.handle_missing(missing_action, kwargs)
            return False

        return True