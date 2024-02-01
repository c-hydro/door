import logging
import os
from typing import Optional
import tempfile
import pandas as pd
import numpy as np
from datetime import datetime
import gzip
import xarray as xr
import requests
import shutil

from ...base_downloaders import HTTPDownloader
from ...utils.time import TimeRange
from ...utils.space import SpatialReference
from ...utils.geotiff import regrid_raster
from ...utils.io import untar_file, decompress_bz2

class ICONDownloader(HTTPDownloader):
    
    name = "ICON"
    default_options = {
        'frc_max_step' : None,
        'variables' : {"tp":"tot_prec",
                       "temp":"t_2m"},
        'cdo_path' : "/usr/bin/"    # Come faccio a dirgli di usare questo se non specificato?
    }
 
    def __init__(self, product: str) -> None:
        self.product = product
        if self.product == "ICON0p125":
            self.url_blank = "https://opendata.dwd.de/weather/nwp/icon/grib/{run_time:%H}/{var}/icon_global_icosahedral_single-level_{run_time:%Y%m%d%H%M}_{step}_{VAR}.grib2.bz2"
            self.ancillary_remote_path = "https://opendata.dwd.de/weather/lib/cdo/"
            self.ancillary_remote_file = "ICON_GLOBAL2WORLD_0125_EASY.tar.bz2"
            self.grid_file = "target_grid_world_0125.txt"
            self.weight_file = "weights_icogl2world_0125.nc"
            self.max_steps = 180
            self.issue_hours = [0,6,12,18]
            self.prelim_nodata = -1
        else:
            logging.error(" --> ERROR! Only ICON0p125 has been implemented until now!")
            raise NotImplementedError()
            
    def get_data(self,
                 time_range: TimeRange,
                 space_ref: SpatialReference,
                 destination: str,
                 options: Optional[dict] = None) -> None:

        # Check options
        options = self.check_options(options)

        # Get the timesteps to download
        timesteps = time_range.get_timesteps_from_issue_hour(self.issue_hours)
        missing_times = []
        
        # Do all of this inside a temporary folder
        with tempfile.TemporaryDirectory() as tmp_path:

            # Download preliminary files for file conversion if not available
            if not os.path.isfile(os.path.join(tmp_path, self.grid_file)) or not os.path.isfile(os.path.join(tmp_path, self.weight_file)):
                logging.info(" ---> Download binary decodification table")
                r = requests.get(self.ancillary_remote_path + self.ancillary_remote_file)
                with open(os.path.join(tmp_path, "binary_grids.tar.bz2"), 'wb') as f:
                    f.write(r.content)
                untar_file(os.path.join(tmp_path, "binary_grids.tar.bz2"))

            # Download the data for the specified issue times
            for run_time in timesteps:

                # Set forecast steps
                logging.info(" ---> Set forecast steps")
                if options['frc_max_step'] is not None:
                    self.max_steps = options['frc_max_step']
                frc_time_range, frc_steps = self.compute_model_steps(time_range.start)

                for var in options['variables'].keys():
                    tmp_destination = os.path.join(tmp_path, var, "")
                    os.makedirs(tmp_destination, exist_ok=True)

                    temp_files = []
                    for step_time, step in zip(frc_time_range, frc_steps):
                        logging.info(f' ---> Downloading data for {step_time:%Y-%m-%d} + {step}h')
                        tmp_filename = f'temp_{self.product}_{run_time:%Y%m%d%H}_{step_time:%Y%m%d%H%M}_{step}_{var}.grib2.bz2'
                        tmp_destination = os.path.join(tmp_path, var, tmp_filename)
                        success = self.download(tmp_destination, min_size = 200, missing_action = 'error', run_time = run_time, step = step, VAR = var.upper, var = var)
                        if success:
                            temp_files.append(self.project_bin_file(tmp_destination))
                            logging.info(f' --> SUCCESS! Downloaded and regridded data for {step_time:%Y-%m-%d} + {step}h')

                    with xr.open_mfdataset(temp_files, concat_dim='valid_time', data_vars='minimal',
                                               combine='nested', coords='minimal',
                                               compat='override', engine="cfgrib") as ds:
                            ds = ds.where((ds.latitude <= self.bbox[3]) &
                                          (ds.latitude >= self.bbox[1]) &
                                          (ds.longitude >= self.bbox[0]) &
                                          (ds.longitude <= self.bbox[2]), drop=True)

                            var_names = [vars for vars in ds.data_vars.variables.mapping]
                            if len(var_names) > 1:
                                logging.error("ERROR! Only one variable should be in the grib file, check file integrity!")
                                raise TypeError
                            var_name = var_names[0]

                            ds = ds[var_name].drop("time").rename({"longitude": "lon", "latitude": "lat", "valid_time": "time"})
                            ds.to_netcdf(destination.format(var = options['variables'][var], time = run_time))    #come gestisco qua il fatto che devo cambiar eil destination name?

    def compute_model_steps(self, time_run: datetime):
        """
        extracts from a .gz file
        """
        max_step = self.max_steps + 1
        if max_step > 181:
            logging.error(" ERROR! Only the first 180 forecast hours are available on the dwd website!")
            raise NotImplementedError()
        if max_step > 78:
            forecast_steps = np.concatenate((np.arange(1, 78, 1), np.arange(78, np.min((max_step + 2, 180)), 3)))
        else:
            forecast_steps = np.arange(1, 79, 1)
        time_range = [time_run + pd.Timedelta(str(i) + "H") for i in forecast_steps]
        return time_range, forecast_steps

    def project_bin_file(self, file_in: str):
        """
        extracts from a .gz file
        """
        decompress_bz2(file_in)
        os.remove(file_in)
        os.system(self.cdo_path + "cdo -O remap," + self.grid_file + "," + self.weight_file + " " + file_in[:-4] + " " + file_in[:-4].replace("frc", "regr_frc"))
        os.remove(file_in[:-4])
        return file_in[:-4].replace("frc", "regr_frc")