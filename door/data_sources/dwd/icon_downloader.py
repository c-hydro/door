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
import subprocess

from ...base_downloaders import HTTPDownloader
from ...utils.time import TimeRange
from ...utils.space import BoundingBox
from ...utils.io import untar_file, decompress_bz2

class ICONDownloader(HTTPDownloader):
    
    name = "ICON"
    default_options = {
        'frc_max_step' : 180,
        'variables' : {"tp": "tot_prec",
                       "temp": "t_2m"},
        'cdo_path' : "/usr/bin/"
    }
 
    def __init__(self, product: str) -> None:
        self.cdo_path = None
        self.working_path = None
        self.product = product
        if self.product == "ICON0p125":
            self.url_blank = "https://opendata.dwd.de/weather/nwp/icon/grib/{run_time:%H}/{var}/icon_global_icosahedral_single-level_{run_time:%Y%m%d%H}_{step}_{VAR}.grib2.bz2"
            self.ancillary_remote_path = "https://opendata.dwd.de/weather/lib/cdo/"
            self.ancillary_remote_file = "ICON_GLOBAL2WORLD_0125_EASY.tar.bz2"
            self.grid_file = "target_grid_world_0125.txt"
            self.weight_file = "weights_icogl2world_0125.nc"
            self.issue_hours = [0,6,12,18]
            self.prelim_nodata = -1
        else:
            logging.error(" --> ERROR! Only ICON0p125 has been implemented until now!")
            raise NotImplementedError()
            
    def get_data(self,
                 time_range: TimeRange,
                 space_bounds: BoundingBox,
                 destination: str,
                 options: Optional[dict] = None) -> None:

        # Check options
        options = self.check_options(options)
        self.cdo_path = options['cdo_path']

        # Get the timesteps to download
        timesteps = time_range.get_timesteps_from_issue_hour(self.issue_hours)
        missing_times = []
        
        # Do all of this inside a temporary folder
        with tempfile.TemporaryDirectory() as tmp_path:

            self.working_path = tmp_path

            # Download preliminary files for file conversion if not available
            if not os.path.isfile(os.path.join(tmp_path, self.grid_file)) or not os.path.isfile(os.path.join(tmp_path, self.weight_file)):
                print(" ---> Download binary decodification table")
                r = requests.get(self.ancillary_remote_path + self.ancillary_remote_file)
                with open(os.path.join(tmp_path, "binary_grids.tar.bz2"), 'wb') as f:
                    f.write(r.content)
                untar_file(os.path.join(tmp_path, "binary_grids.tar.bz2"), move_to_root=True)

            # Download the data for the specified issue times
            for run_time in timesteps:

                print(f' ---> Downloading data for model issue: {run_time:%Y-%m-%d_%H}')
                # Set forecast steps
                print(" ----> Set forecast steps")
                self.max_steps = options['frc_max_step']
                frc_time_range, frc_steps = self.compute_model_steps(time_range.start)

                for var_out in options['variables'].keys():
                    print(f' ----> Downloading data for {var_out}')
                    tmp_destination = os.path.join(tmp_path, var_out, "")
                    os.makedirs(tmp_destination, exist_ok=True)

                    temp_files = []
                    for step_time, step in zip(frc_time_range, frc_steps):
                        print(f' ----> Downloading {var_out} data for +{step}h')
                        tmp_filename = f'temp_frc{self.product}_{run_time:%Y%m%d%H}_{step}_{var_out}.grib2.bz2'
                        tmp_destination = os.path.join(tmp_path, var_out, tmp_filename)
                        var_in = options['variables'][var_out]
                        success = self.download(tmp_destination, min_size = 200, missing_action = 'warn', run_time = run_time, step = str(step).zfill(3), VAR = var_in.upper(), var = var_in)
                        if success:
                            temp_files.append(self.project_bin_file(tmp_destination))
                            print(f' ----> SUCCESS! Downloaded and regridded {var_out} data for +{step}h')
                        else:
                            print(f' ----> ERROR! {var_out} for forecast step {step}h not available, skipping')
                            break

                    if len(temp_files) > 0:
                        with xr.open_mfdataset(temp_files, concat_dim='valid_time', data_vars='minimal',
                                                   combine='nested', coords='minimal',
                                                   compat='override', engine="cfgrib") as ds:
                                ds = ds.where((ds.latitude <= space_bounds.bbox[3]) &
                                              (ds.latitude >= space_bounds.bbox[1]) &
                                              (ds.longitude >= space_bounds.bbox[0]) &
                                              (ds.longitude <= space_bounds.bbox[2]), drop=True)

                                var_names = [vars for vars in ds.data_vars.variables.mapping]
                                if len(var_names) > 1:
                                    logging.error("ERROR! Only one variable should be in the grib file, check file integrity!")
                                    raise TypeError
                                var_name = var_names[0]

                                ds = ds[var_name].drop("time").rename({"longitude": "lon", "latitude": "lat", "valid_time": "time"})
                                out_name = run_time.strftime(destination).format(var = var_name)
                                os.makedirs(os.path.dirname(out_name), exist_ok=True)

                        if not 'frc_out' in locals():
                            frc_out = xr.Dataset({var_out:ds})
                        else:
                            frc_out[var_out] = ds

                        frc_out.drop(["valid_time","step","surface","heightAboveGround"], errors='ignore').to_netcdf(out_name)

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
            forecast_steps = np.arange(1, max_step, 1)
        time_range = [time_run + pd.Timedelta(str(i) + "H") for i in forecast_steps]
        return time_range, forecast_steps

    def project_bin_file(self, file_in: str):
        """
        extracts from a .gz file
        """
        decompress_bz2(file_in)
        os.remove(file_in)
        subprocess.check_output([self.cdo_path + "cdo -O remap," + os.path.join(self.working_path, self.grid_file) + "," + os.path.join(self.working_path, self.weight_file) + " " + file_in[:-4] + " " + file_in[:-4].replace("frc", "regr_frc")],
                       stderr=subprocess.STDOUT, shell=True)
        os.remove(file_in[:-4])
        return file_in[:-4].replace("frc", "regr_frc")