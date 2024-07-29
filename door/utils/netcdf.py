import xarray as xr
import os

import logging
logger = logging.getLogger(__name__)

def save_netcdf(src: xr.DataArray, destination: str) -> None:
    """
    Save a raster to a file.
    """
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    src.to_netcdf(destination)