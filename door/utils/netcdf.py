import xarray as xr
import os

from .space import BoundingBox

import logging
logger = logging.getLogger(__name__)

def crop_netcdf(src: str|xr.Dataset, BBox: BoundingBox) -> xr.DataArray:
    """
    Cut a geotiff to a bounding box.
    """
    if isinstance(src, str):
        engine = "necdf4" if src.endswith(".nc") else "cfgrib"
        src_ds = xr.load_dataset(src, engine=engine)
    else:
        src_ds = src

    # transform the bounding box to the geotiff projection
    if src_ds.rio.crs is not None:
        BBox.transform(src_ds.rio.crs)
    else:
        logger.warning(f' --> WARNING! No CRS found in the raster, assuming it is in {BBox.epsg_code}')
        src_ds = src_ds.rio.write_crs(BBox.proj, inplace=True)
    # otherwise, let's assume that the bounding box is already in the right projection
    #TODO: eventually fix this...

    # Crop the raster
    cropped = src_ds.rio.clip_box(*BBox.bbox)
    # cropped = src_ds.where((src_ds.lat <= BBox.bbox[3]) &
    #          (src_ds.lat >= BBox.bbox[1]) &
    #          (src_ds.lon >= BBox.bbox[0]) &
    #          (src_ds.lon <= BBox.bbox[2]), drop=True)

    return cropped

def save_netcdf(src: xr.DataArray, destination: str) -> None:
    """
    Save a raster to a file.
    """
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    src.to_netcdf(destination)