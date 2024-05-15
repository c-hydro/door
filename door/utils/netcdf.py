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
        engine = "netcdf4" if src.endswith(".nc") else "cfgrib"
        src_ds = xr.load_dataset(src, engine=engine)
    else:
        src_ds = src

    x_dim = src_ds.rio.x_dim
    lon_values = src_ds[x_dim].values
    if (lon_values > 180).any():
        new_lon_values = xr.where(lon_values > 180, lon_values - 360, lon_values)
        new = src_ds.assign_coords({x_dim:new_lon_values}).sortby(x_dim)
        src_ds = new.rio.set_spatial_dims(x_dim, new.rio.y_dim)


    # transform the bounding box to the geotiff projection
    if src_ds.rio.crs is not None:
        transformed_BBox = BBox.transform(src_ds.rio.crs.to_epsg())
    else:
        logger.warning(f' --> WARNING! No CRS found in the raster, assuming it is in {BBox.epsg_code}')
        src_ds = src_ds.rio.write_crs(BBox.proj, inplace=True)
    # otherwise, let's assume that the bounding box is already in the right projection
    #TODO: eventually fix this...

    # Crop the raster
    cropped = src_ds.rio.clip_box(*transformed_BBox.bbox)
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