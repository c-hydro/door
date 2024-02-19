from osgeo import gdal, osr, gdalconst
import numpy as np
import os
import xarray as xr
import rioxarray as rxr

from .space import BoundingBox

def crop_raster(src: str|gdal.Dataset, BBox: BoundingBox, output_file: str) -> None:
    """
    Cut a geotiff to a bounding box.
    """
    
    if isinstance(src, str):
        src_ds = gdal.Open(src, gdalconst.GA_ReadOnly)
    else:
        src_ds = src
    
    geoprojection = src_ds.GetProjection()
    geotransform = src_ds.GetGeoTransform()

    # transform the bounding box to the geotiff projection
    BBox.transform(geoprojection)

    min_x, min_y, max_x, max_y = BBox.bbox
    # in order to not change the grid, we need to make sure that the new bounds were also in the old grid
    in_min_x = geotransform[0]
    in_res_x = geotransform[1]
    in_num_x = src_ds.RasterXSize
    in_min_y = geotransform[3]
    in_res_y = geotransform[5]
    in_num_y = src_ds.RasterYSize
    xcoords_in = np.arange(in_min_x, in_min_x + (in_num_x +1) * in_res_x, in_res_x)
    ycoords_in = np.arange(in_min_y, in_min_y + (in_num_y +1) * in_res_y, in_res_y)

    # the if else statements are used to make sure that the new bounds are not larger than the original ones
    min_x_real = max(xcoords_in[xcoords_in <= min_x]) if any(xcoords_in <= min_x) else min(xcoords_in)
    max_x_real = min(xcoords_in[xcoords_in >= max_x]) if any(xcoords_in >= max_x) else max(xcoords_in)
    min_y_real = max(ycoords_in[ycoords_in <= min_y]) if any(ycoords_in <= min_y) else min(ycoords_in)
    max_y_real = min(ycoords_in[ycoords_in >= max_y]) if any(ycoords_in >= max_y) else max(ycoords_in)

    # Create the output file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    gdal.Warp(output_file, src_ds, outputBounds=(min_x_real, min_y_real, max_x_real, max_y_real),
            outputType = src_ds.GetRasterBand(1).DataType, creationOptions = ['COMPRESS=LZW'])

    # Close the datasets
    src_ds = None

def save_raster(src: gdal.Dataset, output_file: str) -> None:
    """
    Save a gdal dataset to a file
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    gdal.Translate(output_file, src, options=gdal.TranslateOptions(format='GTiff', creationOptions=['COMPRESS=LZW']))

def save_array_to_tiff(src: xr.DataArray, output_file:str) -> None:
    """
    Save a xarray dataset to a tiff file
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    src.rio.to_raster(output_file)