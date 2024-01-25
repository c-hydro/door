from .space import SpatialReference

import os
import numpy as np
from typing import Optional

from osgeo import gdal, gdalconst

def regrid_raster(input_file: str, output_file: str, spatial_reference: SpatialReference,
                  nodata_value: Optional[float] = np.nan, resampling_method: Optional[str] = None,):
    
    if resampling_method is not None:
        spatial_reference.resampling_method = resampling_method

    # Open the input and reference raster files
    input_raster = gdal.Open(input_file, gdalconst.GA_ReadOnly)
    if nodata_value is not None:
        input_raster.GetRasterBand(1).SetNoDataValue(nodata_value)

    # Get the resampling method
    resampling = getattr(gdalconst, f'GRA_{spatial_reference.resampling_method}')

    # Create an output raster file with the same properties as the reference raster
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    output_raster = gdal.GetDriverByName('GTiff').Create(
        output_file, 
        spatial_reference.shape[1],
        spatial_reference.shape[0], 
        input_raster.RasterCount, 
        input_raster.GetRasterBand(1).DataType,
        ['COMPRESS=LZW']
        #see https://gdal.org/drivers/raster/gtiff.html for creation options.
    )
    output_raster.SetGeoTransform(spatial_reference.transform)
    output_raster.SetProjection(spatial_reference.crs)

    # Perform the projection & resampling 
    gdal.ReprojectImage(
        input_raster, 
        output_raster, 
        input_raster.GetProjection(),
        spatial_reference.crs, 
        resampling
    )

    # Apply mask if specified by setting to NaN all values where the mask is True
    if spatial_reference.apply_mask:
        data = output_raster.GetRasterBand(1).ReadAsArray()
        data[spatial_reference.mask == 0] = np.nan
        output_raster.GetRasterBand(1).WriteArray(data)

    # Close the files
    del input_raster
    del output_raster

def keep_valid_range(input_file: str, output_file: str, valid_range: tuple[float]) -> None:
    """
    Keep only the values in the valid range.
    """
    [geotransform, geoprojection, src_data] = read_geotiff_singleband(input_file)

    new_data = src_data.astype(float).copy()
    new_data[src_data < valid_range[0]] = np.nan
    new_data[src_data > valid_range[1]] = np.nan

    write_geotiff_singleband(output_file,geotransform,geoprojection,new_data)

def apply_scale_factor(input_file: str, output_file: str, scale_factor: float) -> None:
    """
    Applies a scale factor to a raster.
    """

    [geotransform, geoprojection, src_data] = read_geotiff_singleband(input_file)
    new_data = src_data * scale_factor
    write_geotiff_singleband(output_file,geotransform,geoprojection,new_data)

def read_geotiff_singleband(filename):
    filehandle = gdal.Open(filename)
    band1 = filehandle.GetRasterBand(1)
    geotransform = filehandle.GetGeoTransform()
    geoproj = filehandle.GetProjection()
    band1data = band1.ReadAsArray()
    filehandle = None
    return geotransform,geoproj,band1data

def write_geotiff_singleband(filename,geotransform,geoprojection,data):
    (x,y) = data.shape
    format = "GTiff"
    driver = gdal.GetDriverByName(format)
    dst_datatype = gdal.GDT_Float32

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    dst_ds = driver.Create(filename,y,x,1,dst_datatype)
    dst_ds.SetGeoTransform(geotransform)
    dst_ds.SetProjection(geoprojection)
    dst_ds.GetRasterBand(1).WriteArray(data)
    dst_ds = None