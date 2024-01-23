from typing import Optional
import os

import numpy as np
from osgeo import gdal, gdalconst
# I very much prefer rasterio over gdal, but gdal has less overhead (rasterio is built on top of gdal)
#TODO: consider using gdal in DRYES as well...

class SpatialReference():

    _resampling_methods = ['NearestNeighbor', 'Bilinear',
                           'Cubic', 'CubicSpline',
                           'Lanczos',
                           'Average', 'Mode',
                           'Max', 'Min',
                           'Med', 'Q1', 'Q3'] # available resampling methods according to GDAL

    def __init__(self,
                 grid_file: str,
                 resampling_method: str = "nearest",
                 mask_values: Optional[float|list[float]] = None,):
        """
        Creates an object to regrid data to a specified grid.
        grid_file is the path to a file containing the grid to use for the data source.
        the boundding box, crs, resolution, shape and transform of the grid will be applied to the downloaded data.
        resampling_method is the method to use to resample the data to the grid specified in grid_file.
        Available resampling are according to the GDAL library.
        If mask_values is specified, data values equal to mask_values will be masked (set to NaN).
        """
        self.check_inputs(grid_file, resampling_method, mask_values)

    def check_inputs(self, grid_file, resampling_method, mask_values):

        if mask_values is not None:
            if isinstance(mask_values, (int, float)):
                self.mask_values = [mask_values]
            elif isinstance(mask_values, list):
                self.mask_values = mask_values
            else:
                raise ValueError("mask_values must be a float or a list of floats")
            self.apply_mask = True
        else:
            self.apply_mask = False

        # make sure grid_file is specified and it exists
        if grid_file is None:
            raise ValueError('grid_file must be specified')
        else:
            if not os.path.exists(grid_file):
                raise ValueError('grid_file does not exist')
        self.get_attrs_from_grid(grid_file)
        
        for method in self._resampling_methods:
            if resampling_method.lower() == method.lower():
                self.resampling_method = method
                break
        else:
            raise ValueError("resampling_method must be one of the following: " + ", ".join(self._resampling_methods) + ".")
    
    def get_attrs_from_grid(self, grid_file):
        """
        Get attributes from grid_file
        We get the bounding box, crs, resolution, shape and transform of the grid.
        """
        self.grid_file = grid_file
        grid_data = gdal.Open(grid_file, gdalconst.GA_ReadOnly)

        self.transform = grid_data.GetGeoTransform()
        self.shape = (grid_data.RasterYSize, grid_data.RasterXSize)

        #bbox in the form (min_lon, min_lat, max_lon, max_lat)
        left   = self.transform[0]
        top    = self.transform[3]
        right  = self.transform[0] + self.shape[0]*self.transform[1]
        bottom = self.transform[3] + self.shape[1]*self.transform[5]
        self.bbox = (left, bottom, right, top)

        self.crs  = grid_data.GetProjection()
        self.xresolution = self.transform[1]
        self.yresolution = self.transform[5]
        if self.apply_mask:
            # if this line throws an error, make sure gdal is installed correctly and after numpy. you can use 
            # pip install --no-cache-dir --force-reinstall gdal[numpy]>=3.4.3
            data = grid_data.GetRasterBand(1).ReadAsArray()
            mask = np.full(self.shape, 1, dtype=bool)
            for mask_value in self.mask_values:
                if np.isnan(mask_value):
                    mask[np.isnan(data)] = 0
                else:
                    mask[data == mask_value] = 0
            self.mask = mask

        del grid_data

    def regrid_raster(self, input_file: str, output_file: str, nodata_value: Optional[float] = None):
        # Open the input and reference raster files
        input_raster = gdal.Open(input_file, gdalconst.GA_ReadOnly)
        if nodata_value is not None:
            input_raster.GetRasterBand(1).SetNoDataValue(nodata_value)

        # Get the resampling method
        resampling = getattr(gdalconst, f'GRA_{self.resampling_method.capitalize()}')

        # Create an output raster file with the same properties as the reference raster
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        output_raster = gdal.GetDriverByName('GTiff').Create(
            output_file, 
            self.shape[1],
            self.shape[0], 
            input_raster.RasterCount, 
            input_raster.GetRasterBand(1).DataType,
            ['COMPRESS=LZW']
            #see https://gdal.org/drivers/raster/gtiff.html for creation options.
        )
        output_raster.SetGeoTransform(self.transform)
        output_raster.SetProjection(self.crs)

        # Perform the projection & resampling 
        gdal.ReprojectImage(
            input_raster, 
            output_raster, 
            input_raster.GetProjection(),
            self.crs, 
            resampling
        )

        # Apply mask if specified by setting to NaN all values where the mask is True
        if self.apply_mask:
            data = output_raster.GetRasterBand(1).ReadAsArray()
            data[self.mask == 0] = np.nan
            output_raster.GetRasterBand(1).WriteArray(data)

        # Close the files
        del input_raster
        del output_raster