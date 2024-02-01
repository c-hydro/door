import os
from typing import Optional

import numpy as np

from osgeo import gdal, gdalconst, osr
# I very much prefer rasterio over gdal, but gdal has less overhead (rasterio is built on top of gdal)
#TODO: consider using gdal in DRYES as well...

class BoundingBox():

    def __init__(self,
                 coords: Optional[list[float]] = None,
                 grid_file: Optional[str] = None,
                 buffer: float = 0.0,):
        """
        Gets the CRS and bounding box of the grid file, this will be used to cut the data.
        No reprojection or resampling is done in DOOR.
        """
        # make sure either grid_file or coords is specified
        if grid_file is None and coords is None:
            raise ValueError('Either grid_file or coords must be specified')
        elif grid_file is not None and coords is not None:
            raise ValueError('Only one of grid_file or coords can be specified')
        elif grid_file is not None:
            if not os.path.exists(grid_file):
                raise ValueError('grid_file does not exist')
            self.get_attrs_from_grid(grid_file)
        else:
            if len(coords) != 4:
                raise ValueError('coords must be a list of 4 values: left, bottom, top, right')
            self.get_attrs_from_coords(coords)

        # buffer the bounding box
        self.buffer_bbox(buffer)
    
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
        self.xresolution = self.transform[1]
        self.yresolution = self.transform[5]
        left   = self.transform[0] 
        top    = self.transform[3]
        right  = self.transform[0] + self.shape[1]*self.transform[1]
        bottom = self.transform[3] + self.shape[0]*self.transform[5]
        self.bbox = (left, bottom, right, top)

        self.proj  = grid_data.GetProjection()

        del grid_data

    def get_attrs_from_coords(self, coords):
        """
        Get attributes from coords
        We leave all the oterh attributes as None.
        """
        self.grid_file = None
        self.transform = None
        self.shape = None
        self.xresolution = None
        self.yresolution = None
        self.bbox = coords
        self.proj = None

    def buffer_bbox(self, buffer: int) -> None:
        """
        Buffer the bounding box, the buffer is in units of coordinates
        """
        self.buffer = buffer
        left, bottom, right, top = self.bbox
        self.bbox = (left - self.buffer,
                     bottom - self.buffer,
                     right + self.buffer,
                     top + self.buffer)

    def crop_raster(self, input_file: str, output_file: str) -> None:
        """
        Cut a geotiff to a bounding box.
        """
        src_ds = gdal.Open(input_file)
        geoprojection = src_ds.GetProjection()
        geotransform = src_ds.GetGeoTransform()

        # Create a spatial reference object for the GeoTIFF projection
        input_srs = osr.SpatialReference()
        input_srs.ImportFromWkt(geoprojection)
        
        if self.proj is None:
            # if the crs is not specified, use the crs of the input file
            self.proj = geoprojection

        # Create a spatial reference object for the bounding box projection
        bbox_srs = osr.SpatialReference()
        bbox_srs.ImportFromWkt(self.proj)
        
        # this is needed to make sure that the axis are in the same order i.e. (lon, lat) or (x, y)
        # see https://gdal.org/tutorials/osr_api_tut.html#coordinate-systems-in-gdal
        input_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        bbox_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

        # make sure that the projection is the same, if it is not, update the 4 corners of the bbox
        if not input_srs.IsSame(bbox_srs):

            # Create a transformer to convert coordinates
            transformer = osr.CoordinateTransformation(bbox_srs, input_srs)

            # Transform the bounding box coordinates - because the image might be warped, we need to transform all 4 corners
            bl_x, bl_y, _ = transformer.TransformPoint(self.bbox[0], self.bbox[1])
            tr_x, tr_y, _ = transformer.TransformPoint(self.bbox[2], self.bbox[3])
            br_x, br_y, _ = transformer.TransformPoint(self.bbox[2], self.bbox[1])
            tl_x, tl_y, _ = transformer.TransformPoint(self.bbox[0], self.bbox[3])

            # get the new bounding box
            min_x = min(bl_x, tl_x)
            max_x = max(br_x, tr_x)
            min_y = min(bl_y, br_y)
            max_y = max(tl_y, tr_y)
        else:
            min_x, min_y, max_x, max_y = self.bbox

        # in order to not change the grid, we need to make sure that the new bounds were also in the old grid
        xcoords_in = np.arange(geotransform[0], geotransform[0] + src_ds.RasterXSize * geotransform[1], geotransform[1])
        ycoords_in = np.arange(geotransform[3], geotransform[3] + src_ds.RasterYSize * geotransform[5], geotransform[5])

        min_x_real = max(xcoords_in[xcoords_in <= min_x])
        max_x_real = min(xcoords_in[xcoords_in >= max_x])
        min_y_real = max(ycoords_in[ycoords_in <= min_y])
        max_y_real = min(ycoords_in[ycoords_in >= max_y])

        # Create the output file
        gdal.Warp(output_file, src_ds, outputBounds=(min_x_real, min_y_real, max_x_real, max_y_real),
                outputType = src_ds.GetRasterBand(1).DataType, creationOptions = ['COMPRESS=LZW'])

        # Close the datasets
        src_ds = None