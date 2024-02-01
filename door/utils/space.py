import os
import numpy as np

from osgeo import gdal, gdalconst, osr
# I very much prefer rasterio over gdal, but gdal has less overhead (rasterio is built on top of gdal)
#TODO: consider using gdal in DRYES as well...

class BoundingBox():

    def __init__(self,
                 grid_file: str,
                 buffer: int = 0,):
        """
        Gets the CRS and bounding box of the grid file, this will be used to cut the data.
        No reprojection or resampling is done in DOOR.
        """
        # make sure grid_file is specified and it exists
        if grid_file is None:
            raise ValueError('grid_file must be specified')
        else:
            if not os.path.exists(grid_file):
                raise ValueError('grid_file does not exist')
        self.buffer = np.ceil(buffer)
        self.get_attrs_from_grid(grid_file)
    
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
        left   = self.transform[0] - self.buffer * self.xresolution
        top    = self.transform[3] + self.buffer * self.yresolution
        right  = self.transform[0] + self.shape[1]*self.transform[1] + self.buffer * self.xresolution
        bottom = self.transform[3] + self.shape[0]*self.transform[5] - self.buffer * self.yresolution
        self.bbox = (left, bottom, right, top)

        self.crs  = grid_data.GetProjection()

        del grid_data

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
        

        # Create a spatial reference object for the bounding box projection
        bbox_srs = osr.SpatialReference()
        bbox_srs.ImportFromWkt(self.crs)
        
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
            min_x, min_y, max_x, max_y = bbox_obj.bbox

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