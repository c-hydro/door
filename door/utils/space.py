import os
from typing import Optional

import numpy as np

from osgeo import gdal, gdalconst, osr
# I very much prefer rasterio over gdal, but gdal has less overhead (rasterio is built on top of gdal)
#TODO: consider using gdal in DRYES as well...

class BoundingBox():

    default_projection = 'EPSG:4326'
    #'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'

    def __init__(self,
                 left: float, bottom: float, right: float, top: float,
                 projection: Optional[str] = None,
                 buffer: float = 0.0,):
        """
        Gets the CRS and bounding box of the grid file, this will be used to cut the data.
        No reprojection or resampling is done in DOOR.
        """
        # default to WGS84
        if projection is None:
            self.epsg_code = self.default_projection
        else:
            self.epsg_code = projection

        # set the projection
        self.proj = get_wkt(self.epsg_code)

        # set the bounding box
        self.bbox = (left, bottom, right, top)

        # buffer the bounding box
        self.buffer_bbox(buffer)
    
    @staticmethod
    def from_file(grid_file, buffer: float = 0.0):
        """
        Get attributes from grid_file
        We get the bounding box, crs, resolution, shape and transform of the grid.
        """

        grid_data = gdal.Open(grid_file, gdalconst.GA_ReadOnly)

        transform = grid_data.GetGeoTransform()
        shape = (grid_data.RasterYSize, grid_data.RasterXSize)

        #bbox in the form (min_lon, min_lat, max_lon, max_lat)
        left   = transform[0] 
        top    = transform[3]
        right  = transform[0] + shape[1]*transform[1]
        bottom = transform[3] + shape[0]*transform[5]

        proj  = grid_data.GetProjection()

        grid_data = None
        return BoundingBox(left, bottom, right, top, projection = proj, buffer = buffer)

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

    def transform(self, new_proj: str) -> None:
        """
        Transform the bounding box to a new projection
        """
        # Create a spatial reference object for the GeoTIFF projection
        new_srs = osr.SpatialReference()
        new_srs.ImportFromWkt(get_wkt(new_proj))

        # Create a spatial reference object for the bounding box projection
        bbox_srs = osr.SpatialReference()
        bbox_srs.ImportFromWkt(self.proj)
        
        # this is needed to make sure that the axis are in the same order i.e. (lon, lat) or (x, y)
        # see https://gdal.org/tutorials/osr_api_tut.html#coordinate-systems-in-gdal
        new_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        bbox_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

        # make sure that the projection is the same, if it is not, update the 4 corners of the bbox
        if not new_srs.IsSame(bbox_srs):

            # Create a transformer to convert coordinates
            transformer = osr.CoordinateTransformation(bbox_srs, new_srs)

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

        self.bbox = (min_x, min_y, max_x, max_y)
        self.proj = new_proj


def get_wkt(proj_string: str) -> str:
    # Create a spatial reference object
    srs = osr.SpatialReference()

    # Import the EPSG code into the spatial reference object
    try:
        srs.ImportFromEPSG(int(proj_string.split(':')[1]))
    except:
        srs.ImportFromWkt(proj_string)

    # Get the WKT string
    wkt_string = srs.ExportToWkt()

    return wkt_string