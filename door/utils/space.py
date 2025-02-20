import os
from typing import Optional

import numpy as np
import tempfile

from osgeo import gdal, gdalconst, osr
import xarray as xr
import rioxarray as rxr
from shapely.constructive import buffer

from ..tools.data import Dataset

class BoundingBox():

    default_datum = 'EPSG:4326'
    #'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'

    def __init__(self,
                 left: float, bottom: float, right: float, top: float,
                 datum: Optional[str] = None,
                 buffer: float = 0.0,):
        """
        Gets the CRS and bounding box of the grid file, this will be used to cut the data.
        No reprojection or resampling is done in DOOR.
        """

        # datum should be able to accept both EPSG codes and WKT strings and should default to WGS84
        if datum is None:
            self.epsg_code = self.default_datum
            self.wkt_datum = get_wkt(self.default_datum)
        else:
            self.epsg_code, self.wkt_datum = get_datum(datum)

        # set the bounding box
        self.bbox = (left, bottom, right, top)

        # buffer the bounding box
        self.buffer_bbox(buffer)

    @staticmethod
    def from_dataset(dataset: Dataset, buffer: float = 0.0):
        data:xr.DataArray = dataset.get_data()

        # get the bounding box of the data
        left, bottom, right, top = data.rio.bounds()

        return BoundingBox(left, bottom, right, top, datum = data.rio.crs.to_wkt(), buffer = buffer)
    
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
        return BoundingBox(left, bottom, right, top, datum = proj, buffer = buffer)

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

    def transform(self, new_datum: str, inplace = False) -> None:
        """
        Transform the bounding box to a new datum
        new_datum: the new datum in the form of an EPSG code
        """
        
        # figure out if we were given an EPSG code or a WKT string
        new_epsg, new_wkt = get_datum(new_datum)

        # Create a spatial reference object for the GeoTIFF projection
        new_srs = osr.SpatialReference()
        new_srs.ImportFromWkt(new_wkt)

        # Create a spatial reference object for the bounding box projection
        bbox_srs = osr.SpatialReference()
        bbox_srs.ImportFromWkt(self.wkt_datum)
        
        # this is needed to make sure that the axis are in the same order i.e. (lon, lat) or (x, y)
        # see https://gdal.org/tutorials/osr_api_tut.html#coordinate-systems-in-gdal
        new_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        bbox_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

        # make sure that the projection is the same, if it is not, update the 4 corners of the bbox
        if not new_epsg==self.epsg_code:

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

        if inplace:
            self.bbox = (min_x, min_y, max_x, max_y)
            self.wkt_datum = new_wkt
            self.epsg_code = new_epsg
        else:
            return BoundingBox(min_x, min_y, max_x, max_y, datum = new_wkt)


def get_wkt(proj_string: str) -> str:
    # Create a spatial reference object
    srs = osr.SpatialReference()

    # Import the EPSG code into the spatial reference object
    srs.ImportFromEPSG(int(proj_string.split(':')[1]))

    # Get the WKT string
    wkt_string = srs.ExportToWkt()

    return wkt_string

def get_epsg(wkt_string: str) -> str:
    # Create a spatial reference object
    srs = osr.SpatialReference()
    srs.ImportFromWkt(wkt_string)

    # Get the EPSG code
    epsg_code = srs.GetAuthorityCode(None)

    return f'EPSG:{epsg_code}'

def get_datum(value: str|int) -> tuple[str]:
    """
    Check if the value is an EPSG code or a WKT string,
    will return a tuple of (EPSG, WKT)
    """
    if isinstance(value, int):
        value = f'EPSG:{value}'

    try: # this will fail if value it is not an EPSG code
        epsg_code = value
        wkt_datum = get_wkt(value)
    except:
        try: # this will fail if value is not a WKT string
            wkt_datum = value
            epsg_code = get_epsg(value)
        except:
            raise ValueError(f'Unknown datum type: {value}, please provide an EPSG code ("EPSG:#####") or a WKT string.')

    return epsg_code, wkt_datum

def crop_to_bb(src: str|gdal.Dataset|xr.DataArray|xr.Dataset,
               BBox: BoundingBox,
               out: str = 'xarray') -> None:
    """
    Cut a map to a bounding box.
    """
    BBox.buffer_bbox(-1e-6)

    if isinstance(src, gdal.Dataset):
        with tempfile.TemporaryDirectory() as tmp_path:
            tmp_file = os.path.join(tmp_path, 'tmp.tif')
            crop_raster(src, BBox, tmp_file)
            if out == 'xarray':
                return rxr.open_rasterio(tmp_file)
            elif out == 'gdal':
                return gdal.Open(tmp_file, gdalconst.GA_ReadOnly)

    elif isinstance(src, str) and src.endswith(".tif"):
        src = rxr.open_rasterio(src)

    cropped_xarray = crop_netcdf(src, BBox)
    if out == 'xarray':
        return cropped_xarray
    elif out == 'gdal':
        with tempfile.TemporaryDirectory() as tmp_path:
            tmp_file = os.path.join(tmp_path, 'tmp.tif')
            cropped_xarray.rio.to_raster(tmp_file)
            return gdal.Open(tmp_file, gdalconst.GA_ReadOnly)

def crop_netcdf(src: str|xr.DataArray|xr.Dataset,
                BBox: BoundingBox) -> xr.DataArray:
    """
    Cut a geotiff to a bounding box.
    """
    if isinstance(src, str):
        if src.endswith(".nc"):
            src_ds = xr.load_dataset(src, engine="netcdf4")
        elif src.endswith(".grib"):
            src_ds = xr.load_dataset(src, engine="cfgrib")
    elif isinstance(src, xr.DataArray) or isinstance(src, xr.Dataset):
        src_ds = src

    x_dim = src_ds.rio.x_dim
    lon_values = src_ds[x_dim].values
    if (lon_values > 180).any():
        new_lon_values = xr.where(lon_values > 180, lon_values - 360, lon_values)
        new = src_ds.assign_coords({x_dim:new_lon_values}).sortby(x_dim)
        src_ds = new.rio.set_spatial_dims(x_dim, new.rio.y_dim)

    # transform the bounding box to the geotiff projection
    if src_ds.rio.crs is not None:
        transformed_BBox = BBox.transform(src_ds.rio.crs.to_wkt())
    else:
        src_ds = src_ds.rio.write_crs(BBox.wkt_datum, inplace=True)
        transformed_BBox = BBox
    # otherwise, let's assume that the bounding box is already in the right projection
    #TODO: eventually fix this...

    # Crop the raster
    cropped = src_ds.rio.clip_box(*transformed_BBox.bbox)

    return cropped

def crop_raster(src: str|gdal.Dataset, BBox: BoundingBox, output_file: str) -> None:
    """
    Cut a geotiff to a bounding box.
    """

    if isinstance(src, str):
        src_ds = gdal.Open(src, gdalconst.GA_ReadOnly)
    else:
        src_ds = src

    ## TODO: check the longitude values and transform them if necessary

    geoprojection = src_ds.GetProjection()
    geotransform = src_ds.GetGeoTransform()

    # transform the bounding box to the geotiff projection
    BBox_trans = BBox.transform(geoprojection, inplace = False)

    min_x, min_y, max_x, max_y = BBox_trans.bbox
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