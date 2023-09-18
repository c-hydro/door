"""
Library Features

Name:          lib_data_io_tiff
Author(s):      Alessandro Masoero (alessandro.masoero@cimafoundation.org)
                Michel Isabellon (michel.isabellon@cimafoundation.org)
Date:          '20230728'
Version:       '1.0.0'
"""

#######################################################################################
# Library
from osgeo import gdal, gdalconst
import numpy as np

# -------------------------------------------------------------------------------------
# Methods to handle raster tiff

def getBoundingBox(geotarget):
    src = gdal.Open(geotarget, gdalconst.GA_ReadOnly)
    ulx, xres, xskew, uly, yskew, yres  = src.GetGeoTransform()
    lrx = ulx + (src.RasterXSize * xres)
    lry = uly + (src.RasterYSize * yres)
    bounding_box = str(ulx) + ',' + str(lry) + ',' + str(lrx) + ',' + str(uly)
    return bounding_box

def rescale (sFileIn, sFileOut, scale = 1, nodata=np.nan, savetype = 'Float32'):
    [xsize, ysize, geotransform, geoproj, data] = readGeotiff(sFileIn)
    data[data==nodata] = np.nan
    data = data * scale
    writeGeotiffSingleBand(sFileOut, geotransform, geoproj, data)

def keepValidRange (sFileIn, sFileOut, ValidRange):
    [xsize, ysize, geotransform, geoproj, data] = readGeotiff(sFileIn)
    data = data.astype(float)
    # print(ValidRange)
    data[data<ValidRange[0]] = np.nan
    data[data>ValidRange[1]] = np.nan
    writeGeotiffSingleBand(sFileOut, geotransform, geoproj, data)

def readGeotiff(filename):
    filehandle = gdal.Open(filename)
    band1 = filehandle.GetRasterBand(1)
    geotransform = filehandle.GetGeoTransform()
    geoproj = filehandle.GetProjection()
    band1data = band1.ReadAsArray()
    xsize = filehandle.RasterXSize
    ysize = filehandle.RasterYSize
    return xsize,ysize,geotransform,geoproj,band1data

def writeGeotiffSingleBand(filename,geotransform,geoprojection,data):
    (x,y) = data.shape
    format = "GTiff"
    driver = gdal.GetDriverByName(format)
    dst_datatype = gdal.GDT_Float32
    dst_ds = driver.Create(filename,y,x,1,dst_datatype)
    dst_ds.GetRasterBand(1).WriteArray(data)
    dst_ds.SetGeoTransform(geotransform)
    dst_ds.SetProjection(geoprojection)
    return 1

def getNoDataV (sFileIn, nameVar):
    filehandle = gdal.Open(sFileIn)
    band1 = filehandle.GetRasterBand(1)
    nodataV = band1.GetNoDataValue()
    # print (nameVar, nodataV)
    return nodataV

def maskMap(sFileIn, sFileMask, sFileOut):
    [data, xsize, ysize, geotransform, geoproj] = readGeotiff(sFileIn)
    [dataMask, xsizeMask, ysizeMask, geotransformMask, geoprojMask] = readGeotiff(sFileMask)
    data[dataMask==0]=np.nan
    writeGeotiffSingleBand(sFileOut, geotransform, geoproj, data)

