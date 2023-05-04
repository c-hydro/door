"""
"""

#######################################################################################
# Libraries
import logging
import os

import numpy as np
import xarray as xr
import pandas as pd

from netCDF4 import Dataset,date2num

import matplotlib.pyplot as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to read data netcdf
def read_data_nc(file_name, geo_ref_x=None, geo_ref_y=None, geo_ref_attrs=None,
                 var_coords=None, var_scale_factor=1, var_name=None, var_time=None, var_no_data=-9999.0,
                 coord_name_time='time', coord_name_geo_x='Longitude', coord_name_geo_y='Latitude',
                 dim_name_time='time', dim_name_geo_x='Longitude', dim_name_geo_y='Latitude',
                 dims_order=None, decimal_round=4, grid_remapping_info=None):

    if var_coords is None:
        var_coords = {'time': 'time', 'x': 'Longitude', 'y': 'Latitude'}

    if dims_order is None:
        dims_order = [dim_name_time, dim_name_geo_y, dim_name_geo_x]

    if os.path.exists(file_name):

        # Open file nc
        file_handle = xr.open_dataset(file_name)
        file_attrs = file_handle.attrs

        file_variables = list(file_handle.variables)
        file_dims = list(file_handle.dims)
        file_coords = list(file_handle.coords)

        idx_coords = {}
        for coord_key, coord_name in var_coords.items():
            if coord_name in file_coords:
                coord_idx = file_coords.index(coord_name)
            else:
                coord_idx = None
            idx_coords[coord_key] = coord_idx

        if var_name in file_variables:

            var_data = file_handle[var_name].values
            var_data = np.float32(var_data / var_scale_factor)

            if 'time' in list(idx_coords.keys()):
                if idx_coords['time'] is not None:
                    coord_name_time = file_coords[idx_coords['time']]
                    if file_handle[coord_name_time].size == 1:
                        if var_data.shape.__len__() < file_coords.__len__():
                            var_data = var_data[:, :, np.newaxis]
                        elif var_data.shape.__len__() == file_coords.__len__():
                            pass
                        else:
                            raise NotImplemented('File shape is greater than expected coords')
                    else:
                        raise NotImplemented('Time size is greater than 1')
                else:
                    raise IOError('Coord name "time" is not available')
            else:
                pass

            if idx_coords['x'] is not None:
                coord_name_x = file_coords[idx_coords['x']]
                geo_data_x = file_handle[coord_name_x].values
            else:
                raise IOError('Coord name "x" is not available')

            if idx_coords['y'] is not None:
                coord_name_y = file_coords[idx_coords['y']]
                geo_data_y = file_handle[coord_name_y].values
            else:
                raise IOError('Coord name "y" is not available')

            if grid_remapping_info['regrid_lon']:
                index_ndarray = np.arange(grid_remapping_info['lon_grid_range'][0],
                                       grid_remapping_info['lon_grid_range'][1],
                                       grid_remapping_info['lon_grid_range'][2])
                index_ndarray = np.round(index_ndarray, grid_remapping_info['lon_grid_round'])
                index_list = index_ndarray.tolist()
                index_loc = index_list.index(grid_remapping_info['lon_grid_index'])
                map1 = var_data[:, :, index_loc:]
                map2 = var_data[:, :, 0:index_loc]
                map_all = np.concatenate((map1, map2), axis=2)
                var_data = map_all
                geo_data_x = index_ndarray

            if geo_data_x.ndim == 1 and geo_data_y.ndim == 1:
                geo_data_x, geo_data_y = np.meshgrid(geo_data_x, geo_data_y)

            geo_y_upper = geo_data_y[0, 0]
            geo_y_lower = geo_data_y[-1, 0]
            if geo_y_lower > geo_y_upper:
                geo_data_y = np.flipud(geo_data_y)
                var_data = np.flip(var_data,idx_coords['y'])

            if (geo_ref_x is not None) and (geo_ref_y is not None):
                geo_check_x, geo_check_y = np.meshgrid(geo_ref_x, geo_ref_y)
            elif (geo_ref_x is None) or (geo_ref_y is None):
                geo_check_x = geo_data_x
                geo_check_y = geo_data_y
                logging.warning(' ===> Variables geo_check_x and geo_check_y assumed equal to geo_data_x and geo_data_y')
            else:
                logging.error(' ===> Problem with file georeference!')
                raise NotImplemented('Problem with file georeference!')

            geo_check_start_x = np.float32(round(geo_check_x[0, 0], decimal_round))
            geo_check_start_y = np.float32(round(geo_check_y[0, 0], decimal_round))
            geo_check_end_x = np.float32(round(geo_check_x[-1, -1], decimal_round))
            geo_check_end_y = np.float32(round(geo_check_y[-1, -1], decimal_round))

            geo_data_start_x = np.float32(round(geo_data_x[0, 0], decimal_round))
            geo_data_start_y = np.float32(round(geo_data_y[0, 0], decimal_round))
            geo_data_end_x = np.float32(round(geo_data_x[-1, -1], decimal_round))
            geo_data_end_y = np.float32(round(geo_data_y[-1, -1], decimal_round))

            assert geo_check_start_x == geo_data_start_x, ' ===> Variable geo x start != Reference geo x start'
            assert geo_check_start_y == geo_data_start_y, ' ===> Variable geo y start != Reference geo y start'
            assert geo_check_end_x == geo_data_end_x, ' ===> Variable geo x end != Reference geo x end'
            assert geo_check_end_y == geo_data_end_y, ' ===> Variable geo y end != Reference geo y end'

        else:
            logging.warning(' ===> Variable ' + var_name + ' not available in loaded datasets!')
            var_data = None
    else:
        logging.warning(' ===> File ' + file_name + ' not available in loaded datasets!')
        var_data = None

    if var_data is not None:

        if dims_order.__len__() == 3:

            if var_time is None:
                var_time = pd.Timestamp.now()

            if isinstance(var_time, pd.Timestamp):
                var_time = pd.DatetimeIndex([var_time])
            elif isinstance(var_time, pd.DatetimeIndex):
                pass
            else:
                logging.error(' ===> Time format is not allowed. Expected Timestamp or Datetimeindex')
                raise NotImplemented('Case not implemented yet')

            var_da = xr.DataArray(var_data, name=var_name, dims=dims_order,
                                  coords={coord_name_time: ([dim_name_time], var_time),
                                          coord_name_geo_y: ([dim_name_geo_y], geo_data_y[:, 0]),
                                          coord_name_geo_x: ([dim_name_geo_x], geo_data_x[0, :])})

        elif dims_order.__len__() == 2:
            var_da = xr.DataArray(var_data, name=var_name, dims=dims_order,
                                  coords={coord_name_geo_y: ([dim_name_geo_y], geo_data_y[:, 0]),
                                          coord_name_geo_x: ([dim_name_geo_x], geo_data_x[0, :])})
        else:
            raise NotImplemented('Case not implemented yet')

        if geo_ref_attrs is not None:
            var_da.attrs = geo_ref_attrs

    else:
        logging.warning(' ===> All filenames in the selected period are not available')
        var_da = None

    return var_da

# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
def h14cdoconverter(gribfile, destfld, path_grib_to_nc, path_cdo, var_list,
                    dim_name_time='time', dim_name_geo_x='Longitude', dim_name_geo_y='Latitude',
                    dims_order=None, rows='1600', columns='800', clean_temp=True):

    # regrid
    filein = gribfile
    fileout = os.path.join(destfld, 'tmp_regrid.grib')
    cdocmd = path_cdo + ' -R remapcon,r' + columns + 'x' + rows + ' -setgridtype,regular'
    cdocmdline = cdocmd + ' ' + filein + ' ' + fileout
    os.system(cdocmdline)

    # convert grib to nc
    filein = fileout
    fileout = os.path.join(destfld, 'tmp_regrid.nc')
    cdocmd = path_cdo + ' -f nc copy'
    cdocmdline = cdocmd + ' ' + filein + ' ' + fileout
    os.system(cdocmdline)
    if clean_temp:
        os.remove(filein)

    #based on some quick visual screening of data in grib and nc files, this is the resulting association:
    #Soil_wetness_index_in_layer_1_surface --> var40
    #Soil_wetness_index_in_layer_2_surface --> var41
    #Soil_wetness_index_in_layer_3_surface --> var42
    #Soil_wetness_index_in_layer_4_surface --> var43
    #Looks like CDO used attribute Grib1_Parameter to rename variables

    #open nc file and create reformatted nc file that is compatible with H141 and H142
    file_handle = xr.open_dataset(fileout)
    file_dset = None

    if dims_order is None:
        dims_order = [dim_name_time, dim_name_geo_y, dim_name_geo_x]

    for var_name in var_list:

        var_data = file_handle[var_name].values
        lons_file = file_handle.lon.values # 1d
        lats_file = file_handle.lat.values # 1d
        time_file = file_handle.time.values # pd:datetimeindex

        var_da = xr.DataArray(var_data,
                              name=var_name,
                              dims=dims_order,
                              coords={dim_name_time: pd.DatetimeIndex([time_file]),
                                      dim_name_geo_y: lats_file,
                                      dim_name_geo_x: lons_file})
        if file_dset is None:
            file_dset = xr.Dataset(coords={dim_name_time: pd.DatetimeIndex([time_file])})

        file_dset[var_name] = var_da

    #create and write nc file
    ds = Dataset(path_grib_to_nc, 'w', format='NETCDF4')
    Dim_Lat = ds.createDimension(dim_name_geo_y, lats_file.__len__())
    Dim_Lon = ds.createDimension(dim_name_geo_x, lons_file.__len__())
    Dim_time = ds.createDimension(dim_name_time, time_file.__len__())
    Longitude = ds.createVariable(dim_name_geo_x, "d", (dim_name_geo_x,), zlib=True)
    Longitude[:] = lons_file
    Latitude = ds.createVariable(dim_name_geo_y, "d", (dim_name_geo_y,), zlib=True)
    Latitude[:] = lats_file
    Time = ds.createVariable(dim_name_time, "d", (dim_name_time,))
    Time[:] = pd.DatetimeIndex([time_file])
    for var_name in var_list:
        Data = ds.createVariable(var_name, "d", dims_order, zlib=True)
        Data[:] = file_dset[var_name].values

    if clean_temp:
        os.remove(fileout)

