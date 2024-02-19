from door.data_sources import ERA5Downloader

from door.utils.time import TimeRange
from door.utils.space import BoundingBox
from door.utils import log

#GRID_FILE = '/home/andrea/Workspace/pyCharm/door/workflow_examples/sample_grid_IT.tif'
HOME = '/home/luca/Documents/CIMA_code/tests/era5_dwl'
log_file = HOME+'/log.txt'

log.set_logging(log_file, 'DEBUG')

time_range = TimeRange(start='2024-01-04', end='2024-01-25 03:00:00')
space_ref  = BoundingBox(-180,-90,180,90, projection = 'EPSG:4326')

test_downloader = ERA5Downloader('reanalysis-era5-single-levels')
test_downloader.get_data(time_range, space_ref,
                         destination=HOME+'/%Y/ERA5_rain_%Y%m%d.tif',
                         options={'variables': ['total_precipitation'],
                                  'output_format': 'GeoTIFF',
                                  'aggregate_in_time': 'sum',
                                  'timesteps_per_year': 36,
                                  })

# # rigridding of ERA5 data
# test_file = '/home/luca/Documents/CIMA_code/tests/era5_dwl/ecmwftest_20230206.grib'

# import xarray as xr
# data = xr.open_dataset(test_file, engine='cfgrib')
# data.latitude