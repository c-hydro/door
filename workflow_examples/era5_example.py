from door.data_sources import ERA5Downloader

from door.utils.time import TimeRange
from door.utils.space import BoundingBox
from door.utils import log

#GRID_FILE = '/home/andrea/Workspace/pyCharm/door/workflow_examples/sample_grid_IT.tif'
HOME = '/home/luca/Documents/CIMA_code/EDO-GDO/SPI/ERA5/output'
log_file = HOME+'/door-log.txt'

log.set_logging(log_file, 'INFO')

time_range = TimeRange(start='2024-01-01', end='2024-02-19 00:00:00')
space_ref  = BoundingBox(-180,-90,180,90, projection = 'EPSG:4326')

test_downloader = ERA5Downloader('reanalysis-era5-single-levels')
test_downloader.get_data(time_range, space_ref,
                         destination=HOME+'/ERA5_dekads/%Y/%m/%d/ERA5_precipitation_%Y%m%d.tif',
                         options={'variables': ['total_precipitation'],
                                  'output_format': 'GeoTIFF',
                                  'aggregate_in_time': 'sum',
                                  'timesteps_per_year': 36,
                                  'n_processes': 4,
                                  })