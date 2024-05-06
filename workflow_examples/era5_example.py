from door.data_sources import ERA5Downloader

from door.utils.time import TimeRange
from door.utils.space import BoundingBox
from door.utils import log

#GRID_FILE = '/home/andrea/Workspace/pyCharm/door/workflow_examples/sample_grid_IT.tif'
HOME = '/home/luca/Documents/CIMA_code/tests/ERA5'
log_file = HOME+'/door-log.txt'

log.set_logging(log_file, 'INFO')

time_range = TimeRange(start='2024-04-01', end='2024-04-10 00:00:00')
space_ref  = BoundingBox(-5.35,2.28,5.79,14.89, projection = 'EPSG:4326')

test_downloader = ERA5Downloader('reanalysis-era5-land')
test_downloader.get_data(time_range, space_ref,
                         destination=HOME+'/ERA5/%Y/%m/%d/ERA5_{variable}_%Y%m%d.tif',
                         options={'variables': [f'volumetric_soil_water_layer_{i}' for i in [1,2,3,4]] +
                                               ['total_precipitation', '2m_temperature'],
                                  'output_format': 'GeoTIFF',
                                  'aggregate_in_time': 'mean',
                                  'timesteps_per_year': 36
                                  })