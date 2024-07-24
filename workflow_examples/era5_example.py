from door.data_sources import ERA5Downloader

from door.utils.space import BoundingBox

from door.tools.timestepping import TimeRange

#GRID_FILE = '/home/andrea/Workspace/pyCharm/door/workflow_examples/sample_grid_IT.tif'
HOME = '/home/luca/Documents/CIMA_code/tests/ERA5'

time_range = TimeRange(start='2024-04-01', end='2024-04-10')
space_ref  = BoundingBox(-5.35,2.28,5.79,14.89, datum = 'EPSG:4326')

test_downloader = ERA5Downloader('reanalysis-era5-land')
test_downloader.get_data(time_range, space_ref,
                         destination={'path': HOME,
                                      'filename': 'ERA5_{agg_method}{variable}_%Y%m%d.tif',
                                      'thumbnail': {'colors':'/home/luca/Desktop/{variable}.txt',
                                                    'overlay':'/home/luca/Documents/viz/countries/countries.shp'}},
                         options={'variables'  : ['total_precipitation', '2m_temperature'],
                                  'agg_method' : ['sum',                 ['mean', 'max', 'min']],
                                  'ts_per_year': 36
                                  })