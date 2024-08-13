from door.data_sources import ERA5Downloader
from door.utils.space import BoundingBox
from door.tools.timestepping import TimeRange

import os

GRID_FILE = '/home/luca/Documents/CIMA_code/door/workflow_examples/sample_grid_IT.tif'
HOME = '/home/luca/Documents/CIMA_code/tests/ERA5'

time_range = TimeRange(start='1992-2-1', end='2024-07-31')
space_ref  = BoundingBox.from_file(GRID_FILE)

test_downloader = ERA5Downloader('reanalysis-era5-land')
test_downloader.get_data(time_range, space_ref,
                         destination={'path': os.path.join(HOME, 'ITA_HCWI', 'data', '%Y/%m/%d'),
                                      'filename': 'ERA5_{agg_method}{variable}_%Y%m%d.tif'},
                         options={'variables'  : ['2m_temperature'],
                                  'agg_method' : [['max', 'min']],
                                  'ts_per_year': 12,
                                  'ts_per_year_agg': 365
                                  })