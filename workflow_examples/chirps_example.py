from door.data_sources import CHIRPSDownloader

from door.utils.time import TimeRange
from door.utils.space import BoundingBox

import numpy as np

GRID_FILE = '/home/luca/Documents/CIMA_code/DOOR/workflow_examples/sample_grid_IT.tif'

time_range = TimeRange(start='2023-12-25', end='2024-01-03')
space_ref  = BoundingBox.from_file(GRID_FILE)

test_downloader = CHIRPSDownloader(product='CHIRPSp25-daily')
test_downloader.get_data(time_range, space_ref,
                         destination='/home/luca/Documents/CIMA_code/tests/CHIRPS_dwl/daily_rain_ITA_%Y%m%d.tif',
                         options={'get_prelim': True})

