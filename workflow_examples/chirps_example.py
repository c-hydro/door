from door.data_sources import CHIRPSDownloader

from door.utils.time import TimeRange
from door.utils.space import BoundingBox
from door.utils import log

import numpy as np

HOME = '/home/luca/Documents/CIMA_code/tests/CHIRPS_dwl'
GRID_FILE = '/home/luca/Documents/CIMA_code/DOOR/workflow_examples/sample_grid_IT.tif'
log_file = HOME+'/log.txt'

log.set_logging(log_file)

time_range = TimeRange(start='2023-12-30', end='2024-01-03')
space_ref  = BoundingBox.from_file(GRID_FILE)

test_downloader = CHIRPSDownloader(product='CHIRPSp25-daily')
test_downloader.get_data(time_range, space_ref,
                         destination=HOME+'/daily_rain_ITA_%Y%m%d.tif',
                         options={'get_prelim': True})

