from door.data_sources import VIIRSDownloader

from door.utils.time import TimeRange
from door.utils.space import SpatialReference

import numpy as np

GRID_FILE = '/home/luca/Documents/CIMA_code/DOOR/workflow_examples/sample_grid_IT.tif'

time_range = TimeRange(start='2023-12-25', end='2024-01-03')
space_ref  = SpatialReference(grid_file=GRID_FILE, resampling_method='Bilinear', mask_values = np.nan)

test_downloader = VIIRSDownloader('FAPAR')
test_downloader.get_data(time_range, space_ref,
                         destination='/home/luca/Documents/CIMA_code/tests/VIIRS_dwl/VIIRS-{layer}_ITA_%Y%m%d.tif',
                         options={'get_prelim': True})

