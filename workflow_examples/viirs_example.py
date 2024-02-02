from door.data_sources import VIIRSDownloader

from door.utils.time import TimeRange
from door.utils.space import BoundingBox

import numpy as np

GRID_FILE = '/home/luca/Documents/CIMA_code/DOOR/workflow_examples/sample_grid_IT.tif'

time_range = TimeRange(start='2017-02-02', end='2017-02-02')
space_ref  = BoundingBox(grid_file=GRID_FILE)

test_downloader = VIIRSDownloader('FAPAR')
test_downloader.get_data(time_range, space_ref,
                         destination='/home/luca/Documents/CIMA_code/tests/VIIRS_dwl/VIIRS-{layer}_ITA_%Y%m%d.tif',
                         options={'layers': [0,2]}) # we don't need the extra QC layer