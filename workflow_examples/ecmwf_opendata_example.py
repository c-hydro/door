from door.data_sources import ECMWFOpenDataDownloader

from door.utils.time import TimeRange
from door.utils.space import BoundingBox

import numpy as np

GRID_FILE = '/home/andrea/Workspace/pyCharm/door/workflow_examples/sample_grid_IT.tif'

time_range = TimeRange(start='2024-02-06 00:00:00', end='2024-02-06 03:00:00')
space_ref  = BoundingBox.from_file(GRID_FILE)

test_downloader = ECMWFOpenDataDownloader(product='HRES')
test_downloader.get_data(time_range, space_ref,
                         destination='/home/andrea/Desktop/Working_dir/ecmwf_new/ecmwfFrc_%Y%m%d%H.nc',
                         options={'variables': ["10u", "10v"],
                                  'frc_max_step' : 8
                                  })