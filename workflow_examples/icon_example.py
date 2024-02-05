from door.data_sources import ICONDownloader

from door.utils.time import TimeRange
from door.utils.space import BoundingBox

import numpy as np

GRID_FILE = '/home/andrea/Workspace/pyCharm/door/workflow_examples/sample_grid_IT.tif'

time_range = TimeRange(start='2024-02-01 00:00:00', end='2024-02-01 03:00:00')
space_ref  = BoundingBox.from_file(GRID_FILE)

test_downloader = ICONDownloader(product='ICON0p125')
test_downloader.get_data(time_range, space_ref,
                         destination='/home/andrea/Desktop/Working_dir/icon_new/iconFrc_%Y%m%d%H.nc',
                         options={'variables': {"tp": "tot_prec",
                                                "temp": "t_2m"},
                                  'frc_max_step' : 3,
                                  'cdo_path' : '/home/andrea/FP/fp_libs_system_cdo/cdo-1.9.8_nc-4.6.0_hdf-1.8.17_eccodes-2.17.0/bin/'})