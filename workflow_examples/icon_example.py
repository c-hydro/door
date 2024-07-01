from door.data_sources import ICONDownloader

from door.tools.timestepping import TimeRange
from door.utils.space import BoundingBox
from door.utils import log

import datetime as dt

#GRID_FILE = '/home/andrea/Workspace/pyCharm/door/workflow_examples/sample_grid_IT.tif'
HOME = '/home/luca/Documents/CIMA_code/tests/icon_dwl'
log_file = HOME+'/log.txt'

log.set_logging(log_file, 'DEBUG')

today = dt.datetime.now()
time_range = TimeRange(start=f'{today:%Y-%m-%d 00:00:00}', end=f'{today:%Y-%m-%d 03:00:00}')
space_ref  = BoundingBox(6, 19, 36, 48, datum = 'EPSG:4326')

test_downloader = ICONDownloader(product='ICON0p125')
test_downloader.get_data(time_range, space_ref,
                         destination=HOME + '/_iconFrc_%Y%m%d%H.nc',
                         options={'variables': ["tot_prec", "t_2m"],
                                  'frc_max_step' : 3,
                                  'cdo_path' : '/usr/bin/'})