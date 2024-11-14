from door.data_sources import CLMSDownloader

from door.utils.space import BoundingBox
from door.utils import log
import door.tools.timestepping as ts

HOME = '/home/luca/Documents/CIMA_code/tests/CHIRPS_dwl'
GRID_FILE = '/home/luca/Documents/CIMA_code/DOOR/workflow_examples/sample_grid_IT.tif'
log_file = HOME+'/log.txt'

log.set_logging(log_file)

time_range = ts.TimeRange(start='2024-06-21', end='2024-07-10')
space_ref  = BoundingBox.from_file(GRID_FILE)

test_downloader = CLMSDownloader(product='SWI')
test_downloader.get_data(time_range, space_ref,
                         destination={'path' : HOME, 'filename':'SWI-{variable}_%Y%m%d.tif'},
                         options={
                             'variables': ["001","020","060"],
                             'crop_to_bounds': True,
                             'ts_per_year': 36
                         })