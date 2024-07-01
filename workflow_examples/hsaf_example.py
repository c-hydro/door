from door.data_sources import HSAFDownloader

from door.utils.space import BoundingBox
from door.utils import log
from door.tools.timestepping import TimeRange


HOME = '/home/luca/Documents/CIMA_code/tests/HSAF_dwl'
GRID_FILE = '/home/luca/Documents/CIMA_code/door/workflow_examples/sample_grid_IT.tif'
log_file = HOME+'/log.txt'

log.set_logging(log_file)

time_range = TimeRange(start='2016-01-01', end='2016-01-05')
space_ref  = BoundingBox(-10,-10,10,10, 'EPSG:4326')#.from_file(GRID_FILE)

test_downloader = HSAFDownloader(product="HSAF-h14")
test_downloader.get_data(time_range, space_ref,
                         destination=HOME+'/%Y/%m/%d/HSAF-h14_{variable}_%Y%m%d.tif',
                         options={'custom_variables': {'var028':[0,0.28]}})

