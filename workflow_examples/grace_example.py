from door.data_sources import GRACEDownloader

from door.utils.space import BoundingBox
from door.utils import log
from door.tools.timestepping import TimeRange

HOME = '/home/luca/Documents/CIMA_code/tests/GRACE_dwl'
log_file = HOME+'/log.txt'

log.set_logging(log_file)

time_range = TimeRange(start='2017-06-01', end='2018-06-30')
space_ref  = BoundingBox(6.6, 42.6, 14.2, 46.4, datum = 'EPSG:4326')

test_downloader = GRACEDownloader('TWS')
print(test_downloader.get_last_published_date())
test_downloader.get_data(time_range, space_ref,
                         destination=HOME+'/%Y/%m/%d/GRACE-{variable}_%Y%m%d.tif',
                                  )