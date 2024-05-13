from door.data_sources import GRACEDownloader

from door.utils.time import TimeRange
from door.utils.space import BoundingBox
from door.utils import log

HOME = '/home/luca/Documents/CIMA_code/tests/GRACE_dwl'
log_file = HOME+'/log.txt'

log.set_logging(log_file)

time_range = TimeRange(start='2017-06-01', end='2018-06-30')
space_ref  = BoundingBox(-18, -9, 18, 9, datum = 'EPSG:4326')

test_downloader = GRACEDownloader('TWS')
test_downloader.get_data(time_range, space_ref,
                         destination=HOME+'/%Y/%m/%d/GRACE-{layer}_%Y%m%d.tif',
                        # options={'layers': [0,1],
                        #         }
                                  )