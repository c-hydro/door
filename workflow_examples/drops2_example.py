from door.data_sources import DROPS2Downloader

from door.tools.timestepping import TimeRange
from door.utils.space import BoundingBox
from door.utils import log


HOME = '/home/'
log_file = HOME+'/door-log.txt'

log.set_logging(log_file, 'INFO')

time_range = TimeRange(start='2024-04-01', end='2024-04-02 00:00:00')
space_ref  = BoundingBox(6,36,18,46, datum = 'EPSG:4326')

test_downloader = DROPS2Downloader()
test_downloader.get_data(time_range, space_ref,
                         destination=HOME+'/%Y/%m/%d/DROPS2_{variable}_%Y%m%d%H%M.csv',
                         options={'sensor_class': 'TERMOMETRO',
                        'aggregation_seconds': 3600,
                        'timestep': 'H',
                        'group': 'Dewetra%Default',
                        'invalid_flags': [-9998, -9999],
                        'ntry': 20,
                        'sec_sleep': 5,
                        'host': "NAN"})