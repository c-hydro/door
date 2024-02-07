from door.data_sources import ECMWFOpenDataDownloader

from door.utils.time import TimeRange
from door.utils.space import BoundingBox
from door.utils import log

#GRID_FILE = '/home/andrea/Workspace/pyCharm/door/workflow_examples/sample_grid_IT.tif'
HOME = '/home/luca/Documents/CIMA_code/tests/ecmwf_dwl'
log_file = HOME+'/log.txt'

log.set_logging(log_file, 'INFO')

time_range = TimeRange(start='2024-02-06 00:00:00', end='2024-02-06 03:00:00')
space_ref  = BoundingBox(6, 19, 36, 48, projection = 'EPSG:4326')

test_downloader = ECMWFOpenDataDownloader(product='HRES')
test_downloader.get_data(time_range, space_ref,
                         destination=HOME+'/ecmwfFrc_%Y%m%d%H.nc',
                         options={'variables': ["10u", "10v"],
                                  'frc_max_step' : 8
                                  })