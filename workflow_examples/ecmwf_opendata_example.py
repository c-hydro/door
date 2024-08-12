from datetime import datetime, timedelta

from door.data_sources import ECMWFOpenDataDownloader
from door.utils.space import BoundingBox
from door.utils import log
from door.tools.timestepping import TimeRange

#GRID_FILE = '/home/andrea/Workspace/pyCharm/door/workflow_examples/sample_grid_IT.tif'
HOME = '/home/luca/Documents/CIMA_code/tests/ecmwf_dwl'
log_file = HOME+'/log.txt'

log.set_logging(log_file, 'DEBUG')

time_range = TimeRange(start = datetime.today().replace(hour=0, minute=0, second=0) - timedelta(1),
                       end   = datetime.today().replace(hour=3, minute=0, second=0) - timedelta(1))
space_ref  = BoundingBox(6, 19, 36, 48, datum = 'EPSG:4326')

test_downloader = ECMWFOpenDataDownloader(product='HRES')
test_downloader.get_data(time_range, space_ref,
                         destination=HOME+'/ecmwfFrc_%Y%m%d%H.nc',
                         options={'variables': ["10u", "10v"],
                                  'frc_max_step' : 8
                                  })