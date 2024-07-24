from door.data_sources import CHIRPSDownloader

from door.utils.space import BoundingBox
from door.utils import log
import door.tools.timestepping as ts

HOME = '/home/luca/Documents/CIMA_code/tests/CHIRPS_dwl'
GRID_FILE = '/home/luca/Documents/CIMA_code/DOOR/workflow_examples/sample_grid_IT.tif'
log_file = HOME+'/log.txt'

log.set_logging(log_file)

time_range = ts.TimeRange(start='2024-06-21', end='2024-07-10')
space_ref  = BoundingBox(-180,-50,180,50)

test_downloader = CHIRPSDownloader(product='CHIRPSp05-dekads')
test_downloader.get_data(time_range, space_ref,
                         destination={'path' : HOME, 'filename':'CHIRPS-precip_10d_%Y%m%d.tif',
                                      'thumbnail': {'colors':'/home/luca/Desktop/chirps_10day.txt',
                                                    'overlay':'/home/luca/Documents/viz/countries/countries.shp'}},
                         options={'get_prelim': False})