from door.data_sources import VIIRSDownloader

from door.utils.time import TimeRange
from door.utils.space import BoundingBox
from door.utils import log

HOME = '/home/luca/Documents/CIMA_code/tests/VIIRS_dwl'
log_file = HOME+'/log.txt'

log.set_logging(log_file)

time_range = TimeRange(start='2017-01-01', end='2017-02-12')
space_ref  = BoundingBox(-5, -2, 5, 18, datum = 'EPSG:4326')

test_downloader = VIIRSDownloader('FAPAR', version = 2)
test_downloader.get_data(time_range, space_ref,
                         destination=HOME+'/%Y/%m/VIIRS-{layer}_tile{tile}_%Y%m%d.tif',
                         options={'layers': [0,2],              # we don't need the extra QC layer
                                  'make_mosaic': False,         # we don't want to make a mosaic, we keep the original tiles
                                  'crop_to_bounds': False,      # we don't want to crop the data to the grid file bounds
                                  'keep_tiles_naming': False,   # we don't want to keep the original tiles naming
                                  })