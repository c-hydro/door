from door.data_sources import VIIRSDownloader

from door.utils.time import TimeRange
from door.utils.space import BoundingBox
from door.utils import log

HOME = '/home/luca/Documents/CIMA_code/tests/VIIRS_dwl'
log_file = HOME+'/log.txt'

log.set_logging(log_file)

time_range = TimeRange(start='2017-02-02', end='2017-02-02')
space_ref  = BoundingBox(-18, -9, 18, 9, projection = 'EPSG:4326')

test_downloader = VIIRSDownloader('FAPAR')
test_downloader.get_data(time_range, space_ref,
                         destination=HOME+'/global_raw_/%Y/%m/%d/VIIRS-{layer}_wld-tile{tile}_%Y%m%d.tif',
                         options={'layers': [0,2],         # we don't need the extra QC layer
                                  'make_mosaic': False,    # we don't want to make a mosaic, we keep the original tiles
                                  'crop_to_bounds': False, # we don't want to crop the data to the grid file bounds
                                  })