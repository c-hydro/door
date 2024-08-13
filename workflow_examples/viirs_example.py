from door.data_sources import VIIRSDownloader

from door.utils.space import BoundingBox
from door.utils import log
from door.tools.timestepping import TimeRange

HOME = '/home/luca/Documents/CIMA_code/tests/VIIRS_dwl'

time_range = TimeRange(start='2020-01-01', end='2020-01-31')
space_ref  = BoundingBox(6.6, 42.6, 14.2, 46.4, datum = 'EPSG:4326')

test_downloader = VIIRSDownloader('snow', satellite='JPSS1')
#print(test_downloader.get_last_published_date())
test_downloader.get_data(time_range, space_ref,
                         destination={'path' : HOME +'/snow_test/%Y/%m/%d',
                                      'filename' : 'VIIRS-JPSS1_{variable}_%Y%m%d.tif',
                                      'time_signature' : 'end'},
                         options={'variables': ['NDSI_Snow_Cover','Snow_QA', 'Snow_AlgQA'],  # we don't need the extra QC layer
                                  'make_mosaic': True,                                       # we don't want to make a mosaic, we keep the original tiles
                                  'crop_to_bounds': True,                                    # we don't want to crop the data to the grid file bounds
                                 })