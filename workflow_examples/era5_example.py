from door.data_sources import CDSDownloader

#from door.utils.time import TimeRange
#from door.utils.space import BoundingBox
from door.utils import log

#GRID_FILE = '/home/andrea/Workspace/pyCharm/door/workflow_examples/sample_grid_IT.tif'
HOME = '/home/luca/Documents/CIMA_code/tests/era5_dwl'
log_file = HOME+'/log.txt'

log.set_logging(log_file, 'DEBUG')

test_cds = CDSDownloader('reanalysis-era5-single-levels')

request = {
        'product_type': 'reanalysis',
        'format': 'grib',
        'variable': 'total_precipitation',
        'year': '2023',
        'month': '02',
        'day': '02',
        'time': '00:00'
    }
destination = HOME+'/ecmwftest_20230206.grib'
test_cds.download(request, destination)