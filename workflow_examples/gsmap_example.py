from door.data_sources import GSMAPDownloader
import pandas as pd
import datetime as dt
import os

from door.utils.space import BoundingBox
from door.utils import log
import door.tools.timestepping as ts

HOME = '/home/andrea/Desktop/Working_dir/door/gsmap'
log_file = os.path.join(HOME, 'log.txt')

log.set_logging(log_file, console    = True)

time_range = ts.TimeRange(start='2022-05-23', end='2022-12-31')
space_ref  = BoundingBox(-180,-60, 180,60)

test_downloader = GSMAPDownloader(product="gsmap-gauge")

test_downloader.get_data(time_range, space_ref,
                         destination={'path' : os.path.join(HOME, "%Y/%m/%d"), 'filename':'BFA_gsmap-gauge_%Y%m%d%H.tif'})