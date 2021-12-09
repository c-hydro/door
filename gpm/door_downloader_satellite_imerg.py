#!/usr/bin/python3

"""
door Tool - SATELLITE IMERG

__date__ = '20211203'
__version__ = '1.0.0'
__author__ =
        'Andrea Libertino (andrea.libertino@cimafoundation.org',
__library__ = 'HyDE'

General command line:
python3 hyde_downloader_satellite_gsmap_nowcasting.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20200313 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import pandas as pd
import os
import requests
import shutil
import numpy as np


def main():
    time_start = pd.datetime(2021,12,8,0,0)
    time_end = pd.datetime(2021,12,9,17,0)
    ancillary_path = '/home/andrea/Desktop/temp_imerg/'
    outcome_path = '/home/andrea/Desktop/outcome_imerg/'
    flag_final = True
    flag_late = True

    os.makedirs(ancillary_path, exist_ok=True)
    os.makedirs(outcome_path, exist_ok=True)

    time_range = pd.date_range(time_start, time_end, freq='D')

    if flag_final:

        list_not_found = []
        for time_now in time_range:
            print(time_now.strftime("%Y-%m-%d"))
            urls = ['https://arthurhouhttps.pps.eosdis.nasa.gov/gpmdata/' + time_now.strftime("%Y/%m/%d")  + '/gis/3B-HHR-GIS.MS.MRG.3IMERG.' + time_now.strftime("%Y%m%d")  + '-S' + (time_now + pd.Timedelta(str(i) + " min")).strftime("%H%M%S") +  '-E' + (time_now + pd.Timedelta(str(i) + " min") + pd.Timedelta("+ 29 min + 59 sec")).strftime("%H%M%S") +  '.' + str(i).zfill(4) + '.V06B.tif' for i in np.arange(0,1440,30)]
            for url in urls:
                local_filename = outcome_path + url.split('/')[-1]
                with requests.get(url, auth=('andrea.libertino@cimafoundation.org', 'andrea.libertino@cimafoundation.org')) as r:
                    if r.status_code == 404:
                        print("File not found... SKIP")
                        list_not_found = list_not_found + [time_now]
                    else:
                        with open(local_filename, 'wb') as f:
                            f.write(r.content)
    else:
        list_not_found = time_range

    if flag_late:
        print('Searching in Late runs')
        list_not_found_late = []
        for time_now in list_not_found:
            print(time_now.strftime("%Y-%m-%d"))
            urls = ['https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/' + time_now.strftime("%Y/%m/")  + '/3B-HHR-L.MS.MRG.3IMERG.' + time_now.strftime("%Y%m%d")  + '-S' + (time_now + pd.Timedelta(str(i) + " min")).strftime("%H%M%S") +  '-E' + (time_now + pd.Timedelta(str(i) + " min") + pd.Timedelta("+ 29 min + 59 sec")).strftime("%H%M%S") +  '.' + str(i).zfill(4) + '.V06B.30min.tif' for i in np.arange(0,1440,30)]

            for url in urls:
                local_filename = outcome_path + url.split('/')[-1]
                with requests.get(url, auth=('andrea.libertino@cimafoundation.org', 'andrea.libertino@cimafoundation.org')) as r:
                    if r.status_code == 404:
                        print("File not found")
                        list_not_found_late = list_not_found_late + [url.split('/')[-1]]
                    else:
                        with open(local_filename, 'wb') as f:
                            f.write(r.content)
    else:
        list_not_found_late = list_not_found

    if flag_early:
        print('Searching in Early runs')
        list_not_found_early = []
        for time_now in list_not_found:
            urls = ['https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/' + time_now.strftime("%Y/%m/")  + '/3B-HHR-E.MS.MRG.3IMERG.' + time_now.strftime("%Y%m%d")  + '-S' + (time_now + pd.Timedelta(str(i) + " min")).strftime("%H%M%S") +  '-E' + (time_now + pd.Timedelta(str(i) + " min") + pd.Timedelta("+ 29 min + 59 sec")).strftime("%H%M%S") +  '.' + str(i).zfill(4) + '.V06B.30min.tif' for i in np.arange(0,1440,30)]
            for url in urls:
                local_filename = outcome_path + url.split('/')[-1]
                with requests.get(url, auth=('andrea.libertino@cimafoundation.org', 'andrea.libertino@cimafoundation.org')) as r:
                    if r.status_code == 404:
                        print("File not found")
                        list_not_found_early = list_not_found_early + [url.split('/')[-1]]
                    else:
                        with open(local_filename, 'wb') as f:
                            f.write(r.content)


    print('ciao')

# ----------------------------------------------------------------------------
# Call script from external library
if __name__ == "__main__":
    main()
# ----------------------------------------------------------------------------