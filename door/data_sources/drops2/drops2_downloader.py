import pandas as pd
import numpy as np
from typing import Optional
from drops2.utils import DropsCredentials
from drops2 import sensors
from drops2.utils import DropsException
from time import sleep

from ...base_downloaders import DOORDownloader
from ...utils.space import BoundingBox
from ...utils.auth import get_credentials_from_netrc
from ...utils.csv import save_csv
from ...utils.parse import format_string

from ...tools.timestepping import TimeRange

class DROPS2Downloader(DOORDownloader):
    
    name = "DROPS2_Downloader"

    default_options = {'group': 'Dewetra%Default',
                        'invalid_flags': [-9998, -9999],
                        'ntry': 20,
                        'sec_sleep': 5,
                       'aggregation_seconds': 3600,
                       'host': 'na',
                       'sensor_class': 'TERMOMETRO',
                       'timestep': 'H',
                       'name_columns': ['station_id', 'station_name', 'lat', 'lon', 'data']}

    credential_env_vars = {'username': 'DROPS2_LOGIN', 'password': 'DROSP2_PWD'}

    spin_up_drops = 2 # hours

    def __init__(self):
        super().__init__()
            
    def get_data(self,
                 time_range: TimeRange,
                 space_bounds: BoundingBox,
                 destination: str,
                 options: Optional[dict] = None) -> None:

        # Check options
        options = self.check_options(options)

        self.log.info(f'------------------------------------------')
        self.log.info(f'Starting download of DROPS2 data')
        self.log.info(f'Data requested between {time_range.start:%Y-%m-%d} and {time_range.end:%Y-%m-%d}')
        self.log.info(f'Bounding box: {space_bounds.bbox}')
        self.log.info(f'------------------------------------------')

        # Get credentials
        credentials = get_credentials_from_netrc(url=options['host'])
        user, password = credentials.split(':')
        DropsCredentials.set(options['host'], user, password)
        self.log.info(f'------------------------------------------')
        self.log.info(f'Credentials set for {options["host"]} -- LOGGED')
        self.log.info(f'------------------------------------------')

        # Compute time range
        times = pd.date_range(start=time_range.start, end=time_range.end, freq=options['timestep'])
        self.log.info(f'------------------------------------------')
        self.log.info(f'Number of time steps to download: {len(times)}')
        self.log.info(f'------------------------------------------')

        # Download the data for the specified times & save as csv
        self.log.info(f'------------------------------------------')
        for time_i, time_date in enumerate(times):

            self.log.info(f'Downloading data for {time_date:%Y-%m-%d %H:%M:%S}')

            sensors_list = None
            ntry = options['ntry']

            # While loop to try to download the data
            while ntry > 0:
                try:
                    ntry -= 1
                    # get sensor list
                    sensors_list = sensors.get_sensor_list(options['sensor_class'], geo_win=(space_bounds.bbox[0],
                                                                                             space_bounds.bbox[1],
                                                                                             space_bounds.bbox[2],
                                                                                             space_bounds.bbox[3]),
                                                           group=options['group'])

                    if len(sensors_list.list) == 0:
                        self.log.warning(' ---> No available station for this time step!')
                        df_dati = pd.DataFrame()
                        dfStations = pd.DataFrame()
                        break

                    else:
                        # create df with station metadata
                        dfStations = pd.DataFrame(np.array([(p.name, p.lat, p.lng) for p in sensors_list]),
                                              index=np.array([(p.id) for p in sensors_list]),
                                              columns=options['name_columns'][1:4])
                        dfStations.index.name = options['name_columns'][0]

                        # get data
                        date_from = time_date - pd.Timedelta(hours=self.spin_up_drops)
                        date_from_str = date_from.strftime("%Y%m%d%H%M")
                        date_to_str = time_date.strftime("%Y%m%d%H%M")
                        df_dati = sensors.get_sensor_data(options['sensor_class'], sensors_list,
                                                      date_from_str, date_to_str, aggr_time=options['aggregation_seconds'],
                                                      as_pandas=True)
                        self.log.info('Successfully downloaded data')
                        break

                except DropsException:

                    self.log.warning(
                        ' ---> Problems with downloading Drops2 data, retrying in ' + str(options['sec_sleep']) + 'seconds')

                    if ntry >= 0:
                        sleep(options['sec_sleep'])
                        self.log.warning(
                            ' ---> ... ')
                    else:
                        self.log.error(' Problem with extraction from drops2!')
                        raise

            # stop procedure if no data is available (it means that something is going wrong with the download or the connection)
            if sensors_list is None:
                self.log.error(' Problem with extraction from drops2! Sensors list is empty!')
                raise

            number_stat_initial = sensors_list.list.__len__()
            self.log.info(f'Number of stations initially available: {len(sensors_list.list)}')

            # For cautionary reasons, we asked drops2 more hours of data than what is actually needed.
            # So here we extract the row we need...
            df_dati = df_dati.loc[df_dati.index == date_to_str]

            # We remove NaNs and invalid points
            self.log.info(' Checking for empty or not-valid series')
            for i_invalid, value_invalid in enumerate(options['invalid_flags']):
                df_dati.values[df_dati.values == value_invalid] = np.nan
            df_dati = df_dati.dropna(axis='columns', how='all')
            dfStations = dfStations.loc[list(df_dati.columns)]

            number_stat_end = dfStations.shape[0]
            number_removed = number_stat_initial - number_stat_end
            self.log.info(' Removed ' + str(number_removed) + ' stations')
            self.log.info(' Number of available stations is ' + str(number_stat_end))

            # add a column to dfStations using df_dati
            dfStations = dfStations.join(df_dati.T)
            dfStations.columns.values[3] = options['name_columns'][4]

            # Save dfStations
            out_name = format_string(destination, {'variable': options['sensor_class']})
            out_name = time_date.strftime(out_name)

            if not dfStations.empty:
                save_csv(dfStations, out_name)
                self.log.info(f'Saved data for {time_date:%Y-%m-%d %H:%M:%S} to ' + out_name)

        self.log.info(f'------------------------------------------')
        self.log.info(f'Download of DROPS2 data completed')
        self.log.info(f'------------------------------------------')