import pandas as pd
import numpy as np
from typing import Optional, Generator, Sequence
from time import sleep
import datetime as dt

from ...base_downloaders import DOORDownloader
from ...utils.auth import get_credentials

from d3tools.spatial import BoundingBox
from d3tools.timestepping import TimeRange
from d3tools.timestepping.timestep import TimeStep
from d3tools.data import Dataset

class DROPS2Downloader(DOORDownloader):
    
    source = "DROPS2"
    name = "DROPS2_Downloader"

    default_options = {'group': 'Dewetra%Default',
                       'invalid_flags': [-9998, -9999],
                       'ntry': 20,
                       'sec_sleep': 5,
                       'aggregation_seconds': 3600,
                       'host': 'na',
                       'sensor_class': 'TERMOMETRO',
                       'frequency': 'H',
                       'name_columns': ['station_id', 'station_name', 'lat', 'lon', 'data']}

    credential_env_vars = {'username': 'DROPS2_LOGIN', 'password': 'DROSP2_PWD'}

    spin_up_drops = 2 # hours
    
    def authenticate(self, host: str) -> None:
        from drops2.utils import DropsCredentials

        credentials = get_credentials(env_variables=self.credential_env_vars, url=host, encode=False)
        user, password = credentials.split(':')
        DropsCredentials.set(host, user, password)

    def get_data(self,
                 time_range: TimeRange|Sequence[dt.datetime],
                 space_bounds:  Optional[BoundingBox] = None,
                 destination: Optional[Dataset|dict|str] = None,
                 options:  Optional[dict] = None) -> None:

        # get options and check them against the default options
        if options is not None: 
            self.set_options(options)

        # handle the credentials
        # I (LT), would rather do this in the __init__, why is the host in the options? isn't it always the same? is the host a secret?
        self.authenticate(self.host)

        # then use the super method to get the data
        super().get_data(time_range, space_bounds, destination)
    
    def _get_data_ts(self, time_range: TimeStep,
                           space_bounds: BoundingBox,
                           tmp_path: str) -> Generator[tuple[pd.DataFrame, dict], None, None]:

        from drops2 import sensors
        from drops2.utils import DropsException

        sensors_list = None
        ntry = self.ntry

        time_date = time_range.start
        # While loop to try to download the data
        while ntry > 0:
            try:
                ntry -= 1
                # get sensor list
                sensors_list = sensors.get_sensor_list(self.sensor_class, geo_win=(space_bounds.bbox),group=self.group)

                # stop procedure if no data is available (it means that something is going wrong with the download or the connection)
                if sensors_list is None:
                    raise DropsException('Sensor list is None')

                if len(sensors_list.list) == 0:
                    return None

                else:
                    # create df with station metadata
                    dfStations = pd.DataFrame(np.array([(p.name, p.lat, p.lng) for p in sensors_list]),
                                            index=np.array([(p.id) for p in sensors_list]),
                                            columns=self.name_columns[1:4])
                    dfStations.index.name = self.name_columns[0]

                    # get data
                    date_from = time_date - pd.Timedelta(hours=self.spin_up_drops)
                    date_from_str = date_from.strftime("%Y%m%d%H%M")
                    date_to_str = time_date.strftime("%Y%m%d%H%M")
                    df_dati = sensors.get_sensor_data(self.sensor_class, sensors_list,
                                                      date_from_str, date_to_str, aggr_time=self.aggregation_seconds,
                                                      as_pandas=True)
                    self.log.info('Successfully downloaded data')
                    break

            except DropsException as e:
                sleep(self.sec_sleep)
                self.log.warning(f'Problem with extraction from drops2 ({e}). Trying again...')
    
        else:
            self.log.error(' Problem with extraction from drops2!')
            raise

        number_stat_initial = len(sensors_list.list)
        #self.log.info(f'Number of stations initially available: {len(sensors_list.list)}')

        # For cautionary reasons, we asked drops2 more hours of data than what is actually needed.
        # So here we extract the row we need...
        df_dati = df_dati.loc[df_dati.index == date_to_str]

        # We remove NaNs and invalid points
        self.log.info(' Checking for empty or not-valid series')
        for i_invalid, value_invalid in enumerate(self.invalid_flags):
            df_dati.values[df_dati.values == value_invalid] = np.nan
        df_dati = df_dati.dropna(axis='columns', how='all')
        dfStations = dfStations.loc[list(df_dati.columns)]

        number_stat_end = dfStations.shape[0]
        number_removed = number_stat_initial - number_stat_end
        #self.log.info(' Removed ' + str(number_removed) + ' stations')
        #self.log.info(' Number of available stations is ' + str(number_stat_end))

        # add a column to dfStations using df_dati
        dfStations = dfStations.join(df_dati.T)
        dfStations.columns.values[3] = self.name_columns[4]

        yield dfStations, {'variable': self.sensor_class}