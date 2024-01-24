from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
from typing import Optional

from .cmr_downloader import CMRDownloader
from ...utils.time import TimeRange
from ...utils.space import SpatialReference


class VIIRSDownloader(CMRDownloader):

    # list of available variables -> those not implementeed/tested yet are commented out
    # for each variable the list indicates
    # [provider, product_name, version, timesteps, (timesteps is 'viirs' for 8-day data following the VIIRS/MODIS calendar and 'annual' for annual data)
    #  layers = [layer_id, name, valid_range, scale_factor]
    # ]
    available_variables = {
        'fapar': ['LPDAAC_ECS', 'VNP15A2H', '001', 'viirs',
                  [[1, 'FAPAR',         (0,100), 0.01],
                   [2, 'FAPAR_QC',      (0,254), 1   ],
                   [3, 'FAPAR_ExtraQC', (0,254), 1   ]]],
        'phenology': ['LPDAAC_ECS', 'VNP22Q2', '001', 'annual',
                      [[6,  'GLSP_QC1',      (0,254),   1],
                       [10, 'GLSP_GSStart1', (1,32766), 1],
                       [12, 'GLSP_GSEnd1',   (1,32766), 1],
                       [25, 'GLSP_QC2',      (0,254),   1],
                       [29, 'GLSP_GSStart2', (1,32766), 1],
                       [31, 'GLSP_GSEnd2',   (1,32766), 1]]]
    }

    def __init__(self, variable) -> None:
        """
        Initializes the CMRDownloader class.
        Available (tested) variables are:
        - FAPAR: 'fapar' (VNP15A2H, 8-day)
        - Phenology: 'phenology' (VNP22Q2, annual)
        """
        super().__init__()

        # set the variable
        self.variable = variable

    @property
    def variable(self):
        return self._variable
    
    @variable.setter
    def variable(self, variable: str):
        available_list = self.get_available_variables().keys()
        # check if the variable is available
        if variable.lower() not in available_list:
            msg = f'Variable {variable} is not available. Available variables are: '
            msg += ', '.join(available_list)
            raise ValueError(msg)
        
        # set the variable
        self._variable = variable.lower()

        # add the variable-specific parameters
        varopts = self.get_available_variables()[self._variable]
        self.provider  = varopts['provider']
        self.product   = varopts['product']
        self.version   = varopts['version']
        if varopts['timesteps'] == 'viirs':
            self.timesteps = 'viirs'
            self.timesteps_doy = list(range(1, 366, 8))
        elif varopts['timesteps'] == 'annual':
            self.timesteps = 'annual'
            self.timesteps_doy = [1]
        self.layers = varopts['layers']
    
    def get_data(self,
                 time_range: TimeRange,
                 space_ref: SpatialReference,
                 destination: str,
                 options: Optional[dict] = None) -> None:
        """
        Get VIIRS data from the CMR.
        """
        timesteps = time_range.get_timesteps_from_DOY(self.timesteps_doy)
        with TemporaryDirectory() as tmpdir:
            for time in timesteps:
                # get the data from the CMR
                url_list = self.cmr_search(time, space_ref.bbox)
            
                file_list = self.download(url_list, tmpdir)
                for layer in self.layers:
                    mosaic = self.build_mosaic_from_hdf5(file_list, layer['id'])
                breakpoint()
    
    def get_start(self):
        start = datetime(2012, 1, 17)
        if self.timesteps == 'viirs':
            return start
        elif self.timesteps == 'annual':
            return datetime(start.year+1, 1, 1)
        return