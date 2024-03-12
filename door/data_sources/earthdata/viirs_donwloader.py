from datetime import datetime

import h5py

from .cmr_downloader import CMRDownloader


class VIIRSDownloader(CMRDownloader):

    source = 'VIIRS'

    # list of available variables -> those not implementeed/tested yet are commented out
    # for each variable the list indicates
    # [provider, product_name, version, timesteps, (timesteps is 'viirs' for 8-day data following the VIIRS/MODIS calendar and 'annual' for annual data)
    #  layers = [layer_id, name, valid_range, scale_factor]
    # ]
    available_variables = {
        'fapar': ['LPDAAC_ECS', 'VNP15A2H', '001', 'viirs',
                  [[0, 'FAPAR',         (0,100), 0.01, 'cont'],
                   [2, 'FAPAR_QC',      (0,254), 1   , '8bit'],
                   [3, 'FAPAR_ExtraQC', (0,254), 1   , '8bit']]],
        'phenology': ['LPDAAC_ECS', 'VNP22Q2', '001', 'annual',
                      [[6,  'GLSP_QC1',      (0,254),   1, '8bit'],
                       [10, 'GLSP_GSStart1', (1,32766), 1, 'cat'],
                       [12, 'GLSP_GSEnd1',   (1,32766), 1, 'cat'],
                       [25, 'GLSP_QC2',      (0,254),   1, '8bit'],
                       [29, 'GLSP_GSStart2', (1,32766), 1, 'cat'],
                       [31, 'GLSP_GSEnd2',   (1,32766), 1, 'cat'],]],
        'snow': ['NSIDCDAAC_ECS', 'VNP10A1', '001', 'daily',
                 [[0, 'Snow_AlgQA',         (0,254), 1, '8bit'],
                  [1, 'Snow_QA',            (0,254), 1, '8bit'],
                  [3, 'NDSI_Snow_Cover',    (0,254), 1, '8bit']]],
    }

    # source: http://spatialreference.org/ref/sr-org/modis-sinusoidal/
    projection = 'PROJCS["unnamed",\
                  GEOGCS["Unknown datum based upon the custom spheroid", \
                  DATUM["Not specified (based on custom spheroid)", \
                  SPHEROID["Custom spheroid",6371007.181,0]], \
                  PRIMEM["Greenwich",0],\
                  UNIT["degree",0.0174532925199433]],\
                  PROJECTION["Sinusoidal"], \
                  PARAMETER["longitude_of_center",0], \
                  PARAMETER["false_easting",0], \
                  PARAMETER["false_northing",0], \
                  UNIT["Meter",1]]'

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
        elif varopts['timesteps'] == 'daily':
            self.timesteps_doy = list(range(1,367))

        self.layers = varopts['layers']
    
    # this is specific to VIIRS, different from MODIS!
    def get_geotransform(self, filename):
        """
        Get geotransform from dataset.
        """
        # get the filename of the metadata containing the geolocation
        metadata_filename = filename.split('"')[1]
        with h5py.File(metadata_filename, 'r') as f:
            metadata = f['HDFEOS INFORMATION']['StructMetadata.0'][()].split()
            metadata_list = [s.decode('utf-8').split('=') for s in metadata]

            # get the coordinates of the upper left and lower right corners
            self.ulx, self.uly = eval([s[1] for s in metadata_list if s[0] == 'UpperLeftPointMtrs'][0])
            self.lrx, self.lry = eval([s[1] for s in metadata_list if s[0] == 'LowerRightMtrs'][0])

            # get the size of the dataset (in pixels)
              # these lines are super hacky, but they work
            self.xsize = eval([s[1] for s in metadata_list if s[0] == 'XDim'][0])
            self.ysize = eval([s[1] for s in metadata_list if s[0] == 'YDim'][0])

        geotransform = (self.ulx, (self.lrx - self.ulx) / self.xsize, 0,
                                self.uly, 0, (self.lry - self.uly) / self.ysize)
        return geotransform

    def get_start(self):
        start = datetime(2012, 1, 17)
        if self.timesteps == 'viirs':
            return start
        elif self.timesteps == 'annual':
            return datetime(start.year+1, 1, 1)
        return