from datetime import datetime

import h5py
from osgeo import gdal
import os
import tempfile
from typing import Optional
import re

from .cmr_downloader import CMRDownloader
from ...utils.space import BoundingBox
from ...utils.geotiff import crop_raster, save_raster
from ...utils.time import TimeRange


import logging
logger = logging.getLogger(__name__)

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
                      [[5,  'GLSP_QC1',      (0,254),   1, '8bit'],
                       [9, 'GLSP_GSStart1', (1,32766), 1, 'cat'],
                       [11, 'GLSP_GSEnd1',   (1,32766), 1, 'cat'],
                       [24, 'GLSP_QC2',      (0,254),   1, '8bit'],
                       [28, 'GLSP_GSStart2', (1,32766), 1, 'cat'],
                       [30, 'GLSP_GSEnd2',   (1,32766), 1, 'cat'],]],
        'snow': ['NSIDCDAAC_ECS', 'VNP10A1', '001', 'daily',
                 [[0, 'Snow_AlgQA',         (0,254), 1, '8bit'],
                  [1, 'Snow_QA',            (0,254), 1, '8bit'],
                  [3, 'NDSI_Snow_Cover',    (0,254), 1, '8bit'],
                  ]],
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
        if self.timesteps != 'annual':
            return start
        else:
            return datetime(start.year+1, 1, 1)
    
    def build_mosaics_from_hdf5(self,
                                file_list: list[str],
                                layer_list: list[dict]) -> None:
        """
        Build a mosaic for each layer from a list of HDF5 files.
        """

        # Get the layers from the HDF5 files
        hdf5_datasets = {l['id']: [] for l in layer_list}
        for file in file_list:
            these_hdf5_datasets = self.get_layers_from_hdf5(file, layer_list)
            for lid in these_hdf5_datasets:
                hdf5_datasets[lid].append(these_hdf5_datasets[lid])

        # Create virtual mosaics of the HDF5 datasets and save to disk
        mosaics = {l['id']: None for l in layer_list}
        for tl in layer_list:
            lid = tl['id']
            vrt_ds = gdal.BuildVRT('', hdf5_datasets[lid])
            mosaics[lid] = vrt_ds
            #output_tif = dest_dict[lid]
            #new_dataset = gdal.Translate(output_tif, vrt_ds, options=gdal.TranslateOptions(format='GTiff', creationOptions=['COMPRESS=LZW']))

        return mosaics
    
    def get_layers_from_hdf5(self,
                             hdf5_file: str,
                             layer_list: list[dict]) -> dict[gdal.Dataset]:
        """
        Get the layers from an HDF5 file.
        """
        # Create a dict to hold the HDF5 datasets, one for each layer
        hdf5_datasets = {l['id']: [] for l in layer_list}

        # Open the HDF5 file and add to the list
        src_ds = gdal.Open(hdf5_file)
        layers = src_ds.GetSubDatasets()
        for tl in layer_list:
            lid = tl['id']
            src_layer = gdal.Open(layers[lid][0])
            geotranform = self.get_geotransform(layers[lid][0])
            projection = self.projection
            src_layer.SetGeoTransform(geotranform)
            src_layer.SetProjection(projection)
            hdf5_datasets[lid] = src_layer

        return hdf5_datasets

    @classmethod
    def get_available_variables(cls) -> dict:
        available_variables = cls.available_variables
        return {v: {'provider' : available_variables[v][0],\
                    'product'  : available_variables[v][1],
                    'version'  : available_variables[v][2],
                    'timesteps': available_variables[v][3],
                    'layers'   : [{'id'   : l[0],
                                   'name' : l[1],
                                   #'range': l[2],
                                   #'scale': l[3],
                                   #'type' : l[4]
                                   } for l in available_variables[v][4]]}\
                for v in available_variables}

    def get_data(self,
                 time_range: TimeRange,
                 space_bounds: BoundingBox,
                 destination: str,
                 options: Optional[dict] = None) -> None:
        """
        Get data from the CMR.
        """

        # Check options
        options = self.check_options(options)

        # Check if all the layers are requested (e.g. we might not need both QC layers for FAPAR, depending on the project)
        if options['layers'] is not None:
            for layer in self.layers:
                if layer['id'] not in options['layers']:
                    self.layers.remove(layer)

        logger.info(f'------------------------------------------')
        logger.info(f'Starting download of {self.source}-{self.variable} data')
        logger.info(f'{len(self.layers)} layers requested between {time_range.start:%Y-%m-%d} and {time_range.end:%Y-%m-%d}')
        logger.info(f'Bounding box: {space_bounds.bbox}')
        logger.info(f'------------------------------------------')

        timesteps = time_range.get_timesteps_from_DOY(self.timesteps_doy)
        logger.info(f'Found {len(timesteps)} timesteps to download.')

        for i,time in enumerate(timesteps):
            logger.info(f' - Timestep {i+1}/{len(timesteps)}: {time:%Y-%m-%d}')

            # get the data from the CMR
            url_list = self.cmr_search(time, space_bounds, extensions=['.hdf', '.h5'])

            if not url_list:
                logger.info(f'  -> No data found for {time:%Y-%m-%d}, skipping to next timestep')
                continue

            # Do all of this inside a temporary folder
            tmpdirs = os.path.join(os.getenv('HOME'), 'tmp')
            os.makedirs(tmpdirs, exist_ok=True)
            with tempfile.TemporaryDirectory(dir = tmpdirs) as tmp_path:
                file_list = self.download(url_list, tmp_path)

                lnames = [layer['name'] for layer in self.layers]
                if options['make_mosaic']:
                    # build the mosaic (one for each layer)
                    # this is better to do all at once, so that we open the HDF5 file only once
                    dest = [f'{tmp_path}/mosaic_{name}.tif' for name in lnames]
                    mosaics = self.build_mosaics_from_hdf5(file_list, self.layers)#,
                                                #destinations = dest)
                                    # from here on, we work on layers individually
                    for layer in self.layers:
                        lname = layer['name']
                        dataset = mosaics[layer['id']]
                        file_out = time.strftime(destination.format(layer=lname))
                        if options['crop_to_bounds']:
                            crop_raster(dataset, space_bounds, file_out)
                        else:
                            save_raster(dataset, file_out)
                        dataset = None
                    logger.info(f'  -> SUCCESS! data for {time:%Y-%m-%d} downloaded and combined into a single mosaic per layer')
                else:
                    for tile, file in enumerate(file_list):
                        these_hdf5_datasets = self.get_layers_from_hdf5(file, self.layers)
                        for layer in self.layers:
                            lname = layer['name']
                            ds = these_hdf5_datasets[layer['id']]
                            if options['keep_tiles_naming']:
                                pattern = re.compile(r'h\d+v\d+')
                                tile_name = re.search(pattern, file).group()
                                file_out = time.strftime(destination.format(layer=lname, tile=tile_name))
                            else:
                                file_out = time.strftime(destination.format(layer=lname, tile=tile))
                            if options['crop_to_bounds']:
                                crop_raster(ds, space_bounds, file_out)
                            else:
                                save_raster(ds, file_out)
                            ds = None
                    logger.info(f'  -> SUCCESS! data for {time:%Y-%m-%d} downloaded, {len(file_list)} tiles per layer')
        logger.info(f'------------------------------------------')                
            