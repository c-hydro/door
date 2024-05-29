from datetime import datetime

from .cmr_downloader import CMRDownloader

import rasterio
import numpy as np
import os
import tempfile
from typing import Optional

from .cmr_downloader import CMRDownloader
from ...utils.space import BoundingBox
from ...utils.geotiff import crop_raster, save_raster
from ...utils.time import TimeRange

class GRACEDownloader(CMRDownloader):

    source = 'GRACE'
    name = 'GRACE_downloader'

    available_variables = {
        'tws': ['POCLOUD', 'TELLUS_GRFO_L3_JPL_RL06.1_LND_v04', 'RL06.1v04', 'monthly',
                [[0, 'TWS'            ],
                 [1, 'TWS_uncertainty']]]
    }

    pre2018_variables = {
        'tws': {
            'product' : 'TELLUS_GRAC_L3_JPL_RL06_LND_v04',
            'version' : 'RL06v04'
        }
    }

    def __init__(self, variable: str): 	
        super().__init__()

        self.variable = variable

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

        self.log.info(f'------------------------------------------')
        self.log.info(f'Starting download of {self.source}-{self.variable} data')
        self.log.info(f'{len(self.layers)} layers requested between {time_range.start:%Y-%m-%d} and {time_range.end:%Y-%m-%d}')
        self.log.info(f'Bounding box: {space_bounds.bbox}')
        self.log.info(f'------------------------------------------')

        if self.timesteps == 'monthly':
            timesteps = time_range.get_timesteps_from_tsnumber(12)
        # this is literally the only available option

        self.log.info(f'Found {len(timesteps)} timesteps to download.')
        
        # order timesteps decreasingly, this way once we hit 2018 we can change the product and version and keep them
        timesteps.sort(reverse=True)
        for i,time in enumerate(timesteps):
            self.log.info(f' - Timestep {i+1}/{len(timesteps)}: {time:%Y-%m-%d}')

            if time.year < 2018:
                self.product = self.pre2018_variables[self.variable]['product']
                self.version = self.pre2018_variables[self.variable]['version']
            else:
                self.product = self.available_variables[self.variable][1]
                self.version = self.available_variables[self.variable][2]

            # get the data from the CMR
            url_list = self.cmr_search(time, space_bounds, extensions=['.tif', '.tiff'])
            if not url_list:
                self.log.info(f'  -> No data found for {time:%Y-%m-%d}, skipping to next timestep')
                continue

            # Do all of this inside a temporary folder
            tmpdirs = os.path.join(os.getenv('HOME'), 'tmp')
            os.makedirs(tmpdirs, exist_ok=True)
            with tempfile.TemporaryDirectory(dir = tmpdirs) as tmp_path:
                file = self.download(url_list, tmp_path)[0] # we should only return a single file for these

                ds = rasterio.open(file)
                if ds is None:
                    self.log.error(f'Could not open file {file}')
                    continue
               
                #lnames = [layer['name'] for layer in self.layers]
                for layer in self.layers:
                    self.log.info(f'  -> Processing layer {layer["name"]}')

                    # create a new dataset with only the current layer (band)
                    band = ds.read(layer['id'] + 1)

                    # Create a new dataset for this band
                    band[band == -99999] = np.nan
                    lname = layer['name']
                    tmp_output_file = os.path.join(tmp_path, time.strftime(f'{lname}.tif'))
                    
                    # Save the band to a temporary file
                    with rasterio.open(tmp_output_file, 'w', driver='GTiff',
                                       height=ds.height, width=ds.width,
                                       count=1, dtype=band.dtype, nodata=np.nan,
                                       crs=ds.crs, transform=ds.transform) as dst:
                        dst.write(band, 1)

                    # Crop this band and save it
                    output = time.strftime(destination.format(layer=layer['name']))
                    crop_raster(tmp_output_file, space_bounds, output)

                # Close the dataset
                ds = None