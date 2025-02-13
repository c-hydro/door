from datetime import datetime
from typing import Generator

import h5py
import os
import re
import xarray as xr
import rioxarray as rxr

from .cmr_downloader import CMRDownloader

from d3tools.spatial import BoundingBox, crop_to_bb
from d3tools.timestepping.timestep import TimeStep
from d3tools.errors import GDAL_ImportError

class VIIRSDownloader(CMRDownloader):

    source = 'VIIRS'
    name = 'VIIRS_downloader'

    available_products = {
        'fapar': {
            'provider'   : 'LPDAAC_ECS',
            'freq'       : '8-day',
            'version'    : '002',
            'product_id' : {'SNPP':'VNP15A2H', 'JPSS1':'VJ115A2H'},
        },
        'phenology': {
            'provider'   : 'LPDAAC_ECS',
            'freq'       : 'annual',
            'version'    : '001',
            'product_id' : {'SNPP':'VNP22Q2'},
        },
        'snow': {
            'provider'   : 'NSIDCDAAC_ECS',
            'freq'       : 'daily',
            'version'    : '002',
            'product_id' : {'SNPP':'VNP10A1', 'JPSS1':'VJ110A1'},
        }
    }

    available_variables = {
        'fapar': {
            'FAPAR'        : {'id': 0, 'valid_range': (0,100), 'fill_value' : 255, 'scale_factor': 0.01},
            'FAPAR_QC'     : {'id': 2, 'valid_range': (0,254), 'fill_value' : 255, 'scale_factor': 1   },
            'FAPAR_ExtraQC': {'id': 3, 'valid_range': (0,254), 'fill_value' : 255, 'scale_factor': 1   },
        },
        'phenology': {
            'GLSP_QC1'      : {'id': 5,  'valid_range': (0,254),   'fill_value' : 255,   'scale_factor': 1},
            'GLSP_GSStart1' : {'id': 9,  'valid_range': (1,32766), 'fill_value' : 32767, 'scale_factor': 1},
            'GLSP_GSEnd1'   : {'id': 11, 'valid_range': (1,32766), 'fill_value' : 32767, 'scale_factor': 1},
            'GLSP_QC2'      : {'id': 24, 'valid_range': (0,254),   'fill_value' : 255,   'scale_factor': 1},
            'GLSP_GSStart2' : {'id': 28, 'valid_range': (1,32766), 'fill_value' : 32767, 'scale_factor': 1},
            'GLSP_GSEnd2'   : {'id': 30, 'valid_range': (1,32766), 'fill_value' : 32767, 'scale_factor': 1},
        },
        'snow': {
            'Snow_AlgQA'      : {'id': 0, 'valid_range': (0,254), 'fill_value' : 255, 'scale_factor': 1},
            'Snow_QA'         : {'id': 1, 'valid_range': (0,254), 'fill_value' : 255, 'scale_factor': 1},
            'NDSI_Snow_Cover' : {'id': 3, 'valid_range': (0,254), 'fill_value' : 255, 'scale_factor': 1},
        }
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

    file_ext = ['.hdf', '.h5']

    # we need to add the version=2.0 to the URL to get the correct response for the snow product (for FAPAR it doesn't matter)
    cmr_url='https://cmr.earthdata.nasa.gov/search/granules.json?version=2.0'

    def __init__(self, product:str, satellite:str) -> None:
        """
        Initializes the CMRDownloader class.
        """
        super().__init__(product.lower())
        
        try:
            from osgeo import gdal
        except ImportError:
            raise GDAL_ImportError(function = 'door.VIIRSDownloader')

        if satellite not in self.product_id:
            raise ValueError(f'Satellite {satellite} not available for product {product}. Choose one of {self.product_id.keys()}')
        self.product_id = self.product_id[satellite]
        self.satellite  = satellite
    
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

    @property
    def start(self):
        if self.satellite == 'SNPP':
            start = datetime(2012, 1, 19)
        elif self.satellite == 'JPSS1':
            start = datetime(2018, 1, 1)

        if self.freq != 'annual':
            return start
        else:
            return datetime(start.year+1, 1, 1)
    
    def build_mosaics_from_hdf5(self,
                                files: list[str],
                                layers: dict) -> None:
        """
        Build a mosaic for each layer from a list of HDF5 files.
        """
        from osgeo import gdal

        lids = [l['id'] for l in layers.values()]

        # Get the layers from the HDF5 files
        hdf5_datasets = {l: [] for l in lids}
        for file in files:
            these_hdf5_datasets = self.get_layers_from_hdf5(file, lids)
            for lid in these_hdf5_datasets:
                hdf5_datasets[lid].append(these_hdf5_datasets[lid])

        # Create virtual mosaics of the HDF5 datasets and save to disk
        mosaics = {l: None for l in lids}
        for lid in lids:
            vrt_ds = gdal.BuildVRT('', hdf5_datasets[lid])
            mosaics[lid] = vrt_ds

        return mosaics
    
    def get_layers_from_hdf5(self,
                             hdf5_file: str,
                             layer_ids: list[int]) -> dict['gdal.Dataset']:
        """
        Get the layers from an HDF5 file.
        """
        from osgeo import gdal

        # Create a dict to hold the HDF5 datasets, one for each layer
        hdf5_datasets = {l: [] for l in layer_ids}

        # Open the HDF5 file and add to the list
        src_ds = gdal.Open(hdf5_file)
        layers_ds = src_ds.GetSubDatasets()
        for lid in layer_ids:
            src_layer = gdal.Open(layers_ds[lid][0])
            geotranform = self.get_geotransform(layers_ds[lid][0])
            projection = self.projection
            src_layer.SetGeoTransform(geotranform)
            src_layer.SetProjection(projection)
            hdf5_datasets[lid] = src_layer

        return hdf5_datasets

    @staticmethod
    def get_urls_in_timestep(url_list: list[str], timestep: TimeStep) -> list[str]:
        """
        Get the URLs that are in the timestep.
        This is an issue only on the first timestep of the year because it overlaps with the previous year.
        """
        filenames = [os.path.basename(url) for url in url_list]
        date_code = [name.split('.')[1] for name in filenames]
        years     = [int(code[1:5]) for code in date_code]
        keep      = [year == timestep.year for year in years]
        return [url for url, k in zip(url_list, keep) if k]

    def _get_data_ts(self,
                     timestep: TimeStep,
                     space_bounds: BoundingBox,
                     tmp_path: str) -> Generator[tuple[xr.DataArray, dict], None, None]:
        """
        Get data from the CMR.
        """
        from osgeo import gdal

        url_list = self.cmr_search(timestep, space_bounds)
        url_list = self.get_urls_in_timestep(url_list, timestep)

        if not url_list:
            return None

        if self.make_mosaic:
            # download the files (all together)
            file_list = self.download(url_list, tmp_path)
            # build the mosaic (one for each layer)
            # this is better to do all at once, so that we open the HDF5 file only once
            mosaics = self.build_mosaics_from_hdf5(file_list, self.variables)
            # from here on, we work on layers (i.e. variables) individually
            for varname, variable in self.variables.items():
                dataset = mosaics[variable['id']]
                if self.crop_to_bounds:
                    data = crop_to_bb(dataset, space_bounds, 'xarray')
                else:
                    tmp_file = os.path.join(tmp_path, f'mosaic_{varname}_{tile}.tif')
                    gdal.Translate(tmp_file, dataset, options=gdal.TranslateOptions(format='GTiff', creationOptions='COMPRESS=LZW'))
                    data = rxr.open_rasterio(tmp_file)

                dataset = None
                data = self.set_attributes(data, variable)
                yield data, {'variable': varname}
        else:
            for tile, url in enumerate(url_list):
                file = self.download([url], tmp_path)[0]
                ids = [variable['id'] for variable in self.variables.values()]
                these_hdf5_datasets = self.get_layers_from_hdf5(file, ids)
                for varname, variable in self.variables.items():
                    dataset = these_hdf5_datasets[variable['id']]
                    if self.keep_tiles_naming:
                        pattern = re.compile(r'h\d+v\d+')
                        tile_name = re.search(pattern, file).group()
                    else:
                        tile_name = str(tile)
                    if self.crop_to_bounds:
                        data = crop_to_bb(dataset, space_bounds, 'xarray')
                    else:
                        tmp_file = os.path.join(tmp_path, f'{varname}_{tile}.tif')
                        gdal.Translate(tmp_file, dataset, options=gdal.TranslateOptions(format='GTiff', creationOptions='COMPRESS=LZW'))
                        data = rxr.open_rasterio(tmp_file)
                    dataset = None
                    data = self.set_attributes(data, variable)
                    yield data, {'variable': varname, 'tile': tile_name}           

    def set_attributes(self, dataset: xr.DataArray, varopts: dict) -> xr.DataArray:
        """
        Set the attributes of the dataset.
        """
        dataset.attrs['valid_range'] = varopts.get('valid_range', None)
        dataset.attrs['_FillValue'] = varopts.get('fill_value', None)
        dataset.attrs['scale_factor'] = varopts.get('scale_factor', None)

        return dataset
        
