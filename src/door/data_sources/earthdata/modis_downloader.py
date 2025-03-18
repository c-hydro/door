from datetime import datetime
from typing import Generator

from pyhdf.SD import SD, SDC
import os
import re
import xarray as xr
import rioxarray as rxr

from .cmr_downloader import CMRDownloader

from d3tools.spatial import BoundingBox, crop_to_bb
from d3tools.timestepping.timestep import TimeStep
from d3tools.errors import GDAL_ImportError

class MODISDownloader(CMRDownloader):

    source = 'MODIS'
    name = 'MODIS_downloader'

    available_products = {
        'et': {
            'provider'   : 'LPCLOUD',
            'freq'       : '8-day',
            'version'    : '061',
            'product_id' : {'AQUA':'MYD16A2', 'TERRA':'MOD16A2'},
        }
    }

    available_variables = {
        'et': {
            'ET'    : {'id': 0, 'valid_range': (-32767, 32700), 'fill_value' : 32767, 'scale_factor': 0.1},
            'PET'   : {'id': 2, 'valid_range': (-32767, 32700), 'fill_value' : 32767, 'scale_factor': 0.1},
            'ET_QC' : {'id': 4, 'valid_range': (0,254), 'fill_value' : 255, 'scale_factor': 1   },
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

    def __init__(self, product:str, satellite: str = 'AQUA') -> None:
        """
        Initializes the CMRDownloader class.
        """
        super().__init__(product.lower())

        try:
            from osgeo import gdal
        except ImportError:
            raise GDAL_ImportError(function = 'door.MODISDownloader')

        if satellite not in self.product_id:
            raise ValueError(f'Satellite {satellite} not available for product {product}. Choose one of {self.product_id.keys()}')
        self.product_id = self.product_id[satellite]
        self.satellite  = satellite

    # this is specific to MODIS, different from VIIRS!
    def get_geotransform(self, filename):
        """
        Get geotransform from dataset.
        """
        # Get the filename of the metadata containing the geolocation
        metadata_filename = filename.split('"')[1]
        file = SD(metadata_filename, SDC.READ)
        metadata = file.attributes()

        # Extract and parse the StructMetadata.0 attribute
        struct_metadata = metadata['StructMetadata.0']

        # Use regular expressions to find the required geolocation information
        ulx_uly_pattern = re.compile(r'UpperLeftPointMtrs=\(([^)]+)\)')
        lrx_lry_pattern = re.compile(r'LowerRightMtrs=\(([^)]+)\)')
        xdim_pattern = re.compile(r'XDim=(\d+)')
        ydim_pattern = re.compile(r'YDim=(\d+)')

        ulx_uly = ulx_uly_pattern.search(struct_metadata).group(1).split(',')
        lrx_lry = lrx_lry_pattern.search(struct_metadata).group(1).split(',')
        xdim = xdim_pattern.search(struct_metadata).group(1)
        ydim = ydim_pattern.search(struct_metadata).group(1)

        # Get the coordinates of the upper left and lower right corners
        self.ulx, self.uly = map(float, ulx_uly)
        self.lrx, self.lry = map(float, lrx_lry)

        # Get the size of the dataset (in pixels)
        self.xsize = int(xdim)
        self.ysize = int(ydim)

        geotransform = (self.ulx, (self.lrx - self.ulx) / self.xsize, 0,
                        self.uly, 0, (self.lry - self.uly) / self.ysize)
        return geotransform

    @property
    def start(self):
        if self.satellite == 'AQUA':
            start = datetime(2002, 1, 1)
        elif self.satellite == 'TERRA':
            start = datetime(2002, 1, 1)

        if self.freq != 'annual':
            return start
        else:
            return datetime(start.year+1, 1, 1)

    def build_mosaics_from_hdf(self,
                                files: list[str],
                                layers: dict) -> None:
        """
        Build a mosaic for each layer from a list of HDF5 files.
        """
        from osgeo import gdal

        lids = [l['id'] for l in layers.values()]

        # Get the layers from the HDF5 files
        hdf_datasets = {l: [] for l in lids}
        for file in files:
            these_hdf_datasets = self.get_layers_from_hdf(file, lids)
            for lid in these_hdf_datasets:
                hdf_datasets[lid].append(these_hdf_datasets[lid])

        # Create virtual mosaics of the HDF5 datasets and save to disk
        mosaics = {l: None for l in lids}
        for lid in lids:
            vrt_ds = gdal.BuildVRT('', hdf_datasets[lid])
            mosaics[lid] = vrt_ds

        return mosaics

    def get_layers_from_hdf(self,
                             hdf_file: str,
                             layer_ids: list[int]) -> dict['gdal.Dataset']:
        """
        Get the layers from an HDF5 file.
        """
        from osgeo import gdal

        # Create a dict to hold the HDF5 datasets, one for each layer
        hdf_datasets = {l: [] for l in layer_ids}

        # Open the HDF5 file and add to the list
        src_ds = gdal.Open(hdf_file)
        layers_ds = src_ds.GetSubDatasets()
        for lid in layer_ids:
            src_layer = gdal.Open(layers_ds[lid][0])
            geotranform = self.get_geotransform(layers_ds[lid][0])
            projection = self.projection
            src_layer.SetGeoTransform(geotranform)
            src_layer.SetProjection(projection)
            hdf_datasets[lid] = src_layer

        return hdf_datasets

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

    def build_cmr_query(self, time_start: datetime, time_end: datetime, bounding_box) -> str:

        cmr_base_url = ('{0}provider={1}'
                        '&sort_key=start_date&sort_key=producer_granule_id'
                        '&page_size={2}'.format(self.cmr_url, self.provider, self.cmr_page_size))

        product_query = self.fomat_product(self.product_id)
        version_query = self.format_version(self.version)
        temporal_query = self.format_temporal(time_start, time_end)
        spatial_query = self.format_spatial(bounding_box)
        #filter_query = self.format_filename_filter(time)

        tail = '&options[producer_granule_id][pattern]=true'
        return cmr_base_url + product_query + version_query + temporal_query + spatial_query + tail# + filter_query

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
            mosaics = self.build_mosaics_from_hdf(file_list, self.variables)
            # from here on, we work on layers (i.e. variables) individually
            for varname, variable in self.variables.items():
                dataset = mosaics[variable['id']]
                # save to a temporary file, to reopen it as a xarray dataset
                tmp_file = os.path.join(tmp_path, f'mosaic_{varname}.tif')

                gdal.Translate(tmp_file, dataset, options=gdal.TranslateOptions(format='GTiff', creationOptions=['COMPRESS=LZW']))
                dataset = None
                if self.crop_to_bounds:
                    data = crop_to_bb(tmp_file, space_bounds)
                else:
                    data = rxr.open_rasterio(tmp_file)

                data = self.set_attributes(data, variable)
                yield data, {'variable': varname}
        else:
            for tile, url in enumerate(url_list):
                file = self.download([url], tmp_path)[0]
                ids = [variable['id'] for variable in self.variables.values()]
                these_hdf_datasets = self.get_layers_from_hdf(file, ids)
                for varname, variable in self.variables.items():
                    dataset = these_hdf_datasets[variable['id']]
                    if self.keep_tiles_naming:
                        pattern = re.compile(r'h\d+v\d+')
                        tile_name = re.search(pattern, file).group()
                    else:
                        tile_name = str(tile)

                    # save to a temporary file, to reopen it as a xarray dataset
                    tmp_file = os.path.join(tmp_path, f'{varname}_{tile}.tif')
                    gdal.Translate(tmp_file, dataset, options=gdal.TranslateOptions(format='GTiff', creationOptions=['COMPRESS=LZW']))
                    dataset = None

                    if self.crop_to_bounds:
                        data = crop_to_bb(tmp_file, space_bounds)
                    else:
                        data = rxr.open_rasterio(tmp_file)

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

