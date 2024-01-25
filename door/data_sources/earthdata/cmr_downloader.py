from urllib.request import Request, urlopen, build_opener, HTTPCookieProcessor
from urllib.error import URLError, HTTPError
import ssl

import json
import itertools
import sys
import numpy as np
from datetime import datetime
from typing import Optional
from tempfile import TemporaryDirectory

from osgeo import gdal

from ...base_downloaders import DOORDownloader
from ...utils.auth import get_credentials
from ...utils.time import TimeRange
from ...utils.space import SpatialReference
from ...utils.geotiff import regrid_raster, keep_valid_range, apply_scale_factor

class CMRDownloader(DOORDownloader):
    """
    This class is a downloader for through the Common Metadata Repository (CMR).
    This includes NASA's Earthdata such as VIIRS and MODIS data.

    In this common class, we will implement the methods used to navigate the CMR and download raw data.
    """

    name = "CMR downloader"

    urs_url='https://urs.earthdata.nasa.gov'
    cmr_url='https://cmr.earthdata.nasa.gov'

    cmr_page_size = 200 #TODO: check if true
    
    def __init__(self) -> None:
        # credentials should be saved in a .netrc file in the user's home directory
        # with the following line:
        # machine urs.earthdata.nasa.gov login <username> password <password>
        #test_url = None #TODO: add test url
        #self.credentials = get_credentials(self.urs_url)
        pass

    def get_credentials(self, url: str) -> str:

        if not hasattr(self, 'credentials') or not isinstance(self.credentials, str):
            self.credentials = get_credentials(self.urs_url, test_url = url)
        
        return self.credentials

    def get_data(self,
                 time_range: TimeRange,
                 space_ref: SpatialReference,
                 destination: str,
                 options: Optional[dict] = None) -> None:
        pass

    def download(self, url_list: list[str], destination: str) -> list[str]:
        """
        Downloads the files from the urls in the list
        """
        if not url_list:
            return

        filename_ls = []

        for url in url_list:
            credentials = self.get_credentials(url)
            filename = url.split('/')[-1]

            try:
                req = Request(url)
                if credentials:
                    req.add_header('Authorization', 'Basic {0}'.format(credentials))
                opener = build_opener(HTTPCookieProcessor())
                data = opener.open(req).read()

                filename_save = destination + '/' + filename
                with open(filename_save, 'wb') as f:
                    f.write(data)
                
                #open(filename_save, 'wb').write(data)
                
                if filename_save.find('.xml') > 0:
                    continue
                if filename_save.find('s3credentials') < 0:
                    filename_ls.append(filename_save)

            except HTTPError as e:
                print('HTTP error {0}, {1}'.format(e.code, e.reason))
            except URLError as e:
                print('URL error: {0}'.format(e.reason))
            except IOError:
                raise
            except KeyboardInterrupt:
                quit()

        with open(destination + '/hdf5names.json', 'w') as f:
            json.dump(filename_ls, f) 

        return filename_ls       

    def build_cmr_query(self, time: datetime, bounding_box) -> str:

        cmr_base_url = ('{0}/search/granules.json?version=2.0provider={1}'
                        '&sort_key[]=start_date&sort_key[]=producer_granule_id'
                        '&scroll=true&page_size={2}'.format(self.cmr_url, self.provider, self.cmr_page_size))

        product_query = self.fomat_product(self.product)
        version_query = self.format_version(self.version)
        temporal_query = self.format_temporal(time)
        spatial_query = self.format_spatial(bounding_box)
        filter_query = self.format_filename_filter(time)

        return cmr_base_url + product_query + version_query + temporal_query + spatial_query + filter_query

    def cmr_search(self, time: datetime, bounding_box) -> dict:
        """
        Search CMR for files matching the query.
        """

        cmr_query_url = self.build_cmr_query(time, bounding_box)
        cmr_scroll_id = None
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        try:
            urls = []
            while True:
                req = Request(cmr_query_url)
                if cmr_scroll_id:
                    req.add_header('cmr-scroll-id', cmr_scroll_id)
                response = urlopen(req, context=ctx)
                if not cmr_scroll_id:
                    # Python 2 and 3 have different case for the http headers
                    headers = {k.lower(): v for k, v in dict(response.info()).items()}
                    cmr_scroll_id = headers['cmr-scroll-id']
                    hits = int(headers['cmr-hits'])
                search_page = response.read()
                search_page = json.loads(search_page.decode('utf-8'))
                url_scroll_results = cmr_filter_urls(search_page)
                if not url_scroll_results:
                    break
                if hits > self.cmr_page_size:
                    print('.', end='')
                    sys.stdout.flush()
                urls += url_scroll_results

            if hits > self.cmr_page_size:
                print()
            return urls
        except KeyboardInterrupt:
            quit()

    @staticmethod
    def fomat_product(product: str) -> str:
        """
        Formats the product name to be used in the CMR query.
        """
        return f'&short_name={product}'
    
    @staticmethod
    def format_version(version: str) -> str:
        """
        Formats the version to be used in the CMR query.
        """
        desired_pad_length = 3
        version = str(int(version))  # Strip off any leading zeros
        query_params = ''
        while len(version) <= desired_pad_length:
            padded_version = version.zfill(desired_pad_length)
            query_params += f'&version={padded_version}'
            desired_pad_length -= 1
        return query_params

    @staticmethod
    def format_temporal(time: datetime) -> str:
        """
        Formats the time to be used in the CMR query.
        """
        date_st = time.strftime('%Y-%m-%d')
        time_start = date_st + 'T00:00:00Z'
        time_end = date_st + 'T23:59:59Z'
        return f'&temporal[]={time_start},{time_end}'
    
    @staticmethod
    def format_spatial(bounding_box) -> str:
        """
        Formats the spatial extent to be used in the CMR query.
        """
        bbformat = '{0},{1},{2},{3}'.format(bounding_box[0], bounding_box[1], bounding_box[2], bounding_box[3])
        return f'&bounding_box[]={bbformat}'
    
    @staticmethod
    def format_filename_filter(time: datetime) -> str:
        """
        Formats the filename filter to be used in the CMR query.
        """
        filename_filter = time.strftime('*A%Y%j*')
        return f'&producer_granule_id[]={filename_filter}&options[producer_granule_id][pattern]=true'
    def get_data(self,
                 time_range: TimeRange,
                 space_ref: SpatialReference,
                 destination: str,
                 options: Optional[dict] = None) -> None:
        """
        Get data from the CMR.
        """
        timesteps = time_range.get_timesteps_from_DOY(self.timesteps_doy)
        with TemporaryDirectory() as tmpdir:
            for time in timesteps:

                # get the data from the CMR
                url_list = self.cmr_search(time, space_ref.bbox)
                file_list = self.download(url_list, tmpdir)

                # build the mosaic (one for each layer)
                # this is better to do all at once, so that we open the HDF5 file only once
                lnames = [layer['name'] for layer in self.layers]
                mosaic_dest = [f'{tmpdir}/mosaic_{name}.tif' for name in lnames]
                self.build_mosaics_from_hdf5(file_list, self.layers,
                                             destinations = mosaic_dest)

                # from here on, we work on layers individually
                for layer, mosaic_file in zip(self.layers, mosaic_dest):
                    lname = layer['name']

                    # keep values in the valid range only
                    valid_range = layer['range']
                    valid_out = f'{tmpdir}/valid_{lname}.tif'
                    keep_valid_range(mosaic_file, valid_out, valid_range)

                    # regrid to the target grid (and mask if requested)
                    regridded_out = f'{tmpdir}/regridded_{lname}.tif'
                        # this line is a hack to make sure categorical data is not interpolated
                    resampling_method = 'NearestNeighbour' if layer['type'] != 'cont' else None
                    regrid_raster(valid_out, regridded_out, space_ref,
                                  nodata_value=np.nan,
                                  resampling_method=resampling_method)

                    # apply the scale factor
                    scale_factor = layer['scale']
                    scaled_out = time.strftime(destination.format(layer=lname))
                    apply_scale_factor(regridded_out, scaled_out, scale_factor)
    
    def build_mosaics_from_hdf5(self,
                                file_list: list[str],
                                layer_list: list[dict],
                                destinations: list[str]) -> None:
        """
        Build a mosaic for each layer from a list of HDF5 files.
        """

        # Create a dict to hold the HDF5 datasets, one for each layer
        hdf5_datasets = {l['id']: [] for l in layer_list}

        # Open each HDF5 file and add to the list
        for file in file_list:
            src_ds = gdal.Open(file)
            layers = src_ds.GetSubDatasets()
            for tl in layer_list:
                lid = tl['id']
                src_layer = gdal.Open(layers[lid][0])
                geotranform = self.get_geotransform(layers[lid][0])
                projection = self.projection
                src_layer.SetGeoTransform(geotranform)
                src_layer.SetProjection(projection)
                hdf5_datasets[lid].append(src_layer)

        # Create virtual mosaics of the HDF5 datasets and save to disk
        for tl in layer_list:
            lid = tl['id']
            vrt_ds = gdal.BuildVRT('', hdf5_datasets[lid])
            output_tif = destinations[lid]
            new_dataset = gdal.Translate(output_tif, vrt_ds, options=gdal.TranslateOptions(format='GTiff', creationOptions=['COMPRESS=LZW']))

        # Close the datasets
        for ds in hdf5_datasets:
            ds = None
        vrt_ds = None
    
    @classmethod
    def get_available_variables(cls) -> dict:
        available_variables = cls.available_variables
        return {v: {'provider' : available_variables[v][0],\
                    'product'  : available_variables[v][1],
                    'version'  : available_variables[v][2],
                    'timesteps': available_variables[v][3],
                    'layers'   : [{'id'   : l[0],
                                   'name' : l[1],
                                   'range': l[2],
                                   'scale': l[3],
                                   'type' : l[4]} for l in available_variables[v][4]]}\
                for v in available_variables}

def cmr_filter_urls(search_results):
    """Select only the desired data files from CMR response."""
    if 'feed' not in search_results or 'entry' not in search_results['feed']:
        return []

    entries = [e['links']
               for e in search_results['feed']['entry']
               if 'links' in e]
    # Flatten "entries" to a simple list of links
    links = list(itertools.chain(*entries))

    urls = []
    unique_filenames = set()
    for link in links:
        if 'href' not in link:
            # Exclude links with nothing to download
            continue
        if 'inherited' in link and link['inherited'] is True:
            # Why are we excluding these links?
            continue
        if 'rel' in link and 'data#' not in link['rel']:
            # Exclude links which are not classified by CMR as "data" or "metadata"
            continue

        if 'title' in link and 'opendap' in link['title'].lower():
            # Exclude OPeNDAP links--they are responsible for many duplicates
            # This is a hack; when the metadata is updated to properly identify
            # non-datapool links, we should be able to do this in a non-hack way
            continue

        filename = link['href'].split('/')[-1]
        if filename in unique_filenames:
            # Exclude links with duplicate filenames (they would overwrite)
            continue
        unique_filenames.add(filename)

        urls.append(link['href'])

    return urls