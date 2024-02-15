from .cds_downloader import CDSDownloader

import logging
logger = logging.getLogger(__name__)

class ERA5Downloader(CDSDownloader):

    available_products = ['reanalysis-era5-single-levels']
    default_options = {
        'output_format': 'netcdf', # one of netcdf, GeoTIFF
        'aggregate_daily': 'mean', # one of 'mean', 'max', 'min', 'sum'
    }

    def __init__(self, product = 'reanalysis-era5-single-levels') -> None:
        if product not in self.available_products:
            msg = f'Product {product} not available for ERA5'
            logger.error(msg)
            raise ValueError(msg)
        
        super().__init__(product)

    def check_options(self, options: dict) -> dict:
        options = super().check_options(options)

        if 'tif' in options['output_format'].lower():
            options['output_format'] = 'GeoTIFF'
        else:
            logger.warning(f'Output format {options["output_format"]} not supported. Using netcdf')
            options['output_format'] = 'netcdf'
        
        if options['aggregate_daily'].lower() not in ['mean', 'max', 'min', 'sum']:
            logger.warning(f'Unknown method {options["aggregate_daily"]}. Using mean')
            options['aggregate_daily'] = 'mean'
    
        return options

    def make_request(self, )