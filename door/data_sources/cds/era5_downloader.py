from .cds_downloader import CDSDownloader

import logging
logger = logging.getLogger(__name__)

class ERA5Downloader(CDSDownloader):

    available_products = ['reanalysis-era5-single-levels']

    def __init__(self, product) -> None:
        if product not in self.available_products:
            msg = f'Product {product} not available for ERA5'
            logger.error(msg)
            raise ValueError(msg)
        
        super().__init__(product)