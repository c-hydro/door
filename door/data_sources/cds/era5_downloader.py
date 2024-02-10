from .cds_downloader import CDSDownloader

class ERA5Downloader(CDSDownloader):
    def __init__(self) -> None:
        super().__init__('reanalysis-era5-single-levels')