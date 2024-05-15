import cdsapi
from ...base_downloaders import APIDownloader

import logging
logger = logging.getLogger(__name__)

class CDSDownloader(APIDownloader):

    def __init__(self, dataset) -> None:
        client = cdsapi.Client()#progress=False, quiet=True)
        super().__init__(client)
        self.dataset = dataset

    def download(self, request: dict, destination: str,
                 min_size: float = None, missing_action: str = 'error') -> None:
        """
        Downloads data from the CDS API based on the request.
        dataset: the name of the dataset to download from
        request: a dictionary with the request parameters
        output: the name of the output file
        """
        return super().download(destination, min_size, missing_action, name = self.dataset, request = request, target = destination)

