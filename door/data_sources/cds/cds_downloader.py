import cdsapi
from ...base_downloaders import APIDownloader

import os

class CDSDownloader(APIDownloader):

    name = "CDS_downloader"
    apikey_env_vars = 'CDSAPI_KEY' # this should be in the form UID:API_KEY already
    cds_url = 'https://cds.climate.copernicus.eu/api'

    def __init__(self, dataset) -> None:

        # if key is None, this will automatically look for the .cdsapirc file
        key = os.getenv(self.apikey_env_vars, None)
        if isinstance(key, str):
            if key.startswith("'" or '"') and key.endswith("'" or '"'):
                key = key[1:-1]
        client = cdsapi.Client(url=self.cds_url, key=key)
        
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

