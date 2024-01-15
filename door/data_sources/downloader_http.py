import requests
import os

from .door_downloader import DOORDownloader

class downloaderHTTP(DOORDownloader):
    def download(self, url: str, output: str, min_size = None) -> str:
        """
        Downloads data from http or https url
        Eventually check file size to avoid empty files
        """
        try:
            r = requests.get(url)
            os.makedirs(os.path.dirname(output), exist_ok=True)
            with open(output, 'wb') as f:
                f.write(r.content)
                if min_size is not None:
                    if os.path.getsize(output) < min_size:
                        os.remove(output)
                        raise FileNotFoundError("ERROR! Data not available")
            return output
        except Exception as e:
            print('Exception in download_url():', e)
            return None


