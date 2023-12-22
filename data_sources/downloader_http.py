import requests

from .door_downloader import DOORDownloader

class downloaderHTTP(DOORDownloader):
    def __init__(self, url: str, filename: str) -> None:
        self.url = url
        self.filename = filename

    def download(self, url: str, output: str) -> str:
        """
        Downloads data from http or https url
        """
        r = requests.get(url)
        try:
            r = requests.get(url)
            with open(output, 'wb') as f:
                f.write(r.content)
            return output
        except Exception as e:
            print('Exception in download_url():', e)
            return None