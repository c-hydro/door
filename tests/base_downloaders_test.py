import unittest
from unittest.mock import patch, call

import numpy as np
import xarray as xr
from datetime import datetime

from door import DOORDownloader, HTTPDownloader

class TestDOORDownloader(unittest.TestCase):
    def setUp(self):
        self.downloader = DOORDownloader()
    
class TestHTTPDownloader(unittest.TestCase):
    def setUp(self):
        self.downloader = HTTPDownloader('http://www.example.com')

    def test_init(self):
        self.assertEqual(self.downloader.url, 'http://www.example.com')

    @patch('requests.get')
    @patch('os.makedirs')
    @patch('builtins.open')
    @patch('os.path.getsize')
    @patch('os.remove')
    def test_download(self, mock_remove, mock_getsize, mock_open, mock_makedirs, mock_get):
        mock_get.return_value.content = b'abc'

        # test 1, no min_size supplied
        self.downloader.download('path/file.tif')
        mock_makedirs.assert_called_once_with('path', exist_ok=True)
        mock_open.assert_called_once_with('path/file.tif', 'wb')
        with mock_open() as handle:
            handle.write.assert_called_once_with(b'abc')
        mock_getsize.assert_not_called()
        mock_remove.assert_not_called()

        # test 2, min_size supplied, file size is greater than min_size
        mock_getsize.return_value = 5
        self.downloader.download('path/file.tif', 10)
        mock_remove.assert_called_once_with('path/file.tif')

        # test 3, min_size supplied, file size is less than min_size
        mock_remove.reset_mock()
        self.downloader.download('path/file.tif', 1)
        mock_remove.assert_not_called()

if __name__ == '__main__':
    unittest.main()