import unittest
from unittest.mock import patch

from datetime import datetime

from door import DOORDownloader, HTTPDownloader

class TestDOORDownloader(unittest.TestCase):
    def setUp(self):
        self.downloader = DOORDownloader()

    def test_init(self):
        self.assertEqual(self.downloader.name, 'DOOR Downloader')
        self.assertEqual(self.downloader.default_options, {})

    def test_check_options(self):
        # give fake default options
        fake_default_options = {'a': 1, 'b': 2}
        self.downloader.default_options = fake_default_options

        # test 1, options is None
        self.assertEqual(self.downloader.check_options(None), fake_default_options)

        # test 2, options is empty
        self.assertEqual(self.downloader.check_options({}), fake_default_options)

        # test 3, options is not empty
        self.assertEqual(self.downloader.check_options({'a': 3}), {'a': 3, 'b': 2})

        # test 4, options is not empty, but contains an invalid key
        self.assertEqual(self.downloader.check_options({'c': 4, 'b': 1}), {'a': 1, 'b': 1})

    def test_get_data(self):
        # test 1, not implemented
        with self.assertRaises(NotImplementedError):
            self.downloader.get_data(None, None, None)

    def test_handle_missing(self):
        # test 1, level is error
        with self.assertRaises(FileNotFoundError):
            self.downloader.handle_missing('error')

        # test 2, level is warning
        with self.assertLogs(level='WARNING'):
            self.downloader.handle_missing('warning')

        # test 3, level is ignore
        self.downloader.handle_missing('ignore')

        # test 4, level is invalid
        with self.assertRaises(ValueError):
            self.downloader.handle_missing('invalid')
    
class TestHTTPDownloader(unittest.TestCase):
    def setUp(self):
        self.downloader = HTTPDownloader('http://www.example{time:%Y%m%d}.com')

    def test_init(self):
        self.assertEqual(self.downloader.url_blank, 'http://www.example{time:%Y%m%d}.com')

    @patch('requests.get')
    @patch('os.makedirs')
    @patch('builtins.open')
    @patch('os.path.getsize')
    @patch('os.path.isfile')
    @patch('door.base_downloaders.HTTPDownloader.handle_missing')
    def test_download(self, mock_handle_missing, mock_isfile, mock_getsize, mock_open, mock_makedirs, mock_get):
        mock_get.return_value.content = b'abc'
        mock_handle_missing.return_value = None

        test_args = {
            'destination': 'path/file.tif',
            'missing_action': 'ignore',
            'time': datetime(2021, 1, 1)
        }

        # test 1, no min_size supplied
        self.downloader.download(**test_args)
        mock_makedirs.assert_called_once_with('path', exist_ok=True)
        mock_open.assert_called_once_with('path/file.tif', 'wb')
        mock_get.assert_called_once_with('http://www.example20210101.com')
        with mock_open() as handle:
            handle.write.assert_called_once_with(b'abc')
        mock_getsize.assert_not_called()

        # test 2, min_size supplied, file size is greater than min_size
        mock_isfile.return_value = True
        mock_getsize.return_value = 5
        self.downloader.download(min_size=10, **test_args)
        mock_handle_missing.assert_called_once_with('ignore', {'time': datetime(2021, 1, 1)})

        # test 3, min_size supplied, file size is less than min_size
        mock_handle_missing.reset_mock()
        self.downloader.download(min_size=1, **test_args)
        mock_handle_missing.assert_not_called()

        # test 4, file not downloaded
        mock_get.reset_mock()
        mock_isfile.return_value = False
        self.downloader.download(**test_args)
        mock_handle_missing.assert_called_once_with('ignore', {'time': datetime(2021, 1, 1)})

if __name__ == '__main__':
    unittest.main()