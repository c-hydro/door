import base64
import logging
import netrc
from typing import Optional
from urllib.parse import urlparse
from urllib.request import Request, build_opener, HTTPCookieProcessor, HTTPError

def get_credentials(url: str, test_url: Optional[str] = None) -> str:
 
    credentials = None

    try:
        info = netrc.netrc()
        username, account, password = info.authenticators(urlparse(url).hostname)
        errprefix = 'File netrc error: '
    except Exception as e:
        logging.error(' ===> File netrc error: {0}'.format(str(e)))
        raise RuntimeError('Credentials are not available on netrc file')
 
    while not credentials:
        credentials = '{0}:{1}'.format(username, password)
        credentials = base64.b64encode(credentials.encode('ascii')).decode('ascii')
 
        if test_url:
            test_credentials(credentials, test_url, errprefix)

    return credentials

def test_credentials(credentials: str, test_url: str, errprefix: str = 'Test credentials error: ') -> None:
    try:
        req = Request(test_url)
        req.add_header('Authorization', 'Basic {0}'.format(credentials))
        opener = build_opener(HTTPCookieProcessor())
        opener.open(req)
    except HTTPError:
        logging.error(' ===> ' + errprefix + 'Incorrect username or password')
        raise RuntimeError('Credentials are not available on netrc file')