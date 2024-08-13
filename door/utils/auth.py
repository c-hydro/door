import base64
import logging
import netrc
import os
from typing import Optional
from urllib.parse import urlparse
from urllib.request import Request, build_opener, HTTPCookieProcessor, HTTPError

def get_credentials(*, env_variables: Optional[dict] = None,
                    url: Optional[str] = None, test_url: Optional[str] = None,
                    encode = True) -> str:

    # Check if credentials are provided in the environment variables
    credentials = get_credentials_from_env(env_variables)

    # If not, check if they are provided in the netrc file
    if credentials is None:
        credentials = get_credentials_from_netrc(url)
    
    if credentials is None:
        raise RuntimeError(f'No credentials provided, either provide them as environment variables {env_variables.values()} or in the netrc file')
    
    if encode:
        credentials = base64.b64encode(credentials.encode('ascii')).decode('ascii')
    if test_url:
        test_credentials(credentials, test_url)

    return credentials

def get_credentials_from_env(env_variables: dict) -> str:
    if not all(key in env_variables.keys() for key in ['username', 'password']):
        return None
    
    username, password = [os.getenv(env_variables[key]) for key in ['username', 'password']]
    if not all([username, password]):
        return None
    else:
        return '{0}:{1}'.format(username, password)


def get_credentials_from_netrc(url: str) -> str:
 
    try:
        info = netrc.netrc()
        username, account, password = info.authenticators(urlparse(url).hostname)
    except FileNotFoundError:
        logging.error(' ===> File netrc error: Could not find netrc file')
        raise FileNotFoundError('Could not find netrc file')
    except TypeError:
        logging.error(' ===> File netrc error: Could not find credentials for {0}'.format(urlparse(url).hostname))
        raise RuntimeError('Could not find credentials for {0}'.format(urlparse(url).hostname))

    return '{0}:{1}'.format(username, password)

def test_credentials(credentials: str, test_url: str) -> None:
    try:
        req = Request(test_url)
        req.add_header('Authorization', 'Basic {0}'.format(credentials))
        opener = build_opener(HTTPCookieProcessor())
        opener.open(req)
    except HTTPError:
        logging.error(' ===> Incorrect username or password for {0}'.format(test_url))
        raise RuntimeError('Incorrect username or password')