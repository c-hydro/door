import base64
import logging
import netrc
import os
from collections.abc import Mapping
from typing import Optional
from urllib.parse import urlparse
from urllib.request import Request, build_opener, HTTPCookieProcessor, HTTPError

from .exceptions import ConfigurationError

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


# Basic/netrc credential helpers ---------
def _host_name(machine: str) -> str:
    """Return the hostname component while accepting legacy URL-style netrc names."""
    value = str(machine).strip()
    parsed = urlparse(value if "://" in value else f"//{value}")
    return parsed.hostname or value.removeprefix("https://").removeprefix("http://")


def _netrc_candidates(machine: str) -> list[str]:
    """Return compatible netrc machine names, preserving the configured value first."""
    value = str(machine).strip()
    host = _host_name(value)
    candidates = [value, host, f"https://{host}", f"http://{host}"]
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def credential_help(service: str, machine: str) -> list[str]:
    """Build the common credential guidance used by configuration errors."""
    return [
        f"Service: {service}",
        f"Host: {_host_name(machine)}",
        "Provide downloader_settings.credentials.username/password or add a matching ~/.netrc entry.",
    ]


def authentication_error(service: str, machine: str) -> ConfigurationError:
    """Return a consistent authentication-rejected error."""
    return ConfigurationError(
        f"{service} authentication was rejected.",
        credential_help(service, machine),
    )


def resolve_basic_credentials(
    settings: Mapping | None,
    *,
    machine: str,
    service: str = "Satellite service",
    required: bool = True,
) -> tuple[str | None, str | None]:
    """Resolve credentials from explicit values first, then ``~/.netrc``.

    Environment-variable names are intentionally not part of the public
    configuration contract. A standard host entry in ``~/.netrc`` is the
    preferred unattended setup.
    """
    values = dict(settings or {})
    username = values.get("username")
    password = values.get("password")
    configured_machine = str(values.get("netrc_machine") or machine)

    if not username or not password:
        try:
            netrc_file = netrc.netrc()
        except (FileNotFoundError, netrc.NetrcParseError, OSError):
            netrc_file = None

        if netrc_file is not None:
            for candidate in _netrc_candidates(configured_machine):
                credentials = netrc_file.authenticators(candidate)
                if not credentials:
                    continue
                netrc_username, _, netrc_password = credentials
                username = username or netrc_username
                password = password or netrc_password
                if username and password:
                    break

    if required and (not username or not password):
        missing = []
        if not username:
            missing.append("username")
        if not password:
            missing.append("password")
        raise ConfigurationError(
            f"{service} credentials are not available.",
            [f"Missing: {', '.join(missing)}", *credential_help(service, configured_machine)],
        )

    return username, password
