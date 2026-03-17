# input standard python (xarray, np ecc)
import xarray as xr
from typing import Iterable
import numpy as np
import requests
import rasterio
import io
import logging
from netrc import netrc, NetrcParseError
from pathlib import Path

# d3tools e other cima
from d3tools import timestepping as ts
from d3tools import spatial as sp

# internal imports
from ...base_downloaders import DOORDownloader
from ...utils.auth import get_credentials

class CDSEDownloader(DOORDownloader):
    source = "cdse"
    name = "CDSE_Downloader"

    credential_env_vars = {'username' : 'CDSE_LOGIN', 'password' : 'CDSE_PWD'}
    TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    PROCESS_URL = "https://sh.dataspace.copernicus.eu/api/v1/process"

    # single_temp_folder = False
    # separate_vars = True

    default_options = {
        "product": "fapar",
        "consolidation": 6, # eventually allow this to be a list, but figure it out after the basic version is working
        "variables": None,  # None means all available variables for the product
        "freq" : "dekad",
        "resolution": 300,
        "mosaicking_order": "mostRecent",
        "sample_type": "FLOAT32",
    }

    available_products = {
        "fapar": {
            "collections": {
                0: "0dfe26be-b9ca-4286-b624-37591ea2addf",
                1: "5e850ca5-2925-40b2-b377-d2410cb7fa21",
                2: "98358a1f-474e-45b0-abd8-7a543cbfe1ea",
                6: "f3d558b9-7f12-46ff-aaef-7ea0dab397ed",
            },
            "frequency": "dekad",
        }
    }

    available_variables = {
        "fapar": {
            "FAPAR": {
                "dtype": "float32",
                "expr": "sample.FAPAR / 250.0",
                "nodata": np.nan,
            },
            "NOBS": {
                "dtype": "int8",
                "expr": "sample.NOBS",
                "nodata": -1,
            },
            "QFLAG": {
                "dtype": "int8",
                "expr": "sample.QFLAG",
                "nodata": -1,
            },
            "RMSE": {
                "dtype": "float32",
                "expr": "sample.RMSE",
                "nodata": np.nan,
            },
            "LENGTH_BEFORE": {
                "dtype": "int8",
                "expr": "sample.LENGTH_BEFORE",
                "nodata": -1,
            },
            "LENGTH_AFTER": {
                "dtype": "int8",
                "expr": "sample.LENGTH_AFTER",
                "nodata": -1,
            },
        }
    }

    def __init__(self, product: str) -> None:
        super().__init__()
        self.set_product(product)
        self.session = self._make_session()

    def check_options(self, options):
        options = super().check_options(options)

        consolidation = options.get("consolidation")
        # if isinstance(consolidation, int):
        #     consolidation = [consolidation]

        # if not all([c in self.collections for c in consolidation]):
        if consolidation not in self.collections:
            raise ValueError(
                f"Invalid consolidation {self.consolidation}. "
                f"Choose one or more of {list(self.collections.keys())}"
            )

        return options

    @staticmethod
    def _make_session() -> requests.Session:
        session = requests.Session()
        session.trust_env = False
        return session

    def _get_credentials(self) -> str:

        # credentials will be looked for in the environment variables
        # username = 'CDSE_LOGIN', password = 'CDSE_PWD'
        # should be saved in a .netrc file in the user's home directory
        # with the following line:
        # machine sh.dataspace.copernicus.eu login <username> password <password>
        if not hasattr(self, 'credentials') or not isinstance(self.credentials, str):
            self.credentials = get_credentials(env_variables=self.credential_env_vars,
                                               url=self.PROCESS_URL, encode = False)
        
        return self.credentials

    def _get_access_token(self):
        """
        Retrieve an access token from the authentication server.
        This token is used for subsequent API calls.
        """

        username, password = self._get_credentials().split(":", 1)

        auth_data = {
            "client_id": "cdse-public",
            "grant_type": "password",
            "username": username,
            "password": password,
        }
        resp = requests.post(self.TOKEN_URL, data=auth_data, verify=True, allow_redirects=False)
        resp.raise_for_status()

        payload = resp.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError(f"No access_token in response: {payload}")
        return token

    @staticmethod
    def _bounds_to_bbox(bounds):

        """
        Convert DOOR-compatible bounds to a CDSE bbox list:
        [minx, miny, maxx, maxy] in EPSG:4326.
        """

        if bounds is None:
            raise ValueError("Bounds are None")

        # Case 1: plain sequence already passed directly
        if isinstance(bounds, (list, tuple)) and len(bounds) == 4:
            bbox = [float(v) for v in bounds]

        # Case 2: DOOR / d3tools BoundingBox
        elif hasattr(bounds, "bbox"):
            bbox = list(bounds.bbox)
            if len(bbox) != 4:
                raise ValueError(f"Invalid bounds.bbox length: {bbox}")
            bbox = [float(v) for v in bbox]

        # Case 3: other common bbox-style objects
        elif all(hasattr(bounds, name) for name in ("minx", "miny", "maxx", "maxy")):
            bbox = [
                float(bounds.minx),
                float(bounds.miny),
                float(bounds.maxx),
                float(bounds.maxy),
            ]

        elif all(hasattr(bounds, name) for name in ("left", "bottom", "right", "top")):
            bbox = [
                float(bounds.left),
                float(bounds.bottom),
                float(bounds.right),
                float(bounds.top),
            ]

        # Case 4: iterable custom object
        else:
            try:
                bbox = [float(v) for v in list(bounds)]
                if len(bbox) != 4:
                    raise ValueError
            except Exception:
                raise ValueError(
                    f"Unsupported bounds format: type={type(bounds)}, repr={bounds!r}"
                )

        minx, miny, maxx, maxy = bbox
        if minx >= maxx or miny >= maxy:
            raise ValueError(f"Invalid bbox coordinates: {bbox}")

        # CDSE payload below assumes geographic lon/lat coordinates
        crs = getattr(bounds, "epsg_code", None) or str(getattr(bounds, "crs", ""))
        if crs and "4326" not in str(crs):
            raise ValueError(
                f"Bounds CRS must be EPSG:4326 for CDSE payload, got {crs}"
            )

        return bbox

    @staticmethod
    def _estimate_output_size(bbox, resolution):
        minx, miny, maxx, maxy = bbox
        width = max(1, int(round((maxx - minx) * 111320 / resolution)))
        height = max(1, int(round((maxy - miny) * 111320 / resolution)))
        return width, height

    def _timestep_to_timerange(self, timestep):
        """
        Convert framework timestep to [from, to] strings.
        Assumes timestep has .start and .end as datetime-like objects.
        """
        t0 = timestep.start.strftime("%Y-%m-%d")
        t1 = timestep.end.strftime("%Y-%m-%d")
        return t0, t1

    def _build_evalscript(self, bands):
        input_list = ", ".join(f'"{band}"' for band in bands)
        output_exprs = ",\n      ".join(self.variables[band]["expr"] for band in bands)

        return f"""
//VERSION=3
function setup() {{
  return {{
    input: [{input_list}],
    output: {{
      bands: {len(bands)},
      sampleType: "{self.sample_type}"
    }}
  }};
}}

function evaluatePixel(sample) {{
  return [
      {output_exprs}
  ];
}}
""".strip()

    def _build_payload(self, timestep, bounds, bands):
        bbox = self._bounds_to_bbox(bounds)
        width, height = self._estimate_output_size(bbox, self.resolution)
        t0, t1 = self._timestep_to_timerange(timestep)
        collection_id = self.collections[self.consolidation]

        return {
            "input": {
                "bounds": {
                    "bbox": bbox,
                    "properties": {
                        "crs": "http://www.opengis.net/def/crs/EPSG/0/4326"
                    },
                },
                "data": [
                    {
                        "type": f"byoc-{collection_id}",
                        "dataFilter": {
                            "timeRange": {
                                "from": f"{t0}T00:00:00Z",
                                "to": f"{t1}T23:59:59Z",
                            },
                            "mosaickingOrder": self.mosaicking_order,
                        },
                    }
                ],
            },
            "output": {
                "width": width,
                "height": height,
                "responses": [
                    {
                        "identifier": "default",
                        "format": {"type": "image/tiff"},
                    }
                ],
            },
            "evalscript": self._build_evalscript(bands),
        }

    def _request_tiff(self, payload, token):
        resp = self.session.post(
            self.PROCESS_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "image/tiff",
            },
            json=payload,
            timeout=180,
        )

        if not resp.ok:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:1000]}")

        content_type = resp.headers.get("Content-Type", "")
        if "image/tiff" not in content_type.lower():
            raise RuntimeError(
                f"Unexpected response Content-Type: {content_type}\n{resp.text[:1000]}"
            )

        return resp.content

    def _tiff_bytes_to_dataarrays(self, raw_bytes, bands):
        out = []

        with rasterio.MemoryFile(raw_bytes) as memfile:
            with memfile.open() as src:
                data = src.read()  # shape: (bands, y, x)
                transform = src.transform
                crs = src.crs

                x = np.arange(src.width) * transform.a + transform.c + transform.a / 2
                y = np.arange(src.height) * transform.e + transform.f + transform.e / 2

                for i, band_name in enumerate(bands):
                    arr = data[i, :, :]

                    da = xr.DataArray(
                        arr,
                        dims=("y", "x"),
                        coords={"y": y, "x": x},
                        name=band_name,
                        attrs={
                            "crs": str(crs) if crs else None,
                            "transform": tuple(transform),
                            "source": self.source,
                            "product": self.product,
                            "variable": band_name,
                            "consolidation": self.consolidation,
                        },
                    )
                    out.append(da)

        return out

    def _get_data_ts(self, timestep, space_bounds, tmp_path):
        """
        Returns:
            list[(xr.DataArray, tags_dict)]
        """
        token = self._get_access_token()
        bands = list(self.variables.keys())

        payload = self._build_payload(
            timestep=timestep,
            bounds=space_bounds,
            bands=bands,
        )

        raw_tiff = self._request_tiff(payload, token)
        arrays = self._tiff_bytes_to_dataarrays(raw_tiff, bands)

        output = []
        for da in arrays:
            tags = {
                "variable": da.name,
                "source": self.source,
                "product": self.product,
            }
            output.append((da, tags))

        return output

    def get_last_published_ts(self):
        """
        Placeholder.
        Implement from product publication rules or a metadata endpoint if you have one.
        """
        raise NotImplementedError("get_last_published_ts() is not implemented yet.")
