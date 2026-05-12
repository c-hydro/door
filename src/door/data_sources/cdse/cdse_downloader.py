# input standard python (xarray, np ecc)
import xarray as xr
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
import requests
from typing import Sequence
import rioxarray as rxr
import datetime as dt

# d3tools e other cima
from d3tools import timestepping as ts
from d3tools import spatial as sp
from d3tools.data import Dataset

# internal imports
from ...base_downloaders import DOORDownloader
from ...utils.auth import get_credentials


class CDSEDownloader(DOORDownloader):
    source = "CDSE"
    name = "CDSE_Downloader"

    credential_env_vars = {
        "username": "CDSE_LOGIN",
        "password": "CDSE_PWD",
    }

    TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    PROCESS_URL = "https://sh.dataspace.copernicus.eu/api/v1/process"
    CATALOGUE_URL = "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/search"

    separate_vars = False

    default_options = {
        "consolidation": None,  # FAPAR only. If None, defaults to [0, 6].
        "tvalue": None,        # SWI only
        "variables": None,      # None means all available variables for the product.
        "version": None,        # 
        "mosaicking_order": "mostRecent",
        "max_workers": 4,
        "make_mosaic": True,
        "mask": None,
    }

    tile_options = {
        "max_tile_pixels": 2500,
        "retry_backoff_base_s": 1.0,
        "retry_on_status": [429, 500, 502, 503, 504],
    }

    available_products = {
        "fapar": {
            # FAPAR collections are selected by consolidation level.
            "collections": {
                0: "0dfe26be-b9ca-4286-b624-37591ea2addf",
                1: "5e850ca5-2925-40b2-b377-d2410cb7fa21",
                2: "98358a1f-474e-45b0-abd8-7a543cbfe1ea",
                6: "f3d558b9-7f12-46ff-aaef-7ea0dab397ed",
            },
            "default_consolidation": [0, 6],
            "frequency" : "dekad",
            "data_type" : "UINT8",
            "fill_value": 255,
            "resolution": 1 / 336,
            "available_bounds": (-180, -60, 180, 80),
        },

        "swi": {
            # SWI collections are selected by product version, not consolidation.
            "collections": {
                0: "d0413fe0-46dc-4c2c-96a2-437e726d89a3",
            },
            "default_tvalue" : [1,5,10,15,20,40,60,100], # all available t-values
            "frequency": "dekad",
            "data_type": "UINT8",
            "fill_value": 255,
            "resolution": 0.1,
            "available_bounds": (-180, -90, 180, 90),
        },
    }

    # Cache of last available timesteps.
    last_available = {}

    available_variables = {
        "fapar": {
            "FAPAR"         : {"scale_factor": 1 / 250},
            "NOBS"          : {"scale_factor": 1      },
            "QFLAG"         : {"scale_factor": 1      },
            "RMSE"          : {"scale_factor": 1 / 250},
            "LENGTH_BEFORE" : {"scale_factor": 1      },
            "LENGTH_AFTER"  : {"scale_factor": 1      },
        },

        "swi": {
            "SWI":   {"scale_factor": 1 / 200}, # 10-daily Soil Water Index
            "QFLAG": {"scale_factor": 1 / 200}, # Quality flags
            "VOBS":  {"scale_factor": 1 / 100}, # Percentage of valid observations in the 10-day synthesis period
        },
    }

    def __init__(self, product: str) -> None:
        super().__init__()
        self.set_product(product)
        self.session = self._make_session()
        self.token = self._get_access_token()

    def check_options(self, options):
        options = super().check_options(options)

        if self.product == "fapar":
            if options['consolidation'] is None:
                options['consolidation'] = self.default_consolidation
            elif isinstance(options['consolidation'], int):
                options['consolidation'] = [options['consolidation']]
            if options['tvalue'] is not None:
                self.log.warning("tvalue option is not applicable for FAPAR product and will be ignored.")
                options['tvalue'] = None
            
            wrong_consolidations = [c for c in options['consolidation'] if c not in self.collections]
            if wrong_consolidations:
                self.log.warning(f"Invalid consolidation levels {wrong_consolidations} for product {self.product} will be ignored.")
            options['consolidation'] = [c for c in options['consolidation'] if c in self.collections]

        elif self.product == "swi":
            if options['consolidation'] is not None:
                self.log.warning("consolidation option is not applicable for SWI product and will be ignored.")
            options['consolidation'] = 0
            if options['tvalue'] is None:
                options['tvalue'] = self.default_t_value
            elif isinstance(options['tvalue'], int):
                options['tvalue'] = [options['tvalue']]

            wrong_tvalues = [t for t in options['tvalue'] if t not in self.default_tvalue]
            if wrong_tvalues:
                self.log.warning(f"Invalid t-values {wrong_tvalues} for product {self.product} will be ignored.")
            options['tvalue'] = [t for t in options['tvalue'] if t in self.default_tvalue]

        if options.get("mask") is not None and isinstance(options["mask"], Dataset):
            mask = options["mask"].get_data()
            mask = mask.chunk({mask.rio.x_dim: 1024, mask.rio.y_dim: 1024})
            options["mask"] = mask

        return options

    def _get_consolidation(self, timestep=None):
        if isinstance(self.consolidation, int):
            return self.consolidation
        elif isinstance(self.consolidation, list):
            for c in sorted(self.consolidation, reverse=True):
                last_ts = self.get_last_published_ts(consolidation=c)
                if last_ts >= timestep:
                    return c
            else:
                return None

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
        if not hasattr(self, "credentials") or not isinstance(self.credentials, str):
            self.credentials = get_credentials(
                env_variables=self.credential_env_vars,
                url=self.PROCESS_URL,
                encode=False,
            )

        return self.credentials

    def _get_access_token(self):
        """
        Retrieve an access token from the authentication server.
        This token is used for subsequent API calls.
        """

        if not hasattr(self, "refresh_token") or self.refresh_token is None:
            username, password = self._get_credentials().split(":", 1)
            auth_data = {
                "client_id": "cdse-public",
                "grant_type": "password",
                "username": username,
                "password": password,
            }
        else:
            auth_data = {
                "client_id": "cdse-public",
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            }

        resp = requests.post(self.TOKEN_URL, data=auth_data, verify=True, allow_redirects=False)
        resp.raise_for_status()

        payload = resp.json()
        access_token = payload.get("access_token")

        if not access_token:
            raise RuntimeError(f"No access_token in response: {payload}")

        self.refresh_token = payload.get("refresh_token")
        return access_token

    @staticmethod
    def _estimate_output_size(bbox, resolution):
        minx, miny, maxx, maxy = bbox
        width  = max(1, int(round((maxx - minx) / resolution)))
        height = max(1, int(round((maxy - miny) / resolution)))
        return width, height

    @staticmethod
    def _tile_edges(length: int, tile_size: int):
        if tile_size <= 0:
            raise ValueError(f"tile_size must be > 0, got {tile_size}")

        n_tiles = np.ceil(length / tile_size)
        tile_size = int(np.ceil(length / n_tiles))
        edges = list(range(0, length, tile_size))

        if edges[-1] != length:
            edges.append(length)

        return edges

    @staticmethod
    def _pixel_edges_to_coords(v0: float, v1: float, edges_px):
        span = v1 - v0
        total_px = edges_px[-1]

        if total_px <= 0:
            raise ValueError(f"Invalid pixel span: {total_px}")

        return [v0 + (span * px / total_px) for px in edges_px]

    def _get_tile_specs(self, bbox):
        if not hasattr(self, "_tile_specs_cache"):
            self._tile_specs_cache = {}

        cache_key = tuple(bbox)

        if cache_key in self._tile_specs_cache:
            return self._tile_specs_cache[cache_key]

        specs = self._compute_tile_specs(bbox)
        self._tile_specs_cache[cache_key] = specs

        return specs

    def _compute_tile_specs(self, bbox):
        self.log.info(f"Computing tile specs for bbox {bbox} with resolution {self.resolution}")

        max_tile_pixels = self.tile_options["max_tile_pixels"]
        full_width, full_height = self._estimate_output_size(bbox, self.resolution)
        minx, miny, maxx, maxy = bbox

        if self.mask is not None:
            mask = self.mask.compute()
        else:
            mask = None

        x_edges_px = self._tile_edges(full_width, max_tile_pixels)
        y_edges_px = self._tile_edges(full_height, max_tile_pixels)

        x_edges = self._pixel_edges_to_coords(minx, maxx, x_edges_px)
        y_edges = self._pixel_edges_to_coords(miny, maxy, y_edges_px)

        specs = []
        nx = len(x_edges_px) - 1
        ny = len(y_edges_px) - 1

        for row in range(ny):
            for col in range(nx):
                x0_px, x1_px = x_edges_px[col], x_edges_px[col + 1]
                y0_px, y1_px = y_edges_px[row], y_edges_px[row + 1]

                if mask is not None:
                    this_mask = mask.sel({self.mask.rio.x_dim: slice(x_edges[col],    x_edges[col + 1]),
                                          self.mask.rio.y_dim: slice(y_edges[row + 1],y_edges[row]    )})
                    if not this_mask.any():
                        continue

                width = x1_px - x0_px
                height = y1_px - y0_px

                if width <= 0 or height <= 0:
                    continue

                specs.append(
                    {
                        "tile_id": f"r{row + 1:02}c{col + 1:02}",
                        "row": row,
                        "col": col,
                        "width": width,
                        "height": height,
                        "bbox": [
                            x_edges[col],
                            y_edges[row],
                            x_edges[col + 1],
                            y_edges[row + 1],
                        ],
                    }
                )

        return specs

    def _build_evalscript(self, bands):

        input_list = ", ".join(f'"{band}"' for band in bands)
        output_exprs = ",\n      ".join(f"sample.{band}" for band in bands)

        return f"""
//VERSION=3
function setup() {{
  return {{
    input: [{input_list}],
    output: {{
      bands: {len(bands)},
      sampleType: "{self.data_type}"
    }}
  }};
}}

function evaluatePixel(sample) {{
  return [
      {output_exprs}
  ];
}}
""".strip()

    def _build_payload(self, timestep, bbox, bands, collection_key):
        collection_id = self.collections[collection_key]

        return {
            "input": {
                "bounds": {
                    "bbox": bbox,
                    "properties": {
                        "crs": "http://www.opengis.net/def/crs/EPSG/0/4326",
                    },
                },
                "data": [
                    {
                        "type": f"byoc-{collection_id}",
                        "dataFilter": {
                            "timeRange": {
                                "from": f"{timestep.start:%Y-%m-%d}T00:00:00Z",
                                "to": f"{timestep.end:%Y-%m-%d}T23:59:59Z",
                            },
                            "mosaickingOrder": self.mosaicking_order,
                        },
                    }
                ],
            },
            "output": {
                "resx": self.resolution,
                "resy": self.resolution,
                "responses": [
                    {
                        "identifier": "default",
                        "format": {"type": "image/tiff"},
                    }
                ],
            },
            "evalscript": self._build_evalscript(bands),
        }

    def _request_tiff(self, payload):
        resp = self.session.post(
            self.PROCESS_URL,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "Accept": "image/tiff",
            },
            json=payload,
            timeout=180,
        )

        if not resp.ok:
            message = f"HTTP {resp.status_code}: {resp.text[:1000]}"
            error = requests.HTTPError(message, response=resp)
            raise error

        content_type = resp.headers.get("Content-Type", "")

        if "image/tiff" not in content_type.lower():
            raise RuntimeError(f"Unexpected response Content-Type: {content_type}\n{resp.text[:1000]}")

        return resp.content

    def _get_retry_delay(self, attempt: int, response: requests.Response | None = None):
        if response is not None:
            retry_after = (
                response.headers.get("Retry-After")
                or response.headers.get("retry-after")
            )

            if retry_after is not None:
                try:
                    retry_after_value = float(retry_after)
                    if retry_after_value > 1000:
                        return retry_after_value / 1000.0
                    return retry_after_value
                except ValueError:
                    pass

        base_delay = float(self.tile_options.get("retry_backoff_base_s", 1.0))
        jitter = random.uniform(0.0, 0.25 * base_delay)
        return base_delay * (2 ** max(0, attempt - 1)) + jitter

    def _download_and_save_tiff(self, payload, tmp_file, tile_id=None):
        max_attempts = 50
        retry_on_status = set(self.tile_options.get("retry_on_status",[429, 500, 502, 503, 504],))

        for attempt in range(1, max_attempts + 1):
            try:
                raw_tiff = self._request_tiff(payload)
                with open(tmp_file, "wb") as f:
                    f.write(raw_tiff)
                return tmp_file

            except requests.HTTPError as exc:
                response = exc.response
                status_code = response.status_code if response is not None else None

                if status_code == 401:  # Unauthorized - token might have expired, try refreshing it
                    self.log.info("Access token may have expired, refreshing token and retrying...")
                    self.token = self._get_access_token()
                    should_retry = True
                else:
                    should_retry = status_code in retry_on_status

                if not should_retry or attempt >= max_attempts:
                    tile_msg = f" for tile {tile_id}" if tile_id is not None else ""
                    raise RuntimeError(f"CDSE download failed{tile_msg} after {attempt} attempt(s): {exc}") from exc

                delay = self._get_retry_delay(attempt, response=response)
                self.log.warning(f"Retrying CDSE download for tile {tile_id} after HTTP {status_code} (attempt {attempt}/{max_attempts}, {delay:.2f})")
                time.sleep(delay)

            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt >= max_attempts:
                    tile_msg = f" for tile {tile_id}" if tile_id is not None else ""
                    raise RuntimeError(f"CDSE download failed{tile_msg} after {attempt} attempt(s): {exc}") from exc

                delay = self._get_retry_delay(attempt)
                self.log.warning(f"Retrying CDSE download for tile {tile_id} after network error {type(exc).__name__} (attempt {attempt}/{max_attempts}, {delay:.2f})")
                time.sleep(delay)

    def _get_data_ts(self, timestep, space_bounds, tmp_path):
        """
        Returns:
            list[(xr.DataArray, tags_dict)]
        """

        minx, miny, maxx, maxy = space_bounds.bbox
        avail_minx, avail_miny, avail_maxx, avail_maxy = self.available_bounds

        minx = max(minx, avail_minx)
        maxx = min(maxx, avail_maxx)
        miny = max(miny, avail_miny)
        maxy = min(maxy, avail_maxy)

        if minx >= maxx or miny >= maxy:
            self.log.warning("Requested bounds {space_bounds.bbox} do not intersect available bounds {self.available_bounds} for product {self.product}.")
            yield None, {}
            return

        space_bounds = sp.BoundingBox(minx, miny, maxx, maxy)
        tile_specs = self._get_tile_specs(space_bounds.bbox)

        if not tile_specs:
            self.log.warning("No tile specs generated for bounds {space_bounds.bbox}.")
            yield None, {}
            return

        consolidation = self._get_consolidation(timestep=timestep)
        if consolidation is None:
            self.log.warning("No data available for timestep {timestep} for product {self.product}.")
            yield None, {}
            return
        
        var_keys = list(self.variables.keys())
        if self.product == 'swi':
            bands = {f"{key}{t:03}" : dict(variable = key, tvalue=t) for key in var_keys for t in self.tvalue}
        elif self.product == 'fapar':
            bands = {key: dict(variable=key) for key in var_keys}

        download_jobs = []
        for spec in tile_specs:
            bbox = spec["bbox"]
            payload = self._build_payload(timestep, bbox, list(bands.keys()), consolidation)
            tmp_file = (f"{tmp_path}/cdse_request_{self.product}_{spec['tile_id']}.tiff")
            download_jobs.append((spec['tile_id'], payload, tmp_file))

        tmp_files_by_tile = {}
        def _register_tile_for_mosaic(i, tile_id, tmp_file):
            tmp_files_by_tile[tile_id] = tmp_file
            if i>1 and (i%10 == 0 or i == len(download_jobs)):
                self.log.info(f"Completed download of {i} tiles of {len(download_jobs)} [{timestep}]")

        def _yield_tile(tile_id, tmp_file):
                da = rxr.open_rasterio(tmp_file)
                for v, key in enumerate(bands.keys()):
                    var = bands[key]['variable']
                    da_var = da.isel(band=v).drop("band").rename(var)
                    tags = bands[key]
                    tags['tile'] = tile_id
                    if self.product == 'fapar': tags['consolidation'] = consolidation
                    da_var = self.set_attributes(da_var, **tags)
                    yield da_var, tags

        max_workers = max(1, int(getattr(self, "max_workers", 1)))
        if max_workers == 1 or len(download_jobs) == 1:
            for i, this_job in enumerate(download_jobs):
                tile_id, payload, tmp_file = this_job
                self._download_and_save_tiff(payload,tmp_file,tile_id=tile_id)
                if self.make_mosaic:
                    _register_tile_for_mosaic(i+1, tile_id, tmp_file)
                else:
                    yield from _yield_tile(tile_id, tmp_file)

        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_job = {
                    executor.submit(self._download_and_save_tiff, payload ,tmp_file, tile_id): (tile_id, tmp_file)
                    for tile_id, payload, tmp_file in download_jobs
                }
                
                for i, future in enumerate(as_completed(future_to_job)):
                    tile_id, tmp_file = future_to_job[future]
                    future.result()
                    if self.make_mosaic:
                        _register_tile_for_mosaic(i+1, tile_id, tmp_file)
                    else:
                        yield from _yield_tile(tile_id, tmp_file)

        if self.make_mosaic:
            tmp_files = list(tmp_files_by_tile.values())

            if not tmp_files:
                self.log.warning(f"No downloaded tiles available to mosaic for {timestep}.")
                yield None, {}
                return

            das = [rxr.open_rasterio(f,chunks={"x": "auto", "y": "auto"},) for f in tmp_files]
            if len(das) == 1:
                da = das[0]
            else:
                self.log.info(f"Mosaicking {len(das)} tiles for {timestep}...")
                da = xr.combine_by_coords(das, combine_attrs="override", join='outer', fill_value=self.fill_value)

            for v, key in enumerate(bands.keys()):
                var = bands[key]['variable']
                da_var = da.isel(band=v).drop("band").rename(var)
                tags = bands[key]
                if self.product == 'fapar': tags['consolidation'] = consolidation
                da_var = self.set_attributes(da_var, **tags)
                yield da_var, tags

    def set_attributes(self, da: xr.DataArray, variable, **kwargs):
        da.name = variable
        da.attrs["scale_factor"] = self.variables[variable]["scale_factor"]
        da.attrs["_FillValue"] = self.fill_value

        for key, value in kwargs.items():
            da.attrs[key] = str(value)

        return da

    def get_last_published_ts(self, consolidation=None):
        """
        Get last available timestep for the selected BYOC collection.

        For FAPAR, the selector is consolidation.
        For SWI, the selector is version.
        """

        if self.product == 'swi':
            consolidation = 0
        elif self.product == 'fapar':
            if consolidation is None:
                consolidation = self.consolidation
        
        # the lower consolidation will have more recent data
        if isinstance(consolidation, list):
            consolidation = min(consolidation)

        if consolidation in self.last_available:
            return self.last_available[consolidation]

        collection_id = self.collections[consolidation]

        timestep = ts.TimeStep.from_unit(self.frequency)
        now = dt.datetime.now()
        this_ts = timestep.from_date(now)

        bbox = self.bounds.bbox or self.available_bounds

        while True:
            payload = {
                "bbox": bbox,
                "datetime": (
                    f"{this_ts.start:%Y-%m-%d}T00:00:00Z/"
                    f"{this_ts.end:%Y-%m-%d}T23:59:59Z"
                ),
                "collections": [f"byoc-{collection_id}"],
                "limit": 1,
            }

            resp = self.session.post(
                self.CATALOGUE_URL,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=60,
            )

            if resp.status_code == 401:
                self.token = self._get_access_token()
                continue

            resp.raise_for_status()

            results = resp.json().get("features", [])

            if not results:
                this_ts -= 1
            else:
                latest_datetime = results[0]["properties"]["datetime"]
                latest_ts = timestep.from_date(latest_datetime[:10])
                self.last_available[consolidation] = latest_ts
                return latest_ts

    def get_last_published_date(self):
        last_ts = self.get_last_published_ts()
        return last_ts.end
