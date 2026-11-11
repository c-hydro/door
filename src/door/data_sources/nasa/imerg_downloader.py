"""Optimised NASA GPM IMERG GIS downloader with legacy-compatible GeoTIFF output."""

from __future__ import annotations

import datetime as dt
import os
import re
import threading
from collections.abc import Mapping
from urllib.parse import urlparse

import numpy as np
import rasterio
import requests
from rasterio.windows import Window, from_bounds

from d3tools.spatial import BoundingBox
from d3tools.timestepping.timestep import TimeStep

from ...utils.exceptions import (
    ConfigurationError,
    DataUnavailableError,
    DataValidationError,
    DownloadError,
)
from ...base_downloaders import RasterDownloader, RasterPayload
from ...utils.auth import authentication_error, resolve_basic_credentials
from ...utils.time import HalfHourTimeStep


class IMERGDownloader(RasterDownloader):
    """Download IMERG Early, Late, and Final GIS GeoTIFF products."""

    source = "IMERG"
    source_aliases = ["NASA_IMERG", "GPM_IMERG"]
    name = "IMERGDownloader"

    product_aliases = {
        "final": "imerg-final",
        "late": "imerg-late",
        "early": "imerg-early",
        "imerg_final": "imerg-final",
        "imerg_late": "imerg-late",
        "imerg_early": "imerg-early",
    }
    available_products = {
        "imerg-final": {
            "code": "GIS",
            "publication_delay_minutes": 110 * 24 * 60,
            "credential_host": "arthurhouhttps.pps.eosdis.nasa.gov",
        },
        "imerg-late": {
            "code": "L",
            "publication_delay_minutes": 12 * 60,
            "credential_host": "jsimpsonhttps.pps.eosdis.nasa.gov",
        },
        "imerg-early": {
            "code": "E",
            "publication_delay_minutes": 4 * 60,
            "credential_host": "jsimpsonhttps.pps.eosdis.nasa.gov",
        },
    }
    available_variables = {
        "precipitation": "precipitation",
        "precipitation_accumulation": "precipitation",
    }
    imerg_priority = {"early": 0, "late": 1, "final": 2}

    default_options = {
        **RasterDownloader.default_options,
        "credentials": {
            "username": None,
            "password": None,
            "netrc_machine": None,
        },
        "version": "latest",
        "historical_version_fallback": True,
        "url_template": None,
        "fallback_to_early": False,
        "scaling_mode": "legacy",
        "input_nodata": 29999,
        "output_dtype": "float32",
        "output_nodata": 29999.0,
        "compression": "deflate",
        "minimum_file_size": 1000,
    }

    def __init__(self, product: str) -> None:
        super().__init__()
        self.set_product(product)
        self.ts_per_year = 17520
        self.variable_name = "precipitation"
        self.variable_units = "mm/30min"
        self._listing_cache: dict[str, tuple[str, ...]] = {}
        self._listing_cache_lock = threading.Lock()

    def set_product(self, product: str) -> None:
        key = str(product).strip().lower().replace("_", "-")
        key = self.product_aliases.get(key, key)
        if key not in self.available_products:
            raise ConfigurationError(
                "Unsupported IMERG product.",
                [f"Value: {product}", f"Available: {sorted(self.available_products)}"],
            )
        self.product = key
        for option, value in self.available_products[key].items():
            setattr(self, option, value)

    def check_options(self, options=None) -> dict:
        checked = super().check_options(options)
        mode = str(checked["scaling_mode"]).strip().lower()
        aliases = {
            "official": "official_accumulation",
            "accumulation": "official_accumulation",
            "same_as_old_door": "legacy",
            "none": "raw",
        }
        mode = aliases.get(mode, mode)
        if mode not in {"legacy", "official_accumulation", "raw"}:
            raise ConfigurationError(
                "Unsupported IMERG scaling mode.",
                [
                    f"Value: {checked['scaling_mode']}",
                    "Allowed: legacy, official_accumulation, raw",
                ],
            )
        checked["scaling_mode"] = mode
        version = str(checked["version"]).strip()
        if not version:
            raise ConfigurationError("IMERG version cannot be empty.")
        if version.lower() == "latest":
            checked["version"] = "latest"
        else:
            version = version.upper()
            checked["version"] = version if version.startswith("V") else "V" + version
        if not isinstance(checked.get("credentials"), Mapping):
            raise ConfigurationError(
                "Invalid IMERG credentials configuration.",
                ["downloader_settings.credentials must be a mapping."],
            )
        if checked.get("input_nodata") is not None:
            try:
                checked["input_nodata"] = float(checked["input_nodata"])
            except (TypeError, ValueError) as error:
                raise ConfigurationError(
                    "IMERG input_nodata must be numeric or null.",
                    [f"Value: {checked['input_nodata']}"],
                ) from error
        return checked

    def set_variables(self, variables) -> None:
        if isinstance(variables, str):
            variable_key = variables
            specs = {}
        elif isinstance(variables, list):
            if len(variables) != 1:
                raise ConfigurationError("IMERG supports one precipitation variable.")
            variable_key = variables[0]
            specs = {}
        elif isinstance(variables, Mapping):
            if len(variables) != 1:
                raise ConfigurationError("IMERG supports one precipitation variable.")
            variable_key, specs = next(iter(variables.items()))
            specs = specs if isinstance(specs, Mapping) else {}
        else:
            raise ConfigurationError("Invalid IMERG variables section.")

        source_key = str(specs.get("source_name") or variable_key).lower()
        if source_key not in self.available_variables:
            raise ConfigurationError(
                "Unsupported IMERG variable.",
                [
                    f"Value: {source_key}",
                    "Allowed: precipitation, precipitation_accumulation",
                ],
            )
        self.variable_name = str(specs.get("output_name") or "precipitation")
        self.variable_units = str(specs.get("units") or "mm/30min")
        self.variables = {str(variable_key): dict(specs)}

    # Download ---------
    def _download_and_prepare(
        self,
        timestep: TimeStep,
        space_bounds: BoundingBox,
        tmp_path: str,
    ) -> RasterPayload:
        def operation() -> RasterPayload:
            try:
                raw_path, source_product, source_version, source_url = self._download_product(
                    timestep, self.product, tmp_path
                )
            except DataUnavailableError:
                if self.product != "imerg-late" or not self.fallback_to_early:
                    raise
                self.log.warning(
                    " ---> IMERG Late missing for %s; try Early fallback",
                    timestep.start.strftime("%Y-%m-%d %H:%M"),
                )
                raw_path, source_product, source_version, source_url = self._download_product(
                    timestep, "imerg-early", tmp_path
                )

            return self._read_crop_and_scale(
                raw_path,
                timestep,
                space_bounds,
                source_product,
                source_version=source_version,
                source_url=source_url,
            )

        return self._run_with_retry(
            operation,
            f"IMERG {self.product} {timestep.start:%Y-%m-%d %H:%M}",
        )

    def _download_product(
        self, timestep: TimeStep, product: str, tmp_path: str
    ) -> tuple[str, str, str, str]:
        credentials = self._credentials_for_product(product)
        url, source_version = self._resolve_source(
            timestep.start, product, credentials
        )
        local_path = os.path.join(tmp_path, os.path.basename(urlparse(url).path))
        self._download_http(url, local_path, credentials)
        return local_path, product, source_version, url

    def _credentials_for_product(self, product: str) -> tuple[str, str]:
        machine = self.available_products[product]["credential_host"]
        credentials = dict(self.credentials or {})
        if not credentials.get("netrc_machine"):
            credentials["netrc_machine"] = machine

        username, password = resolve_basic_credentials(
            credentials,
            machine=machine,
            service="NASA PPS IMERG",
            required=True,
        )
        return str(username), str(password)

    def get_last_ts(self, **kwargs) -> tuple[TimeStep, TimeStep | None]:
        """Return the last output already available at the requested maturity.

        Early, Late, and Final may share one destination. A newer Early file must
        not make a Late workflow look complete, and an Early/Late file must not
        make a Final workflow look complete. Legacy files without maturity tags
        keep the generic behaviour and are treated as complete.
        """
        last_input = self.get_last_published_ts()
        desired_type = self._imerg_type(self.product)
        desired_priority = self.imerg_priority[desired_type]
        search_before = last_input.end

        while True:
            last_date = self.destination.get_last_date(now=search_before, **kwargs)
            if last_date is None:
                return last_input, None

            timestep = HalfHourTimeStep.from_date(last_date)
            output_path = self._get_dataset_key(self.destination, timestep)
            try:
                with rasterio.open(output_path) as source:
                    tags = source.tags()
            except Exception:
                search_before = timestep.start - dt.timedelta(seconds=1)
                continue

            existing_type = str(
                tags.get("imerg_type") or tags.get("type") or ""
            ).strip().lower()
            if not existing_type:
                return last_input, timestep
            if (
                existing_type in self.imerg_priority
                and self.imerg_priority[existing_type] >= desired_priority
            ):
                return last_input, timestep

            search_before = timestep.start - dt.timedelta(seconds=1)

    def _download_http(
        self,
        url: str,
        destination: str,
        credentials: tuple[str, str],
    ) -> None:
        self.log.info(" ---> Download %s", url)
        try:
            with requests.get(
                url,
                auth=credentials,
                timeout=self.timeout_seconds,
                stream=True,
                allow_redirects=True,
                headers={"User-Agent": "DOOR satellite downloader"},
            ) as response:
                if response.status_code == 404:
                    raise DataUnavailableError(
                        "IMERG file was not found.", [url]
                    )
                if response.status_code in {401, 403}:
                    raise authentication_error(
                        "NASA PPS IMERG", urlparse(url).hostname or url
                    )
                if response.status_code >= 400:
                    raise DownloadError(
                        "IMERG server returned an HTTP error.",
                        [f"Status: {response.status_code}", url],
                    )
                with open(destination, "wb") as file_handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            file_handle.write(chunk)
        except (DataUnavailableError, ConfigurationError, DownloadError):
            raise
        except requests.RequestException as error:
            raise DownloadError(
                "Unable to download IMERG data.", [url, str(error)]
            ) from error
        except OSError as error:
            raise DownloadError(
                "Unable to save the downloaded IMERG file.",
                [destination, str(error)],
            ) from error

        try:
            size = os.path.getsize(destination)
        except OSError as error:
            raise DownloadError("IMERG download was not created.", [destination]) from error
        if size < self.minimum_file_size:
            raise DownloadError(
                "IMERG download is unexpectedly small.",
                [
                    destination,
                    f"Size: {size} bytes",
                    f"Minimum: {self.minimum_file_size}",
                ],
            )

    # URL construction ---------
    def _resolve_source(
        self,
        time_now: dt.datetime,
        product: str,
        credentials: tuple[str, str],
    ) -> tuple[str, str]:
        version = self._version_for_time(time_now, product)
        if version != "latest":
            return self.build_url(time_now, product, version=version), version
        if self.url_template:
            raise ConfigurationError(
                "IMERG version='latest' cannot be combined with url_template.",
                ["Pin an explicit IMERG version when using a custom URL template."],
            )
        return self._resolve_latest_source(time_now, product, credentials)

    def _resolve_latest_source(
        self,
        time_now: dt.datetime,
        product: str,
        credentials: tuple[str, str],
    ) -> tuple[str, str]:
        pattern = self._latest_filename_pattern(time_now, product)
        matches: list[tuple[tuple[int, str], str, str]] = []

        for listing_url in self._listing_urls(time_now, product):
            try:
                listing = self._get_listing(listing_url, credentials)
            except DataUnavailableError:
                continue

            for entry in listing:
                filename = os.path.basename(urlparse(entry).path)
                match = pattern.fullmatch(filename)
                if not match:
                    continue
                version = match.group("version").upper()
                source_url = self._download_url_from_listing_entry(product, entry)
                matches.append((self._version_sort_key(version), version, source_url))

        if not matches:
            raise DataUnavailableError(
                "IMERG file was not found in the PPS listing.",
                [
                    f"Product: {product}",
                    f"Timestep: {time_now:%Y-%m-%d %H:%M}",
                ],
            )

        _, version, source_url = max(matches, key=lambda item: item[0])
        return source_url, version

    def _get_listing(
        self,
        listing_url: str,
        credentials: tuple[str, str],
    ) -> tuple[str, ...]:
        with self._listing_cache_lock:
            cached = self._listing_cache.get(listing_url)
            if cached is not None:
                return cached

            self.log.info(" ---> Read PPS listing %s", listing_url)
            try:
                with requests.get(
                    listing_url,
                    auth=credentials,
                    timeout=self.timeout_seconds,
                    allow_redirects=True,
                    headers={"User-Agent": "DOOR satellite downloader"},
                ) as response:
                    if response.status_code == 404:
                        self._listing_cache[listing_url] = ()
                        return ()
                    if response.status_code in {401, 403}:
                        raise authentication_error(
                            "NASA PPS IMERG",
                            urlparse(listing_url).hostname or listing_url,
                        )
                    if response.status_code >= 400:
                        raise DownloadError(
                            "IMERG PPS listing returned an HTTP error.",
                            [f"Status: {response.status_code}", listing_url],
                        )
                    entries = tuple(
                        line.strip()
                        for line in response.text.splitlines()
                        if line.strip()
                    )
            except (DataUnavailableError, ConfigurationError, DownloadError):
                raise
            except requests.RequestException as error:
                raise DownloadError(
                    "Unable to read the IMERG PPS listing.",
                    [listing_url, str(error)],
                ) from error

            self._listing_cache[listing_url] = entries
            return entries

    def _listing_urls(self, time_now: dt.datetime, product: str) -> list[str]:
        if product == "imerg-final":
            return [
                "https://arthurhouhttps.pps.eosdis.nasa.gov/text/gpmdata/"
                f"{time_now:%Y/%m/%d}/gis/"
            ]
        if product == "imerg-early":
            return [
                "https://jsimpsonhttps.pps.eosdis.nasa.gov/text/imerg/gis/early/"
                f"{time_now:%Y/%m}/"
            ]
        urls = [
            "https://jsimpsonhttps.pps.eosdis.nasa.gov/text/imerg/gis/"
            f"{time_now:%Y/%m}/"
        ]
        if time_now < dt.datetime(2024, 6, 1):
            urls.append(
                "https://jsimpsonhttps.pps.eosdis.nasa.gov/text/imerg/gis/"
                f"{time_now:%Y}/V06/{time_now:%m}/"
            )
        return urls

    def _latest_filename_pattern(
        self, time_now: dt.datetime, product: str
    ) -> re.Pattern[str]:
        time_end = time_now + dt.timedelta(minutes=29, seconds=59)
        step = self._step_minutes(time_now)
        code = self.available_products[product]["code"]
        if code == "GIS":
            prefix = (
                f"3B-HHR-GIS.MS.MRG.3IMERG.{time_now:%Y%m%d}"
                f"-S{time_now:%H%M%S}-E{time_end:%H%M%S}.{step:04d}."
            )
            suffix = ".tif"
        else:
            prefix = (
                f"3B-HHR-{code}.MS.MRG.3IMERG.{time_now:%Y%m%d}"
                f"-S{time_now:%H%M%S}-E{time_end:%H%M%S}.{step:04d}."
            )
            suffix = ".30min.tif"
        return re.compile(
            rf"{re.escape(prefix)}(?P<version>V\d+[A-Z]*){re.escape(suffix)}",
            re.IGNORECASE,
        )

    @staticmethod
    def _version_sort_key(version: str) -> tuple[int, str]:
        match = re.fullmatch(r"V(\d+)([A-Z]*)", version.upper())
        if not match:
            return -1, version.upper()
        return int(match.group(1)), match.group(2)

    @staticmethod
    def _download_url_from_listing_entry(product: str, entry: str) -> str:
        if entry.startswith("http://") or entry.startswith("https://"):
            return entry
        path = "/" + entry.lstrip("/")
        host = (
            "arthurhouhttps.pps.eosdis.nasa.gov"
            if product == "imerg-final"
            else "jsimpsonhttps.pps.eosdis.nasa.gov"
        )
        return f"https://{host}{path}"

    @staticmethod
    def _step_minutes(time_now: dt.datetime) -> int:
        start_of_day = time_now.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return int((time_now - start_of_day).total_seconds() / 60)

    def build_url(
        self,
        time_now: dt.datetime,
        product: str | None = None,
        *,
        version: str | None = None,
    ) -> str:
        product = product or self.product
        version = version or self._version_for_time(time_now, product)
        if version == "latest":
            raise ConfigurationError(
                "IMERG latest-version URLs must be resolved from the PPS listing."
            )
        time_end = time_now + dt.timedelta(minutes=29, seconds=59)
        step = self._step_minutes(time_now)
        code = self.available_products[product]["code"]
        if code == "GIS":
            filename = (
                f"3B-HHR-GIS.MS.MRG.3IMERG.{time_now:%Y%m%d}"
                f"-S{time_now:%H%M%S}-E{time_end:%H%M%S}.{step:04d}.{version}.tif"
            )
        else:
            filename = (
                f"3B-HHR-{code}.MS.MRG.3IMERG.{time_now:%Y%m%d}"
                f"-S{time_now:%H%M%S}-E{time_end:%H%M%S}.{step:04d}.{version}.30min.tif"
            )

        if self.url_template:
            return str(self.url_template).format(
                time_now=time_now,
                time_end=time_end,
                step=step,
                version=version,
                filename=filename,
                product=product,
            )

        if product == "imerg-final":
            return (
                f"https://arthurhouhttps.pps.eosdis.nasa.gov/gpmdata/"
                f"{time_now:%Y/%m/%d}/gis/{filename}"
            )
        if product == "imerg-early":
            return (
                f"https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/early/"
                f"{time_now:%Y/%m}/{filename}"
            )
        if version.startswith("V06"):
            return (
                f"https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/"
                f"{time_now:%Y}/V06/{time_now:%m}/{filename}"
            )
        return (
            f"https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/"
            f"{time_now:%Y/%m}/{filename}"
        )

    def _version_for_time(self, time_now: dt.datetime, product: str) -> str:
        configured = str(self.version).strip()
        if configured.lower() == "latest":
            return "latest"
        configured = configured.upper()
        if not configured.startswith("V"):
            configured = "V" + configured
        if (
            self.historical_version_fallback
            and product in {"imerg-early", "imerg-late"}
            and dt.datetime(2024, 5, 1) <= time_now < dt.datetime(2024, 6, 1)
        ):
            return "V06E"
        return configured

    # Raster processing ---------
    def _read_crop_and_scale(
        self,
        raw_path: str,
        timestep: TimeStep,
        space_bounds: BoundingBox,
        source_product: str,
        *,
        source_version: str | None = None,
        source_url: str | None = None,
    ) -> RasterPayload:
        try:
            with rasterio.open(raw_path) as source:
                source_bounds = source.bounds
                west, south, east, north = [
                    float(value) for value in space_bounds.bbox
                ]
                west = max(west, source_bounds.left)
                south = max(south, source_bounds.bottom)
                east = min(east, source_bounds.right)
                north = min(north, source_bounds.top)
                if west >= east or south >= north:
                    raise ConfigurationError(
                        "Requested bounds do not intersect the IMERG raster.",
                        [repr(space_bounds.bbox), repr(tuple(source_bounds))],
                    )
                window = from_bounds(
                    west, south, east, north, transform=source.transform
                )
                window = window.round_offsets().round_lengths()
                window = window.intersection(
                    Window(0, 0, source.width, source.height)
                )
                raw = source.read(1, window=window)
                transform = source.window_transform(window)
                crs = source.crs or "EPSG:4326"
                source_nodata = (
                    self.input_nodata
                    if self.input_nodata is not None
                    else source.nodata
                )
        except (ConfigurationError, DataValidationError):
            raise
        except Exception as error:
            raise DataValidationError(
                "Unable to read the downloaded IMERG GeoTIFF.",
                [raw_path, str(error)],
            ) from error

        data = raw.astype(np.float32, copy=True)
        missing = np.zeros(data.shape, dtype=bool)
        if source_nodata is not None:
            missing |= np.isclose(data, float(source_nodata))
        missing |= ~np.isfinite(data)

        divisor = self._scale_divisor(source_product)
        if divisor != 1.0:
            data = data / divisor
        data[missing] = float(self.output_nodata)

        return RasterPayload(
            data=data,
            transform=transform,
            crs=crs,
            nodata=float(self.output_nodata),
            dtype=str(self.output_dtype),
            raw_path=raw_path,
            tags={
                "product": self.product,
                "source_product": source_product,
                "type": self._imerg_type(source_product),
                "imerg_type": self._imerg_type(source_product),
                "source_version": source_version or self._version_from_filename(raw_path),
                "source_filename": os.path.basename(raw_path),
                "source_url": source_url or "",
                "variable": self.variable_name,
                "quantity": "precipitation_accumulation",
                "units": self.variable_units,
                "temporal_resolution": "30min",
                "timestep": timestep.start.isoformat(),
                "scaling_mode": self.scaling_mode,
                "source": "NASA GPM IMERG GIS",
            },
        )

    @staticmethod
    def _imerg_type(product: str) -> str:
        return str(product).strip().lower().replace("_", "-").split("-")[-1]

    @staticmethod
    def _version_from_filename(path: str) -> str:
        match = re.search(r"\.(V\d+[A-Z]*)\.", os.path.basename(path), re.IGNORECASE)
        return match.group(1).upper() if match else "unknown"

    def _existing_output_policy(
        self,
        output_path: str,
        timestep: TimeStep,
        incoming_tags: dict | None = None,
    ) -> tuple[str, str | None]:
        if not os.path.isfile(output_path):
            return "write", None
        if self.validate_existing and not self._existing_geotiff_is_valid(output_path):
            return "write", "existing GeoTIFF is invalid"

        try:
            with rasterio.open(output_path) as source:
                existing_tags = source.tags()
        except Exception:
            return super()._existing_output_policy(
                output_path, timestep, incoming_tags=incoming_tags
            )

        priority = self.imerg_priority
        existing_type = str(
            existing_tags.get("imerg_type") or existing_tags.get("type") or ""
        ).strip().lower()
        incoming_type = (
            str(
                (incoming_tags or {}).get("imerg_type")
                or (incoming_tags or {}).get("type")
                or ""
            ).strip().lower()
            or self._imerg_type(self.product)
        )

        if existing_type in priority and incoming_type in priority:
            if priority[incoming_type] > priority[existing_type]:
                return (
                    "write",
                    f"Replacing lower-priority IMERG product {existing_type} -> {incoming_type}",
                )
            if priority[incoming_type] < priority[existing_type]:
                return (
                    "skip",
                    f"existing IMERG {existing_type} has higher priority than incoming {incoming_type}",
                )

            existing_version = str(existing_tags.get("source_version", "")).upper()
            incoming_version = str(
                (incoming_tags or {}).get("source_version", "")
            ).upper()

            if not incoming_version and existing_version:
                configured = self._version_for_time(timestep.start, self.product)
                if configured == "latest":
                    try:
                        credentials = self._credentials_for_product(self.product)
                        _, incoming_version = self._resolve_source(
                            timestep.start, self.product, credentials
                        )
                    except DataUnavailableError:
                        incoming_version = ""
                else:
                    incoming_version = configured

            existing_key = self._version_sort_key(existing_version)
            incoming_key = self._version_sort_key(incoming_version)
            if existing_key[0] >= 0 and incoming_key[0] >= 0:
                if incoming_key > existing_key:
                    return (
                        "write",
                        f"Replacing older IMERG revision {existing_version} -> {incoming_version}",
                    )
                if incoming_key < existing_key:
                    return (
                        "skip",
                        f"existing IMERG revision {existing_version} is newer than incoming {incoming_version}",
                    )

        return super()._existing_output_policy(
            output_path, timestep, incoming_tags=incoming_tags
        )

    def _scale_divisor(self, source_product: str | None = None) -> float:
        product = source_product or self.product
        if self.scaling_mode == "raw":
            return 1.0
        if self.scaling_mode == "official_accumulation":
            return 10.0
        # Exact numerical behaviour of the old DOOR GDAL scaleParams:
        # Final 0..100 -> 0..5, Early/Late 0..100 -> 0..10.
        return 20.0 if product == "imerg-final" else 10.0
