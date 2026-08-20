"""ACMAD WWFD heatwave products served as GeoTIFF files by THREDDS."""

from __future__ import annotations

import datetime as dt
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
import requests
import xarray as xr
from d3tools import timestepping as ts

from ...base_downloaders import DOORDownloader
from ...utils.exceptions import (
    ConfigurationError,
    DataUnavailableError,
    DataValidationError,
    DownloadError,
    ProcessingError,
)


THREDDS_NS = {
    "t": "http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0"
}


class ACMADHeatwaveDownloader(DOORDownloader):
    """Build the ACMAD heat-index and indicator NetCDF products."""

    source = "ACMAD_HEATWAVE"
    source_aliases = ["ACMAD", "ACMAD_WWFD", "WWFD_HEATWAVE"]
    name = "ACMAD_heatwave_downloader"
    default_options = {
        "variables": {},
        "catalog_url": None,
        "file_url": None,
        "nodata_values": [-999000000.0],
        "time_steps": 6,
        "time_step_hours": 24,
        "products": {},
        "raw_destination": None,
        "download_attempts": 3,
        "retry_seconds": 5,
        "timeout_seconds": 180,
        "overwrite_existing": False,
    }

    def __init__(self, product: str = "WWFD") -> None:
        super().__init__()
        if product.upper() not in {"WWFD", "HEATWAVE"}:
            raise ConfigurationError(
                "Unsupported ACMAD heatwave product.", [f"Value: {product}"]
            )
        self.product = product.upper()

    def set_variables(self, variables: Any) -> None:
        self.variables = variables

    @staticmethod
    def _reference_time(time_range: Any) -> dt.datetime:
        value = getattr(time_range, "end", None) or getattr(time_range, "start", None)
        value = getattr(value, "start", value)
        if not isinstance(value, dt.datetime):
            raise ConfigurationError("An ACMAD reference time is required.")
        # A d3tools TimeRange end marks the end of its smallest timestep
        # (for example 00:00:59). ACMAD issue and forecast times are minute
        # aligned, so keep the selected minute without leaking boundary seconds
        # into the NetCDF time coordinate.
        return value.replace(second=0, microsecond=0)

    @staticmethod
    def _render(value: str, run_time: dt.datetime, forecast_time: dt.datetime,
                forecast_end: dt.datetime, **tags: str) -> str:
        return value.format(
            run_date=run_time.strftime("%Y%m%d"),
            run_datetime=run_time.strftime("%Y%m%d%H%M"),
            forecast_date=forecast_time.strftime("%Y%m%d"),
            forecast_end_date=forecast_end.strftime("%Y%m%d"),
            **tags,
        )

    @staticmethod
    def _output_path(destination: Any, run_time: dt.datetime, **tags: str) -> str:
        if destination is None or not hasattr(destination, "get_key"):
            raise ConfigurationError(
                "ACMAD destinations must use a workflow Dataset."
            )
        return destination.get_key(ts.Hour.from_date(run_time), **tags)

    def _request(self, session: requests.Session, url: str) -> requests.Response:
        errors: list[str] = []
        for attempt in range(1, int(self.download_attempts) + 1):
            try:
                response = session.get(url, timeout=float(self.timeout_seconds))
                response.raise_for_status()
                return response
            except requests.RequestException as error:
                errors.append(str(error))
                status = getattr(getattr(error, "response", None), "status_code", None)
                if status == 404 or attempt >= int(self.download_attempts):
                    break
                time.sleep(float(self.retry_seconds) * attempt)
        raise DownloadError("Unable to read the ACMAD THREDDS resource.", [url, *errors[-3:]])

    def _read_catalog(self, session: requests.Session, url: str) -> dict[str, str]:
        self.log.info(" --> Read ACMAD THREDDS catalogue: %s", url)
        try:
            root = ET.fromstring(self._request(session, url).content)
        except ET.ParseError as error:
            raise DataValidationError(
                "The ACMAD THREDDS catalogue is not valid XML.", [url, str(error)]
            ) from error
        files: dict[str, str] = {}
        for dataset in root.findall(".//t:dataset", THREDDS_NS):
            name = dataset.get("name", "")
            url_path = dataset.get("urlPath")
            if url_path and name.lower().endswith((".tif", ".tiff")):
                files[name] = url_path
        self.log.info(" ---> Catalogue contains %d GeoTIFF file(s)", len(files))
        return files

    @staticmethod
    def _valid_raster(path: Path) -> bool:
        try:
            with rasterio.open(path) as source:
                return source.count == 1 and source.width > 0 and source.height > 0
        except (OSError, rasterio.errors.RasterioError):
            return False

    def _download_raster(
        self,
        session: requests.Session,
        url: str,
        destination: Path,
    ) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        if (
            destination.exists()
            and not self.overwrite_existing
            and self._valid_raster(destination)
        ):
            self.log.info(" ---> Use cached ACMAD GeoTIFF: %s", destination)
            return destination

        temporary = destination.with_name(destination.name + ".part")
        temporary.unlink(missing_ok=True)
        errors: list[str] = []
        for attempt in range(1, int(self.download_attempts) + 1):
            try:
                self.log.info(
                    " ---> Download ACMAD GeoTIFF (attempt %d/%d): %s",
                    attempt,
                    int(self.download_attempts),
                    url,
                )
                with session.get(
                    url,
                    stream=True,
                    timeout=float(self.timeout_seconds),
                ) as response:
                    response.raise_for_status()
                    with open(temporary, "wb") as stream:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                stream.write(chunk)
                if not self._valid_raster(temporary):
                    raise DataValidationError(
                        "Downloaded ACMAD content is not a valid single-band raster.",
                        [url],
                    )
                os.replace(temporary, destination)
                return destination
            except (requests.RequestException, OSError, DataValidationError) as error:
                temporary.unlink(missing_ok=True)
                errors.append(str(error))
                status = getattr(getattr(error, "response", None), "status_code", None)
                if status == 404 or attempt >= int(self.download_attempts):
                    break
                time.sleep(float(self.retry_seconds) * attempt)
        raise DownloadError("Unable to download an ACMAD GeoTIFF.", [url, *errors[-3:]])

    @staticmethod
    def _read_raster(
        path: Path,
        expected_signature: tuple | None,
        nodata_values: list[float],
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, str], tuple]:
        try:
            with rasterio.open(path) as source:
                signature = (
                    source.height,
                    source.width,
                    tuple(source.transform),
                    source.crs.to_string() if source.crs else None,
                )
                if expected_signature is not None and signature != expected_signature:
                    raise DataValidationError(
                        "An ACMAD raster grid differs from the reference grid.",
                        [str(path)],
                    )
                values = source.read(1).astype("float32")
                if source.nodata is not None:
                    values[values == np.float32(source.nodata)] = np.nan
                for nodata_value in nodata_values:
                    values[values == np.float32(nodata_value)] = np.nan
                transform = source.transform
                longitude = (
                    transform.c
                    + (np.arange(source.width, dtype="float64") + 0.5) * transform.a
                )
                latitude = (
                    transform.f
                    + (np.arange(source.height, dtype="float64") + 0.5) * transform.e
                )
                spatial_attrs = {
                    "crs": source.crs.to_string() if source.crs else "",
                    "GeoTransform": " ".join(
                        str(item) for item in transform.to_gdal()
                    ),
                }
        except DataValidationError:
            raise
        except (OSError, rasterio.errors.RasterioError) as error:
            raise DataValidationError(
                "Unable to read an ACMAD raster.", [str(path), str(error)]
            ) from error
        return values, latitude, longitude, spatial_attrs, signature

    @staticmethod
    def _write_atomic(dataset: xr.Dataset, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(destination.name + ".part")
        temporary.unlink(missing_ok=True)
        encoding = {
            name: {
                "zlib": True,
                "complevel": 4,
                "shuffle": True,
                "dtype": "float32",
                "_FillValue": -9999.0,
            }
            for name in dataset.data_vars
            if name != "spatial_ref"
        }
        try:
            dataset.to_netcdf(
                temporary, engine="netcdf4", encoding=encoding
            )
            with xr.open_dataset(temporary, engine="netcdf4") as check:
                check.load()
            os.replace(temporary, destination)
        except Exception as error:
            temporary.unlink(missing_ok=True)
            raise ProcessingError(
                "Unable to publish an ACMAD NetCDF product.",
                [str(destination), str(error)],
            ) from error

    @staticmethod
    def _attach_spatial_metadata(
        dataset: xr.Dataset, spatial_attrs: dict[str, str]
    ) -> xr.Dataset:
        dataset["spatial_ref"] = xr.DataArray(
            0,
            attrs={
                "grid_mapping_name": "latitude_longitude",
                "epsg_code": spatial_attrs["crs"],
                "GeoTransform": spatial_attrs["GeoTransform"],
            },
        )
        for name in dataset.data_vars:
            if name != "spatial_ref":
                dataset[name].attrs["grid_mapping"] = "spatial_ref"
        dataset["latitude"].attrs.update(
            standard_name="latitude",
            long_name="latitude",
            units="degrees_north",
            axis="Y",
        )
        dataset["longitude"].attrs.update(
            standard_name="longitude",
            long_name="longitude",
            units="degrees_east",
            axis="X",
        )
        return dataset

    def _build_product(
        self,
        product_name: str,
        product: dict[str, Any],
        catalog: dict[str, str],
        session: requests.Session,
        run_time: dt.datetime,
        forecast_end: dt.datetime,
    ) -> list[str]:
        product_steps = int(product.get("time_steps", self.time_steps))
        step_hours = int(product.get("time_step_hours", self.time_step_hours))
        expected_times = [
            run_time + dt.timedelta(hours=index * step_hours)
            for index in range(product_steps)
        ]
        variable_templates = product.get("variables") or {}
        if not variable_templates:
            raise ConfigurationError(
                "An ACMAD product has no variables.", [product_name]
            )
        destination_definition = product.get("destination")
        destination_path = self._output_path(destination_definition, run_time)
        destination = Path(destination_path)
        nodata_values = [
            float(value)
            for value in product.get("nodata_values", self.nodata_values)
        ]
        issues: list[str] = []
        raster_paths: dict[str, list[tuple[dt.datetime, Path]]] = {}

        self.log.info(
            " --> Process ACMAD product %s: %d variable(s), %d time step(s)",
            product_name,
            len(variable_templates),
            len(expected_times),
        )
        for variable, filename_template in variable_templates.items():
            raster_paths[variable] = []
            for forecast_time in expected_times:
                filename = self._render(
                    filename_template,
                    run_time,
                    forecast_time,
                    forecast_end,
                )
                url_path = catalog.get(filename)
                if not url_path:
                    issues.append(
                        f"{product_name}.{variable} missing expected file: {filename}"
                    )
                    continue
                file_url = self._render(
                    str(self.file_url),
                    run_time,
                    forecast_time,
                    forecast_end,
                    url_path=url_path,
                    remote_filename=filename,
                )
                ancillary_path = self._output_path(
                    self.raw_destination,
                    run_time,
                    remote_filename=filename,
                )
                try:
                    path = self._download_raster(
                        session, file_url, Path(ancillary_path)
                    )
                    raster_paths[variable].append((forecast_time, path))
                except (DownloadError, DataValidationError) as error:
                    issues.append(f"{product_name}.{variable}: {error}")

        arrays: dict[str, tuple] = {}
        source_files: dict[str, str] = {}
        reference_signature = None
        latitude = longitude = spatial_attrs = None
        for variable in variable_templates:
            dated_paths = raster_paths[variable]
            if len(dated_paths) != len(expected_times):
                continue
            variable_arrays: list[np.ndarray] = []
            try:
                for _, path in dated_paths:
                    values, lat, lon, attrs, signature = self._read_raster(
                        path, reference_signature, nodata_values
                    )
                    if reference_signature is None:
                        reference_signature = signature
                        latitude, longitude, spatial_attrs = lat, lon, attrs
                    variable_arrays.append(values)
                arrays[variable] = (
                    ("time", "latitude", "longitude"),
                    np.stack(variable_arrays),
                )
                source_files[variable] = ",".join(
                    path.name for _, path in dated_paths
                )
            except DataValidationError as error:
                issues.append(f"{product_name}.{variable}: {error}")

        allow_partial = bool(product.get("allow_partial", False))
        if issues and not allow_partial:
            destination.unlink(missing_ok=True)
            return issues
        if not arrays:
            destination.unlink(missing_ok=True)
            issues.append(f"{product_name}: no complete valid variable is available")
            return issues

        dataset = xr.Dataset(
            arrays,
            coords={
                "time": [np.datetime64(value) for value in expected_times],
                "latitude": latitude,
                "longitude": longitude,
            },
            attrs={
                "title": "ACMAD heatwave forecast",
                "institution": (
                    "African Centre of Meteorological Applications for "
                    "Development (ACMAD)"
                ),
                "source": "ACMAD WWFD heatwave GeoTIFF products served by THREDDS",
                "forecast_reference_time": run_time.strftime("%Y-%m-%dT%H:%M:00Z"),
                "forecast_period_end": forecast_end.strftime("%Y-%m-%dT%H:%M:00Z"),
                "product_type": product_name,
                "Conventions": "CF-1.8",
            },
        )
        dataset["time"].attrs["standard_name"] = "time"
        for variable in arrays:
            dataset[variable].attrs.update(
                long_name=variable.replace("_", " "),
                source_files=source_files[variable],
            )
        self._attach_spatial_metadata(dataset, spatial_attrs)
        self._write_atomic(dataset, destination)
        self.log.info(
            " ---> Published %s with %d variable(s): %s",
            product_name,
            len(arrays),
            destination,
        )
        return issues

    def get_data(self, time_range, space_bounds=None, destination=None, options=None):
        if options is not None:
            self.set_options(options)
        run_time = self._reference_time(time_range)
        if not self.catalog_url or not self.file_url:
            raise ConfigurationError(
                "ACMAD THREDDS endpoints are not configured."
            )
        if not isinstance(self.products, dict) or not self.products:
            raise ConfigurationError("No ACMAD heatwave products are configured.")

        forecast_end = run_time + dt.timedelta(
            hours=(int(self.time_steps) - 1) * int(self.time_step_hours)
        )
        session = requests.Session()
        session.headers["User-Agent"] = "door-acmad-heatwave/2.7.4"
        catalog_url = self._render(
            str(self.catalog_url), run_time, run_time, forecast_end
        )
        catalog = self._read_catalog(session, catalog_url)

        issues: list[str] = []
        for product_name, product in self.products.items():
            issues.extend(
                self._build_product(
                    product_name,
                    product,
                    catalog,
                    session,
                    run_time,
                    forecast_end,
                )
            )

        if issues:
            for issue in issues:
                self.log.error(" ERROR %s", issue)
            raise DataUnavailableError(
                "The ACMAD heatwave publication is incomplete.",
                [f"Issues: {len(issues)}", *issues[:10]],
            )

    def _get_data_ts(self, time_range, space_bounds, tmp_path):
        raise NotImplementedError

    def get_last_published_ts(self):
        raise NotImplementedError
