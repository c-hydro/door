"""Optimised JAXA GSMaP binary downloader with GeoTIFF output."""

from __future__ import annotations

import fnmatch
import ftplib
import gzip
import os
from collections.abc import Mapping

import numpy as np
import paramiko
from rasterio.transform import from_origin
from rasterio.windows import Window, from_bounds, transform as window_transform

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


class GSMAPDownloader(RasterDownloader):
    """Download GSMaP gauge/NRT/NOW binary grids and write cropped GeoTIFFs."""

    source = "GSMAP"
    source_aliases = ["JAXA", "GSMAP_JAXA"]
    name = "GSMAPDownloader"

    product_aliases = {
        "gsmap-gauge-standard": "gsmap-gauge",
        "gauge": "gsmap-gauge",
        "gauge-standard": "gsmap-gauge",
        "gauge-nrt": "gsmap-gauge-nrt",
        "gauge-now": "gsmap-gauge-now",
        "now": "gsmap-now",
    }
    available_products = {
        "gsmap-gauge": {
            "ts_per_year": 8760,
            "publication_delay_minutes": 72 * 60,
            "temporal_resolution": "1h",
            "update_frequency": "1h",
            "remote_path_templates": ["/standard/v8/hourly_G/%Y/%m/%d"],
            "remote_filename_templates": [
                "gsmap_gauge.%Y%m%d.%H%M.v8.*.dat.gz",
                "gsmap_gauge.%Y%m%d.%H%M.v8.*.dat",
            ],
            "preliminary": False,
        },
        "gsmap-gauge-nrt": {
            "ts_per_year": 8760,
            "publication_delay_minutes": 5 * 60,
            "temporal_resolution": "1h",
            "update_frequency": "1h",
            "remote_path_templates": [
                "/realtime_ver/v8/hourly_G/%Y/%m/%d",
                "/realtime/hourly_G/%Y/%m/%d",
            ],
            "remote_filename_templates": [
                "gsmap_gauge.%Y%m%d.%H%M.dat.gz",
                "gsmap_gauge.%Y%m%d.%H%M.dat",
            ],
            "preliminary": True,
        },
        "gsmap-gauge-now": {
            "ts_per_year": 8760,
            "publication_delay_minutes": 30,
            "temporal_resolution": "1h",
            "update_frequency": "30min",
            "remote_path_templates": ["/now/half_hour_G/%Y/%m/%d"],
            "remote_filename_templates": [
                "gsmap_gauge_now.%Y%m%d.%H%M.dat.gz",
                "gsmap_gauge_now.%Y%m%d.%H%M.dat",
            ],
            "preliminary": True,
        },
        "gsmap-now": {
            "ts_per_year": 8760,
            "publication_delay_minutes": 30,
            "temporal_resolution": "1h",
            "update_frequency": "30min",
            "remote_path_templates": ["/now/half_hour/%Y/%m/%d"],
            "remote_filename_templates": [
                "gsmap_now.%Y%m%d.%H%M.dat.gz",
                "gsmap_now.%Y%m%d.%H%M.dat",
            ],
            "preliminary": True,
        },
    }
    available_variables = {
        "precipitation": "precipitation_rate",
        "precipitation_rate": "precipitation_rate",
    }

    default_options = {
        **RasterDownloader.default_options,
        "host": "hokusai.eorc.jaxa.jp",
        "port": 21,
        "protocol": "ftp",
        "credentials": {
            "username": None,
            "password": None,
            "netrc_machine": None,
        },
        "remote_path_template": None,
        "remote_filename_template": None,
        "source_dtype": "<f4",
        "source_rows": 1200,
        "source_columns": 3600,
        "source_nodata": -99.0,
        "source_scale_factor": 1.0,
        "output_dtype": "float32",
        "output_nodata": -99.0,
        "compression": "deflate",
        "minimum_file_size": 100000,
    }

    def __init__(self, product: str) -> None:
        super().__init__()
        self.set_product(product)
        self.variable_name = "precipitation_rate"
        self.variable_units = "mm h-1"

    def set_product(self, product: str) -> None:
        key = str(product).strip().lower().replace("_", "-")
        key = self.product_aliases.get(key, key)
        if key not in self.available_products:
            raise ConfigurationError(
                "Unsupported GSMaP product.",
                [f"Value: {product}", f"Available: {sorted(self.available_products)}"],
            )
        self.product = key
        for option, value in self.available_products[key].items():
            setattr(self, option, value)

    def check_options(self, options=None) -> dict:
        checked = super().check_options(options)
        protocol = str(checked["protocol"]).strip().lower()
        if protocol not in {"ftp", "ftps", "sftp"}:
            raise ConfigurationError(
                "Unsupported GSMaP transfer protocol.",
                [f"Value: {checked['protocol']}", "Allowed: ftp, ftps, sftp"],
            )
        checked["protocol"] = protocol
        try:
            checked["port"] = int(checked["port"])
            checked["source_rows"] = int(checked["source_rows"])
            checked["source_columns"] = int(checked["source_columns"])
            checked["source_nodata"] = float(checked["source_nodata"])
            checked["source_scale_factor"] = float(checked["source_scale_factor"])
            np.dtype(checked["source_dtype"])
        except (TypeError, ValueError) as error:
            raise ConfigurationError(
                "Invalid GSMaP binary or connection setting.", [str(error)]
            ) from error
        if checked["port"] <= 0:
            raise ConfigurationError("GSMaP port must be positive.")
        if protocol == "sftp" and checked["port"] == 21:
            checked["port"] = 22
        if checked["source_rows"] <= 0 or checked["source_columns"] <= 0:
            raise ConfigurationError(
                "GSMaP source grid dimensions must be positive."
            )
        if checked["source_scale_factor"] == 0:
            raise ConfigurationError("GSMaP source_scale_factor cannot be zero.")
        if not isinstance(checked.get("credentials"), Mapping):
            raise ConfigurationError("GSMaP credentials must be a mapping.")
        return checked

    def set_variables(self, variables) -> None:
        if isinstance(variables, str):
            variable_key = variables
            specs = {}
        elif isinstance(variables, list):
            if len(variables) != 1:
                raise ConfigurationError("GSMaP supports one precipitation variable.")
            variable_key = variables[0]
            specs = {}
        elif isinstance(variables, Mapping):
            if len(variables) != 1:
                raise ConfigurationError("GSMaP supports one precipitation variable.")
            variable_key, specs = next(iter(variables.items()))
            specs = specs if isinstance(specs, Mapping) else {}
        else:
            raise ConfigurationError("Invalid GSMaP variables section.")

        source_key = str(specs.get("source_name") or variable_key).lower()
        if source_key not in self.available_variables:
            raise ConfigurationError(
                "Unsupported GSMaP variable.",
                [f"Value: {source_key}", "Allowed: precipitation, precipitation_rate"],
            )
        self.variable_name = str(
            specs.get("output_name") or self.available_variables[source_key]
        )
        self.variable_units = str(specs.get("units") or "mm h-1")
        self.variables = {str(variable_key): dict(specs)}

    # Download ---------
    def _download_and_prepare(
        self,
        timestep: TimeStep,
        space_bounds: BoundingBox,
        tmp_path: str,
    ) -> RasterPayload:
        def operation() -> RasterPayload:
            raw_path = self._download_binary(timestep, tmp_path)
            return self._decode_binary(raw_path, timestep, space_bounds)

        return self._run_with_retry(
            operation,
            f"GSMaP {self.product} {timestep.start:%Y-%m-%d %H:%M}",
        )

    def _remote_candidates(self, timestep: TimeStep) -> list[tuple[str, str]]:
        date = timestep.start
        paths = (
            [self.remote_path_template]
            if self.remote_path_template
            else list(self.remote_path_templates)
        )
        if self.remote_filename_template:
            filenames = (
                list(self.remote_filename_template)
                if isinstance(self.remote_filename_template, (list, tuple))
                else [self.remote_filename_template]
            )
        else:
            product_settings = self.available_products[self.product]
            filenames = product_settings.get("remote_filename_templates") or [
                product_settings["remote_filename_template"]
            ]
        return [
            (date.strftime(path), date.strftime(filename))
            for path in paths
            for filename in filenames
        ]

    def _download_binary(self, timestep: TimeStep, tmp_path: str) -> str:
        errors: list[str] = []
        for remote_path, remote_name in self._remote_candidates(timestep):
            try:
                return self._download_remote_file(
                    remote_path, remote_name, tmp_path
                )
            except DataUnavailableError as error:
                errors.append(str(error))
                continue
        raise DataUnavailableError(
            "GSMaP file is not available.",
            [
                f"Product: {self.product}",
                f"Timestep: {timestep.start:%Y-%m-%d %H:%M}",
                *errors,
            ],
        )

    def _download_remote_file(
        self, remote_path: str, remote_name_pattern: str, tmp_path: str
    ) -> str:
        username, password = resolve_basic_credentials(
            self.credentials,
            machine=self.host,
            service="JAXA GSMaP",
            required=True,
        )
        protocol = str(self.protocol).lower()
        if protocol in {"ftp", "ftps"}:
            return self._download_ftp(
                remote_path,
                remote_name_pattern,
                tmp_path,
                username or "anonymous",
                password or "anonymous",
                tls=protocol == "ftps",
            )
        if protocol == "sftp":
            return self._download_sftp(
                remote_path,
                remote_name_pattern,
                tmp_path,
                username,
                password,
            )
        raise ConfigurationError(
            "Unsupported GSMaP transfer protocol.",
            [f"Value: {self.protocol}", "Allowed: ftp, ftps, sftp"],
        )

    def _download_ftp(
        self,
        remote_path: str,
        remote_name_pattern: str,
        tmp_path: str,
        username: str,
        password: str,
        *,
        tls: bool,
    ) -> str:
        client_class = ftplib.FTP_TLS if tls else ftplib.FTP
        client = client_class(timeout=self.timeout_seconds)
        try:
            client.connect(self.host, int(self.port))
            client.login(username, password)
            if tls:
                client.prot_p()
            client.cwd(remote_path)
            remote_name = self._select_remote_name(
                client.nlst(), remote_name_pattern
            )
            local_path = os.path.join(tmp_path, remote_name)
            with open(local_path, "wb") as file_handle:
                client.retrbinary(f"RETR {remote_name}", file_handle.write)
        except ftplib.error_perm as error:
            code = str(error)[:3]
            if code == "550":
                raise DataUnavailableError(
                    "GSMaP remote file or directory was not found.",
                    [f"{remote_path}/{remote_name_pattern}"],
                ) from error
            if code == "530":
                raise authentication_error("JAXA GSMaP", self.host) from error
            raise DownloadError(
                "GSMaP FTP server rejected the request.",
                [f"{remote_path}/{remote_name_pattern}", str(error)],
            ) from error
        except (OSError, EOFError, ftplib.Error) as error:
            raise DownloadError(
                "Unable to download GSMaP data from FTP.",
                [self.host, f"{remote_path}/{remote_name_pattern}", str(error)],
            ) from error
        finally:
            try:
                client.quit()
            except Exception:
                try:
                    client.close()
                except Exception:
                    pass

        self._validate_download(local_path)
        return local_path

    def _download_sftp(
        self,
        remote_path: str,
        remote_name_pattern: str,
        tmp_path: str,
        username: str | None,
        password: str | None,
    ) -> str:
        transport = None
        client = None
        try:
            transport = paramiko.Transport((self.host, int(self.port or 22)))
            transport.banner_timeout = self.timeout_seconds
            transport.connect(username=username, password=password)
            client = paramiko.SFTPClient.from_transport(transport)
            remote_name = self._select_remote_name(
                client.listdir(remote_path), remote_name_pattern
            )
            local_path = os.path.join(tmp_path, remote_name)
            client.get(f"{remote_path.rstrip('/')}/{remote_name}", local_path)
        except FileNotFoundError as error:
            raise DataUnavailableError(
                "GSMaP remote file or directory was not found.",
                [f"{remote_path}/{remote_name_pattern}"],
            ) from error
        except DataUnavailableError:
            raise
        except paramiko.AuthenticationException as error:
            raise authentication_error("JAXA GSMaP", self.host) from error
        except Exception as error:
            raise DownloadError(
                "Unable to download GSMaP data from SFTP.",
                [self.host, f"{remote_path}/{remote_name_pattern}", str(error)],
            ) from error
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
            if transport is not None:
                try:
                    transport.close()
                except Exception:
                    pass

        self._validate_download(local_path)
        return local_path

    @staticmethod
    def _select_remote_name(names: list[str], pattern: str) -> str:
        basename_names = [os.path.basename(name.rstrip("/")) for name in names]
        matches = sorted(name for name in basename_names if fnmatch.fnmatch(name, pattern))
        if not matches:
            raise DataUnavailableError(
                "GSMaP remote filename was not found.", [f"Pattern: {pattern}"]
            )
        return matches[-1]

    def _validate_download(self, path: str) -> None:
        try:
            size = os.path.getsize(path)
        except OSError as error:
            raise DownloadError("GSMaP download was not created.", [path]) from error
        if size < self.minimum_file_size:
            raise DownloadError(
                "GSMaP download is unexpectedly small.",
                [path, f"Size: {size} bytes", f"Minimum: {self.minimum_file_size}"],
            )

    # Binary processing ---------
    def _decode_binary(
        self,
        raw_path: str,
        timestep: TimeStep,
        space_bounds: BoundingBox,
    ) -> RasterPayload:
        try:
            with open(raw_path, "rb") as file_handle:
                signature = file_handle.read(2)
            opener = gzip.open if signature == b"\x1f\x8b" else open
            with opener(raw_path, "rb") as file_handle:
                binary = file_handle.read()
            values = np.frombuffer(binary, dtype=np.dtype(self.source_dtype))
        except (OSError, ValueError) as error:
            raise DataValidationError(
                "Unable to decode the GSMaP binary.", [raw_path, str(error)]
            ) from error

        expected = int(self.source_rows) * int(self.source_columns)
        if values.size != expected:
            raise DataValidationError(
                "GSMaP binary grid has an unexpected size.",
                [f"Expected cells: {expected}", f"Found: {values.size}", raw_path],
            )

        data = values.reshape((int(self.source_rows), int(self.source_columns)))
        # Native longitudes are 0.05..359.95. Roll the grid so the GeoTIFF uses
        # the conventional -180..180 longitude domain used by old DOOR outputs.
        half = int(self.source_columns) // 2
        data = np.concatenate((data[:, half:], data[:, :half]), axis=1)
        transform = from_origin(-180.0, 60.0, 0.1, 0.1)
        cropped, cropped_transform = self._crop_array(
            data, transform, space_bounds
        )

        source_nodata = float(self.source_nodata)
        scale = float(self.source_scale_factor)
        if scale == 0:
            raise ConfigurationError("GSMaP source_scale_factor cannot be zero.")
        output = cropped.astype(np.float32, copy=True)
        missing = np.isclose(output, source_nodata)
        output = output / scale
        output[missing] = float(self.output_nodata)

        return RasterPayload(
            data=output,
            transform=cropped_transform,
            crs="EPSG:4326",
            nodata=float(self.output_nodata),
            dtype=str(self.output_dtype),
            raw_path=raw_path,
            tags={
                "product": self.product,
                "source_product": self.product,
                "variable": self.variable_name,
                "quantity": "rain_rate",
                "units": self.variable_units,
                "temporal_resolution": self.temporal_resolution,
                "update_frequency": self.update_frequency,
                "timestep": timestep.start.isoformat(),
                "PRELIMINARY": str(bool(self.preliminary)),
                "source": "JAXA GSMaP",
            },
        )

    @staticmethod
    def _crop_array(data, transform, bounds: BoundingBox):
        west, south, east, north = [float(value) for value in bounds.bbox]
        if west >= east:
            raise ConfigurationError(
                "GSMaP dateline-crossing bounds are not supported in one output.",
                [repr(bounds.bbox)],
            )
        west = max(-180.0, west)
        east = min(180.0, east)
        south = max(-60.0, south)
        north = min(60.0, north)
        if west >= east or south >= north:
            raise ConfigurationError(
                "Requested bounds do not intersect the GSMaP domain.",
                [repr(bounds.bbox)],
            )

        window = from_bounds(west, south, east, north, transform=transform)
        window = window.round_offsets().round_lengths()
        window = window.intersection(Window(0, 0, data.shape[1], data.shape[0]))
        row_start = int(window.row_off)
        row_end = row_start + int(window.height)
        col_start = int(window.col_off)
        col_end = col_start + int(window.width)
        cropped = data[row_start:row_end, col_start:col_end]
        if cropped.size == 0:
            raise DataValidationError(
                "GSMaP crop produced an empty raster.", [repr(bounds.bbox)]
            )
        return cropped, window_transform(window, transform)
