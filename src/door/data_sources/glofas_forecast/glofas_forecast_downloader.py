"""Operational GLOFAS forecast downloader from the Copernicus EWDS API."""

from __future__ import annotations

import datetime as dt
import os
import shutil
import ssl
import subprocess
import time
from collections.abc import Generator

import cdsapi
import numpy as np
import requests
import xarray as xr
from requests.adapters import HTTPAdapter

from d3tools.spatial import BoundingBox
from d3tools.timestepping.timestep import TimeStep

from ...base_forecast_downloader import ForecastDownloader
from ...utils.forecast import (
    crop_to_bounds,
    standardize_lat_lon,
    validate_dataset,
    validate_file,
)
from ...utils.exceptions import (
    ConfigurationError,
    DataValidationError,
    DownloadError,
    ExternalToolError,
    ForecastUnavailableError,
    ProcessingError,
)

EWDS_URL = "https://ewds.climate.copernicus.eu/api"
TEMPORARY_STATUS_CODES = {429, 500, 502, 503, 504}


class _TLS12Adapter(HTTPAdapter):
    def __init__(self, cipher: str, *args, **kwargs) -> None:
        self.cipher = cipher
        super().__init__(*args, **kwargs)

    def _context(self) -> ssl.SSLContext:
        context = ssl.create_default_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.maximum_version = ssl.TLSVersion.TLSv1_2
        context.set_ciphers(self.cipher)
        return context

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["ssl_context"] = self._context()
        return super().init_poolmanager(connections, maxsize, block=block, **pool_kwargs)

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        proxy_kwargs["ssl_context"] = self._context()
        return super().proxy_manager_for(proxy, **proxy_kwargs)


class GLOFASForecastDownloader(ForecastDownloader):
    """Download GLOFAS v4 forecasts and write one ensemble mean per lead time."""

    source = "GLOFAS_FORECAST"
    source_aliases = ["glofas-forecast", "EWDS_GLOFAS", "GLOFAS_V4_FORECAST"]
    name = "GLOFAS_Forecast_downloader"
    issue_hours = [0]
    publication_delay_hours = 8

    available_products = {"CEMS-GLOFAS-FORECAST": {}}

    default_options = {
        "dataset": "cems-glofas-forecast",
        "api_url": None,
        "time_steps": ["24", "48", "72", "96", "120"],
        "ensemble_members": 51,
        "variable": "river_discharge_in_the_last_24_hours",
        "system_version": "operational",
        "hydrological_model": "lisflood",
        "product_type": ["control_forecast", "ensemble_perturbed_forecasts"],
        "processing_backend": "cdo",
        "cdo_path": "cdo",
        "grib_copy_path": "grib_copy",
        "retry_attempts": 5,
        "retry_seconds": 180.0,
        "force_tls12_cipher": True,
        "tls12_cipher": "ECDHE-RSA-AES128-GCM-SHA256",
        "raw_destination": None,
        "standardize_coordinates": False,
    }

    def __init__(self, product: str = "cems-glofas-forecast") -> None:
        super().__init__()
        key = product.upper()
        if key not in self.available_products:
            raise ConfigurationError(
                "Unsupported GLOFAS forecast product.",
                [f"Value: {product}", f"Available: {sorted(self.available_products)}"],
            )
        self.product = key

    def set_variables(self, variables) -> None:
        # The operational API exposes one discharge variable in this workflow.
        if variables is None:
            return
        if isinstance(variables, str):
            self.variable = variables
        elif isinstance(variables, dict):
            selected = [key for key in variables if not str(key).startswith("__")]
            if not selected:
                raise ConfigurationError("No GLOFAS forecast variable is configured.")
            self.variable = str(selected[0])
        elif isinstance(variables, (list, tuple)) and variables:
            self.variable = str(variables[0])
        else:
            raise ConfigurationError(
                "Invalid GLOFAS forecast variables configuration.",
                ["Expected a string, mapping or non-empty list."],
            )
        self.variables = [self.variable]

    def check_options(self, options: dict | None = None) -> dict:
        checked = super().check_options(options)
        try:
            checked["time_steps"] = [str(int(step)) for step in checked["time_steps"]]
            checked["ensemble_members"] = max(1, int(checked["ensemble_members"]))
            checked["retry_attempts"] = max(1, int(checked["retry_attempts"]))
        except (TypeError, ValueError) as error:
            raise ConfigurationError(
                "Invalid GLOFAS forecast downloader setting.", [str(error)]
            ) from error
        backend = str(checked["processing_backend"]).lower()
        if backend not in ("cdo", "xarray"):
            raise ConfigurationError(
                "Unsupported GLOFAS processing backend.",
                [f"Value: {checked['processing_backend']}", "Allowed: cdo, xarray"],
            )
        checked["processing_backend"] = backend
        return checked

    @staticmethod
    def _resolve_executable(path: str, executable: str) -> str:
        return os.path.join(path, executable) if os.path.isdir(path) else path

    def _configure_client_session(self, client: object) -> None:
        if not self.force_tls12_cipher:
            return
        adapter = _TLS12Adapter(self.tls12_cipher)
        candidates = [client]
        for name in ("client", "_client", "api", "_api"):
            candidate = getattr(client, name, None)
            if candidate is not None:
                candidates.append(candidate)
        for candidate in candidates:
            session = getattr(candidate, "session", None)
            if isinstance(session, requests.Session):
                session.mount(self.api_url or EWDS_URL, adapter)

    def _build_request(self, issue_time: dt.datetime, bounds: BoundingBox) -> dict:
        west, south, east, north = (float(value) for value in bounds.bbox)
        return {
            "system_version": [self.system_version],
            "variable": self.variable,
            "hydrological_model": [self.hydrological_model],
            "data_format": "grib2",
            "download_format": "unarchived",
            "product_type": list(self.product_type),
            "year": [str(issue_time.year)],
            "month": [f"{issue_time.month:02d}"],
            "day": [f"{issue_time.day:02d}"],
            "leadtime_hour": list(self.time_steps),
            "area": [north, west, south, east],
        }

    @staticmethod
    def _is_unpublished(error: BaseException, issue_time: dt.datetime) -> bool:
        response = getattr(error, "response", None)
        if getattr(response, "status_code", None) not in (400, 404):
            return False
        text = (getattr(response, "text", "") or "").lower()
        invalid = (
            "invalid request" in text
            or "valid combination of values" in text
            or "not produced a valid combination" in text
        )
        return invalid and issue_time.date() >= dt.datetime.now(dt.timezone.utc).date()

    @staticmethod
    def _is_temporary(error: BaseException) -> bool:
        status = getattr(getattr(error, "response", None), "status_code", None)
        text = str(error).lower()
        return (
            status in TEMPORARY_STATUS_CODES
            or isinstance(
                error,
                (
                    requests.exceptions.SSLError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                ),
            )
            or "unexpected_eof_while_reading" in text
            or "ssl_error_syscall" in text
            or "max retries exceeded" in text
            or "bad gateway" in text
            or "service temporarily unavailable" in text
        )

    def _retrieve(self, issue_time: dt.datetime, bounds: BoundingBox, target: str) -> str:
        request = self._build_request(issue_time, bounds)
        try:
            client = cdsapi.Client(url=self.api_url) if self.api_url else cdsapi.Client()
        except Exception as error:
            raise ConfigurationError(
                "GLOFAS EWDS API credentials are not available or invalid.",
                ["Check the CDS/EWDS API client configuration.", str(error)],
            ) from error
        self._configure_client_session(client)
        last_error: BaseException | None = None

        for attempt in range(1, self.retry_attempts + 1):
            try:
                if os.path.exists(target):
                    os.remove(target)
                self.log.info(
                    "Downloading GLOFAS forecast from EWDS (attempt %s/%s)",
                    attempt,
                    self.retry_attempts,
                )
                client.retrieve(self.dataset, request, target)
                validate_file(target, min_size=1000)
                if self.raw_destination:
                    raw_path = self._resolve_output_destination(
                        self.raw_destination,
                        issue_time,
                    )
                    raw_folder = os.path.dirname(raw_path)
                    if raw_folder:
                        os.makedirs(raw_folder, exist_ok=True)
                    shutil.copy2(target, raw_path)
                return target
            except Exception as error:
                last_error = error
                try:
                    if os.path.exists(target):
                        os.remove(target)
                except OSError:
                    pass
                if self._is_unpublished(error, issue_time):
                    raise ForecastUnavailableError(
                        "GLOFAS forecast is not available yet.",
                        [f"Requested date: {issue_time:%Y-%m-%d}"],
                    ) from error
                if self._is_temporary(error) and attempt < self.retry_attempts:
                    self.log.warning("Temporary EWDS error: %s", error)
                    time.sleep(self.retry_seconds * attempt)
                    continue
                break

        if last_error is not None and self._is_temporary(last_error):
            raise DownloadError(
                "GLOFAS download failed because EWDS or the TLS connection is temporarily unavailable.",
                [f"Date: {issue_time:%Y-%m-%d}", str(last_error)],
            ) from last_error
        raise DownloadError(
            "GLOFAS forecast download failed.",
            [f"Date: {issue_time:%Y-%m-%d}", str(last_error)],
        ) from last_error


    def _prepare_output(
        self,
        dataset: xr.Dataset,
        bounds: BoundingBox,
    ) -> xr.Dataset:
        output = dataset
        if self.standardize_coordinates:
            output = crop_to_bounds(standardize_lat_lon(output), bounds)
        return validate_dataset(output.load())

    def _process_cdo(
        self,
        grib_path: str,
        bounds: BoundingBox,
        tmp_path: str,
    ) -> Generator[tuple[xr.Dataset, dict], None, None]:
        cdo = self._resolve_executable(self.cdo_path, "cdo")
        grib_copy = self._resolve_executable(self.grib_copy_path, "grib_copy")
        for executable in (cdo, grib_copy):
            if shutil.which(executable) is None and not os.path.isfile(executable):
                raise ExternalToolError(
                    "A required GLOFAS executable was not found.",
                    [f"Executable: {executable}"],
                )

        split_template = os.path.join(
            tmp_path, "glofas_fc_[perturbationNumber]_[step].grb"
        )
        try:
            subprocess.run(
                [grib_copy, grib_path, split_template],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except subprocess.CalledProcessError as error:
            raise ProcessingError(
                "Unable to split the GLOFAS GRIB fieldset.",
                [error.stdout[-1000:] if error.stdout else str(error)],
            ) from error
        split_template = split_template.replace("[", "{").replace("]", "}")

        for lead_time in self.time_steps:
            member_files: list[str] = []
            for member in range(self.ensemble_members):
                input_grib = split_template.format(
                    perturbationNumber=member,
                    step=lead_time,
                )
                if not os.path.isfile(input_grib):
                    raise DataValidationError(
                        "A GLOFAS ensemble member is missing after GRIB splitting.",
                        [f"Lead time: {lead_time}", f"Member: {member}", input_grib],
                    )
                member_dir = os.path.join(tmp_path, f"ens_{member}")
                os.makedirs(member_dir, exist_ok=True)
                member_nc = os.path.join(
                    member_dir,
                    f"glofas_fc_{member}_time_{lead_time}.nc",
                )
                try:
                    subprocess.run(
                        [cdo, "-O", "-s", "-f", "nc", "copy", input_grib, member_nc],
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                except subprocess.CalledProcessError as error:
                    raise ProcessingError(
                        "CDO failed while converting a GLOFAS ensemble member.",
                        [
                            f"Lead time: {lead_time}",
                            f"Member: {member}",
                            error.stdout[-1000:] if error.stdout else str(error),
                        ],
                    ) from error
                member_files.append(member_nc)

            mean_file = os.path.join(tmp_path, f"glofas_fc_avg_time_{lead_time}.nc")
            try:
                subprocess.run(
                    [cdo, "-O", "-s", "ensmean", *member_files, mean_file],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
            except subprocess.CalledProcessError as error:
                raise ProcessingError(
                    "CDO failed while computing the GLOFAS ensemble mean.",
                    [
                        f"Lead time: {lead_time}",
                        error.stdout[-1000:] if error.stdout else str(error),
                    ],
                ) from error
            self.log.info(" ----> GLOFAS lead time: %s h", lead_time)
            with xr.open_dataset(mean_file) as dataset:
                output = self._prepare_output(dataset, bounds)
            yield output, {"lead_time": str(lead_time)}

    def _process_xarray(
        self,
        grib_path: str,
        bounds: BoundingBox,
    ) -> Generator[tuple[xr.Dataset, dict], None, None]:
        import cfgrib

        datasets = cfgrib.open_datasets(
            grib_path,
            backend_kwargs={"indexpath": ""},
        )
        try:
            candidates = [
                dataset
                for dataset in datasets
                if "number" in dataset.dims and "step" in dataset.dims
            ]
            if not candidates:
                raise DataValidationError(
                    "No GLOFAS ensemble cube with number/step dimensions was found.",
                    [f"Cubes: {len(datasets)}"],
                )
            dataset = xr.merge([candidate.load() for candidate in candidates], compat="override")
        finally:
            for item in datasets:
                item.close()

        mean = dataset.mean(dim="number")
        for lead_time in self.time_steps:
            target = np.timedelta64(int(lead_time), "h")
            selected = mean.sel(step=target)
            selected = selected.drop_vars(["time", "valid_time", "step"], errors="ignore")
            yield self._prepare_output(selected, bounds), {"lead_time": str(lead_time)}

    def _get_data_ts(
        self,
        timestep: TimeStep,
        space_bounds: BoundingBox,
        tmp_path: str,
    ) -> Generator[tuple[xr.Dataset, dict], None, None]:
        issue_time = timestep.start
        if issue_time.hour not in self.issue_hours:
            raise ForecastUnavailableError(
                "Invalid GLOFAS issue hour.",
                [f"Requested: {issue_time:%Y-%m-%d %H:%M UTC}"],
            )
        self.log.info(
            " --> Process GLOFAS run %s UTC...",
            issue_time.strftime("%Y-%m-%d %H:%M"),
        )
        self.log.info(
            " ---> GLOFAS request: %s lead times, %s ensemble members, backend=%s",
            len(self.time_steps),
            self.ensemble_members,
            self.processing_backend,
        )
        grib_path = os.path.join(tmp_path, f"glofas_fc_{issue_time:%Y%m%d%H%M}.grib")
        self.log.info(" ---> Download GLOFAS forecast from EWDS...")
        self._retrieve(issue_time, space_bounds, grib_path)
        self.log.info(" ---> Download GLOFAS forecast from EWDS...DONE")
        self.log.info(" ---> Compute GLOFAS ensemble means...")
        if self.processing_backend == "cdo":
            outputs = self._process_cdo(grib_path, space_bounds, tmp_path)
        else:
            outputs = self._process_xarray(grib_path, space_bounds)
        for data, tags in outputs:
            # Every lead time has a different valid-time coordinate. Refresh
            # the writer template before each output so d3tools does not align
            # later leads to the first (24-hour) product and fill them with NaN.
            if hasattr(self.destination, "set_template"):
                self.destination.set_template(data)
            yield data, tags
        self.log.info(" ---> Compute GLOFAS ensemble means...DONE")
        self.log.info(
            " --> Process GLOFAS run %s UTC...DONE",
            issue_time.strftime("%Y-%m-%d %H:%M"),
        )

    def _download_run(self, issue_time, space_bounds, tmp_path):
        raise NotImplementedError(
            "GLOFAS forecast yields one dataset per lead time; use get_data()."
        )
