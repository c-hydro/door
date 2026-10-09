"""ECMWF Open Data deterministic forecast downloader."""

from __future__ import annotations

import datetime as dt
import inspect
import os
import shutil
import time
from copy import deepcopy

import cfgrib
import numpy as np
import xarray as xr
from ecmwf.opendata import Client
import ecmwf.opendata.client as ecmwf_client_module
from multiurl import download as multiurl_download
from multiurl import robust as multiurl_robust

from d3tools.spatial import BoundingBox

from ...base_forecast_downloader import ForecastDownloader
from ...utils.forecast import (
    assign_valid_time,
    compact_error,
    crop_to_bounds,
    decumulate,
    disaggregate_interval_to_hourly,
    drop_grib_coordinates,
    interpolate_hourly,
    relative_humidity_from_dewpoint,
    remove_file_family,
    standardize_lat_lon,
    validate_dataset,
    validate_file,
)
from ...utils.exceptions import ConfigurationError, DataValidationError, ForecastUnavailableError

_ORIGINAL_MULTIURL_DOWNLOAD = multiurl_download
_ORIGINAL_MULTIURL_ROBUST = multiurl_robust


class ECMWFOpenDataDownloader(ForecastDownloader):
    """Download one IFS or AIFS issue from the ECMWF Open Data service."""

    source = "ECMWF_OPEN_DATA"
    source_aliases = ["ecmwf-opendata", "ECMWF", "IFS", "AIFS"]
    name = "ECMWF_OpenData_downloader"
    issue_hours = [0, 6, 12, 18]
    publication_delay_hours = 8

    default_options = {
        "frc_max_step": 144,
        "variables": ["tp", "10u", "10v", "2t", "2d", "ssrd"],
        "providers": ["ecmwf", "aws", "google", "azure"],
        "attempts_by_provider": {"ecmwf": 1, "aws": 2, "google": 1, "azure": 1},
        "retry_seconds": 15.0,
        "timeout_seconds": 180.0,
        "internal_retries": 1,
        "convert_temperature_to_c": True,
        "aggregate_wind_components": True,
        "decumulate_precipitation": True,
        "decumulate_radiation": True,
        "calculate_relative_humidity": True,
        "hourly_output": True,
        "raw_destination": None,
    }

    available_products = {
        "IFS": {
            "model": "ifs",
            "model_frequency_hours": 3,
            "rain_factor": 1000.0,
            "max_main_step": 144,
            "max_intermediate_step": 90,
        },
        "AIFS": {
            "model": "aifs-single",
            "model_frequency_hours": 6,
            "rain_factor": 1.0,
            "max_main_step": 144,
            "max_intermediate_step": 90,
        },
        "AIFS-SINGLE": {
            "model": "aifs-single",
            "model_frequency_hours": 6,
            "rain_factor": 1.0,
            "max_main_step": 144,
            "max_intermediate_step": 90,
        },
    }

    _cfgrib_names = {
        "10u": "u10",
        "10v": "v10",
        "2t": "t2m",
        "2d": "d2m",
    }

    def __init__(self, product: str = "IFS") -> None:
        super().__init__()
        self.set_product(product)

    def set_product(self, product: str) -> None:
        key = product.upper()
        if key == "IFS-HRES":
            key = "IFS"
        if key not in self.available_products:
            raise ConfigurationError(
                "Unsupported ECMWF Open Data product.",
                [f"Value: {product}", f"Available: {sorted(self.available_products)}"],
            )
        self.product = key
        for name, value in self.available_products[key].items():
            setattr(self, name, value)

    def set_variables(self, variables: list[str] | dict[str, str] | str | None) -> None:
        if variables is None:
            variables = deepcopy(self.default_options["variables"])
        if isinstance(variables, str):
            variables = [variables]
        if isinstance(variables, dict):
            self.variable_map = {
                str(remote): str(output)
                for remote, output in variables.items()
                if not str(remote).startswith("__")
            }
        elif isinstance(variables, (list, tuple)):
            self.variable_map = {str(variable): str(variable) for variable in variables}
        else:
            raise ConfigurationError(
                "Invalid ECMWF Open Data variables configuration.",
                ["Expected a mapping, list or string."],
            )
        if not self.variable_map:
            raise ConfigurationError("No ECMWF variables are configured.")
        self.variables = self.variable_map

    def check_options(self, options: dict | None = None) -> dict:
        checked = super().check_options(options)
        try:
            checked["frc_max_step"] = max(1, int(checked["frc_max_step"]))
            checked["internal_retries"] = max(1, int(checked["internal_retries"]))
        except (TypeError, ValueError) as error:
            raise ConfigurationError(
                "Invalid ECMWF Open Data downloader setting.", [str(error)]
            ) from error
        return checked

    @staticmethod
    def _configure_retry_policy(
        max_retries: int,
        retry_after_seconds: float,
        timeout_seconds: float,
    ) -> None:
        """Bound the very long default multiurl retry policy."""

        def short_robust(call, *args, **kwargs):
            return _ORIGINAL_MULTIURL_ROBUST(call, max_retries, retry_after_seconds)

        def short_download(url, target, **kwargs):
            kwargs["retry_after"] = retry_after_seconds
            kwargs["timeout"] = timeout_seconds
            retry_key = "maximum_tries"
            try:
                from multiurl.http import HTTPDownloaderBase

                parameters = inspect.signature(HTTPDownloaderBase.__init__).parameters
                if "maximum_retries" in parameters:
                    retry_key = "maximum_retries"
            except Exception:
                pass
            kwargs[retry_key] = max_retries
            return _ORIGINAL_MULTIURL_DOWNLOAD(url, target=target, **kwargs)

        ecmwf_client_module.robust = short_robust
        ecmwf_client_module.download = short_download

    def _effective_max_step(self, issue_time: dt.datetime) -> int:
        available = (
            self.max_main_step
            if issue_time.hour in (0, 12)
            else self.max_intermediate_step
        )
        if self.frc_max_step > available:
            self.log.warning(
                "Requested forecast horizon %sh limited to %sh for the %02d UTC issue",
                self.frc_max_step,
                available,
                issue_time.hour,
            )
        return min(self.frc_max_step, available)

    def _download_grib(
        self,
        issue_time: dt.datetime,
        steps: list[int],
        destination: str,
    ) -> str:
        self._configure_retry_policy(
            self.internal_retries,
            self.retry_seconds,
            self.timeout_seconds,
        )
        provider_errors: list[str] = []

        for provider_index, provider in enumerate(self.providers, start=1):
            attempts = max(1, int(self.attempts_by_provider.get(provider, 1)))
            for attempt in range(1, attempts + 1):
                remove_file_family(destination)
                self.log.info(
                    "Trying ECMWF provider %s/%s: %s (attempt %s/%s)",
                    provider_index,
                    len(self.providers),
                    provider,
                    attempt,
                    attempts,
                )
                try:
                    client = Client(
                        source=provider,
                        model=self.model,
                        resol="0p25",
                        preserve_request_order=False,
                        infer_stream_keyword=True,
                    )
                    client.retrieve(
                        type="fc",
                        date=issue_time.strftime("%Y%m%d"),
                        time=issue_time.hour,
                        step=steps,
                        param=list(self.variable_map),
                        target=destination,
                    )
                    validate_file(destination, min_size=1000)
                    with open(destination, "rb") as stream:
                        if stream.read(4) != b"GRIB":
                            raise DataValidationError(
                                "ECMWF provider returned a non-GRIB file.",
                                [f"Provider: {provider}", f"File: {destination}"],
                            )
                    self.log.info("ECMWF forecast downloaded from %s", provider)
                    return destination
                except Exception as error:
                    remove_file_family(destination)
                    summary = f"{provider} attempt {attempt}: {compact_error(error, 350)}"
                    provider_errors.append(summary)
                    self.log.warning(summary)
                    if attempt < attempts:
                        time.sleep(self.retry_seconds * attempt)

        raise ForecastUnavailableError(
            "ECMWF forecast is unavailable from all configured providers.",
            [
                f"Model: {self.model}",
                f"Run: {issue_time:%Y-%m-%d %H:%M UTC}",
                *provider_errors[-8:],
            ],
        )

    def _open_grib(self, grib_path: str, issue_time: dt.datetime) -> xr.Dataset:
        datasets = cfgrib.open_datasets(
            grib_path,
            backend_kwargs={"indexpath": ""},
        )
        loaded: list[xr.Dataset] = []
        try:
            for dataset in datasets:
                dataset = assign_valid_time(dataset, issue_time)
                loaded.append(dataset.load())
        finally:
            for dataset in datasets:
                dataset.close()

        if not loaded:
            raise DataValidationError("The ECMWF GRIB file contains no readable datasets.")

        merged = xr.merge(loaded, compat="override", join="outer")
        rename: dict[str, str] = {}
        for remote_name, output_name in self.variable_map.items():
            internal_name = self._cfgrib_names.get(remote_name, remote_name)
            if internal_name in merged.data_vars:
                rename[internal_name] = output_name
            elif remote_name in merged.data_vars:
                rename[remote_name] = output_name
            else:
                raise DataValidationError(
                    "A requested ECMWF variable is missing from the GRIB file.",
                    [
                        f"Requested: {remote_name}",
                        f"Available: {list(merged.data_vars)}",
                    ],
                )
        return merged[list(rename)].rename(rename)

    def _postprocess(self, data: xr.Dataset, issue_time: dt.datetime, max_step: int) -> xr.Dataset:
        interval_variables: list[str] = []
        variable_map = self.variable_map

        if "tp" in variable_map:
            name = variable_map["tp"]
            data[name] = data[name] * self.rain_factor
            if self.decumulate_precipitation:
                data[name] = decumulate(data[name]) / float(self.model_frequency_hours)
                data[name] = data[name].where(data[name] >= 0, 0)
            data[name].attrs["units"] = "mm h-1"
            # Total precipitation represents the interval ending at each
            # forecast timestamp: hourly expansion must use back-fill semantics.
            interval_variables.append(name)

        if "2t" in variable_map and self.convert_temperature_to_c:
            name = variable_map["2t"]
            data[name] = data[name] - 273.15
            data[name].attrs.update(
                long_name="2 metre temperature",
                units="C",
                standard_name="air_temperature",
            )

        if (
            "2t" in variable_map
            and "2d" in variable_map
            and self.calculate_relative_humidity
        ):
            temperature = data[variable_map["2t"]]
            if not self.convert_temperature_to_c:
                temperature = temperature - 273.15
            dewpoint = data[variable_map["2d"]] - 273.15
            data["rh"] = relative_humidity_from_dewpoint(temperature, dewpoint)
            data["rh"].attrs.update(
                long_name="relative humidity",
                units="%",
                standard_name="relative_humidity",
            )

        if (
            "10u" in variable_map
            and "10v" in variable_map
            and self.aggregate_wind_components
        ):
            data["10wind"] = np.sqrt(
                data[variable_map["10u"]] ** 2 + data[variable_map["10v"]] ** 2
            )
            data["10wind"].attrs.update(
                long_name="10 m wind",
                units="m s-1",
                standard_name="wind_speed",
            )

        if "ssrd" in variable_map and self.decumulate_radiation:
            name = variable_map["ssrd"]
            data[name] = decumulate(data[name]) / (3600.0 * self.model_frequency_hours)
            data[name] = data[name].where(data[name] >= 0, 0)
            data[name].attrs["units"] = "W m-2"
            interval_variables.append(name)

        if self.hourly_output:
            interval_variables = list(dict.fromkeys(interval_variables))
            instant_variables = [
                name for name in data.data_vars if name not in interval_variables
            ]
            parts: list[xr.Dataset] = []

            if instant_variables:
                parts.append(
                    interpolate_hourly(
                        data[instant_variables],
                        issue_time,
                        max_step,
                        method="nearest",
                    )
                )

            if interval_variables:
                parts.append(
                    xr.Dataset(
                        {
                            name: disaggregate_interval_to_hourly(
                                data[name],
                                issue_time,
                                max_step,
                            )
                            for name in interval_variables
                        }
                    )
                )

            data = xr.merge(parts, compat="override", join="exact")

        return validate_dataset(drop_grib_coordinates(data))

    def _download_run(
        self,
        issue_time: dt.datetime,
        space_bounds: BoundingBox,
        tmp_path: str,
    ) -> xr.Dataset:
        if issue_time.hour not in self.issue_hours:
            raise ForecastUnavailableError(
                "Invalid ECMWF issue hour.",
                [
                    f"Requested: {issue_time:%Y-%m-%d %H:%M UTC}",
                    f"Available hours: {self.issue_hours}",
                ],
            )

        max_step = self._effective_max_step(issue_time)
        steps = list(range(self.model_frequency_hours, max_step + 1, self.model_frequency_hours))
        self.log.info(
            " ---> ECMWF request: model=%s, %s variables, %s forecast steps",
            self.model,
            len(self.variable_map),
            len(steps),
        )
        grib_path = os.path.join(
            tmp_path,
            f"ecmwf_{self.model}_{issue_time:%Y%m%d%H}.grib2",
        )
        self.log.info(" ---> Download ECMWF forecast from configured providers...")
        self._download_grib(issue_time, steps, grib_path)
        if self.raw_destination:
            raw_path = self._resolve_output_destination(
                self.raw_destination,
                issue_time,
            )
            raw_folder = os.path.dirname(raw_path)
            if raw_folder:
                os.makedirs(raw_folder, exist_ok=True)
            shutil.copy2(grib_path, raw_path)
            self.log.info(" ----> Raw ECMWF GRIB saved to %s", raw_path)
        self.log.info(" ---> Download ECMWF forecast from configured providers...DONE")
        self.log.info(" ---> Decode and merge ECMWF GRIB fields...")
        data = self._open_grib(grib_path, issue_time)
        self.log.info(" ---> Decode and merge ECMWF GRIB fields...DONE")
        self.log.info(" ---> Postprocess ECMWF variables...")
        data = standardize_lat_lon(data)
        data = crop_to_bounds(data, space_bounds)
        data = self._postprocess(data, issue_time, max_step)
        self.log.info(" ---> Postprocess ECMWF variables...DONE")
        return data
