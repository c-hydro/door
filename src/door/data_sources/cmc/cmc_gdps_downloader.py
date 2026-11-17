"""Environment and Climate Change Canada GDPS downloader."""

from __future__ import annotations

import datetime as dt
import html
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from urllib.parse import unquote, urljoin, urlparse

import numpy as np
import requests
import xarray as xr

from d3tools.spatial import BoundingBox

from ...base_forecast_downloader import ForecastDownloader
from ...utils.forecast import (
    crop_to_bounds,
    decumulate,
    disaggregate_interval_to_hourly,
    drop_grib_coordinates,
    interpolate_hourly,
    standardize_lat_lon,
    validate_dataset,
)
from ...utils.exceptions import (
    ConfigurationError,
    DataValidationError,
    DownloadError,
    ForecastUnavailableError,
    VariableUnavailableError,
)

REQUEST_HEADERS = {
    "User-Agent": "CIMA-DOOR-GDPS/2.0 (+https://www.cimafoundation.org)"
}
TRANSIENT_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}

DEFAULT_REMOTE_FIELDS = {
    "APCP_SFC_0": {
        "kind": "precipitation",
        "rules": [
            ["apcp-accum3h", "_sfc_"],
            ["precip-accum3h", "_sfc_"],
            ["precipaccum3h", "_sfc_"],
            ["precipitationaccumulation", "_sfc_"],
            ["totalprecipitation", "_sfc_"],
            ["apcp", "_sfc_"],
        ],
        "exclude": ["prob", "type", "rate", "snow", "convective"],
    },
    "TMP_TGL_2": {
        "kind": "temperature",
        "rules": [["airtemp", "agl-2m"], ["tmp", "agl-2m"], ["tmp_tgl_2"]],
        "exclude": [],
    },
    "UGRD_TGL_10": {
        "kind": "wind_u",
        "rules": [["windu", "agl-10m"], ["ugrd", "agl-10m"], ["ugrd_tgl_10"]],
        "exclude": [],
    },
    "VGRD_TGL_10": {
        "kind": "wind_v",
        "rules": [["windv", "agl-10m"], ["vgrd", "agl-10m"], ["vgrd_tgl_10"]],
        "exclude": [],
    },
    "RH_TGL_2": {
        "kind": "relative_humidity",
        "rules": [["relhum", "agl-2m"], ["rh", "agl-2m"], ["rh_tgl_2"]],
        "exclude": [],
    },
    "DSWRF_SFC_0": {
        "kind": "radiation",
        "rules": [
            ["downwardshortwaveradiationflux", "ntat"],
            ["dswrf", "ntat"],
            ["downwardshortwaveradiationflux", "sfc"],
            ["dswrf", "sfc"],
            ["dswrf_sfc_0"],
        ],
        "exclude": [],
    },
}


def _unique(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            output.append(value)
            seen.add(value)
    return output


def _validate_grib(path: str) -> None:
    if not os.path.isfile(path) or os.path.getsize(path) < 16:
        raise ValueError("downloaded GRIB is missing or too small")
    with open(path, "rb") as stream:
        if stream.read(4) != b"GRIB":
            raise ValueError("downloaded file has no GRIB signature")


def _parse_accumulation_hours(filename: str) -> int | None:
    match = re.search(r"accum(?:ulated)?[-_ ]?(\d{1,3})h", filename, re.IGNORECASE)
    return int(match.group(1)) if match else None


class CMCGDPSDownloader(ForecastDownloader):
    """Download the 0.15 degree GDPS forecast from ECCC Datamart mirrors."""

    source = "CMC_GDPS"
    source_aliases = ["CMC", "GDPS", "GDPS0p15"]
    name = "CMC_GDPS_downloader"
    supports_ancillary = False
    issue_hours = [0, 12]
    publication_delay_hours = 7

    available_products = {"GDPS0P15": {}}

    default_options = {
        "frc_max_step": 120,
        "variables": {
            "APCP_SFC_0": "tp",
            "UGRD_TGL_10": "10u",
            "VGRD_TGL_10": "10v",
        },
        "download_workers": 1,
        "download_attempts": 3,
        "retry_seconds": 5.0,
        "timeout_seconds": 180.0,
        "convert_temperature_to_c": True,
        "aggregate_wind_components": True,
        "decumulate_precipitation": True,
        "decumulate_radiation": True,
        "hourly_output": True,
    }

    def __init__(self, product: str = "GDPS0p15") -> None:
        super().__init__()
        key = product.upper()
        if key not in self.available_products:
            raise ConfigurationError(
                "Unsupported CMC GDPS product.",
                [f"Value: {product}", f"Available: {sorted(self.available_products)}"],
            )
        self.product = key

    @staticmethod
    def _parse_variable_spec(key: str, value: object) -> dict:
        default = DEFAULT_REMOTE_FIELDS.get(key)
        if isinstance(value, str):
            if default is None:
                raise ConfigurationError(
                    "No default GDPS remote mapping exists for a variable.",
                    [f"Variable: {key}"],
                )
            return {
                "output_name": value,
                "kind": default["kind"],
                "rules": deepcopy(default["rules"]),
                "exclude": deepcopy(default.get("exclude", [])),
            }
        if not isinstance(value, dict):
            raise ConfigurationError(
                "Invalid GDPS variable configuration.",
                [f"Variable: {key}", "Expected a string or object."],
            )
        output_name = value.get("output_name") or value.get("name")
        if not output_name:
            raise ConfigurationError(
                "Missing output_name in GDPS variable configuration.",
                [f"Variable: {key}"],
            )
        raw_patterns = value.get("remote_patterns")
        if raw_patterns is None:
            if default is None:
                raise ConfigurationError(
                    "Missing remote_patterns in GDPS variable configuration.",
                    [f"Variable: {key}"],
                )
            rules = deepcopy(default["rules"])
        else:
            rules = []
            for pattern in raw_patterns:
                if isinstance(pattern, str):
                    rules.append([pattern])
                elif isinstance(pattern, list) and all(
                    isinstance(token, str) for token in pattern
                ):
                    rules.append(pattern)
                else:
                    raise ConfigurationError(
                        "Invalid GDPS remote_patterns entry.",
                        [f"Variable: {key}", repr(pattern)],
                    )
        return {
            "output_name": str(output_name),
            "kind": value.get("kind") or (default["kind"] if default else "generic"),
            "rules": rules,
            "exclude": value.get("exclude")
            or (deepcopy(default.get("exclude", [])) if default else []),
        }

    def set_variables(self, variables: dict | list[str] | str | None) -> None:
        if variables is None:
            variables = deepcopy(self.default_options["variables"])
        if isinstance(variables, str):
            variables = [variables]
        if isinstance(variables, list):
            variables = {key: key for key in variables}
        if not isinstance(variables, dict):
            raise ConfigurationError(
                "Invalid CMC GDPS variables configuration.",
                ["Expected a mapping, list or string."],
            )
        self.variable_specs = {
            str(key): self._parse_variable_spec(str(key), value)
            for key, value in variables.items()
            if not str(key).startswith("__")
        }
        if not self.variable_specs:
            raise ConfigurationError("No CMC GDPS variables are configured.")
        self.variables = self.variable_specs

    def check_options(self, options: dict | None = None) -> dict:
        checked = super().check_options(options)
        try:
            checked["frc_max_step"] = min(240, max(3, int(checked["frc_max_step"])))
            checked["download_workers"] = max(1, int(checked["download_workers"]))
            checked["download_attempts"] = max(1, int(checked["download_attempts"]))
        except (TypeError, ValueError) as error:
            raise ConfigurationError(
                "Invalid CMC GDPS downloader setting.", [str(error)]
            ) from error
        return checked

    @staticmethod
    def _directory_candidates(issue_time: dt.datetime, step: int) -> list[str]:
        date = issue_time.strftime("%Y%m%d")
        hour = issue_time.strftime("%H")
        lead = f"{step:03d}"
        return [
            f"https://dd.weather.gc.ca/{date}/WXO-DD/model_gdps/15km/{hour}/{lead}/",
            f"https://dd.weather.gc.ca/today/model_gdps/15km/{hour}/{lead}/",
            f"http://hpfx.collab.science.gc.ca/{date}/WXO-DD/model_gdps/15km/{hour}/{lead}/",
            f"http://hpfx.collab.science.gc.ca/today/model_gdps/15km/{hour}/{lead}/",
        ]

    def _fetch_listing(self, candidates: list[str]) -> tuple[str | None, list[str], list[str]]:
        diagnostics: list[str] = []
        for url in candidates:
            host = urlparse(url).netloc
            try:
                response = requests.get(
                    url,
                    headers=REQUEST_HEADERS,
                    timeout=(20, self.timeout_seconds),
                )
                if response.status_code == 404:
                    diagnostics.append(f"{host}: HTTP 404")
                    continue
                if response.status_code in (401, 403):
                    diagnostics.append(f"{host}: HTTP {response.status_code} access denied")
                    continue
                response.raise_for_status()
                hrefs = re.findall(
                    r'href=["\']([^"\']+\.grib2)["\']',
                    response.text,
                    flags=re.IGNORECASE,
                )
                filenames = []
                for href in hrefs:
                    path = urlparse(unquote(html.unescape(href))).path
                    name = os.path.basename(path)
                    if name.lower().endswith(".grib2"):
                        filenames.append(name)
                filenames = _unique(filenames)
                if filenames:
                    return url, filenames, diagnostics
                diagnostics.append(f"{host}: directory contains no GRIB2 files")
            except requests.RequestException as error:
                diagnostics.append(f"{host}: {error.__class__.__name__}: {error}")
        return None, [], diagnostics

    @staticmethod
    def _select_filename(filenames: list[str], spec: dict) -> str | None:
        excludes = [token.lower() for token in spec.get("exclude", [])]
        for rule in spec["rules"]:
            tokens = [token.lower() for token in rule]
            matches = [
                filename
                for filename in filenames
                if all(token in filename.lower() for token in tokens)
                and not any(token in filename.lower() for token in excludes)
            ]
            if matches:
                return sorted(matches, key=lambda value: (len(value), value))[0]
        return None

    def _download_task(self, task: dict) -> dict:
        local_path = task["local_path"]
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        errors: list[str] = []
        for url in task["urls"]:
            host = urlparse(url).netloc
            for attempt in range(1, self.download_attempts + 1):
                part = local_path + ".part"
                for candidate in (local_path, part):
                    try:
                        if os.path.exists(candidate):
                            os.remove(candidate)
                    except OSError:
                        pass
                try:
                    with requests.get(
                        url,
                        headers=REQUEST_HEADERS,
                        stream=True,
                        timeout=(20, self.timeout_seconds),
                    ) as response:
                        if response.status_code == 404:
                            errors.append(f"{host}: HTTP 404")
                            break
                        if response.status_code in (401, 403):
                            errors.append(f"{host}: HTTP {response.status_code}")
                            break
                        if response.status_code in TRANSIENT_HTTP_CODES:
                            raise requests.HTTPError(
                                f"HTTP {response.status_code} {response.reason}",
                                response=response,
                            )
                        response.raise_for_status()
                        content_type = (response.headers.get("Content-Type") or "").lower()
                        if "html" in content_type or "xml" in content_type:
                            raise ValueError(f"server returned {content_type} instead of GRIB")
                        with open(part, "wb") as stream:
                            for chunk in response.iter_content(1024 * 1024):
                                if chunk:
                                    stream.write(chunk)
                    _validate_grib(part)
                    os.replace(part, local_path)
                    return {**task, "ok": True, "url": url}
                except Exception as error:
                    errors.append(
                        f"{host} attempt {attempt}/{self.download_attempts}: "
                        f"{error.__class__.__name__}: {error}"
                    )
                    status = getattr(getattr(error, "response", None), "status_code", None)
                    retryable = isinstance(
                        error, (requests.Timeout, requests.ConnectionError)
                    ) or status in TRANSIENT_HTTP_CODES
                    if retryable and attempt < self.download_attempts:
                        time.sleep(self.retry_seconds * attempt)
                        continue
                    break
        return {**task, "ok": False, "error": " | ".join(errors[-6:])}

    def _resolve_tasks(
        self,
        issue_time: dt.datetime,
        steps: list[int],
        tmp_path: str,
    ) -> list[dict]:
        tasks: list[dict] = []
        for step_index, step in enumerate(steps):
            candidates = self._directory_candidates(issue_time, step)
            selected, filenames, diagnostics = self._fetch_listing(candidates)
            if selected is None:
                error_class = ForecastUnavailableError if step_index == 0 else DownloadError
                raise error_class(
                    "GDPS forecast is unavailable."
                    if step_index == 0
                    else "GDPS forecast is incomplete.",
                    [
                        f"Run: {issue_time:%Y-%m-%d %H:%M UTC}",
                        f"Missing step: f{step:03d}",
                        *diagnostics,
                    ],
                )
            for key, spec in self.variable_specs.items():
                remote_name = self._select_filename(filenames, spec)
                if remote_name is None:
                    raise VariableUnavailableError(
                        "Requested GDPS variable is unavailable.",
                        [
                            f"Run: {issue_time:%Y-%m-%d %H:%M UTC}",
                            f"Step: f{step:03d}",
                            f"Variable: {key}",
                            "Rules: " + " OR ".join(" + ".join(rule) for rule in spec["rules"]),
                        ],
                    )
                variable_dir = os.path.join(tmp_path, key)
                local_path = os.path.join(variable_dir, f"frc_{step:03d}.grib2")
                urls = _unique(
                    [urljoin(selected, remote_name)]
                    + [urljoin(candidate, remote_name) for candidate in candidates]
                )
                tasks.append(
                    {
                        "key": key,
                        "step": step,
                        "remote_name": remote_name,
                        "local_path": local_path,
                        "urls": urls,
                    }
                )
        return tasks

    def _decode_variable(
        self,
        key: str,
        tasks: list[dict],
        issue_time: dt.datetime,
        space_bounds: BoundingBox,
    ) -> tuple[xr.DataArray, bool]:
        arrays: list[xr.DataArray] = []
        intervals: list[int | None] = []
        spec = self.variable_specs[key]
        for task in sorted(tasks, key=lambda item: item["step"]):
            with xr.open_dataset(
                task["local_path"],
                engine="cfgrib",
                backend_kwargs={"indexpath": ""},
            ) as dataset:
                names = list(dataset.data_vars)
                if len(names) != 1:
                    raise DataValidationError(
                        "Unexpected GDPS GRIB content.",
                        [f"File: {task['local_path']}", f"Variables: {names}"],
                    )
                array = dataset[names[0]].squeeze(drop=True)
                for coord in ["time", "step", "valid_time", "surface", "heightAboveGround"]:
                    if coord in array.coords and coord not in array.dims:
                        array = array.reset_coords(coord, drop=True)
                array = crop_to_bounds(standardize_lat_lon(array), space_bounds)
                interval = _parse_accumulation_hours(task["remote_name"])
                intervals.append(interval)
                if spec["kind"] in {"precipitation", "radiation"} and interval is not None:
                    if interval != 3:
                        raise DataValidationError(
                            "Unsupported GDPS accumulation interval.",
                            [
                                f"File: {task['remote_name']}",
                                f"Found interval: {interval} h",
                                "Required interval for this workflow: 3 h",
                            ],
                        )
                if spec["kind"] == "precipitation" and interval is not None:
                    array = array / float(interval)
                if spec["kind"] == "radiation" and interval is not None:
                    array = array / float(interval * 3600)
                valid_time = issue_time + dt.timedelta(hours=task["step"])
                arrays.append(array.expand_dims(time=[valid_time]).load())
        non_null = [value for value in intervals if value is not None]
        if non_null and len(non_null) != len(intervals):
            raise DataValidationError(
                "Inconsistent GDPS accumulation products.",
                [f"Variable: {key}"],
            )
        return xr.concat(arrays, dim="time"), bool(non_null)

    def _postprocess(
        self,
        data: xr.Dataset,
        interval_processed: dict[str, bool],
        issue_time: dt.datetime,
    ) -> xr.Dataset:
        interval_variables: list[str] = []

        for key, spec in self.variable_specs.items():
            name = spec["output_name"]
            if (
                spec["kind"] == "precipitation"
                and self.decumulate_precipitation
                and not interval_processed[key]
            ):
                data[name] = decumulate(data[name]) / 3.0
                data[name] = data[name].where(data[name] >= 0, 0)
            if spec["kind"] == "precipitation":
                data[name].attrs["units"] = "mm h-1"
                # Precipitation values describe the interval ending at their
                # timestamp. They must never use nearest-neighbour interpolation.
                interval_variables.append(name)
            if (
                spec["kind"] == "radiation"
                and self.decumulate_radiation
                and not interval_processed[key]
            ):
                data[name] = decumulate(data[name]) / (3.0 * 3600.0)
                data[name] = data[name].where(data[name] >= 0, 0)
            if spec["kind"] == "radiation":
                data[name].attrs["units"] = "W m-2"
                if interval_processed[key] or self.decumulate_radiation:
                    interval_variables.append(name)
            if spec["kind"] == "temperature" and self.convert_temperature_to_c:
                data[name] = data[name] - 273.15
                data[name].attrs.update(
                    long_name="2 metre temperature",
                    units="C",
                    standard_name="air_temperature",
                )

        u_names = [
            spec["output_name"]
            for spec in self.variable_specs.values()
            if spec["kind"] == "wind_u"
        ]
        v_names = [
            spec["output_name"]
            for spec in self.variable_specs.values()
            if spec["kind"] == "wind_v"
        ]
        if self.aggregate_wind_components and u_names and v_names:
            data["10wind"] = np.sqrt(data[u_names[0]] ** 2 + data[v_names[0]] ** 2)
            data["10wind"].attrs.update(
                long_name="10 m wind",
                units="m s-1",
                standard_name="wind_speed",
            )

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
                        self.frc_max_step,
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
                                self.frc_max_step,
                            )
                            for name in interval_variables
                        }
                    )
                )

            data = xr.merge(parts, compat="override", join="exact")

        data = validate_dataset(drop_grib_coordinates(data))

        # Keep a conventional north-to-south raster orientation.
        if "lat" in data.coords and data.lat.size > 1:
            if float(data.lat.values[0]) < float(data.lat.values[-1]):
                data = data.sortby("lat", ascending=False)

        return data

    def _download_run(
        self,
        issue_time: dt.datetime,
        space_bounds: BoundingBox,
        tmp_path: str,
    ) -> xr.Dataset:
        if issue_time.hour not in self.issue_hours:
            raise ForecastUnavailableError(
                "Invalid GDPS issue hour.",
                [
                    f"Requested: {issue_time:%Y-%m-%d %H:%M UTC}",
                    "GDPS runs are available at 00 and 12 UTC.",
                ],
            )
        steps = list(range(3, self.frc_max_step + 1, 3))
        self.log.info(
            " ---> GDPS request: %s variables, %s forecast steps, %s workers",
            len(self.variable_specs),
            len(steps),
            self.download_workers,
        )
        self.log.info(" ---> Resolve GDPS remote files...")
        tasks = self._resolve_tasks(issue_time, steps, tmp_path)
        self.log.info(" ---> Resolve GDPS remote files...DONE (%s files)", len(tasks))

        self.log.info(" ---> Download GDPS forecast files...")
        results: list[dict] = []
        with ThreadPoolExecutor(max_workers=self.download_workers) as executor:
            futures = [executor.submit(self._download_task, task) for task in tasks]
            for future in as_completed(futures):
                results.append(future.result())
        failures = [result for result in results if not result["ok"]]
        if failures:
            self.log.error(
                " ---> Download GDPS forecast files...FAILED (%s/%s files)",
                len(failures),
                len(tasks),
            )
            first = failures[0]
            raise DownloadError(
                "GDPS forecast download failed.",
                [
                    f"Failed files: {len(failures)}/{len(tasks)}",
                    f"First failure: {first['key']} f{first['step']:03d}",
                    first.get("error", "unknown error"),
                ],
            )

        self.log.info(" ---> Download GDPS forecast files...DONE")
        self.log.info(" ---> Decode and merge GDPS variables...")
        dataset = xr.Dataset()
        interval_processed: dict[str, bool] = {}
        for key, spec in self.variable_specs.items():
            variable_tasks = [task for task in tasks if task["key"] == key]
            array, processed = self._decode_variable(
                key,
                variable_tasks,
                issue_time,
                space_bounds,
            )
            dataset[spec["output_name"]] = array
            interval_processed[key] = processed
        self.log.info(" ---> Decode and merge GDPS variables...DONE")
        self.log.info(" ---> Postprocess GDPS variables...")
        dataset = self._postprocess(dataset, interval_processed, issue_time)
        self.log.info(" ---> Postprocess GDPS variables...DONE")
        return dataset
