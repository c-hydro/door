"""NOAA GFS 0.25 degree downloader using the NOMADS GRIB filter."""

from __future__ import annotations

import datetime as dt
import os
import time
from copy import deepcopy

import numpy as np
import xarray as xr

from d3tools.spatial import BoundingBox

from ...base_forecast_downloader import ForecastDownloader
from ...utils.forecast import (
    crop_to_bounds,
    drop_grib_coordinates,
    gfs_bucket_decumulate,
    index_path_for,
    remove_file_family,
    robust_http_download,
    standardize_lat_lon,
    validate_dataset,
)
from ...utils.exceptions import ConfigurationError, DataValidationError, DownloadError, ForecastUnavailableError


class GFSDownloader(ForecastDownloader):
    """Download spatial/variable subsets from NOMADS, one request per lead hour."""

    source = "GFS"
    source_aliases = ["GFS_GRIBFILTER", "NOAA_GFS", "GFS0p25"]
    name = "GFS_GRIBFilter_downloader"
    supports_ancillary = False
    issue_hours = [0, 6, 12, 18]
    publication_delay_hours = 6

    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"

    default_variable_specs = {
        "tp": {
            "nomads_var": "APCP",
            "cfgrib_filters": [
                {"shortName": "tp", "typeOfLevel": "surface"},
                {"typeOfLevel": "surface", "stepType": "accum"},
            ],
            "kind": "precipitation",
            "output_name": "tp",
        },
        "t2m": {
            "nomads_var": "TMP",
            "cfgrib_filters": [
                {"shortName": "2t", "typeOfLevel": "heightAboveGround", "level": 2},
                {"typeOfLevel": "heightAboveGround", "level": 2},
            ],
            "kind": "temperature",
            "output_name": "t2m",
        },
        "2t": {
            "nomads_var": "TMP",
            "cfgrib_filters": [
                {"shortName": "2t", "typeOfLevel": "heightAboveGround", "level": 2},
                {"typeOfLevel": "heightAboveGround", "level": 2},
            ],
            "kind": "temperature",
            "output_name": "2t",
        },
        "10u": {
            "nomads_var": "UGRD",
            "cfgrib_filters": [
                {"shortName": "10u", "typeOfLevel": "heightAboveGround", "level": 10},
                {"typeOfLevel": "heightAboveGround", "level": 10},
            ],
            "kind": "wind_u",
            "output_name": "10u",
        },
        "10v": {
            "nomads_var": "VGRD",
            "cfgrib_filters": [
                {"shortName": "10v", "typeOfLevel": "heightAboveGround", "level": 10},
                {"typeOfLevel": "heightAboveGround", "level": 10},
            ],
            "kind": "wind_v",
            "output_name": "10v",
        },
        "2r": {
            "nomads_var": "RH",
            "cfgrib_filters": [
                {"shortName": "2r", "typeOfLevel": "heightAboveGround", "level": 2},
                {"typeOfLevel": "heightAboveGround", "level": 2},
            ],
            "kind": "relative_humidity",
            "output_name": "2r",
        },
        "dswrf": {
            "nomads_var": "DSWRF",
            "cfgrib_filters": [
                {"shortName": "dswrf", "typeOfLevel": "surface"},
                {"typeOfLevel": "surface"},
            ],
            "kind": "radiation",
            "output_name": "dswrf",
        },
    }

    variable_aliases = {
        "tot_prec": "tp",
        "tp": "tp",
        "t_2m": "t2m",
        "temperature": "t2m",
        "t2m": "t2m",
        "2t": "2t",
        "u_10m": "10u",
        "10u": "10u",
        "v_10m": "10v",
        "10v": "10v",
        "relhum_2m": "2r",
        "2r": "2r",
        "aswdir_s": "dswrf",
        "dswrf": "dswrf",
    }

    default_options = {
        "frc_max_step": 120,
        "variables": {
            "tp": default_variable_specs["tp"],
            "t2m": default_variable_specs["t2m"],
            "10u": default_variable_specs["10u"],
            "10v": default_variable_specs["10v"],
        },
        "download_attempts": 5,
        "retry_seconds": 3.0,
        "timeout_seconds": 240.0,
        "sleep_between_requests": 0.0,
        "convert_temperature_to_c": True,
        "aggregate_wind_components": True,
        "decumulate_precipitation": True,
        "precipitation_bucket_hours": 6,
    }

    available_products = {
        "GFS0P25": {},
    }

    def __init__(self, product: str = "GFS0p25") -> None:
        super().__init__()
        key = product.upper()
        if key not in self.available_products:
            raise ConfigurationError(
                "Unsupported GFS product.",
                [f"Value: {product}", f"Available: {sorted(self.available_products)}"],
            )
        self.product = key

    def _normalise_spec(self, key: str, value: object) -> dict:
        default = deepcopy(self.default_variable_specs.get(key, {}))
        if isinstance(value, str):
            if not default:
                raise ConfigurationError(
                    "No default GFS mapping exists for the configured variable.",
                    [f"Variable: {key}", "Use a JSON object with nomads_var and cfgrib_filters."],
                )
            default["output_name"] = value
            return default

        if not isinstance(value, dict):
            raise ConfigurationError(
                "Invalid GFS variable configuration.",
                [f"Variable: {key}", "Expected a string or object."],
            )

        spec = default
        spec.update(deepcopy(value))
        spec.setdefault("output_name", key)
        spec.setdefault("kind", "generic")
        filters = spec.get("cfgrib_filters")
        if isinstance(filters, dict):
            filters = [filters]
        if not spec.get("nomads_var") or not filters:
            raise ConfigurationError(
                "Incomplete GFS variable configuration.",
                [f"Variable: {key}", "nomads_var and cfgrib_filters are required."],
            )
        spec["cfgrib_filters"] = filters
        return spec

    def set_variables(self, variables: dict | list[str] | str | None) -> None:
        if variables is None:
            variables = deepcopy(self.default_options["variables"])
        if isinstance(variables, str):
            variables = [variables]
        if isinstance(variables, list):
            variable_map = {}
            for key in variables:
                normalised_key = self.variable_aliases.get(str(key), str(key))
                if normalised_key not in self.default_variable_specs:
                    raise ConfigurationError(
                        "Unsupported GFS variable.",
                        [f"Value: {key}"],
                    )
                variable_map[normalised_key] = deepcopy(
                    self.default_variable_specs[normalised_key]
                )
            variables = variable_map
        if not isinstance(variables, dict):
            raise ConfigurationError(
                "Invalid GFS variables configuration.",
                ["Expected a mapping, list or string."],
            )

        normalised_variables = {}
        for key, value in variables.items():
            if str(key).startswith("__"):
                continue
            normalised_key = self.variable_aliases.get(str(key), str(key))
            normalised_variables[normalised_key] = value

        self.variable_specs = {
            key: self._normalise_spec(key, value)
            for key, value in normalised_variables.items()
        }
        if not self.variable_specs:
            raise ConfigurationError("No GFS variables are configured.")
        self.variables = self.variable_specs

    def check_options(self, options: dict | None = None) -> dict:
        checked = super().check_options(options)
        try:
            requested_step = max(1, int(checked["frc_max_step"]))
            checked["download_attempts"] = max(1, int(checked["download_attempts"]))
            checked["precipitation_bucket_hours"] = max(
                1, int(checked["precipitation_bucket_hours"])
            )
        except (TypeError, ValueError) as error:
            raise ConfigurationError(
                "Invalid GFS downloader setting.", [str(error)]
            ) from error
        if requested_step > 120:
            self.log.warning(
                "GFS GRIB Filter migration currently supports the hourly range up to 120 h; "
                "the requested horizon %s h has been limited to 120 h",
                requested_step,
            )
        checked["frc_max_step"] = min(120, requested_step)
        return checked

    @staticmethod
    def _level_parameter(filter_keys: dict) -> str | None:
        level_type = filter_keys.get("typeOfLevel")
        level = filter_keys.get("level")
        if level_type == "surface":
            return "lev_surface"
        if level_type == "heightAboveGround" and level == 2:
            return "lev_2_m_above_ground"
        if level_type == "heightAboveGround" and level == 10:
            return "lev_10_m_above_ground"
        if level_type == "meanSea":
            return "lev_mean_sea_level"
        return None

    def _build_params(
        self,
        issue_time: dt.datetime,
        forecast_hour: int,
        bounds: BoundingBox,
    ) -> dict[str, str]:
        west, south, east, north = (float(value) for value in bounds.bbox)
        cycle = f"{issue_time.hour:02d}"
        params = {
            "file": f"gfs.t{cycle}z.pgrb2.0p25.f{forecast_hour:03d}",
            "dir": f"/gfs.{issue_time:%Y%m%d}/{cycle}/atmos",
            "subregion": "",
            "leftlon": str(west),
            "rightlon": str(east),
            "toplat": str(north),
            "bottomlat": str(south),
        }
        for spec in self.variable_specs.values():
            params[f"var_{spec['nomads_var']}"] = "on"
            for filter_keys in spec["cfgrib_filters"]:
                level_parameter = self._level_parameter(filter_keys)
                if level_parameter:
                    params[level_parameter] = "on"
        return params

    @staticmethod
    def _open_variable(grib_path: str, filters: list[dict]) -> xr.Dataset:
        errors: list[str] = []
        index_path = index_path_for(grib_path)
        for filter_keys in filters:
            try:
                return xr.open_dataset(
                    grib_path,
                    engine="cfgrib",
                    backend_kwargs={
                        "filter_by_keys": filter_keys,
                        "indexpath": index_path,
                    },
                )
            except Exception as error:
                errors.append(f"{filter_keys}: {error}")
        raise DataValidationError(
            "Unable to decode a requested GFS variable.",
            [f"File: {grib_path}", *errors[-3:]],
        )

    def _decode_step(
        self,
        grib_path: str,
        valid_time: dt.datetime,
    ) -> xr.Dataset:
        variables: list[xr.Dataset] = []
        opened: list[xr.Dataset] = []
        try:
            for key, spec in self.variable_specs.items():
                dataset = self._open_variable(grib_path, spec["cfgrib_filters"])
                opened.append(dataset)
                names = list(dataset.data_vars)
                if len(names) != 1:
                    raise DataValidationError(
                        "Unexpected number of fields in a GFS variable cube.",
                        [f"Variable: {key}", f"Fields: {names}"],
                    )
                array = dataset[names[0]].squeeze(drop=True)
                for coord in ["time", "step", "valid_time", "surface", "heightAboveGround"]:
                    if coord in array.coords and coord not in array.dims:
                        array = array.reset_coords(coord, drop=True)
                array = standardize_lat_lon(array)
                output_name = spec["output_name"]
                variables.append(
                    array.expand_dims(time=[valid_time]).to_dataset(name=output_name).load()
                )
            return xr.merge(variables, compat="override")
        finally:
            for dataset in opened:
                dataset.close()

    def _postprocess(self, data: xr.Dataset, issue_time: dt.datetime) -> xr.Dataset:
        by_kind: dict[str, list[str]] = {}
        for spec in self.variable_specs.values():
            by_kind.setdefault(spec.get("kind", "generic"), []).append(spec["output_name"])

        if self.decumulate_precipitation:
            for name in by_kind.get("precipitation", []):
                data[name] = gfs_bucket_decumulate(
                    data[name],
                    issue_time,
                    bucket_hours=self.precipitation_bucket_hours,
                )
                data[name].attrs["units"] = "mm h-1"

        if self.convert_temperature_to_c:
            for name in by_kind.get("temperature", []):
                data[name] = data[name] - 273.15
                data[name].attrs.update(
                    long_name="2 metre temperature",
                    units="C",
                    standard_name="air_temperature",
                )

        if self.aggregate_wind_components:
            u_names = by_kind.get("wind_u", [])
            v_names = by_kind.get("wind_v", [])
            if u_names and v_names:
                data["10wind"] = np.sqrt(data[u_names[0]] ** 2 + data[v_names[0]] ** 2)
                data["10wind"].attrs.update(
                    long_name="10 m wind",
                    units="m s-1",
                    standard_name="wind_speed",
                )

        return validate_dataset(drop_grib_coordinates(data))

    def _download_run(
        self,
        issue_time: dt.datetime,
        space_bounds: BoundingBox,
        tmp_path: str,
    ) -> xr.Dataset:
        if issue_time.hour not in self.issue_hours:
            raise ForecastUnavailableError(
                "Invalid GFS issue hour.",
                [f"Requested: {issue_time:%Y-%m-%d %H:%M UTC}"],
            )

        self.log.info(
            " ---> GFS request: %s variables, %s hourly forecast steps",
            len(self.variable_specs),
            self.frc_max_step,
        )
        self.log.info(" ---> Download and decode GFS forecast steps...")
        steps: list[xr.Dataset] = []
        for forecast_hour in range(1, self.frc_max_step + 1):
            valid_time = issue_time + dt.timedelta(hours=forecast_hour)
            grib_path = os.path.join(
                tmp_path,
                f"gfs_{issue_time:%Y%m%d%H}_f{forecast_hour:03d}.grib2",
            )
            params = self._build_params(issue_time, forecast_hour, space_bounds)
            try:
                robust_http_download(
                    self.base_url,
                    grib_path,
                    params=params,
                    attempts=self.download_attempts,
                    retry_seconds=self.retry_seconds,
                    timeout_seconds=self.timeout_seconds,
                    min_size=100,
                )
                with open(grib_path, "rb") as stream:
                    if stream.read(4) != b"GRIB":
                        raise DataValidationError(
                            "NOMADS returned a non-GRIB response.",
                            [f"Run: {issue_time:%Y-%m-%d %H:%M UTC}", f"Step: f{forecast_hour:03d}"],
                        )
                steps.append(self._decode_step(grib_path, valid_time))
                if (
                    forecast_hour == 1
                    or forecast_hour == self.frc_max_step
                    or forecast_hour % 12 == 0
                ):
                    self.log.info(
                        " ----> GFS progress: %s/%s steps",
                        forecast_hour,
                        self.frc_max_step,
                    )
            except ForecastUnavailableError as error:
                summary = (
                    "GFS forecast is not available on NOMADS."
                    if forecast_hour == 1
                    else "GFS forecast is incomplete on NOMADS."
                )
                raise ForecastUnavailableError(
                    summary,
                    [
                        f"Run: {issue_time:%Y-%m-%d %H:%M UTC}",
                        f"Missing step: f{forecast_hour:03d}",
                        str(error),
                    ],
                ) from error
            finally:
                remove_file_family(grib_path)

            if self.sleep_between_requests > 0:
                time.sleep(self.sleep_between_requests)

        if not steps:
            raise DownloadError("No GFS forecast step was decoded.")
        self.log.info(" ---> Download and decode GFS forecast steps...DONE")
        self.log.info(" ---> Merge and postprocess GFS variables...")
        data = xr.concat(steps, dim="time")
        data = standardize_lat_lon(data)
        data = crop_to_bounds(data, space_bounds)
        data = self._postprocess(data, issue_time)
        self.log.info(" ---> Merge and postprocess GFS variables...DONE")
        return data
