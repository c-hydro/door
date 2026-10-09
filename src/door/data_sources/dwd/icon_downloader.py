"""DWD ICON global forecast downloader."""

from __future__ import annotations

import bz2
import datetime as dt
import os
import shutil
import subprocess
import tarfile
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import numpy as np
import xarray as xr

from d3tools.spatial import BoundingBox

from ...base_forecast_downloader import ForecastDownloader
from ...utils.forecast import (
    crop_to_bounds,
    deaverage_to_interval_rate,
    decumulate_to_hourly_rate,
    drop_grib_coordinates,
    interpolate_hourly,
    robust_http_download,
    standardize_lat_lon,
    validate_dataset,
)
from ...utils.exceptions import (
    ConfigurationError,
    DataValidationError,
    DownloadError,
    ExternalToolError,
    ForecastUnavailableError,
)


class ICONDownloader(ForecastDownloader):
    """Download ICON global 0.125 degree deterministic forecasts from DWD."""

    source = "ICON"
    source_aliases = ["DWD_ICON", "ICON0p125"]
    name = "ICON_downloader"
    supports_ancillary = False
    issue_hours = [0, 6, 12, 18]
    publication_delay_hours = 6

    default_options = {
        "frc_max_step": 180,
        "variables": {"tot_prec": "tp", "u_10m": "10u", "v_10m": "10v"},
        "cdo_path": "cdo",
        "cache_dir": os.path.expanduser("~/.cache/door/icon"),
        "download_workers": 1,
        "download_attempts": 3,
        "retry_seconds": 3.0,
        "timeout_seconds": 180.0,
        "convert_temperature_to_c": True,
        "aggregate_wind_components": True,
        "decumulate_precipitation": True,
        "decumulate_radiation": True,
        "hourly_output": True,
    }

    available_products = {
        "ICON0P125": {
            "url_template": (
                "https://opendata.dwd.de/weather/nwp/icon/grib/{hour:02d}/{variable}/"
                "icon_global_icosahedral_single-level_{date}{hour:02d}_{step:03d}_{variable_upper}.grib2.bz2"
            ),
            "ancillary_url": "https://opendata.dwd.de/weather/lib/cdo/ICON_GLOBAL2WORLD_0125_EASY.tar.bz2",
            "grid_relative_path": "ICON_GLOBAL2WORLD_0125_EASY/target_grid_world_0125.txt",
            "weights_relative_path": "ICON_GLOBAL2WORLD_0125_EASY/weights_icogl2world_0125.nc",
        }
    }

    variable_aliases = {
        "tp": "tot_prec",
        "tot_prec": "tot_prec",
        "2t": "t_2m",
        "t2m": "t_2m",
        "t_2m": "t_2m",
        "10u": "u_10m",
        "u10": "u_10m",
        "u_10m": "u_10m",
        "10v": "v_10m",
        "v10": "v_10m",
        "v_10m": "v_10m",
        "rh": "relhum_2m",
        "2r": "relhum_2m",
        "relhum_2m": "relhum_2m",
        "dswrf": "aswdir_s",
        "aswdir_s": "aswdir_s",
    }

    default_output_names = {
        "tot_prec": "tp",
        "t_2m": "2t",
        "u_10m": "10u",
        "v_10m": "10v",
        "relhum_2m": "2r",
        "aswdir_s": "dswrf",
    }

    def __init__(self, product: str = "ICON0p125") -> None:
        super().__init__()
        self.set_product(product)

    def set_product(self, product: str) -> None:
        key = product.upper()
        if key not in self.available_products:
            raise ConfigurationError(
                "Unsupported ICON product.",
                [f"Value: {product}", f"Available: {sorted(self.available_products)}"],
            )
        self.product = key
        for name, value in self.available_products[key].items():
            setattr(self, name, value)

    def set_variables(self, variables: dict[str, str] | list[str] | str | None) -> None:
        if variables is None:
            variables = deepcopy(self.default_options["variables"])
        if isinstance(variables, str):
            variables = [variables]

        variable_map: dict[str, str] = {}
        if isinstance(variables, dict):
            for remote, output in variables.items():
                if str(remote).startswith("__"):
                    continue
                remote_name = self.variable_aliases.get(str(remote), str(remote))
                variable_map[remote_name] = str(output)
        elif isinstance(variables, (list, tuple)):
            for variable in variables:
                remote_name = self.variable_aliases.get(str(variable), str(variable))
                variable_map[remote_name] = self.default_output_names.get(
                    remote_name, str(variable)
                )
        else:
            raise ConfigurationError(
                "Invalid ICON variables configuration.",
                ["Expected a mapping, list or string."],
            )

        if not variable_map:
            raise ConfigurationError("No ICON variables are configured.")
        self.variable_map = variable_map
        self.variables = variable_map

    def check_options(self, options: dict | None = None) -> dict:
        checked = super().check_options(options)
        try:
            checked["frc_max_step"] = min(180, max(1, int(checked["frc_max_step"])))
            checked["download_workers"] = max(1, int(checked["download_workers"]))
        except (TypeError, ValueError) as error:
            raise ConfigurationError(
                "Invalid ICON downloader setting.", [str(error)]
            ) from error
        return checked

    @staticmethod
    def _resolve_executable(path: str, executable: str) -> str:
        if os.path.isdir(path):
            return os.path.join(path, executable)
        return path

    def _forecast_steps(self) -> list[int]:
        if self.frc_max_step <= 77:
            return list(range(1, self.frc_max_step + 1))
        return list(range(1, 78)) + list(range(78, self.frc_max_step + 1, 3))

    def _prepare_remapping_files(self) -> tuple[str, str]:
        cache_dir = os.path.expanduser(self.cache_dir)
        grid_file = os.path.join(cache_dir, self.grid_relative_path)
        weights_file = os.path.join(cache_dir, self.weights_relative_path)
        if os.path.isfile(grid_file) and os.path.isfile(weights_file):
            return grid_file, weights_file

        os.makedirs(cache_dir, exist_ok=True)
        archive = os.path.join(cache_dir, "ICON_GLOBAL2WORLD_0125_EASY.tar.bz2")
        self.log.info("Downloading ICON remapping tables")
        robust_http_download(
            self.ancillary_url,
            archive,
            attempts=self.download_attempts,
            retry_seconds=self.retry_seconds,
            timeout_seconds=self.timeout_seconds,
            min_size=1000,
        )
        with tarfile.open(archive, "r:bz2") as tar:
            root = os.path.realpath(cache_dir)
            for member in tar.getmembers():
                target = os.path.realpath(os.path.join(cache_dir, member.name))
                if not target.startswith(root + os.sep):
                    raise DataValidationError(
                        "Unsafe path found in the ICON ancillary archive.",
                        [member.name],
                    )
            tar.extractall(cache_dir)
        os.remove(archive)

        if not os.path.isfile(grid_file) or not os.path.isfile(weights_file):
            raise DataValidationError(
                "ICON remapping files were not found after extraction.",
                [grid_file, weights_file],
            )
        return grid_file, weights_file

    def _download_and_remap(
        self,
        issue_time: dt.datetime,
        variable: str,
        step: int,
        tmp_path: str,
        grid_file: str,
        weights_file: str,
    ) -> str:
        url = self.url_template.format(
            hour=issue_time.hour,
            date=issue_time.strftime("%Y%m%d"),
            variable=variable,
            variable_upper=variable.upper(),
            step=step,
        )
        variable_dir = os.path.join(tmp_path, variable)
        os.makedirs(variable_dir, exist_ok=True)
        compressed = os.path.join(variable_dir, f"frc_{step:03d}.grib2.bz2")
        source_grib = compressed[:-4]
        remapped = os.path.join(variable_dir, f"regr_frc_{step:03d}.grib2")

        robust_http_download(
            url,
            compressed,
            attempts=self.download_attempts,
            retry_seconds=self.retry_seconds,
            timeout_seconds=self.timeout_seconds,
            min_size=200,
        )
        with bz2.open(compressed, "rb") as source, open(source_grib, "wb") as target:
            shutil.copyfileobj(source, target)
        os.remove(compressed)

        cdo = self._resolve_executable(self.cdo_path, "cdo")
        if shutil.which(cdo) is None and not os.path.isfile(cdo):
            raise ExternalToolError(
                "CDO executable was not found.", [f"Executable: {cdo}"]
            )
        command = [
            cdo,
            "-O",
            f"remap,{grid_file},{weights_file}",
            source_grib,
            remapped,
        ]
        try:
            subprocess.run(
                command,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except subprocess.CalledProcessError as error:
            raise DownloadError(
                "ICON remapping with CDO failed.",
                [f"Variable: {variable}", f"Step: {step}", error.stdout[-1000:]],
            ) from error
        finally:
            if os.path.isfile(source_grib):
                os.remove(source_grib)
        return remapped

    def _open_variable(
        self,
        files: list[str],
        issue_time: dt.datetime,
        steps: list[int],
        output_name: str,
    ) -> xr.DataArray:
        arrays: list[xr.DataArray] = []
        for path, step in zip(files, steps):
            with xr.open_dataset(
                path,
                engine="cfgrib",
                backend_kwargs={"indexpath": ""},
            ) as dataset:
                names = list(dataset.data_vars)
                if len(names) != 1:
                    raise DataValidationError(
                        "Unexpected ICON GRIB content.",
                        [f"File: {path}", f"Variables: {names}"],
                    )
                array = dataset[names[0]].squeeze(drop=True)
                for coord in ["time", "step", "valid_time", "surface", "heightAboveGround"]:
                    if coord in array.coords and coord not in array.dims:
                        array = array.reset_coords(coord, drop=True)
                array = standardize_lat_lon(array)
                valid_time = issue_time + dt.timedelta(hours=step)
                arrays.append(array.expand_dims(time=[valid_time]).load())
        output = xr.concat(arrays, dim="time")
        output.name = output_name
        return output

    def _postprocess(self, data: xr.Dataset, issue_time: dt.datetime) -> xr.Dataset:
        inverse = {remote: output for remote, output in self.variable_map.items()}

        if "tot_prec" in inverse and self.decumulate_precipitation:
            name = inverse["tot_prec"]
            data[name] = decumulate_to_hourly_rate(data[name], issue_time)
            data[name].attrs["units"] = "mm h-1"

        if "t_2m" in inverse and self.convert_temperature_to_c:
            name = inverse["t_2m"]
            data[name] = data[name] - 273.15
            data[name].attrs.update(
                long_name="2 metre temperature",
                units="C",
                standard_name="air_temperature",
            )

        if (
            "u_10m" in inverse
            and "v_10m" in inverse
            and self.aggregate_wind_components
        ):
            data["10wind"] = np.sqrt(
                data[inverse["u_10m"]] ** 2 + data[inverse["v_10m"]] ** 2
            )
            data["10wind"].attrs.update(
                long_name="10 m wind",
                units="m s-1",
                standard_name="wind_speed",
            )

        if "aswdir_s" in inverse and self.decumulate_radiation:
            name = inverse["aswdir_s"]
            data[name] = deaverage_to_interval_rate(data[name], issue_time)
            data[name] = data[name].where(data[name] >= 1, 0)
            data[name].attrs["units"] = "W m-2"

        if self.hourly_output:
            data = interpolate_hourly(
                data,
                issue_time,
                self.frc_max_step,
                method="nearest",
                include_end=True,
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
                "Invalid ICON issue hour.",
                [f"Requested: {issue_time:%Y-%m-%d %H:%M UTC}"],
            )

        steps = self._forecast_steps()
        self.log.info(
            " ---> ICON request: %s variables, %s forecast steps, %s workers",
            len(self.variable_map),
            len(steps),
            self.download_workers,
        )
        self.log.info(" ---> Prepare ICON remapping tables...")
        grid_file, weights_file = self._prepare_remapping_files()
        self.log.info(" ---> Prepare ICON remapping tables...DONE")
        self.log.info(" ---> Download and remap ICON forecast fields...")
        outputs: dict[str, list[str]] = {variable: [] for variable in self.variable_map}
        tasks = []
        with ThreadPoolExecutor(max_workers=self.download_workers) as executor:
            for variable in self.variable_map:
                for step in steps:
                    future = executor.submit(
                        self._download_and_remap,
                        issue_time,
                        variable,
                        step,
                        tmp_path,
                        grid_file,
                        weights_file,
                    )
                    tasks.append((future, variable, step))

            errors: list[str] = []
            for future, variable, step in tasks:
                try:
                    outputs[variable].append(future.result())
                except Exception as error:
                    errors.append(f"{variable} f{step:03d}: {error}")

        if errors:
            self.log.error(
                " ---> Download and remap ICON forecast fields...FAILED (%s errors)",
                len(errors),
            )
            first_step_missing = any("f001" in item for item in errors)
            error_class = ForecastUnavailableError if first_step_missing else DownloadError
            raise error_class(
                "ICON forecast is unavailable or incomplete.",
                [f"Run: {issue_time:%Y-%m-%d %H:%M UTC}", *errors[:10]],
            )

        self.log.info(" ---> Download and remap ICON forecast fields...DONE")
        self.log.info(" ---> Decode and merge ICON variables...")
        dataset = xr.Dataset()
        for variable, output_name in self.variable_map.items():
            files = sorted(outputs[variable])
            dataset[output_name] = self._open_variable(
                files,
                issue_time,
                steps,
                output_name,
            )

        self.log.info(" ---> Decode and merge ICON variables...DONE")
        self.log.info(" ---> Postprocess ICON variables...")
        dataset = crop_to_bounds(dataset, space_bounds)
        dataset = self._postprocess(dataset, issue_time)
        self.log.info(" ---> Postprocess ICON variables...DONE")
        return dataset
