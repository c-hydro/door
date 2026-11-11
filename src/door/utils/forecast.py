"""Shared helpers for operational NWP and hydrological forecast downloaders."""

from __future__ import annotations

import datetime as dt
import glob
import hashlib
import os
import time
from collections.abc import Iterable, Mapping, Sequence

import numpy as np
import requests
import xarray as xr

from .exceptions import DataValidationError, DownloadError, ForecastUnavailableError

TRANSIENT_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}


def compact_error(error: BaseException, max_length: int = 700) -> str:
    message = " ".join(str(error).split()) or error.__class__.__name__
    output = f"{error.__class__.__name__}: {message}"
    return output if len(output) <= max_length else output[: max_length - 3] + "..."


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def remove_file_family(path: str) -> None:
    """Remove a data file and any persistent cfgrib indexes created for it."""
    for candidate in [path, path + ".idx", *glob.glob(path + ".*.idx")]:
        try:
            if os.path.isfile(candidate):
                os.remove(candidate)
        except OSError:
            pass


def validate_file(path: str, min_size: int = 1) -> None:
    if not os.path.isfile(path):
        raise DownloadError("Downloaded file is missing.", [path])
    size = os.path.getsize(path)
    if size < min_size:
        raise DownloadError(
            "Downloaded file is empty or incomplete.",
            [f"File: {path}", f"Size: {size} bytes", f"Minimum: {min_size} bytes"],
        )


def robust_http_download(
    url: str,
    destination: str,
    *,
    params: Mapping[str, object] | None = None,
    headers: Mapping[str, str] | None = None,
    attempts: int = 3,
    retry_seconds: float = 3.0,
    timeout_seconds: float = 180.0,
    min_size: int = 1,
    unavailable_statuses: Sequence[int] = (403, 404),
) -> str:
    """Download a file with bounded retries and useful operational errors."""
    ensure_parent(destination)
    errors: list[str] = []

    for attempt in range(1, max(1, attempts) + 1):
        remove_file_family(destination)
        try:
            with requests.get(
                url,
                params=params,
                headers=headers,
                stream=True,
                timeout=(20, timeout_seconds),
            ) as response:
                if response.status_code in unavailable_statuses:
                    raise ForecastUnavailableError(
                        "Forecast file is not available.",
                        [f"HTTP {response.status_code}", response.reason or url],
                    )
                response.raise_for_status()

                content_type = (response.headers.get("Content-Type") or "").lower()
                if "text/html" in content_type:
                    sample = response.text[:500].replace("\n", " ")
                    raise ForecastUnavailableError(
                        "Remote service returned HTML instead of forecast data.",
                        [sample],
                    )

                with open(destination, "wb") as stream:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            stream.write(chunk)

            validate_file(destination, min_size=min_size)
            return destination

        except ForecastUnavailableError:
            remove_file_family(destination)
            raise
        except requests.HTTPError as error:
            remove_file_family(destination)
            status = getattr(error.response, "status_code", None)
            errors.append(compact_error(error))
            if status not in TRANSIENT_HTTP_CODES or attempt >= attempts:
                break
        except (requests.Timeout, requests.ConnectionError, OSError, DownloadError) as error:
            remove_file_family(destination)
            errors.append(compact_error(error))
            if attempt >= attempts:
                break

        time.sleep(retry_seconds * attempt)

    raise DownloadError(
        "Download failed after the configured attempts.",
        [f"URL: {url}", *errors[-3:]],
    )


def standardize_lat_lon(data: xr.Dataset | xr.DataArray) -> xr.Dataset | xr.DataArray:
    rename: dict[str, str] = {}
    for old, new in (("latitude", "lat"), ("longitude", "lon")):
        if old in data.dims or old in data.coords:
            if new not in data.dims and new not in data.coords:
                rename[old] = new
    if rename:
        data = data.rename(rename)

    if "lon" in data.coords and data.lon.size:
        if float(data.lon.max()) > 180:
            data = data.assign_coords(lon=(((data.lon + 180) % 360) - 180)).sortby("lon")

    if "lat" in data.coords and data.lat.size > 1:
        if float(data.lat.values[0]) > float(data.lat.values[-1]):
            data = data.sortby("lat")

    if "lat" in data.coords:
        data.lat.attrs.setdefault("units", "degrees_north")
    if "lon" in data.coords:
        data.lon.attrs.setdefault("units", "degrees_east")
    return data


def bbox_tuple(bounds: object) -> tuple[float, float, float, float]:
    if hasattr(bounds, "bbox"):
        values = getattr(bounds, "bbox")
    else:
        values = bounds
    if not isinstance(values, (list, tuple)) or len(values) != 4:
        raise ValueError("Bounds must be [west, south, east, north]")
    west, south, east, north = (float(value) for value in values)
    return west, south, east, north


def crop_to_bounds(
    data: xr.Dataset | xr.DataArray,
    bounds: object,
) -> xr.Dataset | xr.DataArray:
    data = standardize_lat_lon(data)
    west, south, east, north = bbox_tuple(bounds)
    if "lat" not in data.coords or "lon" not in data.coords:
        raise DataValidationError(
            "Forecast data do not expose latitude/longitude coordinates.",
            [f"Coordinates: {list(data.coords)}"],
        )

    lat_mask = (data.lat >= south) & (data.lat <= north)
    if west <= east:
        lon_mask = (data.lon >= west) & (data.lon <= east)
    else:
        lon_mask = (data.lon >= west) | (data.lon <= east)
    data = data.where(lat_mask & lon_mask, drop=True)

    if data.sizes.get("lat", 0) == 0 or data.sizes.get("lon", 0) == 0:
        raise DataValidationError(
            "The requested bounding box does not intersect the forecast grid.",
            [f"Bounding box: {(west, south, east, north)}"],
        )
    return data


def drop_grib_coordinates(data: xr.Dataset | xr.DataArray) -> xr.Dataset | xr.DataArray:
    names = [
        "valid_time",
        "step",
        "surface",
        "heightAboveGround",
        "meanSea",
        "entireAtmosphere",
    ]
    return data.drop_vars(names, errors="ignore")


def assign_valid_time(
    data: xr.Dataset | xr.DataArray,
    issue_time: dt.datetime,
    lead_hours: Sequence[int] | None = None,
) -> xr.Dataset | xr.DataArray:
    """Convert GRIB issue/step coordinates to a normal datetime ``time`` dimension."""
    if "valid_time" in data.coords and data.valid_time.ndim == 1:
        dim = data.valid_time.dims[0]
        # GRIB cubes normally expose the issue time as a scalar ``time``
        # coordinate and lead time on ``step``. Drop the scalar before
        # renaming ``step`` to ``time`` to avoid an xarray name conflict.
        if dim != "time" and "time" in data.coords and "time" not in data.dims:
            data = data.drop_vars("time")
        data = data.assign_coords({dim: data.valid_time.values})
        if dim != "time":
            data = data.rename({dim: "time"})
    elif "step" in data.dims:
        if lead_hours is None:
            step_values = data.step.values / np.timedelta64(1, "h")
            lead_hours = [int(value) for value in step_values]
        valid_times = [issue_time + dt.timedelta(hours=int(value)) for value in lead_hours]
        data = data.assign_coords(step=valid_times).rename({"step": "time"})
    elif "time" not in data.dims and lead_hours is not None:
        valid_times = [issue_time + dt.timedelta(hours=int(value)) for value in lead_hours]
        data = data.expand_dims(time=valid_times)

    if "time" in data.coords and data.time.ndim == 0:
        data = data.expand_dims(time=[data.time.values])
    return data.drop_vars("valid_time", errors="ignore")


def decumulate(data: xr.DataArray, *, clip_negative: bool = True) -> xr.DataArray:
    first = data.isel(time=0).copy(deep=True)
    diff = data.diff("time")
    output = xr.concat(
        [first.expand_dims(time=[data.time.values[0]]), diff],
        dim="time",
    ).assign_coords(time=data.time)
    if clip_negative:
        output = output.where(output >= 0, 0)
    return output


def lead_interval_hours(time_values: Iterable[object], issue_time: dt.datetime) -> np.ndarray:
    values = np.asarray(list(time_values), dtype="datetime64[ns]")
    lead = (values - np.datetime64(issue_time)) / np.timedelta64(1, "h")
    lead = lead.astype(float)
    intervals = np.diff(np.concatenate(([0.0], lead)))
    intervals[intervals <= 0] = 1.0
    return intervals


def decumulate_to_hourly_rate(data: xr.DataArray, issue_time: dt.datetime) -> xr.DataArray:
    output = decumulate(data)
    intervals = xr.DataArray(
        lead_interval_hours(output.time.values, issue_time),
        coords={"time": output.time},
        dims=("time",),
    )
    return output / intervals


def deaverage_to_interval_rate(data: xr.DataArray, issue_time: dt.datetime) -> xr.DataArray:
    """Convert an average-from-start field to interval-average values."""
    lead = xr.DataArray(
        ((data.time.values - np.datetime64(issue_time)) / np.timedelta64(1, "h")).astype(float),
        coords={"time": data.time},
        dims=("time",),
    )
    accumulated = data * lead
    return decumulate_to_hourly_rate(accumulated, issue_time)


def gfs_bucket_decumulate(data: xr.DataArray, issue_time: dt.datetime, bucket_hours: int = 6) -> xr.DataArray:
    values = data.values
    output = np.zeros_like(values)
    leads = ((data.time.values - np.datetime64(issue_time)) / np.timedelta64(1, "h")).astype(int)
    output[0] = values[0]
    for index in range(1, values.shape[0]):
        if leads[index] % bucket_hours == 1:
            output[index] = values[index]
        else:
            output[index] = values[index] - values[index - 1]
    result = data.copy(data=output)
    return result.where(result >= 0, 0)


def hourly_time_axis(
    issue_time: dt.datetime,
    end_hour: int,
    *,
    include_end: bool = True,
) -> np.ndarray:
    last_hour = end_hour if include_end else max(1, end_hour - 1)
    return np.array(
        [issue_time + dt.timedelta(hours=hour) for hour in range(1, last_hour + 1)],
        dtype="datetime64[ns]",
    )


def interpolate_hourly(
    data: xr.Dataset | xr.DataArray,
    issue_time: dt.datetime,
    end_hour: int,
    *,
    method: str = "nearest",
    include_end: bool = True,
) -> xr.Dataset | xr.DataArray:
    target = hourly_time_axis(issue_time, end_hour, include_end=include_end)
    return data.reindex(time=target, method=method)


def disaggregate_interval_to_hourly(
    data: xr.DataArray,
    issue_time: dt.datetime,
    end_hour: int,
    *,
    include_end: bool = True,
) -> xr.DataArray:
    """Assign each interval value to the hourly bins ending at its timestamp.

    The value stored at +3 h is applied to +1, +2 and +3; the value at
    +6 h is applied to +4, +5 and +6. This is the temporal equivalent of
    a back-fill, implemented explicitly so precipitation never passes through
    nearest-neighbour interpolation.
    """
    if "time" not in data.dims:
        raise ValueError("Interval disaggregation requires a time dimension")

    target = hourly_time_axis(issue_time, end_hour, include_end=include_end)
    source = np.asarray(data.time.values, dtype="datetime64[ns]")

    if source.size == 0:
        return data.reindex(time=target)

    order = np.argsort(source)
    source = source[order]
    ordered = data.isel(time=order)

    # For each target hour, select the first source timestamp >= target.
    # This is exact back-fill semantics for values representing the interval
    # that ends at the source timestamp.
    indices = np.searchsorted(source, target, side="left")
    valid = indices < source.size
    safe_indices = np.clip(indices, 0, source.size - 1)

    indexer = xr.DataArray(safe_indices, dims=("time",))
    output = ordered.isel(time=indexer).assign_coords(time=target)

    if not np.all(valid):
        valid_mask = xr.DataArray(valid, dims=("time",), coords={"time": target})
        output = output.where(valid_mask)

    return output


def relative_humidity_from_dewpoint(
    temperature_c: xr.DataArray,
    dewpoint_c: xr.DataArray,
) -> xr.DataArray:
    saturation = 6.112 * np.exp((17.67 * temperature_c) / (temperature_c + 243.5))
    actual = 6.112 * np.exp((17.67 * dewpoint_c) / (dewpoint_c + 243.5))
    return (100.0 * actual / saturation).clip(min=0, max=100)


def validate_dataset(data: xr.Dataset) -> xr.Dataset:
    if not data.data_vars:
        raise DataValidationError("Forecast dataset contains no variables.")
    for name in data.data_vars:
        if bool(data[name].isnull().all()):
            raise DataValidationError(
                "Output variable contains only missing values.",
                [f"Variable: {name}"],
            )
    return data


def index_path_for(grib_path: str) -> str:
    digest = hashlib.sha1(grib_path.encode("utf-8")).hexdigest()[:8]
    return grib_path + f".{digest}.{{short_hash}}.idx"
