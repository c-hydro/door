"""Base class for forecast products organised by model issue time."""

from __future__ import annotations

import datetime as dt
import time
from collections.abc import Generator
from typing import Any

import xarray as xr

from d3tools import timestepping as ts
from d3tools.spatial import BoundingBox
from d3tools.timestepping import TimeRange
from d3tools.timestepping.timestep import TimeStep

from .base_downloaders import DOORDownloader
from .utils.exceptions import OperationalError


class ForecastDownloader(DOORDownloader):
    """Common issue-time behaviour for forecasts."""

    issue_hours: list[int] = []
    publication_delay_hours: int = 6

    def _resolve_output_destination(
        self,
        destination: Any,
        issue_time: dt.datetime,
        **tags: Any,
    ) -> str | None:
        if destination is None:
            return None
        timestep = ts.Hour.from_date(issue_time)
        if hasattr(destination, "get_key"):
            return destination.get_key(timestep, **tags)
        if isinstance(destination, str):
            return issue_time.strftime(destination).format(**tags)
        raise TypeError("Forecast raw destination must be a Dataset or path string")

    # Time management ---------
    def _get_timesteps(self, time_range: TimeRange) -> list[TimeStep]:
        if not self.issue_hours:
            return time_range.hours

        timesteps = time_range.get_timesteps_from_issue_hour(self.issue_hours)
        return [
            timestep
            for timestep in timesteps
            if time_range.start <= timestep.start <= time_range.end
        ]

    def get_last_published_ts(self, **kwargs) -> TimeStep:
        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        candidate = now - dt.timedelta(hours=self.publication_delay_hours)
        valid_hours = [hour for hour in self.issue_hours if hour <= candidate.hour]
        if valid_hours:
            issue = candidate.replace(
                hour=max(valid_hours), minute=0, second=0, microsecond=0
            )
        else:
            issue = (candidate - dt.timedelta(days=1)).replace(
                hour=max(self.issue_hours), minute=0, second=0, microsecond=0
            )
        return ts.Hour.from_date(issue)

    # Run lifecycle ---------
    def _get_data_ts(
        self,
        timestep: TimeStep,
        space_bounds: BoundingBox,
        tmp_path: str,
    ) -> Generator[tuple[xr.Dataset | xr.DataArray, dict], None, None]:
        issue_time = timestep.start
        start_time = time.monotonic()
        self.log.info(
            " --> Process forecast run %s UTC...",
            issue_time.strftime("%Y-%m-%d %H:%M"),
        )
        try:
            data = self._download_run(issue_time, space_bounds, tmp_path)
        except OperationalError:
            self.log.error(
                " --> Process forecast run %s UTC...FAILED",
                issue_time.strftime("%Y-%m-%d %H:%M"),
            )
            raise
        except Exception:
            self.log.error(
                " --> Process forecast run %s UTC...FAILED (unexpected error)",
                issue_time.strftime("%Y-%m-%d %H:%M"),
            )
            raise

        if data is not None:
            self.log.info(
                " --> Process forecast run %s UTC...DONE (%.1f seconds)",
                issue_time.strftime("%Y-%m-%d %H:%M"),
                time.monotonic() - start_time,
            )

            # Forecast runs can have different time-axis lengths depending on
            # the issue hour. Refresh the destination template from the current
            # run so d3tools does not reuse the previous run's temporal shape.
            if hasattr(self.destination, "set_template"):
                self.destination.set_template(data)

            yield data, {}

    def _download_run(
        self,
        issue_time: dt.datetime,
        space_bounds: BoundingBox,
        tmp_path: str,
    ) -> xr.Dataset | xr.DataArray:
        raise NotImplementedError
