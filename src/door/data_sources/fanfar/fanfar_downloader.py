"""Read FANFAR hydrographs from the FloodProofs PostgreSQL database."""

from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path
from typing import Any

from d3tools import timestepping as ts

from ...base_downloaders import DOORDownloader
from ...utils.exceptions import ConfigurationError, DataUnavailableError, DownloadError


class FANFARDownloader(DOORDownloader):
    """Download a configured FANFAR run as one text file per section."""

    source = "FANFAR_DATABASE"
    source_aliases = ["FANFAR", "FLOODPROOFS_DATABASE", "FANFAR_AGRHYMET"]
    name = "FANFAR_database_downloader"
    default_options = {
        "variables": {"hydrograph": "data"},
        "database": {},
        "series": "fanfar.insitu",
        "subset_type": "section",
        "subset": [],
        "run_selection": "daily",
        "statement_timeout_seconds": 120,
        "overwrite_existing": True,
    }

    def __init__(self, product: str = "AGRHYMET") -> None:
        super().__init__()
        if product.upper() not in {"AGRHYMET", "FANFAR"}:
            raise ConfigurationError(
                "Unsupported FANFAR product.", [f"Value: {product}"]
            )
        self.product = product.upper()

    def set_variables(self, variables: Any) -> None:
        self.variables = variables

    @staticmethod
    def _reference_time(time_range: Any) -> dt.datetime:
        value = getattr(time_range, "end", None) or getattr(time_range, "start", None)
        value = getattr(value, "start", value)
        if not isinstance(value, dt.datetime):
            raise ConfigurationError("A FANFAR reference time is required.")
        return value

    @staticmethod
    def _clean_name(value: Any, allow_empty: bool = False) -> str:
        value = "" if value is None else str(value).strip()
        if allow_empty and value in {"", "-"}:
            return ""
        value = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
        if not value and not allow_empty:
            return "unknown"
        return value

    def _validate_options(self) -> None:
        required_database = {"host", "port", "name", "user", "password"}
        if not isinstance(self.database, dict):
            raise ConfigurationError("FANFAR database settings must be a mapping.")
        missing = required_database - set(self.database)
        if missing:
            raise ConfigurationError(
                "FANFAR database settings are incomplete.", sorted(missing)
            )
        if self.subset_type not in {None, "basin", "section"}:
            raise ConfigurationError(
                "Invalid FANFAR subset type.", [f"Value: {self.subset_type}"]
            )
        if self.subset_type is not None and not self.subset:
            raise ConfigurationError("The FANFAR subset cannot be empty.")
        if self.run_selection not in {"daily", "exact"}:
            raise ConfigurationError(
                "Invalid FANFAR run selection.", [f"Value: {self.run_selection}"]
            )

    def _connect(self):
        try:
            import pg8000.dbapi
        except ImportError as error:
            raise ConfigurationError(
                "The PostgreSQL client is not installed.", ["Package: pg8000"]
            ) from error
        try:
            connection = pg8000.dbapi.connect(
                host=self.database["host"],
                port=self.database["port"],
                database=self.database["name"],
                user=self.database["user"],
                password=self.database["password"],
                timeout=int(
                    self.database.get("connect_timeout_seconds", 15)
                ),
            )
            connection.autocommit = True
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;"
                )
            finally:
                cursor.close()
            return connection
        except Exception as error:
            raise DownloadError(
                "Unable to connect to the read-only FANFAR database.",
                [
                    f"Host: {self.database.get('host')}",
                    f"Database: {self.database.get('name')}",
                    str(error),
                ],
            ) from error

    def _resolve_run(self, cursor, requested: dt.datetime) -> dt.datetime:
        if self.run_selection == "exact":
            return requested
        start = requested.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + dt.timedelta(days=1)
        cursor.execute(
            """
            SELECT DISTINCT dtrun
            FROM public.fpdata_new
            WHERE serie = %s
              AND dtrun >= %s
              AND dtrun < %s
            ORDER BY dtrun;
            """,
            (self.series, start, end),
        )
        available = [row[0] for row in cursor.fetchall()]
        if not available:
            raise DataUnavailableError(
                "No FANFAR run is available for the requested day.",
                [f"Series: {self.series}", f"Date: {start:%Y-%m-%d}"],
            )
        if len(available) > 1:
            raise DataUnavailableError(
                "More than one FANFAR run is available for the requested day.",
                [
                    f"Series: {self.series}",
                    *(run.strftime("%Y-%m-%d %H:%M") for run in available),
                    "Set run_selection to exact and provide the required time.",
                ],
            )
        return available[0]

    def _build_query(self, selected_run: dt.datetime) -> tuple[str, list[Any]]:
        query = """
            SELECT serie, section, basin, dtrun, data
            FROM public.fpdata_new
            WHERE serie = %s
              AND dtrun = %s
        """
        parameters: list[Any] = [self.series, selected_run]
        if self.subset_type == "basin":
            query += "\n AND basin = ANY(%s)\n"
            parameters.append(self.subset)
        elif self.subset_type == "section":
            query += "\n AND section = ANY(%s)\n"
            parameters.append(self.subset)
        query += "\n ORDER BY basin, section;\n"
        return query, parameters

    def _output_path(
        self,
        selected_run: dt.datetime,
        basin: Any,
        section: Any,
    ) -> Path:
        clean_basin = self._clean_name(basin, allow_empty=True)
        clean_section = self._clean_name(section)
        try:
            timestep = ts.Day.from_date(selected_run)
            path = self.destination.get_key(
                timestep,
                basin=clean_basin,
                section=clean_section,
                file_format="txt",
            )
        except Exception as error:
            raise ConfigurationError(
                "Unable to resolve a FANFAR output path.",
                [f"Section: {clean_section}", str(error)],
            ) from error
        return Path(path)

    @staticmethod
    def _write_atomic(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".part")
        temporary.unlink(missing_ok=True)
        try:
            with open(temporary, "w", encoding="utf-8") as stream:
                stream.write(content)
            os.replace(temporary, path)
        except OSError as error:
            temporary.unlink(missing_ok=True)
            raise DownloadError(
                "Unable to write a FANFAR hydrograph.", [str(path), str(error)]
            ) from error

    def get_data(self, time_range, space_bounds=None, destination=None, options=None):
        if options is not None:
            self.set_options(options)
        if destination is not None:
            self.set_destination(destination)
        if not hasattr(self, "destination"):
            raise ConfigurationError("The FANFAR destination is not configured.")
        self._validate_options()
        requested_run = self._reference_time(time_range)

        connection = self._connect()
        cursor = None
        try:
            cursor = connection.cursor()
            timeout = int(
                self.database.get(
                    "statement_timeout_seconds", self.statement_timeout_seconds
                )
            )
            cursor.execute(f"SET statement_timeout = {timeout * 1000};")
            selected_run = self._resolve_run(cursor, requested_run)
            self.log.info(
                " --> Selected FANFAR run: %s", selected_run.strftime("%Y-%m-%d %H:%M")
            )
            query, parameters = self._build_query(selected_run)
            cursor.execute(query, parameters)

            found_values: set[str] = set()
            records = 0
            written = 0
            skipped = 0
            for _, section, basin, _, data in cursor:
                records += 1
                selected_value = basin if self.subset_type == "basin" else section
                if self.subset_type is not None:
                    found_values.add(str(selected_value))
                output = self._output_path(selected_run, basin, section)
                if output.exists() and not self.overwrite_existing:
                    skipped += 1
                    continue
                self._write_atomic(output, "" if data is None else data)
                written += 1

            self.log.info(
                " --> FANFAR records=%d written=%d skipped=%d",
                records,
                written,
                skipped,
            )
            if records == 0:
                raise DataUnavailableError(
                    "No FANFAR hydrograph was returned.",
                    [f"Run: {selected_run:%Y-%m-%d %H:%M}"],
                )
            if self.subset_type is not None:
                requested_values = {str(value) for value in self.subset}
                missing = sorted(requested_values - found_values)
                if missing:
                    raise DataUnavailableError(
                        "The FANFAR hydrograph set is incomplete.",
                        [
                            f"Run: {selected_run:%Y-%m-%d %H:%M}",
                            f"Expected: {len(requested_values)}",
                            f"Found: {len(found_values)}",
                            "Missing: " + ", ".join(missing),
                        ],
                    )
        except (ConfigurationError, DataUnavailableError, DownloadError):
            raise
        except Exception as error:
            raise DownloadError(
                "The FANFAR database query failed.",
                [f"Series: {self.series}", str(error)],
            ) from error
        finally:
            if cursor is not None:
                cursor.close()
            connection.close()

    def _get_data_ts(self, time_range, space_bounds, tmp_path):
        raise NotImplementedError

    def get_last_published_ts(self):
        raise NotImplementedError
