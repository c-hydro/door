"""DOOR exceptions with concise operational diagnostics."""

from __future__ import annotations

from collections.abc import Iterable


class DoorError(RuntimeError):
    """Base exception for DOOR failures."""


class OperationalError(DoorError):
    """Expected operational failure that should be logged without a traceback."""

    exit_code = 1

    def __init__(self, summary: str, details: Iterable[object] | None = None) -> None:
        super().__init__(summary)
        self.summary = summary
        self.details = [str(item) for item in (details or [])]

    def __str__(self) -> str:
        if not self.details:
            return self.summary
        return self.summary + " | " + " | ".join(self.details)


class ConfigurationError(OperationalError):
    """The downloader configuration is missing, malformed or inconsistent."""


class ForecastUnavailableError(OperationalError):
    """The requested forecast issue or lead time is not published."""

    exit_code = 2


class DataUnavailableError(OperationalError):
    """A requested observation timestep is not yet published or is absent."""

    exit_code = 2


class VariableUnavailableError(OperationalError):
    """A requested variable cannot be located in the remote product."""


class DownloadError(OperationalError):
    """A remote file could not be downloaded or validated."""


class DataValidationError(OperationalError):
    """Downloaded data are present but unsuitable for processing."""


class ExternalToolError(OperationalError):
    """A required external executable is unavailable or failed."""


class ProcessingError(OperationalError):
    """Downloaded data could not be transformed into the requested output."""
