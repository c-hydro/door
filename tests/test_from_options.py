"""Tests for DOORDownloader.from_options factory behavior."""

import pytest

from door.base_downloaders import DOORDownloader


class _TestDownloader(DOORDownloader):
    source = "__test_source__"
    default_options = {}

    def __init__(self, required_arg: str, extra: str | None = None):
        super().__init__()
        self.required_arg = required_arg
        self.extra = extra

    def _get_data_ts(self, time_range, space_bounds, tmp_path):
        return []


class TestDoorDownloaderFromOptions:
    """Test DOOR downloader object construction from options payloads."""

    def test_from_options_with_mapping_builds_downloader(self, monkeypatch):
        source_cfg = {
            "source": "__test_source__",
            "required_arg": "A",
            "extra": "from_mapping",
            "bounds": [1.0, 2.0, 3.0, 4.0],
            "destination": "/tmp/file.tif",
            "options": {"x": 1},
        }
        original_cfg = source_cfg.copy()
        original_cfg["options"] = source_cfg["options"].copy()

        calls = {}

        monkeypatch.setattr(DOORDownloader, "set_bounds", lambda self, bounds: calls.setdefault("bounds", bounds))
        monkeypatch.setattr(
            DOORDownloader,
            "set_destination",
            lambda self, destination: calls.setdefault("destination", destination),
        )
        monkeypatch.setattr(DOORDownloader, "set_options", lambda self, options: calls.setdefault("options", options))

        downloader = DOORDownloader.from_options(source_cfg)

        assert isinstance(downloader, _TestDownloader)
        assert downloader.required_arg == "A"
        assert downloader.extra == "from_mapping"
        assert calls["bounds"] == [1.0, 2.0, 3.0, 4.0]
        assert calls["destination"] == "/tmp/file.tif"
        assert calls["options"] == {"x": 1}
        assert source_cfg == original_cfg

    def test_from_options_with_source_string_and_kwargs(self, monkeypatch):
        calls = {}

        monkeypatch.setattr(DOORDownloader, "set_bounds", lambda self, bounds: calls.setdefault("bounds", bounds))
        monkeypatch.setattr(
            DOORDownloader,
            "set_destination",
            lambda self, destination: calls.setdefault("destination", destination),
        )
        monkeypatch.setattr(DOORDownloader, "set_options", lambda self, options: calls.setdefault("options", options))

        downloader = DOORDownloader.from_options(
            "__test_source__",
            required_arg="A",
            extra="from_kwargs",
            options={"foo": "bar"},
        )

        assert isinstance(downloader, _TestDownloader)
        assert downloader.required_arg == "A"
        assert downloader.extra == "from_kwargs"
        assert calls["bounds"] is None
        assert calls["destination"] is None
        assert calls["options"] == {"foo": "bar"}

    def test_from_options_allows_none_options(self, monkeypatch):
        calls = {}

        monkeypatch.setattr(DOORDownloader, "set_bounds", lambda self, bounds: None)
        monkeypatch.setattr(DOORDownloader, "set_destination", lambda self, destination: None)
        monkeypatch.setattr(DOORDownloader, "set_options", lambda self, options: calls.setdefault("options", options))

        DOORDownloader.from_options(
            "__test_source__",
            required_arg="A",
            options=None,
        )

        assert calls["options"] == {}

    def test_from_options_rejects_invalid_source_type(self):
        with pytest.raises(TypeError, match="'source' must be a mapping, a string, or None"):
            DOORDownloader.from_options(123, required_arg="A")

    def test_from_options_requires_source_on_base_factory(self):
        with pytest.raises(ValueError, match="No data source specified in downloader options"):
            DOORDownloader.from_options({}, required_arg="A")

    def test_from_options_rejects_unknown_source(self):
        with pytest.raises(ValueError, match="Invalid data source: unknown-source"):
            DOORDownloader.from_options("unknown-source", required_arg="A")

    def test_from_options_rejects_non_mapping_options(self):
        with pytest.raises(TypeError, match="'options' must be a mapping"):
            DOORDownloader.from_options(
                "__test_source__",
                required_arg="A",
                options="invalid",
            )
