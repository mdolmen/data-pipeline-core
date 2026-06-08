"""Settings defaults and environment overrides."""

from __future__ import annotations

import pytest

from data_pipeline_core import Settings


def test_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    settings = Settings()
    assert settings.log_level == "INFO"
    assert settings.log_format == "json"


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("LOG_FORMAT", "console")
    settings = Settings()
    assert settings.log_level == "DEBUG"
    assert settings.log_format == "console"
