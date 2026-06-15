"""make_client: httpx by default, curl_cffi (browser TLS) when impersonating."""

from __future__ import annotations

from typing import Any, ClassVar

import curl_cffi.requests
import httpx
import pytest
from curl_cffi.requests.exceptions import RequestException

from data_pipeline_core.ingestion.impersonation import _CurlClient, make_client


class _FakeCurlResponse:
    status_code = 200
    content = b"payload"
    text = "payload"

    def json(self) -> dict[str, bool]:
        return {"ok": True}


class _FakeSession:
    """Records construction + request kwargs; returns a canned response."""

    last: ClassVar[dict[str, Any]] = {}

    def __init__(self, **kwargs: Any) -> None:
        _FakeSession.last["init"] = kwargs

    def request(self, method: str, url: str, **kwargs: Any) -> _FakeCurlResponse:
        _FakeSession.last["request"] = {"method": method, "url": url, **kwargs}
        return _FakeCurlResponse()

    def close(self) -> None:
        _FakeSession.last["closed"] = True


def test_default_backend_is_httpx() -> None:
    client = make_client(None, timeout=5.0)
    assert isinstance(client, httpx.Client)
    client.close()


def test_impersonate_uses_curl_cffi(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(curl_cffi.requests, "Session", _FakeSession)

    client = make_client("chrome", timeout=7.0)
    assert isinstance(client, _CurlClient)
    assert _FakeSession.last["init"]["impersonate"] == "chrome"

    response = client.request(
        "POST", "https://x", headers={"a": "b"}, content=b"body", params={"p": "1"}
    )

    sent = _FakeSession.last["request"]
    assert sent["method"] == "POST"
    assert sent["data"] == b"body"  # content → data
    assert sent["params"] == {"p": "1"}
    assert sent["timeout"] == 7.0
    assert response.status_code == 200
    assert response.content == b"payload"
    assert response.is_success
    assert response.json() == {"ok": True}


def test_curl_error_normalized_to_httpx_transport_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _BoomSession(_FakeSession):
        def request(self, method: str, url: str, **kwargs: Any) -> _FakeCurlResponse:
            raise RequestException("boom")

    monkeypatch.setattr(curl_cffi.requests, "Session", _BoomSession)
    client = make_client("chrome", timeout=5.0)

    # Normalized so the run loop retries it like any other transport failure.
    with pytest.raises(httpx.TransportError):
        client.request("GET", "https://x")


def test_read_until_aborts_when_predicate_met(monkeypatch: pytest.MonkeyPatch) -> None:
    import curl_cffi
    from curl_cffi import CurlError, CurlOpt
    from curl_cffi.curl import CURL_WRITEFUNC_ERROR

    class _FakeCurl:
        """Drives the write callback with canned chunks, like a streaming POST."""

        chunks: ClassVar[list[bytes]] = [b"ab", b"cdef", b"ghij"]

        def __init__(self) -> None:
            self._writer: Any = None

        def setopt(self, opt: int, value: Any) -> int:
            if opt == CurlOpt.WRITEFUNCTION:
                self._writer = value
            return 0

        def impersonate(self, target: str, default_headers: bool = True) -> int:
            return 0

        def perform(self) -> None:
            for chunk in self.chunks:
                if self._writer(chunk) == CURL_WRITEFUNC_ERROR:
                    raise CurlError("aborted by callback")

        def getinfo(self, info: int) -> int:
            return 200

        def close(self) -> None: ...

    monkeypatch.setattr(curl_cffi, "Curl", _FakeCurl)
    client = make_client("chrome", timeout=5.0)
    assert isinstance(client, _CurlClient)

    # Predicate trips once >=3 bytes are buffered: ab (2) -> abcdef (6) -> abort.
    status, body = client.read_until(
        "POST", "https://x", until=lambda b: len(b) >= 3, content=b"q"
    )
    assert status == 200
    assert body == b"abcdef"  # third chunk never delivered — we aborted
