"""Browser-TLS impersonation via curl_cffi, behind an httpx-compatible shim.

Anti-bot defences (DataDome/Akamai/Cloudflare) fingerprint the TLS + HTTP/2
handshake (JA3/JA4), not the IP — a standard Python ``httpx`` client is
fingerprinted and blocked where a real browser is not (a residential proxy does
not help: same handshake). Setting ``impersonate`` (e.g. ``"chrome"``) swaps the
backend to ``curl_cffi``, which reproduces a browser's handshake.

The swap is invisible to the rest of the SDK: ``_CurlClient`` mimics the slice of
``httpx.Client`` the run loop uses — ``request(...)`` returning a response with
``status_code`` / ``content`` / ``text`` / ``json()`` / ``is_success`` — and
raises ``httpx.TransportError`` on failure, so retry/breaker handling is unchanged.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import httpx


@runtime_checkable
class Response(Protocol):
    """The response surface the SDK and its sources rely on (read-only)."""

    @property
    def status_code(self) -> int: ...
    @property
    def content(self) -> bytes: ...
    @property
    def text(self) -> str: ...
    def json(self) -> Any: ...


class _CurlResponse:
    """Adapts a curl_cffi response to the ``httpx.Response`` surface we use."""

    def __init__(self, raw: Any) -> None:
        self._raw = raw

    @property
    def status_code(self) -> int:
        return int(self._raw.status_code)

    @property
    def content(self) -> bytes:
        return bytes(self._raw.content)

    @property
    def text(self) -> str:
        return str(self._raw.text)

    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> Any:
        return self._raw.json()


class _CurlClient:
    """An ``httpx.Client``-shaped wrapper around a curl_cffi session."""

    def __init__(
        self, *, impersonate: str, timeout: float, proxy: str | None = None
    ) -> None:
        from curl_cffi import requests as cffi

        self._session = cffi.Session(impersonate=impersonate)
        self._timeout = timeout
        self._proxies = {"http": proxy, "https": proxy} if proxy else None

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        content: bytes | None = None,
        params: dict[str, Any] | None = None,
    ) -> _CurlResponse:
        from curl_cffi.requests.exceptions import RequestException

        try:
            # curl_cffi uses `data` for the raw body (httpx calls it `content`).
            raw = self._session.request(
                method,
                url,
                headers=headers,
                data=content,
                params=params,
                timeout=self._timeout,
                proxies=self._proxies,
            )
        except RequestException as exc:
            # Normalize to httpx's transport error so the run loop retries it.
            raise httpx.TransportError(str(exc)) from exc
        return _CurlResponse(raw)

    def close(self) -> None:
        self._session.close()


# Either backend; both expose the request/close surface the run loop needs.
Client = httpx.Client | _CurlClient


def make_client(
    impersonate: str | None,
    *,
    timeout: float,
    proxy: str | None = None,
    transport: httpx.BaseTransport | None = None,
) -> Client:
    """Build the HTTP client: httpx by default, curl_cffi when impersonating."""
    if impersonate:
        return _CurlClient(impersonate=impersonate, timeout=timeout, proxy=proxy)
    return httpx.Client(timeout=timeout, proxy=proxy, transport=transport)
