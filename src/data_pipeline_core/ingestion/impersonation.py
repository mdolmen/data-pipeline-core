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

from collections.abc import Callable
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

        self._impersonate = impersonate
        self._session = cffi.Session(impersonate=impersonate)
        self._timeout = timeout
        self._proxy = proxy
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

    def read_until(
        self,
        method: str,
        url: str,
        *,
        until: Callable[[bytes], bool],
        headers: dict[str, str] | None = None,
        content: bytes | None = None,
    ) -> tuple[int, bytes]:
        """Stream a request and abort the instant ``until(buffer)`` is true.

        For server streams that never close (e.g. gRPC-web ``…WithNotifications``):
        the snapshot is the first frame and arrives at once, but the connection
        then stays open. curl_cffi's graceful stop only fires on the *next* write
        callback — which an idle stream never delivers — so a normal close blocks
        for tens of seconds. Here we drive curl at the low level and abort from
        inside the write callback (``CURL_WRITEFUNC_ERROR``) the moment ``until``
        is satisfied, so teardown is immediate. The caller owns ``headers`` in
        full (browser identity included); we keep the TLS fingerprint but let curl
        own ``Accept-Encoding`` so it decompresses for us.
        """
        from curl_cffi import Curl, CurlError, CurlInfo, CurlOpt
        from curl_cffi.curl import CURL_WRITEFUNC_ERROR

        buffer = bytearray()
        aborted = False

        abort_code = int(CURL_WRITEFUNC_ERROR)

        def writer(chunk: bytes) -> int:
            nonlocal aborted
            buffer.extend(chunk)
            if not aborted and until(bytes(buffer)):
                aborted = True
                return abort_code  # stop perform now — predicate met
            return len(chunk)

        c = Curl()
        c.setopt(CurlOpt.URL, url.encode())
        if method.upper() == "POST":
            c.setopt(CurlOpt.POST, 1)
            if content is not None:
                c.setopt(CurlOpt.POSTFIELDS, content)
                c.setopt(CurlOpt.POSTFIELDSIZE, len(content))
        elif method.upper() != "GET":
            c.setopt(CurlOpt.CUSTOMREQUEST, method.upper().encode())
        if headers:
            c.setopt(
                CurlOpt.HTTPHEADER, [f"{k}: {v}".encode() for k, v in headers.items()]
            )
        c.setopt(CurlOpt.WRITEFUNCTION, writer)
        c.setopt(CurlOpt.ACCEPT_ENCODING, b"")  # advertise + auto-decompress
        c.setopt(CurlOpt.TIMEOUT, max(1, int(self._timeout)))
        if self._proxy:
            c.setopt(CurlOpt.PROXY, self._proxy.encode())
        # Headers first, then impersonate, matching curl_cffi's own ordering.
        c.impersonate(self._impersonate, default_headers=False)
        try:
            c.perform()
        except CurlError as exc:
            # A predicate-driven abort surfaces as CurlError; that's expected.
            # Anything else with no HTTP status is a real transport failure.
            if not aborted:
                status = int(c.getinfo(CurlInfo.RESPONSE_CODE))
                c.close()
                if status == 0:
                    raise httpx.TransportError(str(exc)) from exc
                return status, bytes(buffer)
        status = int(c.getinfo(CurlInfo.RESPONSE_CODE))
        c.close()
        return status, bytes(buffer)

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
