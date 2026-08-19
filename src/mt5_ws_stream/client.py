"""Async WebSocket client for consuming a bridge.

.. code-block:: python

    import asyncio
    from mt5_ws_stream import TickStreamClient

    async def main() -> None:
        async with TickStreamClient("ws://127.0.0.1:8765/ws", symbols=["EURUSD"]) as stream:
            async for tick in stream:
                print(tick.symbol, tick.bid, tick.ask, tick.spread)

    asyncio.run(main())

The client hides the two payload formats behind one :class:`~mt5_ws_stream.protocol.Tick`
stream, so switching ``payload_format`` is a one-word change with no effect on
consumer code.

This module owns exactly one job: joining the ``websockets`` transport to
:func:`~mt5_ws_stream.decoder.decode_frame`. Everything about what a frame
*means* lives in :mod:`~mt5_ws_stream.frames`, the one home of the frame
grammar, which needs no socket to test.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Sequence
from types import TracebackType
from typing import Any, Final, Protocol
from urllib.parse import urlsplit

from websockets.asyncio.client import connect as ws_connect

from .decoder import ControlFrame, DecodedFrame, FrameKind, TickFrame, decode_frame
from .protocol import BackpressurePolicy, PayloadFormat, Tick
from .subscription import SubscriptionRequest, normalize_symbols

__all__ = ["STREAM_PATH", "Connection", "HandshakeError", "TickStreamClient"]

STREAM_PATH: Final = "/ws"
"""Where a bridge serves the tick stream.

Spelled out here rather than imported from :mod:`mt5_ws_stream.api`, which the
client must not pull in: naming it keeps the client's default in step with the
route the server actually mounts (`docs/protocol.md §2`).
"""


class HandshakeError(ConnectionError):
    """Raised when a bridge's first frame is not the promised ``hello``.

    A :class:`ConnectionError` because that is what it is: the connection came
    up but the peer is not speaking this frame grammar, so nothing after it can
    be trusted. The ``hello``-first guarantee is part of the grammar
    (`docs/protocol.md §2`), not a nicety -- see ADR-0003.
    """


class Connection(Protocol):
    """The slice of a WebSocket connection this client uses.

    Naming it makes the transport an argument rather than a hard-wired import:
    :class:`TickStreamClient` takes a ``connect_fn`` returning one of these, so
    the connect handshake and the streaming loop can be exercised against a fake
    with no server involved.
    """

    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...

    def __aiter__(self) -> AsyncIterator[str | bytes]: ...


#: How a connection is opened. The default is ``websockets``' ``connect``.
ConnectFn = Callable[..., Awaitable[Connection]]


class TickStreamClient:
    """Connects to a bridge and yields :class:`~mt5_ws_stream.protocol.Tick` objects.

    Args:
        url: Bridge WebSocket URL, e.g. ``ws://127.0.0.1:8765/ws``. A URL that
            names no path gets :data:`STREAM_PATH` (``/ws``) appended -- ``/``
            is the bridge's HTTP index, not the stream.
        symbols: Optional allow-list. ``None`` subscribes to everything.
        payload_format: ``json`` is easiest to debug; ``binary`` is cheaper to decode.
        backpressure: ``conflate`` keeps only the newest tick per symbol when the
            consumer falls behind -- right for display, wrong for recording.
        include_heartbeats: Yield keep-alive records too. Useful to distinguish
            "market is quiet" from "the link died".
        handshake_timeout: Seconds to wait for the ``hello`` frame once the
            socket is up, or ``None`` to wait forever. The transport's own
            ``open_timeout`` bounds the *WebSocket* handshake and stops there:
            a peer that completes the upgrade and then says nothing -- a
            wrong-but-live port, a wedged bridge -- would otherwise leave
            :meth:`connect` blocked with no error to report.
        connect_fn: How to open the transport. Defaults to
            :func:`websockets.asyncio.client.connect`; it is called as
            ``connect_fn(url, **connect_kwargs)``.
        connect_kwargs: Passed through to *connect_fn*.
    """

    def __init__(
        self,
        url: str = "ws://127.0.0.1:8765/ws",
        *,
        symbols: Iterable[str] | None = None,
        payload_format: PayloadFormat | str = PayloadFormat.JSON,
        backpressure: BackpressurePolicy | str = BackpressurePolicy.LOSSLESS,
        include_heartbeats: bool = False,
        handshake_timeout: float | None = 10.0,
        connect_fn: ConnectFn | None = None,
        **connect_kwargs: Any,
    ) -> None:
        self._url = _build_url(
            url,
            symbols=symbols,
            payload_format=PayloadFormat(payload_format),
            backpressure=BackpressurePolicy(backpressure),
            include_heartbeats=include_heartbeats,
        )
        self._handshake_timeout = handshake_timeout
        self._connect_fn: ConnectFn = ws_connect if connect_fn is None else connect_fn
        self._connect_kwargs = {"max_queue": None, "compression": None, **connect_kwargs}
        self._connection: Connection | None = None
        self._hello: dict[str, Any] | None = None

    @property
    def url(self) -> str:
        """The fully-built URL, query string included."""
        return self._url

    @property
    def hello(self) -> dict[str, Any] | None:
        """The server's ``hello`` payload; set by :meth:`connect`, ``None`` before it."""
        return self._hello

    async def connect(self) -> TickStreamClient:
        """Open the connection and consume the ``hello`` frame.

        Raises:
            HandshakeError: the first frame was not a ``hello``, or none
                arrived within ``handshake_timeout``. Anything the transport
                itself raises -- refused, closed before saying anything --
                propagates unchanged; a client that cannot tell a bridge from a
                wrong port is worse than one that fails loudly.
        """
        connection = await self._connect_fn(self._url, **self._connect_kwargs)
        self._connection = connection
        try:
            # The socket being up is not the peer being a bridge: a live port
            # that never sends `hello` must fail, not hang.
            async with asyncio.timeout(self._handshake_timeout):
                frame = decode_frame(await connection.recv())
        except TimeoutError as exc:
            await self.aclose()
            if self._handshake_timeout is None:
                raise
            raise HandshakeError(f"no `hello` frame within {self._handshake_timeout}s") from exc
        except BaseException:
            await self.aclose()
            raise
        if not isinstance(frame, ControlFrame) or frame.kind != FrameKind.HELLO:
            await self.aclose()
            raise HandshakeError(f"expected a `hello` frame first, got {_describe(frame)}")
        self._hello = frame.payload
        return self

    async def aclose(self) -> None:
        if self._connection is not None:
            connection, self._connection = self._connection, None
            with contextlib.suppress(Exception):
                await connection.close()

    async def __aenter__(self) -> TickStreamClient:
        return await self.connect()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def __aiter__(self) -> AsyncIterator[Tick]:
        return self.ticks()

    async def ticks(self) -> AsyncIterator[Tick]:
        """Yield ticks, dropping frame boundaries and control frames.

        The right loop when a consumer only wants prices. Use :meth:`stream` when
        it also wants the hop, the batch size, or the control frames.
        """
        async for frame in self.stream():
            if isinstance(frame, TickFrame):
                for tick in frame.ticks:
                    yield tick

    async def stream(self) -> AsyncIterator[DecodedFrame]:
        """Yield one decoded frame per WebSocket message, in arrival order.

        Each :class:`~mt5_ws_stream.decoder.TickFrame` carries its own ``rx`` and
        receive time, so ``frame.hop`` and ``len(frame.ticks)`` are properties of
        the value in hand -- no ordering constraint, nothing to read "before the
        next one arrives".
        """
        connection = self._require_connection()
        async for message in connection:
            yield decode_frame(message)

    # -- control ---------------------------------------------------------

    async def subscribe(self, symbols: Sequence[str]) -> None:
        """Add symbols to this connection's allow-list."""
        await self._send({"op": "subscribe", "symbols": list(symbols)})

    async def unsubscribe(self, symbols: Sequence[str]) -> None:
        """Remove symbols from this connection's allow-list."""
        await self._send({"op": "unsubscribe", "symbols": list(symbols)})

    async def set_format(self, payload_format: PayloadFormat | str) -> None:
        """Switch payload encoding mid-stream."""
        await self._send({"op": "format", "value": PayloadFormat(payload_format).value})

    async def request_stats(self) -> None:
        """Ask for a ``stats`` frame; it arrives via :meth:`stream`."""
        await self._send({"op": "stats"})

    async def ping(self, echo: Any = None) -> None:
        """Application-level ping; the ``pong`` arrives via :meth:`stream`."""
        await self._send({"op": "ping", "echo": echo})

    async def _send(self, payload: dict[str, Any]) -> None:
        await self._require_connection().send(json.dumps(payload, separators=(",", ":")))

    def _require_connection(self) -> Connection:
        if self._connection is None:
            raise RuntimeError("not connected; use `async with TickStreamClient(...)`")
        return self._connection


def _describe(frame: DecodedFrame) -> str:
    if isinstance(frame, TickFrame):
        return f"a `ticks` frame ({len(frame.ticks)} ticks)"
    return f"a `{frame.kind}` frame"


def _build_url(
    url: str,
    *,
    symbols: Iterable[str] | None,
    payload_format: PayloadFormat,
    backpressure: BackpressurePolicy,
    include_heartbeats: bool,
) -> str:
    request = SubscriptionRequest(
        symbols=None if symbols is None else normalize_symbols(symbols),
        payload_format=payload_format,
        backpressure=backpressure,
        include_heartbeats=include_heartbeats,
    )
    base = url.rstrip("/")
    # A URL that names no path gets the stream path rather than a bare root
    # slash: the bridge serves the stream at "/ws" and "/" is its HTTP index
    # (ADR-0002/0003), so "ws://h:1/?..." would reach an endpoint that does not
    # speak WebSocket. A URL that already names a path is left exactly as given
    # -- it must not gain a trailing slash either, because the bridge's ASGI
    # router answers "/ws/" with a 307 redirect and WebSocket clients cannot
    # follow a redirect: the connection just fails.
    if not urlsplit(base).path:
        base = f"{base}{STREAM_PATH}"
    return f"{base}?{request.to_query_string()}"
