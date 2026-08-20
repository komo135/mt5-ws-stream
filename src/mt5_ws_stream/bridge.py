"""The bridge process: a TCP listener for feeders, an HTTP+WebSocket server for
consumers.

::

    MetaTrader 5  --TCP-->  Bridge  --WebSocket-->  browsers / bots / recorders
      (EA, OnTick)          (here)   --REST------>  dashboards / health checks

One port serves everything a consumer needs: the tick stream at ``/ws``, the
read-only REST API under ``/api/v1``, the bundled dashboard at ``/dashboard`` and
the OpenAPI page at ``/docs``. The routes live in :mod:`mt5_ws_stream.api`; this
module owns the sockets and the process lifecycle.

Run it with ``mt5-ws-stream bridge``, or embed it:

.. code-block:: python

    import asyncio
    import contextlib
    import signal

    from mt5_ws_stream import Bridge, BridgeConfig

    async def main() -> None:
        # Wait for Ctrl-C / SIGTERM, not for the bridge to close itself --
        # nothing closes it until this handler fires.
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig_name in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, sig_name, None)
            if sig is None:
                continue
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.add_signal_handler(sig, stop.set)

        async with Bridge(BridgeConfig(http_port=9001)):
            await stop.wait()

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Final

import uvicorn

from .api import create_app, stats_line
from .hub import FeederLink, Hub, HubStats
from .protocol import ProtocolError
from .session import Session

__all__ = ["Bridge", "BridgeConfig"]

log = logging.getLogger("mt5_ws_stream.bridge")

_READ_CHUNK: Final[int] = 65_536

_LISTEN_BACKLOG: Final[int] = 128

#: uvicorn's WebSocket implementation.
#:
#: ``websockets-sansio`` rather than the default ``auto``: ``auto`` resolves to
#: uvicorn's legacy ``websockets`` protocol, which imports ``websockets.legacy``
#: and therefore emits a DeprecationWarning on websockets >= 14 -- fatal under
#: this project's ``filterwarnings = error``. The sansio protocol reuses the
#: ``websockets`` dependency the client already needs, so it also avoids pulling
#: in ``wsproto``, the only other clean option.
_WS_IMPL: Final[str] = "websockets-sansio"


@dataclass(slots=True)
class BridgeConfig:
    """Everything the bridge needs to start.

    Defaults bind to loopback.
    """

    tcp_host: str = "127.0.0.1"
    tcp_port: int = 9800

    ws_host: str = "127.0.0.1"
    http_port: int = 8765
    """Bind address and port of the consumer server.

    One socket serves *both* the WebSocket stream (``/ws``) and the REST API,
    dashboard and OpenAPI docs over plain HTTP -- hence ``http_port``, not
    ``ws_port`` (ADR-0003 removed that alias).
    """

    queue_limit: int = 20_000
    """Ticks buffered per lossless subscriber before the oldest half is shed."""

    stats_interval_s: float = 10.0
    """How often to log stats and push a ``stats`` frame. ``0`` disables both."""

    stats_send_timeout_s: float = 1.0
    """Per-session cap on the periodic ``stats`` broadcast (:meth:`Bridge.report_once`).

    Sessions are fanned out concurrently, but each send is capped on its own: a
    consumer whose sink never drains -- uvicorn's sansio protocol sends no
    server-initiated pings, see ``ws_ping_interval_s`` above -- must not delay
    the frame everyone else is waiting on. A timed-out or failing session is
    logged at debug and skipped; its own connection handler discovers the dead
    socket separately. Kept small and independently configurable so tests can
    drive it well below ``stats_interval_s``.
    """

    ws_ping_interval_s: float | None = 20.0
    ws_ping_timeout_s: float | None = 20.0
    """Forwarded to uvicorn as ``ws_ping_interval`` / ``ws_ping_timeout``.

    uvicorn's sansio and wsproto WebSocket protocols do not currently send
    server-initiated keep-alive pings, so these are advisory: dead peers are
    detected by TCP, and by the feeder's own heartbeat records. Kept because the
    values are correct the moment uvicorn honours them again.
    """

    allowed_origins: frozenset[str] | None = None
    """If set, reject browser connections whose ``Origin`` is not listed.

    Only meaningful against browsers -- non-browser clients choose their own
    headers. It is a CSRF-style guard, not authentication. Applies to the
    WebSocket only; the REST API is read-only and CORS-open.
    """

    extra_serve_kwargs: dict[str, Any] = field(default_factory=dict)
    """Escape hatch passed through to :class:`uvicorn.Config`."""


class Bridge:
    """Owns the TCP listener, the HTTP/WebSocket server and the
    :class:`~mt5_ws_stream.hub.Hub`."""

    def __init__(self, config: BridgeConfig | None = None) -> None:
        self.config = config or BridgeConfig()
        self.hub = Hub(queue_limit=self.config.queue_limit)
        self._tcp_server: asyncio.Server | None = None
        self._http_socket: socket.socket | None = None
        self._http_port = self.config.http_port
        self._http_server: uvicorn.Server | None = None
        self._http_task: asyncio.Task[None] | None = None
        self._feeders: dict[int, FeederLink] = {}
        self._next_feeder_id = 0
        # Filled and emptied by the WebSocket handler; read by the periodic
        # stats broadcast, which is the one thing here that writes to consumers.
        self._sessions: set[Session] = set()
        self._stats_task: asyncio.Task[None] | None = None
        # Teardown for each resource that has actually been started, in start
        # order: TCP feeder listener, HTTP socket, HTTP/uvicorn server, stats
        # loop. `aclose` (and a failed `start`, via `_unwind`) both just pop
        # this LIFO -- one mechanism for "shut down cleanly" and "a half-bound
        # bridge would keep the feeder port hostage", instead of two hand-
        # written teardown paths that have to be kept in step by hand.
        self._teardown: list[Callable[[], Awaitable[None]]] = []
        self._closed = asyncio.Event()

    # -- lifecycle -------------------------------------------------------

    async def start(self) -> None:
        """Bind both listeners and start the stats task.

        Each stage below pushes its own teardown onto ``self._teardown`` only
        once it has actually succeeded. If a later stage fails, the
        ``except`` unwinds everything that made it onto the stack so far, in
        reverse -- see the class-level note on ``self._teardown`` for why
        that is also exactly what ``aclose`` does.
        """
        cfg = self.config
        try:
            # reuse_address only on POSIX, where it merely skips TIME_WAIT. On
            # Windows SO_REUSEADDR lets a *second* bridge bind the same port
            # while the first is still listening, and feeders then land on
            # whichever socket the kernel picks -- a silent split-brain.
            # asyncio's default already makes that distinction; be explicit
            # so nobody "fixes" it.
            self._tcp_server = await asyncio.start_server(
                self._handle_feeder,
                cfg.tcp_host,
                cfg.tcp_port,
                reuse_address=(os.name != "nt"),
            )
            self._teardown.append(self._close_tcp_server)
            log.info("feeder listener (TCP) on %s:%d", cfg.tcp_host, cfg.tcp_port)

            await self._start_http()

            if cfg.stats_interval_s > 0:
                self._stats_task = asyncio.create_task(
                    self._stats_loop(), name="mt5-ws-stream-stats"
                )
                self._teardown.append(self._close_stats_task)
        except BaseException:
            await self._unwind()
            raise

    async def _start_http(self) -> None:
        """Run uvicorn inside *this* event loop, on a socket we bind ourselves.

        Pre-binding is what makes ``http_port=0`` usable: uvicorn would otherwise
        own the socket and there would be nothing to read the real port back
        from. Driving ``startup``/``main_loop``/``shutdown`` by hand instead of
        ``Server.serve()`` also keeps uvicorn from installing signal handlers --
        the CLI has its own, and a library must not steal SIGINT from its host.
        """
        cfg = self.config
        sock = _bind_listener(cfg.ws_host, cfg.http_port)
        self._http_socket = sock
        self._http_port = int(sock.getsockname()[1])
        self._teardown.append(self._close_http_socket)

        config = uvicorn.Config(
            create_app(
                self.hub,
                allowed_origins=cfg.allowed_origins,
                # A callable, not `self._feeders`: the registry is rebuilt as
                # feeders connect and drop, and /feeders must see the live set.
                feeders=lambda: self.feeders,
                sessions=self._sessions,
            ),
            host=cfg.ws_host,
            port=self._http_port,
            log_config=None,  # keep the host application's logging setup
            access_log=False,  # one line per tick request would swamp the log
            ws=_WS_IMPL,
            ws_per_message_deflate=False,  # compression costs latency; ticks are tiny
            ws_ping_interval=cfg.ws_ping_interval_s,
            ws_ping_timeout=cfg.ws_ping_timeout_s,
            lifespan="off",
            timeout_graceful_shutdown=5,
            **cfg.extra_serve_kwargs,
        )
        config.load()
        server = uvicorn.Server(config)
        # Server.serve() would do these two lines and then capture signals.
        server.lifespan = config.lifespan_class(config)
        await server.startup(sockets=[sock])
        if server.should_exit:  # pragma: no cover - only on a lifespan failure
            raise RuntimeError("uvicorn refused to start")
        self._http_server = server
        self._http_task = asyncio.create_task(server.main_loop(), name="mt5-ws-stream-http")
        self._teardown.append(self._close_http_server)
        log.info(
            "consumer listener on http://%s:%d (ws://%s:%d/ws)",
            cfg.ws_host,
            self._http_port,
            cfg.ws_host,
            self._http_port,
        )

    async def aclose(self) -> None:
        """Stop listening, cancel tasks and drop every subscriber.

        Unwinds ``self._teardown`` (see the note where it is declared) and
        then closes the hub -- safe to call whether or not ``start`` ever
        succeeded: an unstarted or already-unwound bridge just has nothing
        left on the stack, and ``Hub.aclose`` is itself a no-op with no
        subscribers. Also safe to call twice: the second call finds an empty
        stack.
        """
        await self._unwind()
        await self.hub.aclose()
        self._closed.set()

    async def _unwind(self) -> None:
        """Pop ``self._teardown`` in reverse, releasing whatever was started.

        Used both by a failed ``start`` (only the first few stages made it
        onto the stack) and by ``aclose`` after a clean start (all of them
        did) -- one mechanism either way. Every step runs even if an earlier
        one raises: a crash re-raised from the stats task's ``await task``
        must not leave the HTTP server, HTTP socket or feeder listener open
        just because it happens to unwind first. Escapes are logged as they
        happen and collected; once every step has run, the first one is
        re-raised (picked over an ``ExceptionGroup`` to keep the single-cause
        case -- by far the common one -- a plain traceback).
        """
        first_exc: BaseException | None = None
        while self._teardown:
            step = self._teardown.pop()
            try:
                await step()
            except Exception as exc:
                log.exception("teardown step %s failed", getattr(step, "__name__", step))
                if first_exc is None:
                    first_exc = exc
        if first_exc is not None:
            raise first_exc

    async def _close_tcp_server(self) -> None:
        server = self._tcp_server
        if server is None:
            return
        server.close()
        with contextlib.suppress(Exception):
            await server.wait_closed()
        self._tcp_server = None

    async def _close_http_socket(self) -> None:
        sock = self._http_socket
        if sock is None:
            return
        # shutdown() already closed it when the HTTP server stage ran; closing
        # twice is a no-op. This also covers the pre-bind-only failure case:
        # uvicorn startup raised before ``_close_http_server`` was ever pushed,
        # so this is the only teardown the socket gets.
        sock.close()
        self._http_socket = None

    async def _close_http_server(self) -> None:
        server = self._http_server
        if server is None:
            return
        server.should_exit = True
        task = self._http_task
        if task is not None:
            with contextlib.suppress(Exception):
                await task
            self._http_task = None
        listeners = [self._http_socket] if self._http_socket is not None else []
        with contextlib.suppress(Exception):
            await server.shutdown(sockets=listeners)
        self._http_server = None

    async def _close_stats_task(self) -> None:
        task = self._stats_task
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        self._stats_task = None

    async def wait_closed(self) -> None:
        """Block until :meth:`aclose` completes.

        For a task that does not itself own the bridge's lifecycle -- it did not
        call :meth:`aclose` and is not the ``async with`` block that will -- but
        still needs to wait for *another* task, or an external event, to close
        it. Nothing sets the underlying event on its own: calling this from the
        only task that will ever call :meth:`aclose`, or from outside any
        ``async with Bridge(...)`` block, waits forever.
        """
        await self._closed.wait()

    async def __aenter__(self) -> Bridge:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    @property
    def tcp_port(self) -> int:
        """Actual bound TCP port -- resolves ``port=0`` for tests."""
        server = self._tcp_server
        if server is None or not server.sockets:
            return self.config.tcp_port
        return int(server.sockets[0].getsockname()[1])

    @property
    def http_port(self) -> int:
        """Actual bound consumer port -- resolves ``port=0`` for tests.

        HTTP and WebSocket share this one port (ADR-0003 removed the
        ``ws_port`` alias).
        """
        return self._http_port

    @property
    def feeders(self) -> list[FeederLink]:
        """Currently connected feeders, oldest first."""
        return [self._feeders[key] for key in sorted(self._feeders)]

    # -- feeder side -----------------------------------------------------

    async def _handle_feeder(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        _disable_nagle(writer.get_extra_info("socket"))
        link = FeederLink(name=str(peer), connected_at=time.time())
        self._next_feeder_id += 1
        feeder_key = self._next_feeder_id
        self._feeders[feeder_key] = link
        log.info("feeder connected: %s", peer)

        prev_chunk_at: float | None = None
        try:
            while True:
                chunk = await reader.read(_READ_CHUNK)
                if not chunk:
                    break

                # Guarded so the default (INFO) path pays only this one boolean
                # check -- no timing, no formatting, no string building. The
                # clock is read at arrival, so what the line reports is the gap
                # between chunks (docs/troubleshooting.md), not the ingest cost.
                debug = log.isEnabledFor(logging.DEBUG)
                arrived_at = time.monotonic() if debug else 0.0

                # Where a record starts is the link's business; a chunk is just
                # bytes. Whatever it does not complete waits for the next one.
                records = self.hub.feed(chunk, link)

                if debug:
                    log.debug(
                        "feeder %s: %d bytes -> %d records, +%.0f ms since previous chunk",
                        peer,
                        len(chunk),
                        records,
                        0.0 if prev_chunk_at is None else (arrived_at - prev_chunk_at) * 1000.0,
                    )
                    prev_chunk_at = arrived_at
        except ProtocolError as exc:
            log.error("feeder %s speaks a different protocol (%s); dropping it", peer, exc)
        except ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError:
            pass
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive
            log.exception("feeder %s failed", peer)
        finally:
            self._feeders.pop(feeder_key, None)
            log.info(
                "feeder disconnected: %s (ticks=%d heartbeats=%d gaps=%d)",
                peer,
                link.ticks,
                link.heartbeats,
                link.seq_gaps,
            )
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    # -- stats -----------------------------------------------------------

    async def report_once(self) -> None:
        """Consume one interval and report it to both audiences.

        One snapshot, two audiences: the operator's log line and every
        connected consumer, all reporting the same closed interval. Public and
        clock-free -- callable directly (as :meth:`_stats_loop` does, on a
        timer) or from a test that wants a report on demand instead of waiting
        out ``stats_interval_s``.

        The per-session sends are fanned out concurrently, each bounded by
        ``stats_send_timeout_s`` -- one session with a stalled sink must not
        serialize behind, or hold up, everyone else's frame.
        """
        stats = self.hub.consume_interval()
        log.info("%s", stats_line(stats))
        sessions = list(self._sessions)
        if not sessions:
            return
        await asyncio.gather(*(self._send_stats(session, stats) for session in sessions))

    async def _send_stats(self, session: Session, stats: HubStats) -> None:
        """Send one session's ``stats`` frame, isolated from every other session.

        A consumer whose socket died -- or whose sink just never drains, since
        uvicorn's sansio protocol sends no server-initiated pings -- is its own
        session's problem to unwind; it must not stop, or delay past
        ``stats_send_timeout_s``, anyone else's broadcast.
        """
        try:
            async with asyncio.timeout(self.config.stats_send_timeout_s):
                await session.send_stats(stats)
        except Exception:
            log.debug("stats broadcast skipped a session", exc_info=True)

    async def _stats_loop(self) -> None:
        """Call :meth:`report_once` on ``stats_interval_s``, forever."""
        while True:
            await asyncio.sleep(self.config.stats_interval_s)
            await self.report_once()


# -- helpers -------------------------------------------------------------


def _bind_listener(host: str, port: int) -> socket.socket:
    """Bind and listen, so uvicorn can be handed an already-open socket.

    Mirrors the feeder listener's address-reuse policy: on POSIX ``SO_REUSEADDR``
    only skips TIME_WAIT, but on Windows it would let a second bridge bind a port
    that is already being listened on -- consumers would then land on whichever
    socket the kernel picks. ``SO_EXCLUSIVEADDRUSE`` is the Windows way to say
    "fail loudly instead".
    """
    sock = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM)
    try:
        if sys.platform == "win32":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen(_LISTEN_BACKLOG)
        sock.setblocking(False)
    except BaseException:
        sock.close()
        raise
    return sock


def _disable_nagle(sock: socket.socket | None) -> None:
    """Turn off Nagle's algorithm.

    Without this, small records can sit in the kernel for up to 40 ms waiting for
    a companion packet -- which would dwarf every other latency source here.
    """
    if sock is None:
        return
    with contextlib.suppress(OSError):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
