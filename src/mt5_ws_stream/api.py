"""The HTTP + WebSocket surface: one FastAPI app, one port.

The bridge used to run a bare ``websockets`` server, which meant a WebSocket and
nothing else -- no way to ask "which symbols are live?", no dashboard to open, no
schema to read. This module puts the tick stream and a small read-only REST API
behind a single ASGI app so ``http://127.0.0.1:8765/docs`` describes everything
the process can do.

The stream is still the hot path and still goes through
:class:`~mt5_ws_stream.hub.Hub`; FastAPI only supplies the socket. Everything
here is a thin adapter:

* :class:`_WebSocketSink` turns Starlette's ``WebSocket`` into the hub's
  :class:`~mt5_ws_stream.hub.Sink` protocol (``str`` -> text, ``bytes`` -> binary).
* the WebSocket handler accepts the socket, checks the origin, builds a
  :class:`~mt5_ws_stream.session.Session` and runs its writer loop alongside the
  receive loop. The conversation itself -- ``hello``, control ops, encoding --
  belongs to the session, not to this module.
* the REST handlers read hub state and never mutate it. So does the WebSocket
  ``stats`` control frame: both go through ``Hub.snapshot_stats()``, which is a
  pure read, so no client can consume the interval another observer is measuring.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Collection, Mapping, MutableSet
from importlib import resources
from typing import Final

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from . import __version__
from .frames import stats_payload
from .hub import (
    FeederLink,
    Hub,
    HubStats,
    SubscriptionOptions,
    SymbolSnapshot,
)
from .protocol import split_symbols
from .session import Session
from .subscription import SubscriptionRequest

__all__ = ["create_app", "lag_text", "parse_subscription", "stats_line"]

log = logging.getLogger("mt5_ws_stream.api")

API_PREFIX: Final[str] = "/api/v1"

_DASHBOARD_RESOURCE: Final[str] = "web/dashboard.html"


# -- response models -----------------------------------------------------
#
# These exist for `/docs`: without them the schema is `{}` and the page is
# decoration rather than documentation.


class IndexResponse(BaseModel):
    """What lives where, for someone who just opened the root URL."""

    ws: str = Field(description="WebSocket tick stream path")
    dashboard: str
    docs: str
    api: str


class HealthResponse(BaseModel):
    status: str = Field(description='Always "ok" when the process answers at all')
    uptime_s: float
    version: str


class SymbolResponse(BaseModel):
    """Latest quote for one symbol, plus how long ago it arrived.

    This class is the *only* definition of the shape: the hub's
    :class:`~mt5_ws_stream.hub.SymbolSnapshot` is a domain value that knows
    nothing about HTTP, and :meth:`from_snapshot` is the single seam between
    them -- so a new field is added here and mypy points at the one call site
    that has to fill it.
    """

    symbol: str
    bid: float
    ask: float
    last: float
    volume: float
    spread: float = Field(description="ask - bid")
    time_msc: int = Field(description="Broker server clock, UTC milliseconds")
    flags: int
    seq: int
    received_at: float = Field(description="Unix seconds when the bridge decoded it")
    age_ms: float = Field(description="Milliseconds since received_at; staleness detector")
    ticks: int = Field(description="Quotes seen for this symbol since the bridge started")

    @classmethod
    def from_snapshot(cls, snapshot: SymbolSnapshot, *, now: float) -> SymbolResponse:
        """Render *snapshot* as of *now* (Unix seconds, i.e. ``time.time()``).

        ``now`` is a parameter rather than a call to the clock so that one
        request reports one consistent set of ages across every symbol.
        """
        tick = snapshot.tick
        return cls(
            symbol=tick.symbol,
            bid=tick.bid,
            ask=tick.ask,
            last=tick.last,
            volume=tick.volume,
            spread=tick.spread,
            time_msc=tick.time_msc,
            flags=tick.flags,
            seq=tick.seq,
            received_at=snapshot.received_at,
            # Clamped: a tick decoded microseconds ago can land "in the future"
            # once the clock is read again, and a negative age reads as a bug.
            age_ms=max(0.0, (now - snapshot.received_at) * 1000.0),
            ticks=snapshot.ticks,
        )


class StatsResponse(BaseModel):
    """The same payload the WebSocket ``stats`` frame carries.

    The fields are declared here because ``/docs`` needs a schema, but their
    *values* come from :func:`mt5_ws_stream.frames.stats_payload` -- the one
    field list -- so REST ``/stats`` and the streamed frame cannot drift apart.
    ``tests/test_api.py`` asserts the two are equal.
    """

    t: str = "stats"
    uptime_s: float
    ticks: int
    tick_rate: float
    subscribers: int
    symbols: list[str]
    seq_gaps: int
    heartbeats: int
    dropped: int
    broker_lag_ms_p50: float | None = None
    broker_lag_ms_p99: float | None = None

    @classmethod
    def from_stats(cls, stats: HubStats) -> StatsResponse:
        """Render a hub :class:`~mt5_ws_stream.hub.HubStats` for the wire.

        Field values -- and the rounding -- come from the frame grammar, not
        from here: this is the REST *schema* of a shape the wire already
        defines.
        """
        return cls(**stats_payload(stats))


def lag_text(value: float | None) -> str:
    """Render one optional latency figure for a human.

    ``None`` means *nothing was measured in this interval* -- see
    :func:`~mt5_ws_stream.protocol.percentile`, which keeps that distinct from a
    measured zero. Every human-facing renderer has to say so in words: printed
    raw, the Python ``None`` reads as a crashed metric rather than a quiet
    market.
    """
    return "n/a" if value is None else f"{value}"


def stats_line(stats: HubStats) -> str:
    """The operator-facing one-line rendering of *stats*, for the periodic log.

    The text sibling of :func:`mt5_ws_stream.frames.stats_frame`: same domain
    value, same one place to change, different audience. It groups the fields by
    what they measure, because :class:`~mt5_ws_stream.hub.HubStats` mixes three
    kinds of number and a flat line makes them look like one. ``ticks=1
    (0.0/s)`` -- the old format -- reads as a contradiction until you know the
    count is cumulative and the rate is not.

    The ``key=value`` spellings are kept verbatim from that older line so that
    ``grep 'gaps='`` on an existing runbook still finds it.
    """
    return (
        f"interval: tick_rate={stats.tick_rate:.1f}/s "
        f"broker_lag_p50={lag_text(stats.broker_lag_ms_p50)} ms "
        f"broker_lag_p99={lag_text(stats.broker_lag_ms_p99)} ms "
        f"| total: ticks={stats.ticks} heartbeats={stats.heartbeats} "
        f"gaps={stats.seq_gaps} dropped={stats.dropped} "
        f"| now: symbols={len(stats.symbols)} consumers={stats.subscribers}"
    )


class FeederResponse(BaseModel):
    """One currently-connected feeder."""

    peer: str
    connected_at: float
    ticks: int
    heartbeats: int
    seq_gaps: int
    last_record_at: float = Field(description="Unix seconds; 0 if nothing arrived yet")
    symbols: list[str]


# -- app -----------------------------------------------------------------


def create_app(
    hub: Hub,
    *,
    allowed_origins: Collection[str] | None = None,
    feeders: Callable[[], list[FeederLink]] | None = None,
    sessions: MutableSet[Session] | None = None,
    version: str = __version__,
) -> FastAPI:
    """Build the ASGI app that serves *hub*.

    A :class:`~mt5_ws_stream.hub.Hub` is everything the routes need, so an app
    can be built and exercised over ``httpx.ASGITransport`` without a socket,
    a uvicorn server or a :class:`~mt5_ws_stream.bridge.Bridge`.

    Args:
        hub: The fan-out the WebSocket subscribes to and the REST handlers read.
        allowed_origins: If set, browser WebSocket connections whose ``Origin``
            is not listed are closed with 1008. ``None`` allows every origin.
        feeders: Called on each ``/feeders`` request. It is a callable rather
            than a list because the caller's registry changes as feeders
            connect and drop, and the response has to reflect that. ``None``
            reports no feeders.
        sessions: Registry the app adds each live consumer session to and
            removes it from on close. The mirror image of *feeders*: that one
            lets the app read the caller's state, this one lets the caller
            reach the app's -- the bridge's periodic ``stats`` broadcast walks
            it. ``None`` keeps a private one.
        version: Advertised by ``/health`` and the OpenAPI schema.

    One app per hub: the routes close over their arguments rather than reaching
    for module state, so two bridges in one process (the test suite does exactly
    that) stay independent.
    """
    origins = frozenset(allowed_origins) if allowed_origins is not None else None
    feeder_states: Callable[[], list[FeederLink]] = list if feeders is None else feeders
    live_sessions: MutableSet[Session] = set() if sessions is None else sessions
    started_at = time.monotonic()

    app = FastAPI(
        title="mt5-ws-stream",
        version=version,
        summary="Low-latency MetaTrader 5 tick streaming over WebSocket.",
        description=(
            f"Live ticks stream over WebSocket at `/ws`. Everything under "
            f"`{API_PREFIX}` is read-only market-data introspection."
        ),
    )
    # Read-only market data on a loopback-by-default port: a permissive CORS
    # policy costs nothing here and saves every browser tool from a proxy. It is
    # GET-only -- there is no endpoint that changes state -- and it does not
    # apply to the WebSocket, which keeps its own `allowed_origins` guard.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    # -- the stream ------------------------------------------------------

    async def stream(websocket: WebSocket) -> None:
        """Run one consumer session on this socket until either end goes away.

        An adapter, and only that: accept, check the origin, build the session,
        then run its writer loop next to the receive loop in a task group. The
        group is what makes a dead consumer a *structural* non-event -- a sink
        failure raises out of ``session.run()``, which cancels the receive loop
        and unwinds the whole session, instead of being swallowed by a writer
        task nobody is watching.
        """
        origin = websocket.headers.get("origin")
        # Accept first, then close with 1008: rejecting before the handshake
        # completes surfaces as an HTTP 403 that WebSocket clients report as a
        # connection error rather than a policy close.
        await websocket.accept()
        if not _origin_allowed(origins, origin):
            await websocket.close(code=1008, reason="origin not allowed")
            return

        options = parse_subscription(websocket.query_params)
        session = Session(hub, _WebSocketSink(websocket), options)
        live_sessions.add(session)
        log.info(
            "consumer #%d connected (format=%s backpressure=%s symbols=%s)",
            session.id,
            options.payload_format.value,
            options.backpressure.value,
            ",".join(sorted(options.symbols)) if options.symbols else "*",
        )

        try:
            # Before the writer loop and the receive loop exist, so no other
            # frame can overtake it: hello is first by construction.
            await session.send_hello()
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(session.run(), name=f"mt5-ws-stream-session-{session.id}")
                try:
                    await _receive_control(websocket, session)
                finally:
                    # Closing the queue is what ends the writer loop; without
                    # it the group would wait for a session nobody is reading.
                    session.close()
        except* WebSocketDisconnect:
            pass
        except* Exception:
            log.debug("consumer #%d session ended", session.id, exc_info=True)
        finally:
            session.close()
            live_sessions.discard(session)
            log.info(
                "consumer #%d disconnected (frames=%d ticks=%d dropped=%d)",
                session.id,
                session.sent_frames,
                session.sent_ticks,
                session.dropped,
            )

    app.add_api_websocket_route("/ws", stream, name="stream")

    # -- REST ------------------------------------------------------------

    @app.get("/", summary="What this process serves")
    async def index() -> IndexResponse:
        return IndexResponse(ws="/ws", dashboard="/dashboard", docs="/docs", api=API_PREFIX)

    @app.get(f"{API_PREFIX}/health", summary="Liveness probe")
    async def health() -> HealthResponse:
        return HealthResponse(
            status="ok",
            uptime_s=round(time.monotonic() - started_at, 3),
            version=version,
        )

    @app.get(f"{API_PREFIX}/symbols", summary="Latest quote for every symbol seen")
    async def symbols(
        symbols: str | None = Query(
            default=None,
            description="Comma-separated filter; omit for every symbol.",
            examples=["EURUSD,USDJPY"],
        ),
    ) -> list[SymbolResponse]:
        wanted = (split_symbols(symbols) or None) if symbols else None
        now = time.time()
        return [
            SymbolResponse.from_snapshot(snapshot, now=now)
            for snapshot in hub.snapshot_symbols(wanted)
        ]

    @app.get(f"{API_PREFIX}/symbols/{{symbol}}", summary="Latest quote for one symbol")
    async def symbol(symbol: str) -> SymbolResponse:
        snapshot = hub.snapshot_symbol(symbol)
        if snapshot is None:
            raise HTTPException(status_code=404, detail=f"unknown symbol: {symbol}")
        return SymbolResponse.from_snapshot(snapshot, now=time.time())

    @app.get(f"{API_PREFIX}/stats", summary="Throughput and latency counters")
    async def stats() -> StatsResponse:
        return StatsResponse.from_stats(hub.snapshot_stats())

    # `name=` is explicit because the handler cannot also be called `feeders`
    # (that is the argument it reads); the route name drives the OpenAPI
    # operationId, so pinning it keeps the published schema unchanged.
    @app.get(f"{API_PREFIX}/feeders", summary="Currently connected feeders", name="feeders")
    async def list_feeders() -> list[FeederResponse]:
        return [
            FeederResponse(
                peer=state.name,
                connected_at=state.connected_at,
                ticks=state.ticks,
                heartbeats=state.heartbeats,
                seq_gaps=state.seq_gaps,
                last_record_at=state.last_record_at,
                symbols=sorted(state.symbols),
            )
            for state in feeder_states()
        ]

    @app.get(
        "/dashboard",
        summary="The bundled single-file dashboard",
        response_class=HTMLResponse,
        responses={200: {"content": {"text/html": {}}}},
    )
    async def dashboard() -> HTMLResponse:
        return HTMLResponse(_dashboard_html())

    return app


# -- helpers -------------------------------------------------------------


async def _receive_control(websocket: WebSocket, session: Session) -> None:
    """Feed inbound text frames to *session* until the peer disconnects."""
    while True:
        message = await websocket.receive()
        if message["type"] == "websocket.disconnect":
            return
        text = message.get("text")
        # Binary uploads are not part of the control protocol; ignore them
        # rather than guessing at an encoding.
        if text is not None:
            await session.handle(text)


class _WebSocketSink:
    """Adapts Starlette's ``WebSocket`` to the hub's :class:`Sink` protocol.

    Sends are serialised: the writer loop, control-frame acks and the periodic
    stats broadcast all target the same socket, and interleaved ASGI sends would
    corrupt the frame stream. An uncontended :class:`asyncio.Lock` does not yield,
    so this costs nothing on the common path.

    A failed send is *not* caught here. It used to be -- the sink latched itself
    closed and went on silently accepting frames, which meant a dead consumer
    stayed subscribed for as long as its socket object lived. Letting the error
    out is what lets the session's task group notice and unwind.
    """

    __slots__ = ("_lock", "_websocket")

    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket
        self._lock = asyncio.Lock()

    async def send(self, payload: str | bytes) -> None:
        async with self._lock:
            if isinstance(payload, str):
                await self._websocket.send_text(payload)
            else:
                await self._websocket.send_bytes(payload)


def parse_subscription(source: str | Mapping[str, str]) -> SubscriptionOptions:
    """Build :class:`SubscriptionOptions` from a request path or a query mapping.

    The read half of :class:`~mt5_ws_stream.subscription.SubscriptionRequest`
    does the actual parsing -- lenient spellings, unknown-parameter tolerance
    and all are documented on
    :meth:`~mt5_ws_stream.subscription.SubscriptionRequest.from_query`. This
    function only adapts the result to :class:`SubscriptionOptions`, the shape
    :class:`~mt5_ws_stream.hub.Hub` and :class:`~mt5_ws_stream.session.Session`
    actually want.
    """
    request = SubscriptionRequest.from_query(source)
    return SubscriptionOptions(
        symbols=request.symbols,
        payload_format=request.payload_format,
        backpressure=request.backpressure,
        include_heartbeats=request.include_heartbeats,
    )


def _origin_allowed(allowed: frozenset[str] | None, origin: str | None) -> bool:
    if allowed is None:
        return True
    # No Origin header means a non-browser client; the guard targets browsers.
    return origin is None or origin in allowed


def _dashboard_html() -> str:
    """Read the packaged dashboard. Works from a wheel, a zip or the source tree."""
    try:
        resource = resources.files("mt5_ws_stream")
        for part in _DASHBOARD_RESOURCE.split("/"):
            resource = resource / part
        return resource.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError) as exc:  # pragma: no cover - packaging bug
        raise HTTPException(status_code=404, detail="dashboard.html is not packaged") from exc
