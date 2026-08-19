"""REST API tests.

Most of this file talks HTTP to the same port the WebSocket lives on: that
co-tenancy is the point of the FastAPI rewrite, and a mounted ASGI transport
would not prove it. Records go in through the real feeder socket so the numbers
the API reports come from the real ingest path.

The block at the end is the other half of the story. ``create_app`` takes a
:class:`~mt5_ws_stream.hub.Hub`, not a :class:`~mt5_ws_stream.bridge.Bridge`, so
routes that only read hub state can be driven over ``httpx.ASGITransport`` with
no socket, no uvicorn and no bridge at all.
"""

from __future__ import annotations

import asyncio
import socket
import time
from collections.abc import AsyncIterator
from typing import NamedTuple

import httpx
import pytest
import websockets
from fastapi.routing import APIWebSocketRoute

from conftest import heartbeat, tick, wait_until
from mt5_ws_stream import Bridge, create_app
from mt5_ws_stream.api import StatsResponse, SymbolResponse, stats_line

# `bridge_is_up` is the `dashboard` subcommand's blocking probe, public because
# this test (and the CLI's own dashboard command) both need to drive it without
# going through `main(["dashboard"])`, which would open a browser on the CI
# machine.
from mt5_ws_stream.cli import bridge_is_up
from mt5_ws_stream.client import STREAM_PATH, TickStreamClient
from mt5_ws_stream.frames import stats_payload
from mt5_ws_stream.hub import FeederLink, Hub, HubStats, SymbolSnapshot
from mt5_ws_stream.protocol import Tick, pack_tick

# asyncio_mode = "auto" in pyproject.toml means async tests need no marker.


@pytest.fixture
async def api(http_url: str) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=http_url, timeout=5.0) as client:
        yield client


class AsgiApi(NamedTuple):
    """An app built straight from a hub, plus the state its routes read.

    ``feed`` is synchronous all the way down -- ``Hub.feed`` decodes and
    publishes inline -- so tests using this need no ``wait_until`` polling.
    """

    client: httpx.AsyncClient
    hub: Hub
    feeder: FeederLink

    def feed(self, *ticks: Tick) -> int:
        return self.hub.feed(b"".join(pack_tick(t) for t in ticks), self.feeder)


@pytest.fixture
async def asgi_api() -> AsyncIterator[AsgiApi]:
    """The REST surface with no server behind it: `create_app(hub, ...)` only."""
    hub = Hub()
    feeder = FeederLink(name="127.0.0.1:59999", connected_at=time.time())
    app = create_app(hub, feeders=lambda: [feeder], version="9.9.9-test")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://asgi.test"
    ) as client:
        yield AsgiApi(client, hub, feeder)


def feed(sock: socket.socket, *ticks: Tick) -> None:
    """Push records into the bridge. Pair with ``wait_until`` to wait for them."""
    sock.sendall(b"".join(pack_tick(t) for t in ticks))


# -- health & index ------------------------------------------------------


async def test_health(api: httpx.AsyncClient) -> None:
    response = await api.get("/api/v1/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["uptime_s"] >= 0
    assert body["version"]


async def test_index_points_at_everything_else(api: httpx.AsyncClient) -> None:
    body = (await api.get("/")).json()
    assert body == {
        "ws": "/ws",
        "dashboard": "/dashboard",
        "docs": "/docs",
        "api": "/api/v1",
    }


async def test_openapi_docs_are_served(api: httpx.AsyncClient) -> None:
    assert (await api.get("/docs")).status_code == 200
    schema = (await api.get("/openapi.json")).json()
    assert "/api/v1/symbols" in schema["paths"]


async def test_dashboard_is_served_as_html(api: httpx.AsyncClient) -> None:
    response = await api.get("/dashboard")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "WebSocket" in response.text


# -- symbols -------------------------------------------------------------


async def test_symbols_is_empty_before_any_tick(api: httpx.AsyncClient) -> None:
    assert (await api.get("/api/v1/symbols")).json() == []


async def test_symbols_reports_the_latest_quote(
    api: httpx.AsyncClient, bridge: Bridge, feeder_socket: socket.socket
) -> None:
    feed(
        feeder_socket,
        tick("EURUSD", seq=0, bid=1.10, spread=0.0002),
        tick("USDJPY", seq=1, bid=157.0, spread=0.0002),
        tick("EURUSD", seq=2, bid=1.20, spread=0.0002),
    )
    await wait_until(lambda: bridge.hub.symbols == ["EURUSD", "USDJPY"])

    body = (await api.get("/api/v1/symbols")).json()
    assert [row["symbol"] for row in body] == ["EURUSD", "USDJPY"]

    eurusd = body[0]
    assert eurusd["bid"] == pytest.approx(1.20), "must report the newest quote"
    assert eurusd["ask"] == pytest.approx(1.2002)
    assert eurusd["spread"] == pytest.approx(0.0002)
    assert eurusd["seq"] == 2
    assert eurusd["ticks"] == 2, "two EURUSD quotes since start"
    assert body[1]["ticks"] == 1
    assert eurusd["received_at"] > 0
    assert 0 <= eurusd["age_ms"] < 60_000


async def test_symbols_age_grows_between_reads(
    api: httpx.AsyncClient, bridge: Bridge, feeder_socket: socket.socket
) -> None:
    feed(feeder_socket, tick("EURUSD", seq=0))
    await wait_until(lambda: bridge.hub.symbols == ["EURUSD"])

    first = (await api.get("/api/v1/symbols")).json()[0]["age_ms"]
    await asyncio.sleep(0.05)
    second = (await api.get("/api/v1/symbols")).json()[0]["age_ms"]
    assert second > first


async def test_symbols_filter(
    api: httpx.AsyncClient, bridge: Bridge, feeder_socket: socket.socket
) -> None:
    feed(
        feeder_socket,
        tick("EURUSD", seq=0),
        tick("USDJPY", seq=1),
        tick("GBPUSD", seq=2),
    )
    await wait_until(lambda: len(bridge.hub.symbols) == 3)

    body = (await api.get("/api/v1/symbols", params={"symbols": "USDJPY,GBPUSD"})).json()
    assert [row["symbol"] for row in body] == ["GBPUSD", "USDJPY"]


async def test_heartbeats_do_not_become_symbols(
    api: httpx.AsyncClient, feeder_socket: socket.socket
) -> None:
    feed(feeder_socket, heartbeat())
    assert (await api.get("/api/v1/symbols")).json() == []


async def test_one_symbol(
    api: httpx.AsyncClient, bridge: Bridge, feeder_socket: socket.socket
) -> None:
    feed(feeder_socket, tick("XAUUSD", seq=0, bid=2400.0))
    await wait_until(lambda: bridge.hub.symbols == ["XAUUSD"])

    body = (await api.get("/api/v1/symbols/XAUUSD")).json()
    assert body["symbol"] == "XAUUSD"
    assert body["bid"] == pytest.approx(2400.0)


async def test_unknown_symbol_is_404(api: httpx.AsyncClient) -> None:
    response = await api.get("/api/v1/symbols/NOPE")
    assert response.status_code == 404
    assert "NOPE" in response.json()["detail"]


# -- stats ---------------------------------------------------------------


async def test_stats_reports_the_hub_counters(
    api: httpx.AsyncClient, bridge: Bridge, feeder_socket: socket.socket
) -> None:
    feed(feeder_socket, tick(seq=0), tick(seq=1))
    await wait_until(lambda: bridge.hub.snapshot_stats().ticks == 2)

    body = (await api.get("/api/v1/stats")).json()
    assert body["t"] == "stats"
    assert body["ticks"] == 2
    assert body["symbols"] == ["EURUSD"]
    assert body["seq_gaps"] == 0


async def test_reading_stats_does_not_consume_the_interval(
    api: httpx.AsyncClient, bridge: Bridge, feeder_socket: socket.socket
) -> None:
    """The periodic log reports percentiles for the interval since it last ran.
    A REST poll must not steal those samples, or the log silently degrades to
    whatever happened since the last curl."""
    feed(feeder_socket, tick(seq=0), tick(seq=1))
    await wait_until(lambda: bridge.hub.snapshot_stats().ticks == 2)

    for _ in range(3):
        assert (await api.get("/api/v1/stats")).json()["broker_lag_ms_p50"] is not None

    # The periodic report still sees both samples...
    assert bridge.hub.consume_interval().broker_lag_ms_p50 is not None
    # ...and it, unlike every read path, is the one that consumes them.
    assert bridge.hub.consume_interval().broker_lag_ms_p50 is None


# -- feeders -------------------------------------------------------------


async def test_feeders_lists_the_connected_feeder(
    api: httpx.AsyncClient, bridge: Bridge, feeder_socket: socket.socket
) -> None:
    feed(feeder_socket, tick("EURUSD", seq=0), tick("USDJPY", seq=1))
    await wait_until(lambda: len(bridge.hub.symbols) == 2)

    body = (await api.get("/api/v1/feeders")).json()
    assert len(body) == 1
    feeder = body[0]
    assert "127.0.0.1" in feeder["peer"]
    assert feeder["connected_at"] > 0
    assert feeder["ticks"] == 2
    assert feeder["heartbeats"] == 0
    assert feeder["seq_gaps"] == 0
    assert feeder["last_record_at"] > 0
    assert feeder["symbols"] == ["EURUSD", "USDJPY"]


async def test_feeders_counts_heartbeats_and_gaps(
    api: httpx.AsyncClient, bridge: Bridge, feeder_socket: socket.socket
) -> None:
    feed(
        feeder_socket,
        tick(seq=0),
        heartbeat(seq=1),
        tick(seq=99),  # a jump: seq 2 was expected
    )
    await wait_until(lambda: bridge.hub.snapshot_stats().seq_gaps == 1)

    feeder = (await api.get("/api/v1/feeders")).json()[0]
    assert feeder["heartbeats"] == 1
    assert feeder["seq_gaps"] == 1


async def test_a_disconnected_feeder_disappears(api: httpx.AsyncClient, bridge: Bridge) -> None:
    sock = socket.create_connection(("127.0.0.1", bridge.tcp_port), timeout=5.0)
    try:
        await wait_until(lambda: len(bridge.feeders) == 1)
        assert len((await api.get("/api/v1/feeders")).json()) == 1
    finally:
        sock.close()

    await wait_until(lambda: not bridge.feeders)
    assert (await api.get("/api/v1/feeders")).json() == []


# -- cross-cutting -------------------------------------------------------


async def test_cors_is_open_for_reads(api: httpx.AsyncClient) -> None:
    response = await api.get("/api/v1/health", headers={"Origin": "https://example.com"})
    assert response.headers["access-control-allow-origin"] == "*"


async def test_the_client_default_path_is_the_route_the_server_mounts(
    asgi_api: AsgiApi,
) -> None:
    """``/ws`` is spelled twice on purpose -- the client must not import
    :mod:`mt5_ws_stream.api` (FastAPI) to know where a bridge serves the
    stream, and the server must not import the client (``websockets``) to
    mount it. What that duplication needs is this check: the one thing two
    literals cannot do for themselves is notice they have drifted.

    Three spellings, in fact: the mounted route, the path ``/`` advertises,
    and the one a client with no path in its URL falls back to.
    """
    mounted = [
        route.path for route in create_app(Hub()).routes if isinstance(route, APIWebSocketRoute)
    ]
    assert mounted == [STREAM_PATH]

    advertised = (await asgi_api.client.get("/")).json()["ws"]
    assert advertised == STREAM_PATH

    assert TickStreamClient("ws://host:1").url.startswith(f"ws://host:1{STREAM_PATH}?")


async def test_rest_and_websocket_share_one_port(ws_url: str, api: httpx.AsyncClient) -> None:
    """One bound socket answers both protocols -- proven by using each, not by
    comparing accessors (:attr:`Bridge.ws_port` no longer exists; ADR-0003
    removed the alias, so :attr:`Bridge.http_port` is the only one left to
    read)."""
    async with websockets.connect(ws_url) as ws:
        await ws.recv()  # hello
        response = await api.get("/api/v1/health")
        assert response.status_code == 200


async def test_dashboard_command_detects_a_running_bridge(http_url: str) -> None:
    """`mt5-ws-stream dashboard` prefers the served copy over the file:// one.

    ``bridge_is_up`` blocks on urllib, and the bridge answers on *this* loop, so
    it has to run off-thread or the probe would deadlock against its own server.
    """
    loop = asyncio.get_running_loop()
    assert await loop.run_in_executor(None, bridge_is_up, http_url)
    # A port nothing is listening on must fail fast, not hang.
    with socket.socket() as spare:
        spare.bind(("127.0.0.1", 0))
        dead = f"http://127.0.0.1:{spare.getsockname()[1]}"
    assert not await loop.run_in_executor(None, bridge_is_up, dead)


# -- the app on its own, no socket ---------------------------------------


async def test_asgi_health_reports_the_version_it_was_given(asgi_api: AsgiApi) -> None:
    """`version` is an argument now, not something api.py imports for itself."""
    body = (await asgi_api.client.get("/api/v1/health")).json()
    assert body == {
        "status": "ok",
        "uptime_s": pytest.approx(0, abs=5),
        "version": "9.9.9-test",
    }


async def test_asgi_symbols_reads_hub_state(asgi_api: AsgiApi) -> None:
    assert (await asgi_api.client.get("/api/v1/symbols")).json() == []

    fed = asgi_api.feed(
        tick("EURUSD", seq=0, bid=1.10, spread=0.0002),
        tick("USDJPY", seq=1, bid=157.0, spread=0.0002),
    )
    assert fed == 2

    body = (await asgi_api.client.get("/api/v1/symbols")).json()
    assert [row["symbol"] for row in body] == ["EURUSD", "USDJPY"]
    assert body[0]["bid"] == pytest.approx(1.10)
    assert body[0]["spread"] == pytest.approx(0.0002)

    one = (await asgi_api.client.get("/api/v1/symbols/USDJPY")).json()
    assert one["bid"] == pytest.approx(157.0)
    assert (await asgi_api.client.get("/api/v1/symbols/NOPE")).status_code == 404


async def test_asgi_stats_counts_the_fed_records(asgi_api: AsgiApi) -> None:
    asgi_api.feed(tick(seq=0), tick(seq=1), tick(seq=99))

    body = (await asgi_api.client.get("/api/v1/stats")).json()
    assert body["t"] == "stats"
    assert body["ticks"] == 3
    assert body["symbols"] == ["EURUSD"]
    assert body["seq_gaps"] == 1
    assert body["subscribers"] == 0


async def test_asgi_feeders_reflects_the_callable_it_was_given(asgi_api: AsgiApi) -> None:
    """`feeders` is a callable so the route sees the caller's live registry."""
    before = (await asgi_api.client.get("/api/v1/feeders")).json()
    assert [row["ticks"] for row in before] == [0]

    asgi_api.feed(tick("EURUSD", seq=0), heartbeat(seq=1))

    feeder = (await asgi_api.client.get("/api/v1/feeders")).json()[0]
    assert feeder["peer"] == "127.0.0.1:59999"
    assert feeder["ticks"] == 1
    assert feeder["heartbeats"] == 1
    assert feeder["symbols"] == ["EURUSD"]


async def test_asgi_feeders_defaults_to_empty_without_a_registry() -> None:
    """A hub is enough to build the app; `feeders` is genuinely optional."""
    app = create_app(Hub())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://asgi.test"
    ) as client:
        assert (await client.get("/api/v1/feeders")).json() == []


# -- response models are the shape ---------------------------------------
#
# `SymbolSnapshot` / `HubStats` no longer carry `as_dict()`; the pydantic
# models below are the single definition of what goes on the wire, so these
# tests pin the translation rather than a second copy of the field list.


def test_symbol_response_reads_every_field_off_the_snapshot() -> None:
    snapshot = SymbolSnapshot(
        tick=tick(seq=7, bid=1.25, spread=0.0002, volume=3.0),
        received_at=1_000.0,
        ticks=42,
    )

    row = SymbolResponse.from_snapshot(snapshot, now=1_000.5)

    assert row.model_dump() == {
        "symbol": "EURUSD",
        "bid": pytest.approx(1.25),
        "ask": pytest.approx(1.2502),
        "last": 0.0,
        "volume": 3.0,
        "spread": pytest.approx(0.0002),
        "time_msc": 1_700_000_000_007,
        "flags": 6,
        "seq": 7,
        "received_at": 1_000.0,
        "age_ms": pytest.approx(500.0),
        "ticks": 42,
    }


def test_symbol_response_never_reports_a_negative_age() -> None:
    """A tick decoded microseconds ago can land ahead of the next clock read."""
    snapshot = SymbolSnapshot(tick=tick(), received_at=1_000.0, ticks=1)

    assert SymbolResponse.from_snapshot(snapshot, now=999.5).age_ms == 0.0


def test_the_rest_stats_model_is_the_frame_grammar_field_for_field() -> None:
    """``StatsResponse`` exists for ``/docs``; the fields are the frame's.

    The frame's exact text is pinned in ``test_frames.py``. What has to hold
    here is that the REST schema did not grow, lose or rename one of them --
    which is what would let ``GET /api/v1/stats`` and the streamed frame drift.
    """
    stats = HubStats(
        uptime_s=12.3456,
        ticks=9,
        tick_rate=41.789,
        subscribers=2,
        symbols=["EURUSD"],
        seq_gaps=1,
        heartbeats=3,
        dropped=4,
        broker_lag_ms_p50=1.5,
        broker_lag_ms_p99=9.5,
    )

    assert StatsResponse.from_stats(stats).model_dump() == stats_payload(stats)


async def test_stats_line_says_n_a_for_an_interval_with_no_quotes() -> None:
    """A quiet interval has no latency samples, so the percentiles are ``None``
    -- that is the documented contract of :class:`HubStats`, not a bug. What is
    a bug is printing that ``None`` at an operator: the periodic bridge log used
    to read ``broker_lag_p50=None ms``, which reads as a crash, not as "no
    quotes arrived in the last ten seconds"."""
    hub = Hub()
    hub.feed(pack_tick(tick()), FeederLink())
    hub.consume_interval()  # closes the window the quote landed in

    line = stats_line(hub.consume_interval())

    assert "None" not in line
    assert "broker_lag_p50=n/a" in line
    assert "broker_lag_p99=n/a" in line
    await hub.aclose()


def test_stats_line_separates_the_interval_from_the_totals() -> None:
    """``ticks`` is cumulative while ``tick_rate`` and the percentiles cover the
    interval just closed. Read on one undifferentiated line, ``ticks=1 (0.0/s)``
    looks self-contradictory, so the rendering has to label the two halves."""
    stats = HubStats(
        uptime_s=12.3,
        ticks=1,
        tick_rate=0.0,
        subscribers=0,
        symbols=["ETHUSD#"],
        seq_gaps=0,
        heartbeats=18,
        dropped=0,
        broker_lag_ms_p50=None,
        broker_lag_ms_p99=None,
    )

    line = stats_line(stats)

    interval, _, total = line.partition("|")
    assert "0.0/s" in interval
    assert "broker_lag_p50=n/a" in interval
    # The grep keys operators already use have to survive the relabelling.
    for key in ("ticks=1", "heartbeats=18", "symbols=1", "consumers=0", "gaps=0", "dropped=0"):
        assert key in total, key


async def test_rest_stats_and_the_ws_frame_agree(asgi_api: AsgiApi) -> None:
    """One definition means the two routes cannot drift apart."""
    asgi_api.feed(tick(seq=0), tick(seq=1))

    body = (await asgi_api.client.get("/api/v1/stats")).json()
    frame = stats_payload(asgi_api.hub.snapshot_stats())

    # `uptime_s` and `tick_rate` move between the two reads; everything else
    # is a counter and must match exactly.
    clock_dependent = {"uptime_s", "tick_rate"}
    assert body.keys() == frame.keys()
    assert {k: v for k, v in body.items() if k not in clock_dependent} == {
        k: v for k, v in frame.items() if k not in clock_dependent
    }
