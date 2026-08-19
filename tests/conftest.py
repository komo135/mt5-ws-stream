"""Shared fixtures and test builders.

Every test binds to port 0 and reads back the real port, so the suite can run in
parallel and on CI machines where 8765 is already taken.

The builders below (:func:`tick`, :func:`heartbeat`, :func:`blob`,
:class:`RecordingSink`) and :func:`wait_until` are
imported by the test modules directly --
``from conftest import tick`` -- rather than being fixtures, because they take
arguments and are called many times inside a single test. One definition here is
what keeps the record they build identical everywhere.
"""

from __future__ import annotations

import asyncio
import json
import socket
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from mt5_ws_stream import Bridge, BridgeConfig
from mt5_ws_stream.protocol import FLAG_HEARTBEAT, Tick, pack_tick

#: Base broker timestamp for built records. Records are spaced by ``seq`` so a
#: batch has strictly increasing times without any test having to spell one out.
_BASE_TIME_MSC = 1_700_000_000_000


def tick(
    symbol: str = "EURUSD",
    *,
    seq: int = 0,
    bid: float = 1.1,
    spread: float = 0.0001,
    last: float = 0.0,
    volume: float = 0.0,
    flags: int = 6,
) -> Tick:
    """A quote record.

    Defaults describe a plain two-way EURUSD quote. Callers that assert on a
    field pass it explicitly (``spread=``/``volume=`` in the REST tests, ``bid=``
    where the *value* is what is being followed through the system) so the
    expectation sits next to the assertion rather than in this default list.
    """
    return Tick(
        symbol=symbol,
        time_msc=_BASE_TIME_MSC + seq,
        bid=bid,
        ask=bid + spread,
        last=last,
        volume=volume,
        flags=flags,
        seq=seq,
    )


def heartbeat(*, seq: int = 0) -> Tick:
    """The keep-alive record a feeder emits when no quote has arrived.

    Empty symbol, zeroed prices, ``FLAG_HEARTBEAT`` set -- exactly what
    ``FeederConnection.make_heartbeat`` puts on the wire.
    """
    return Tick(
        symbol="",
        time_msc=_BASE_TIME_MSC,
        bid=0.0,
        ask=0.0,
        last=0.0,
        volume=0.0,
        flags=FLAG_HEARTBEAT,
        seq=seq,
    )


def blob(*ticks: Tick) -> bytes:
    """The bytes a feeder would put on the wire for *ticks*."""
    return b"".join(pack_tick(t) for t in ticks)


class RecordingSink:
    """The :class:`~mt5_ws_stream.hub.Sink` adapter for tests: keeps the frames.

    A stalled consumer needs no machinery here -- a session that is never
    flushed is a session whose frames are still queued -- so this stays a list
    with three readers on it.
    """

    def __init__(self) -> None:
        self.frames: list[str | bytes] = []

    async def send(self, payload: str | bytes) -> None:
        self.frames.append(payload)

    def json(self) -> list[dict[str, Any]]:
        """Every text frame, decoded."""
        out = []
        for frame in self.frames:
            assert isinstance(frame, str), "expected a JSON frame, got binary"
            out.append(json.loads(frame))
        return out

    def last(self) -> dict[str, Any]:
        """The most recent text frame, decoded."""
        return self.json()[-1]

    def ticks(self) -> list[dict[str, Any]]:
        """Every tick carried by every ``ticks`` frame, in order."""
        out: list[dict[str, Any]] = []
        for payload in self.json():
            if payload.get("t") == "ticks":
                out.extend(payload["d"])
        return out


async def wait_until(predicate: Callable[[], bool], *, timeout: float = 5.0) -> None:
    """Poll *predicate* until it is truthy, or fail the test after *timeout*.

    For state that another task owns -- an ingest task, a writer task, uvicorn's
    connection handler -- where the test can name the condition it is waiting
    for. Prefer this to sleeping a guessed interval.
    """

    async def poll() -> None:
        while not predicate():  # noqa: ASYNC110 - polling state owned by another task
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


# -- event loop hygiene --------------------------------------------------
#
# pytest-asyncio (1.3) calls ``asyncio.get_event_loop()`` while setting up each
# test's runner. On Python 3.12+ that *creates* a loop when none is current, and
# nothing ever closes that one: it survives as the policy's current loop and is
# only finalised when something drops that reference -- an in-process
# ``asyncio.run`` elsewhere in the session, or interpreter shutdown. Finalising
# an unclosed ProactorEventLoop emits ResourceWarnings (the loop plus its
# self-pipe socket pair), and ``filterwarnings = error`` turns those into a
# session-level failure attributed to whatever test happened to be running.
#
# Pinning one loop of our own, and closing it at the end, means that
# ``get_event_loop()`` never has to invent one. ``pytest_runtest_setup`` runs
# before any fixture, so the pin is back in place even if something clears it.

_pinned_loop: asyncio.AbstractEventLoop | None = None


def pytest_sessionstart(session: pytest.Session) -> None:
    global _pinned_loop
    _pinned_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_pinned_loop)


def pytest_runtest_setup(item: pytest.Item) -> None:
    if _pinned_loop is not None and not _pinned_loop.is_closed():
        asyncio.set_event_loop(_pinned_loop)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    global _pinned_loop
    if _pinned_loop is not None:
        asyncio.set_event_loop(None)
        _pinned_loop.close()
        _pinned_loop = None


# -- fixtures ------------------------------------------------------------


@pytest.fixture
async def bridge() -> AsyncIterator[Bridge]:
    """A running bridge on ephemeral ports, with stats reporting disabled."""
    instance = Bridge(
        BridgeConfig(
            tcp_host="127.0.0.1",
            tcp_port=0,
            ws_host="127.0.0.1",
            http_port=0,
            stats_interval_s=0.0,
        )
    )
    await instance.start()
    try:
        yield instance
    finally:
        await instance.aclose()


@pytest.fixture
def ws_url(bridge: Bridge) -> str:
    """The stream endpoint. Note: no trailing slash -- ".../ws/" is a redirect,
    which a WebSocket client cannot follow, so tests append "?x=y" directly."""
    return f"ws://127.0.0.1:{bridge.http_port}/ws"


@pytest.fixture
def http_url(bridge: Bridge) -> str:
    """Base URL of the same port, for the REST API and the dashboard."""
    return f"http://127.0.0.1:{bridge.http_port}"


@pytest.fixture
async def feeder_socket(bridge: Bridge) -> AsyncIterator[socket.socket]:
    """A blocking TCP socket already connected to the bridge's feeder port."""
    sock = socket.create_connection(("127.0.0.1", bridge.tcp_port), timeout=5.0)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    # The bridge accepts on the event loop. Wait for the accept the bridge can
    # be asked about rather than for a guessed interval: `connect()` returning
    # only means the kernel queued the connection.
    await wait_until(lambda: len(bridge.feeders) == 1)
    try:
        yield sock
    finally:
        sock.close()
