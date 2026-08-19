"""End-to-end tests over real sockets: feeder TCP in, WebSocket out.

Where :mod:`test_hub` checks framing and delivery policy and :mod:`test_session`
checks one consumer's conversation, these check the wiring -- chunks reaching
the hub, connection lifecycle, two servers on one port -- the things that only
break once a real socket is involved. The control protocol is exercised
in-process in :mod:`test_session`; what a socket adds to it is one smoke test.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
from typing import Any

import pytest
import websockets

from conftest import RecordingSink, heartbeat, tick, wait_until
from mt5_ws_stream import Bridge, TickStreamClient
from mt5_ws_stream.bridge import BridgeConfig
from mt5_ws_stream.protocol import (
    RECORD_SIZE,
    PayloadFormat,
    Tick,
    pack_tick,
    unpack_tick,
)
from mt5_ws_stream.session import Session

# asyncio_mode = "auto" in pyproject.toml means async tests need no marker.


async def collect_ticks(
    connection: websockets.ClientConnection, count: int, timeout: float = 5.0
) -> list[dict[str, Any]]:
    """Read frames until *count* ticks have arrived."""

    async def read() -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        while len(out) < count:
            message = await connection.recv()
            if isinstance(message, bytes):
                continue
            payload = json.loads(message)
            if payload.get("t") == "ticks":
                out.extend(payload["d"])
        return out

    return await asyncio.wait_for(read(), timeout)


async def read_until(
    connection: websockets.ClientConnection, kind: str, timeout: float = 5.0
) -> dict[str, Any]:
    """Read frames until one tagged *kind* arrives."""

    async def read() -> dict[str, Any]:
        while True:
            message = await connection.recv()
            if isinstance(message, bytes):
                continue
            payload = json.loads(message)
            if payload.get("t") == kind:
                return dict(payload)

    return await asyncio.wait_for(read(), timeout)


async def first_binary_frame(
    connection: websockets.ClientConnection, timeout: float = 5.0
) -> bytes:
    """Read frames until a binary one arrives."""

    async def read() -> bytes:
        while True:
            message = await connection.recv()
            if isinstance(message, bytes):
                return message

    return await asyncio.wait_for(read(), timeout)


# -- delivery ------------------------------------------------------------


async def test_tick_reaches_a_websocket_consumer(
    ws_url: str, feeder_socket: socket.socket
) -> None:
    async with websockets.connect(ws_url) as ws:
        await read_until(ws, "hello")
        feeder_socket.sendall(pack_tick(tick(bid=1.2345)))

        ticks = await collect_ticks(ws, 1)
        assert ticks[0]["s"] == "EURUSD"
        assert ticks[0]["b"] == pytest.approx(1.2345)


async def test_the_reader_hands_every_chunk_to_the_link(
    ws_url: str, feeder_socket: socket.socket
) -> None:
    """The wiring smoke test for ingest: whatever the socket produces reaches
    ``hub.feed`` unmangled, and the DEBUG diagnostic does not sit in the way.

    Framing itself is not what this proves -- that is table-tested against the
    link in ``test_hub.py`` with no socket at all. What only a real socket can
    show is that the reader loop passes chunks through as they come, with the
    per-chunk DEBUG branch live: 7 does not divide 64, so a kernel that
    coalesces the writes and one that does not both produce chunks that cut
    records in half.
    """
    logger = logging.getLogger("mt5_ws_stream.bridge")
    previous = logger.level
    logger.setLevel(logging.DEBUG)
    payload = b"".join(pack_tick(tick(seq=i, bid=1.1 + i)) for i in range(20))
    try:
        async with websockets.connect(ws_url) as ws:
            await read_until(ws, "hello")
            assert logger.isEnabledFor(logging.DEBUG), "the debug branch must be live"

            for start in range(0, len(payload), 7):
                feeder_socket.sendall(payload[start : start + 7])
                await asyncio.sleep(0)

            ticks = await collect_ticks(ws, 20)
    finally:
        logger.setLevel(previous)

    assert [t["q"] for t in ticks] == list(range(20))


async def test_symbol_filter(ws_url: str, feeder_socket: socket.socket) -> None:
    async with websockets.connect(f"{ws_url}?symbols=EURUSD") as ws:
        await read_until(ws, "hello")
        for i in range(10):
            feeder_socket.sendall(pack_tick(tick("EURUSD", seq=2 * i)))
            feeder_socket.sendall(pack_tick(tick("USDJPY", seq=2 * i + 1)))

        ticks = await collect_ticks(ws, 10)
        assert {t["s"] for t in ticks} == {"EURUSD"}


async def test_binary_format_round_trips(ws_url: str, feeder_socket: socket.socket) -> None:
    async with websockets.connect(f"{ws_url}?format=binary") as ws:
        await read_until(ws, "hello")
        feeder_socket.sendall(pack_tick(tick(bid=1.5)))

        message = await first_binary_frame(ws)
        assert len(message) % RECORD_SIZE == 0
        assert unpack_tick(message).bid == pytest.approx(1.5)


async def test_heartbeats_are_hidden_unless_requested(
    ws_url: str, feeder_socket: socket.socket
) -> None:
    beat = heartbeat()
    async with websockets.connect(ws_url) as plain:
        await read_until(plain, "hello")
        async with websockets.connect(f"{ws_url}?heartbeats=1") as opted_in:
            await read_until(opted_in, "hello")

            feeder_socket.sendall(pack_tick(beat))
            feeder_socket.sendall(pack_tick(tick(seq=1)))

            # The opt-in consumer sees both; the default consumer only the quote.
            assert len(await collect_ticks(opted_in, 2)) == 2
            plain_ticks = await collect_ticks(plain, 1)
            assert all(t["s"] != "" for t in plain_ticks)


async def test_json_heartbeats_are_recognisable_as_heartbeats(
    ws_url: str, feeder_socket: socket.socket
) -> None:
    """BUG-2: the JSON format used to mask FLAG_HEARTBEAT out of "f", so a JSON
    consumer could never tell a heartbeat from a quote -- only binary could."""
    beat = heartbeat()
    async with TickStreamClient(
        ws_url, payload_format=PayloadFormat.JSON, include_heartbeats=True
    ) as stream:
        feeder_socket.sendall(pack_tick(beat))
        received = await asyncio.wait_for(anext(aiter(stream)), 5)

    assert received.is_heartbeat


# -- control protocol ----------------------------------------------------


async def test_a_control_frame_survives_a_real_socket(ws_url: str) -> None:
    """The one end-to-end control case: a text frame sent by a real client
    reaches the session, and its answer comes back over the same socket. The
    semantics of every op are pinned in-process in ``test_session.py``; what
    this adds is the socket, the upgrade and uvicorn's framing.

    ``hello`` arriving before the ack is part of it: the handler sends the
    handshake before the receive loop and the writer loop exist.
    """
    async with websockets.connect(f"{ws_url}?symbols=EURUSD") as ws:
        first = json.loads(await asyncio.wait_for(ws.recv(), 5))
        assert first["t"] == "hello"

        await ws.send(json.dumps({"op": "subscribe", "symbols": ["USDJPY"]}))
        assert (await read_until(ws, "ack"))["symbols"] == ["EURUSD", "USDJPY"]


# -- lifecycle -----------------------------------------------------------


async def test_feeder_reconnect_resumes_delivery(bridge: Bridge, ws_url: str) -> None:
    async with websockets.connect(ws_url) as ws:
        await read_until(ws, "hello")

        first = socket.create_connection(("127.0.0.1", bridge.tcp_port))
        first.sendall(pack_tick(tick(seq=0, bid=1.0)))
        await collect_ticks(ws, 1)
        first.close()
        # The reader task notices the EOF on its own schedule; wait for the
        # bridge to have dropped the link rather than for a guessed interval.
        await wait_until(lambda: not bridge.feeders)

        second = socket.create_connection(("127.0.0.1", bridge.tcp_port))
        second.sendall(pack_tick(tick(seq=0, bid=2.0)))
        ticks = await collect_ticks(ws, 1)
        second.close()

    assert ticks[0]["b"] == pytest.approx(2.0)


async def test_foreign_protocol_drops_only_that_feeder(bridge: Bridge, ws_url: str) -> None:
    async with websockets.connect(ws_url) as ws:
        await read_until(ws, "hello")

        bad = socket.create_connection(("127.0.0.1", bridge.tcp_port))
        bad.sendall(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n" + b"\x00" * 64)
        # The bridge drops the bad feeder; wait for that rather than for a
        # guessed interval, so what follows proves the *others* survived it.
        await wait_until(lambda: not bridge.feeders)
        bad.close()

        # The bridge is still serving.
        good = socket.create_connection(("127.0.0.1", bridge.tcp_port))
        good.sendall(pack_tick(tick(bid=3.0)))
        ticks = await collect_ticks(ws, 1)
        good.close()

    assert ticks[0]["b"] == pytest.approx(3.0)


async def test_consumer_disconnect_is_cleaned_up(bridge: Bridge, ws_url: str) -> None:
    async with websockets.connect(ws_url) as ws:
        await read_until(ws, "hello")
        assert len(bridge.hub.subscribers) == 1

    # The server notices the close asynchronously; poll rather than guess a sleep.
    await wait_until(lambda: not bridge.hub.subscribers)


async def test_origin_allow_list_rejects_unknown_browsers() -> None:
    config = BridgeConfig(
        tcp_port=0,
        http_port=0,
        stats_interval_s=0.0,
        allowed_origins=frozenset({"https://example.com"}),
    )
    async with Bridge(config) as bridge:
        url = f"ws://127.0.0.1:{bridge.http_port}/ws"

        with pytest.raises(websockets.exceptions.ConnectionClosed):
            async with websockets.connect(
                url, additional_headers={"Origin": "https://evil.example"}
            ) as ws:
                await ws.recv()

        async with websockets.connect(
            url, additional_headers={"Origin": "https://example.com"}
        ) as ws:
            assert (await read_until(ws, "hello"))["protocol"] == 1


async def test_periodic_stats_frame_reports_symbols_and_heartbeats() -> None:
    """``report_once()`` is the one operation the periodic timer loops over;
    driving it directly, instead of waiting out ``stats_interval_s`` and
    polling for a broadcast, is what keeps this test timing-independent. Pin
    the wire form: it must carry the symbols seen and the heartbeat count, not
    merely the quote counter."""
    async with (
        Bridge(BridgeConfig(tcp_port=0, http_port=0, stats_interval_s=0.0)) as inner,
        websockets.connect(f"ws://127.0.0.1:{inner.http_port}/ws") as ws,
    ):
        await read_until(ws, "hello")
        sock = socket.create_connection(("127.0.0.1", inner.tcp_port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            sock.sendall(pack_tick(tick(bid=1.0)))
            sock.sendall(pack_tick(heartbeat(seq=1)))
            # Wait for the hub to have actually ingested both records before
            # asking for a report -- delivery over the feeder socket is async.
            await wait_until(lambda: inner.hub.snapshot_stats().heartbeats == 1)

            await inner.report_once()
            stats = await asyncio.wait_for(read_until(ws, "stats"), 5)
        finally:
            sock.close()

    assert stats["symbols"] == ["EURUSD"]
    assert stats["heartbeats"] == 1


async def test_report_once_does_not_let_one_hung_session_delay_another() -> None:
    """A sink that never drains -- uvicorn's sansio protocol sends no
    server-initiated pings, so a half-open consumer looks exactly like this --
    must not hold up the ``stats`` frame for every other connected consumer.
    Register two sessions straight onto the bridge's own ``_sessions`` set,
    bypassing the WebSocket handshake entirely, and drive ``report_once()``
    directly: one session's sink blocks forever, the other's is a
    :class:`RecordingSink`. The whole call must finish inside a small multiple
    of the configured per-session timeout, and the healthy session must still
    get its frame."""

    class HangingSink:
        async def send(self, payload: str | bytes) -> None:
            await asyncio.sleep(60)

    async with Bridge(
        BridgeConfig(
            tcp_port=0,
            http_port=0,
            stats_interval_s=0.0,
            stats_send_timeout_s=0.05,
        )
    ) as bridge:
        hanging_session = Session(bridge.hub, HangingSink())
        recording_sink = RecordingSink()
        healthy_session = Session(bridge.hub, recording_sink)
        bridge._sessions.update({hanging_session, healthy_session})

        await asyncio.wait_for(bridge.report_once(), timeout=1.0)

    assert recording_sink.last()["t"] == "stats"


async def test_bridge_can_be_started_and_stopped_repeatedly() -> None:
    for _ in range(3):
        async with Bridge(BridgeConfig(tcp_port=0, http_port=0, stats_interval_s=0.0)) as b:
            assert b.tcp_port > 0
            assert b.http_port > 0


async def test_aclose_is_idempotent_after_a_clean_start() -> None:
    """A second ``aclose()`` after a normal start finds the teardown stack
    already empty and does nothing -- same guarantee the failed-start case
    below pins, but for the ordinary path."""
    instance = Bridge(BridgeConfig(tcp_port=0, http_port=0, stats_interval_s=0.0))
    await instance.start()
    await instance.aclose()
    await instance.aclose()


async def test_a_failed_http_bind_unwinds_the_already_bound_feeder_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``start()`` binds the feeder TCP listener before the HTTP listener. If
    the HTTP stage then fails, the TCP listener must not be left holding the
    port hostage -- the case the code comments describe as "a half-bound
    bridge would keep the feeder port hostage". Monkeypatching
    ``_bind_listener`` to fail makes this assertable without wedging two real
    bridges against the same port, and it proves ``start``'s unwinding and
    ``aclose`` share one mechanism (`Bridge._unwind`), not two.
    """
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        tcp_port = probe.getsockname()[1]

    def _boom(host: str, port: int) -> socket.socket:
        raise OSError("simulated http bind failure")

    monkeypatch.setattr("mt5_ws_stream.bridge._bind_listener", _boom)

    instance = Bridge(
        BridgeConfig(
            tcp_host="127.0.0.1",
            tcp_port=tcp_port,
            ws_host="127.0.0.1",
            http_port=0,
            stats_interval_s=0,
        )
    )
    with pytest.raises(OSError, match="simulated http bind failure"):
        await instance.start()

    # The feeder port is free again: a plain socket can bind it with no
    # reuse option needed (the bridge's own listener has actually closed).
    with socket.socket() as retry:
        retry.bind(("127.0.0.1", tcp_port))

    # aclose() after a failed start() is a no-op: nothing left on the stack,
    # and calling it twice is still safe.
    await instance.aclose()
    await instance.aclose()


async def test_unwind_runs_every_step_even_when_one_raises() -> None:
    """A teardown step that raises must not stop the ones below it on the
    stack -- the stats task crashing must not leave the HTTP server, HTTP
    socket and feeder listener open. Push three steps directly onto
    ``_teardown``, bypassing ``start()``, so the ordering is exact: the
    middle one always raises, and both its neighbours must still run, in the
    same LIFO order the real teardown stack uses. The raised exception must
    still surface once every step has run."""
    instance = Bridge(BridgeConfig(tcp_port=0, http_port=0, stats_interval_s=0.0))
    ran: list[str] = []

    async def first() -> None:
        ran.append("first")

    async def boom() -> None:
        ran.append("boom")
        raise RuntimeError("simulated teardown failure")

    async def last() -> None:
        ran.append("last")

    instance._teardown.extend([first, boom, last])

    with pytest.raises(RuntimeError, match="simulated teardown failure"):
        await instance._unwind()

    assert ran == ["last", "boom", "first"], "every step ran, in LIFO order"
    assert instance._teardown == []


# -- client library ------------------------------------------------------
#
# The client's decoding, URL building and handshake live in test_client.py,
# where they need no bridge. What only a real bridge can prove is that the two
# payload formats travel the whole path and land as the same value -- so that is
# the one client test kept here.


async def test_client_binary_and_json_agree(ws_url: str, feeder_socket: socket.socket) -> None:
    """Format is a transport detail; the consumer sees identical Tick objects."""
    sent = tick(bid=2.5, seq=11)

    async def first_tick(fmt: PayloadFormat) -> Tick:
        async with TickStreamClient(ws_url, payload_format=fmt) as stream:
            assert stream.hello is not None
            feeder_socket.sendall(pack_tick(sent))
            return await asyncio.wait_for(anext(aiter(stream)), 5)

    assert await first_tick(PayloadFormat.JSON) == await first_tick(PayloadFormat.BINARY)


async def test_second_bridge_cannot_steal_the_feeder_port(bridge: Bridge) -> None:
    """Two bridges on one feeder port would split the feeder connections between
    them silently. Windows' SO_REUSEADDR allows exactly that, so the listener must
    not opt in there -- binding the busy port has to fail loudly on every OS."""
    second = Bridge(
        BridgeConfig(
            tcp_host="127.0.0.1",
            tcp_port=bridge.tcp_port,
            ws_host="127.0.0.1",
            http_port=0,
            stats_interval_s=0,
        )
    )
    # asyncio wraps EADDRINUSE / WSAEADDRINUSE alike as "error while attempting to bind".
    with pytest.raises(OSError, match="bind"):
        await second.start()
    await second.aclose()


# -- wait_closed / BUG-3 -----------------------------------------------------


async def test_wait_closed_unblocks_once_another_task_closes_the_bridge() -> None:
    """Regression pin for BUG-3: ``wait_closed()`` only ever completes once some
    other task calls :meth:`Bridge.aclose` -- awaiting it from the same task
    that would call ``aclose()`` (or from outside any task that will) hangs
    forever. Bounded by ``wait_for`` so a regression fails fast instead of
    hanging the suite."""
    instance = Bridge(
        BridgeConfig(
            tcp_host="127.0.0.1",
            tcp_port=0,
            ws_host="127.0.0.1",
            http_port=0,
            stats_interval_s=0,
        )
    )
    await instance.start()
    closer = asyncio.create_task(instance.aclose())
    await asyncio.wait_for(instance.wait_closed(), timeout=5.0)
    await closer


async def test_docstring_example_does_not_hang() -> None:
    """Runs the module docstring's example shape: an external stop event, not
    ``wait_closed()``, is what should be awaited inside ``async with
    Bridge(...)``. Pins that this pattern actually returns once the event
    fires, unlike the old example (``await bridge.wait_closed()``), which
    could never return because nothing had called ``aclose()`` yet."""
    stop = asyncio.Event()

    async def run() -> None:
        async with Bridge(
            BridgeConfig(
                tcp_host="127.0.0.1",
                tcp_port=0,
                ws_host="127.0.0.1",
                http_port=0,
                stats_interval_s=0,
            )
        ):
            await stop.wait()

    task = asyncio.create_task(run())
    await asyncio.sleep(0)  # let start() run before we ask it to stop
    stop.set()
    await asyncio.wait_for(task, timeout=5.0)
