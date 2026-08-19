"""Session tests: the handshake, the control protocol and the writer loop.

A :class:`~mt5_ws_stream.session.Session` is one consumer's whole conversation
minus the socket, so all of it is exercised here in-process against a
:class:`RecordingSink`. What a real socket adds -- an upgrade, a framed text
message, uvicorn -- is worth exactly one smoke test, which lives in
``test_bridge.py``; running the ten control cases through it as well would buy
nothing but seconds.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from conftest import RecordingSink, blob, tick
from mt5_ws_stream.hub import FeederLink, Hub, SubscriptionOptions
from mt5_ws_stream.protocol import RECORD_SIZE, PayloadFormat
from mt5_ws_stream.session import Session


def op(name: str, **fields: Any) -> str:
    """One control frame as a client would send it."""
    return json.dumps({"op": name, **fields})


# -- handshake -----------------------------------------------------------


async def test_hello_describes_the_session() -> None:
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)

    await session.send_hello()

    hello = sink.json()[0]
    assert hello["t"] == "hello", "hello is the first frame a consumer sees"
    assert hello["id"] == session.id
    assert hello["protocol"] == 1
    assert hello["record_size"] == RECORD_SIZE
    assert hello["format"] == "json"
    assert hello["backpressure"] == "lossless"
    assert hello["snapshot"] == []
    assert hello["symbols"] is None, "no filter means every symbol, not no symbol"


async def test_hello_snapshot_carries_the_latest_price_per_symbol() -> None:
    """So a chart can draw on connect instead of waiting for the next trade."""
    hub = Hub()
    state = FeederLink()
    hub.feed(blob(tick("EURUSD", seq=0, bid=1.0), tick("EURUSD", seq=1, bid=2.0)), state)
    hub.feed(blob(tick("USDJPY", seq=2, bid=157.0)), state)

    sink = RecordingSink()
    session = Session(hub, sink, SubscriptionOptions(symbols=frozenset({"EURUSD"})))
    await session.send_hello()

    hello = sink.last()
    assert {t["s"]: t["b"] for t in hello["snapshot"]} == {"EURUSD": 2.0}
    assert hello["symbols"] == ["EURUSD"], "my subscription"
    assert hello["available"] == ["EURUSD", "USDJPY"], "every symbol seen, not only mine"


# -- control protocol ----------------------------------------------------


async def test_ping_is_answered_with_the_echo() -> None:
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)

    await session.handle(op("ping", echo=42))

    pong = sink.last()
    assert pong["t"] == "pong"
    assert pong["echo"] == 42
    assert isinstance(pong["rx"], float)


async def test_subscribe_adds_to_the_filter_and_unsubscribe_removes() -> None:
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink, SubscriptionOptions(symbols=frozenset({"EURUSD"})))

    await session.handle(op("subscribe", symbols=["USDJPY"]))
    assert sink.last() == {
        "t": "ack",
        "op": "subscribe",
        "symbols": ["EURUSD", "USDJPY"],
        "format": "json",
    }

    await session.handle(op("unsubscribe", symbols=["EURUSD"]))
    assert sink.last()["symbols"] == ["USDJPY"]

    hub.feed(blob(tick("EURUSD", seq=0), tick("USDJPY", seq=1)), FeederLink())
    await session.flush()
    assert [t["s"] for t in sink.ticks()] == ["USDJPY"], "the ack described real delivery"


async def test_an_unfiltered_session_cannot_narrow_itself_by_asking_for_nothing() -> None:
    """``subscribe`` with no symbols means "everything", which is what a session
    with no filter already has -- it must not silently become a filter on the
    empty set."""
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)

    await session.handle(op("subscribe", symbols=[]))
    await session.handle(op("unsubscribe", symbols=["EURUSD"]))

    assert session.options.symbols is None
    hub.feed(blob(tick("EURUSD")), FeederLink())
    await session.flush()
    assert len(sink.ticks()) == 1


async def test_unsubscribing_from_everything_is_not_the_same_as_no_filter() -> None:
    """The two ends of the ``symbols`` convention, told apart on the wire: a
    session that dropped its last symbol gets ``[]`` and no ticks, where an
    unfiltered one gets ``null`` and all of them (backlog E9)."""
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink, SubscriptionOptions(symbols=frozenset({"EURUSD"})))

    await session.handle(op("unsubscribe", symbols=["EURUSD"]))

    assert sink.last()["symbols"] == []
    hub.feed(blob(tick("EURUSD")), FeederLink())
    await session.flush()
    assert sink.ticks() == []


async def test_format_switches_the_encoding_mid_stream() -> None:
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)

    await session.handle(op("format", value="binary"))
    assert sink.last() == {
        "t": "ack",
        "op": "format",
        "symbols": None,
        "format": "binary",
    }
    assert session.options.payload_format is PayloadFormat.BINARY

    hub.feed(blob(tick(bid=9.0)), FeederLink())
    await session.flush()
    assert sink.frames[-1] == blob(tick(bid=9.0))


async def test_a_misspelled_format_falls_back_to_the_readable_one() -> None:
    """A consumer that silently receives bytes it cannot decode is worse off
    than one whose typo left it reading JSON."""
    hub = Hub()
    options = SubscriptionOptions(payload_format=PayloadFormat.BINARY)
    session = Session(hub, RecordingSink(), options)

    await session.handle(op("format", value="nonsense"))

    assert session.options.payload_format is PayloadFormat.JSON


async def test_naming_no_format_keeps_the_current_one() -> None:
    """``{"op": "format"}`` with nothing to switch to is not a switch."""
    hub = Hub()
    options = SubscriptionOptions(payload_format=PayloadFormat.BINARY)
    session = Session(hub, RecordingSink(), options)

    await session.handle(op("format"))

    assert session.options.payload_format is PayloadFormat.BINARY


async def test_stats_answers_with_the_wire_stats_frame() -> None:
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)
    hub.feed(blob(tick()), FeederLink())

    await session.handle(op("stats"))

    stats = sink.last()
    assert stats["t"] == "stats"
    assert stats["ticks"] == 1
    assert stats["subscribers"] == 1
    assert stats["symbols"] == ["EURUSD"]


async def test_asking_for_stats_does_not_steal_them() -> None:
    """A consumer requesting ``stats`` used to consume the interval, blanking the
    percentiles for the periodic log and for every other consumer."""
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)
    hub.feed(blob(tick()), FeederLink())

    for _ in range(2):
        await session.handle(op("stats"))
        assert sink.last()["broker_lag_ms_p50"] is not None

    # The periodic report, which runs after those requests, still has samples.
    assert hub.consume_interval().broker_lag_ms_p50 is not None


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        ("not json at all", "invalid json"),
        ("[1,2,3]", "expected an object"),
        ('{"op":"nope"}', "unknown op: 'nope'"),
        ('{"no_op":true}', "unknown op: None"),
    ],
)
async def test_garbage_is_answered_not_fatal(payload: str, reason: str) -> None:
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)

    await session.handle(payload)
    assert sink.last() == {"t": "error", "reason": reason}

    # Still a working session afterwards.
    await session.handle(op("ping", echo="alive"))
    assert sink.last()["echo"] == "alive"


# -- writer loop ---------------------------------------------------------


async def test_a_failing_sink_raises_out_of_the_writer() -> None:
    """The sink failure has to reach the session's caller. Swallowing it is how
    a dead consumer used to stay subscribed forever, with the hub none the
    wiser."""

    class DeadSink:
        async def send(self, payload: str | bytes) -> None:
            raise ConnectionResetError("peer gone")

    hub = Hub()
    session = Session(hub, DeadSink())
    hub.feed(blob(tick()), FeederLink())

    with pytest.raises(ConnectionResetError):
        await session.flush()


async def test_closing_a_session_stops_delivery_to_it() -> None:
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)

    session.close()
    hub.feed(blob(tick()), FeederLink())
    await session.flush()

    assert sink.frames == []
    assert not hub.subscribers
