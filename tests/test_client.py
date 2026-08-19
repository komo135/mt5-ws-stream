"""The client adapter, with no bridge in sight.

:mod:`mt5_ws_stream.client` owns exactly one thing of its own that is tested
here: the handshake it insists on, exercised against a fake connection, so
nothing in this file needs a socket.

Neither of the two things it borrows is tested here. The URL it builds is the
write half of a subscription request and is covered in ``test_subscription.py``
next to the read half it has to agree with. What a frame *means* is the frame
grammar, and that has one home (:mod:`mt5_ws_stream.frames`) and one test file
(``test_frames.py``); this file only uses frames as fixtures. The end-to-end
proof that a real bridge and this client agree lives in ``test_bridge.py``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from conftest import tick
from mt5_ws_stream.client import HandshakeError, TickStreamClient
from mt5_ws_stream.decoder import ControlFrame, FrameDecodeError, TickFrame
from mt5_ws_stream.protocol import Tick, pack_tick

# asyncio_mode = "auto" in pyproject.toml means async tests need no marker.

# One `ticks` frame exactly as the bridge writes it -- the reference vector from
# `test_frames.py`, repeated as a literal so this file needs no encoder to
# produce the input its subject reads.
TICKS_JSON = (
    '{"t":"ticks","rx":100.0,"d":['
    '{"s":"EURUSD","ms":1700000000123,"b":1.085,"a":1.0851,"l":0.5,"v":2.0,"f":6,"q":42}'
    "]}"
)

EXPECTED_TICK = Tick(
    symbol="EURUSD",
    time_msc=1_700_000_000_123,
    bid=1.085,
    ask=1.0851,
    last=0.5,
    volume=2.0,
    flags=6,
    seq=42,
)


# -- client: the fake transport ------------------------------------------

HELLO = '{"t":"hello","id":1,"symbols":["EURUSD"],"rx":100.0}'


class FakeConnection:
    """A scripted stand-in for a WebSocket connection.

    Satisfies :class:`~mt5_ws_stream.client.Connection` structurally. Messages
    are handed out in order; a :class:`BaseException` in the script is raised
    instead, which is how "the link died mid-handshake" is expressed.
    """

    def __init__(self, *messages: str | bytes | BaseException) -> None:
        self.pending = list(messages)
        self.sent: list[str] = []
        self.closed = False

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str | bytes:
        if not self.pending:
            raise ConnectionResetError("nothing left to read")
        item = self.pending.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    async def close(self) -> None:
        self.closed = True

    async def __aiter__(self) -> AsyncIterator[str | bytes]:
        while self.pending:
            item = self.pending.pop(0)
            if isinstance(item, BaseException):
                raise item
            yield item


def client_for(connection: FakeConnection) -> TickStreamClient:
    async def connect_fn(url: str, **kwargs: object) -> FakeConnection:
        return connection

    return TickStreamClient("ws://host:1/ws", connect_fn=connect_fn)


# -- client: handshake ---------------------------------------------------


async def test_connect_keeps_the_hello_payload() -> None:
    connection = FakeConnection(HELLO)

    async with client_for(connection) as stream:
        assert stream.hello is not None
        assert stream.hello["symbols"] == ["EURUSD"]

    assert connection.closed


async def test_connect_rejects_a_first_frame_that_is_not_hello() -> None:
    """The hello-first guarantee is part of the grammar, so silence is not an option."""
    connection = FakeConnection(TICKS_JSON)
    client = client_for(connection)

    with pytest.raises(HandshakeError, match="expected a `hello` frame first"):
        await client.connect()

    assert client.hello is None
    assert connection.closed


async def test_connect_rejects_a_first_frame_that_is_not_a_frame() -> None:
    connection = FakeConnection("<html>404</html>")
    client = client_for(connection)

    with pytest.raises(FrameDecodeError):
        await client.connect()

    assert connection.closed


async def test_connect_surfaces_a_hello_that_never_arrives() -> None:
    """The link dying before the handshake used to be swallowed silently."""
    connection = FakeConnection(ConnectionResetError("closed before hello"))
    client = client_for(connection)

    with pytest.raises(ConnectionResetError):
        await client.connect()

    assert client.hello is None
    assert connection.closed


async def test_connect_gives_up_on_a_peer_that_says_nothing() -> None:
    """A socket that comes up is not a bridge. The transport's ``open_timeout``
    ends at the WebSocket upgrade, so without a bound of its own ``connect()``
    would block forever against a live port that never sends ``hello``.
    """

    class SilentConnection(FakeConnection):
        async def recv(self) -> str | bytes:
            await asyncio.Event().wait()  # never set
            raise AssertionError("unreachable")

    connection = SilentConnection()

    async def connect_fn(url: str, **kwargs: object) -> FakeConnection:
        return connection

    client = TickStreamClient("ws://host:1/ws", handshake_timeout=0.05, connect_fn=connect_fn)

    with pytest.raises(HandshakeError, match="no `hello` frame within"):
        await client.connect()

    assert client.hello is None
    assert connection.closed


async def test_using_the_client_before_connecting_is_an_error() -> None:
    client = TickStreamClient("ws://host:1/ws")

    with pytest.raises(RuntimeError, match="not connected"):
        await client.ping()


# -- client: streaming ---------------------------------------------------


async def test_stream_yields_one_decoded_frame_per_message() -> None:
    connection = FakeConnection(HELLO, TICKS_JSON, '{"t":"stats","ticks":1}')

    async with client_for(connection) as stream:
        frames = [frame async for frame in stream.stream()]

    assert [type(f) for f in frames] == [TickFrame, ControlFrame]
    assert isinstance(frames[0], TickFrame)
    assert frames[0].ticks == (EXPECTED_TICK,)


async def test_ticks_flattens_frames_and_drops_control_frames() -> None:
    connection = FakeConnection(
        HELLO,
        TICKS_JSON,
        '{"t":"ack","op":"subscribe"}',
        pack_tick(tick(symbol="USDJPY", seq=7)),
    )

    async with client_for(connection) as stream:
        received = [t async for t in stream]

    assert [t.symbol for t in received] == ["EURUSD", "USDJPY"]


async def test_control_ops_are_sent_as_compact_json() -> None:
    connection = FakeConnection(HELLO)

    async with client_for(connection) as stream:
        await stream.subscribe(["EURUSD"])
        await stream.unsubscribe(["GBPUSD"])
        await stream.set_format("binary")
        await stream.request_stats()
        await stream.ping(echo=7)

    assert connection.sent == [
        '{"op":"subscribe","symbols":["EURUSD"]}',
        '{"op":"unsubscribe","symbols":["GBPUSD"]}',
        '{"op":"format","value":"binary"}',
        '{"op":"stats"}',
        '{"op":"ping","echo":7}',
    ]
