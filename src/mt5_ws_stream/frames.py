"""The frame grammar: one home for every bridge -> consumer WebSocket message.

:mod:`~mt5_ws_stream.protocol` is the single definition of the 64-byte binary
record; this module is the same thing for the JSON half. One WebSocket message
is one **frame** (`docs/protocol.md §2`), and every frame kind is defined here
exactly once, on both sides:

* the **encode** side -- :func:`hello_frame`, :func:`ticks_frame`,
  :func:`stats_frame`, :func:`ack_frame`, :func:`pong_frame`,
  :func:`error_frame`, plus :func:`binary_ticks_frame` for the ``binary``
  payload format. Each returns the exact bytes that go on the wire, so
  ``tests/test_frames.py`` can pin them as text the way ``test_protocol.py``
  pins the record layout as hex.
* the **decode** side -- :func:`decode_frame`, returning a :class:`TickFrame` or
  a :class:`ControlFrame`. :mod:`mt5_ws_stream.decoder` re-exports it under the
  name consumers know.

Producers therefore build no frame literals: :mod:`~mt5_ws_stream.session`
calls the encoders, :mod:`~mt5_ws_stream.api` calls :func:`stats_payload`, and
:mod:`~mt5_ws_stream.hub` -- which fans ticks out but never says what a frame
looks like -- calls none of them.

Two vocabulary rules the grammar keeps, both worth stating because JSON cannot:

* **A ``symbols`` list is a subscription filter, and ``null`` means "every
  symbol".** ``[]`` means the opposite -- subscribed to nothing -- which is a
  reachable state (unsubscribe from everything you asked for). Collapsing the
  two, as the old ``ack`` did, left a consumer unable to tell "you get it all"
  from "you get nothing". ``hello``'s ``available`` is the other list: every
  symbol the bridge has *seen*, which is what a symbol picker draws.
* **Timestamps are arguments, not clock reads.** ``rx`` is passed in by the
  caller that owns the send, exactly like ``SymbolResponse.from_snapshot(now=)``
  -- which is also what makes an exact-text reference vector possible.

At runtime this module imports :mod:`~mt5_ws_stream.protocol` and nothing else
from the package, so the whole grammar stays testable with literals: no socket,
no server, no clock.
"""

from __future__ import annotations

import enum
import json
import math
import time
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

from .protocol import (
    RECORD_SIZE,
    BackpressurePolicy,
    PayloadFormat,
    ProtocolError,
    Tick,
    compact_json,
    iter_ticks,
)

_isfinite = math.isfinite

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .hub import HubStats

__all__ = [
    "FRAME_TAG",
    "PROTOCOL_VERSION",
    "ControlFrame",
    "DecodedFrame",
    "FrameDecodeError",
    "FrameKind",
    "TickFrame",
    "ack_frame",
    "binary_ticks_frame",
    "decode_frame",
    "error_frame",
    "hello_frame",
    "pong_frame",
    "stats_frame",
    "stats_payload",
    "ticks_frame",
]

#: The key every JSON frame carries its kind in.
FRAME_TAG: Final[str] = "t"

#: Version of the frame grammar, announced in ``hello``.
PROTOCOL_VERSION: Final[int] = 1


class FrameKind(str, enum.Enum):
    """The kinds of frame a bridge sends. The value is the ``t`` tag.

    A :class:`str` enum so a comparison against a decoded tag
    (``frame.kind == FrameKind.HELLO``) works without unwrapping, and so an
    unknown tag from a newer bridge is still just a string -- :func:`decode_frame`
    deliberately does not validate against this enum.
    """

    HELLO = "hello"
    """Sent once, first, before any other frame."""

    TICKS = "ticks"
    """One or more quotes. The only kind with a binary spelling."""

    STATS = "stats"
    """Throughput and latency counters; periodic, or on request."""

    ACK = "ack"
    """The subscription as it stands after a control op changed it."""

    PONG = "pong"
    """Reply to the ``ping`` control op."""

    ERROR = "error"
    """A rejected control frame. Never fatal to the connection."""


# -- encode --------------------------------------------------------------


def hello_frame(
    *,
    session_id: int,
    payload_format: PayloadFormat,
    backpressure: BackpressurePolicy,
    symbols: Collection[str] | None,
    available: Sequence[str],
    snapshot: Sequence[Tick],
    rx: float,
) -> str:
    """The frame announcing a session to its consumer, sent before anything else.

    Everything a client needs before the first tick arrives: which id it is,
    what the record layout is, the subscription it actually got, which symbols
    the bridge has seen, and the latest quote of each subscribed one -- so a
    chart can draw immediately instead of waiting for every symbol to trade.

    Args:
        session_id: The subscriber id, which the bridge's logs also use.
        payload_format: Encoding of this session's ``ticks`` frames.
        backpressure: What happens to this session's ticks when it falls behind.
        symbols: The subscription filter, or ``None`` for every symbol.
        available: Every symbol the bridge has seen since it started.
        snapshot: Latest quote per subscribed symbol; may be empty.
        rx: Send timestamp, epoch seconds.
    """
    return _dumps(
        {
            FRAME_TAG: FrameKind.HELLO.value,
            "id": session_id,
            "protocol": PROTOCOL_VERSION,
            "format": payload_format.value,
            "backpressure": backpressure.value,
            "record_size": RECORD_SIZE,
            "symbols": _symbol_filter(symbols),
            "available": list(available),
            "snapshot": [tick.as_dict() for tick in snapshot],
            "rx": rx,
        }
    )


#: The fixed head of every ``ticks`` frame, spelled from the grammar's own
#: names so a renamed tag cannot leave the hot path emitting the old one.
_TICKS_HEAD: Final[str] = f'{{"{FRAME_TAG}":"{FrameKind.TICKS.value}","rx":'


def ticks_frame(ticks: Iterable[Tick], *, rx: float) -> str:
    """One or more quotes, as the ``json`` payload format spells them.

    The hot path, and the only encoder here that does not go through
    :func:`_dumps`: the frame is a list of identically shaped objects, so
    :meth:`~mt5_ws_stream.protocol.Tick.as_json` writes each one directly and
    this joins them. Byte for byte the same frame the dict-and-``json.dumps``
    spelling produced -- ``as_json``'s docstring says what keeps it that way --
    with no validation layer between a decoded tick and the socket.

    Args:
        ticks: The batch, in wire order. May be empty -- an empty frame is
            still a frame, and its boundary is information.
        rx: Send timestamp, epoch seconds. Subtracted from the consumer's
            receive time it gives the bridge -> consumer hop.
    """
    stamp = repr(rx) if _isfinite(rx) else compact_json(rx)
    body = ",".join([tick.as_json() for tick in ticks])
    return f'{_TICKS_HEAD}{stamp},"d":[{body}]}}'


def binary_ticks_frame(records: Iterable[bytes]) -> bytes:
    """The same frame under the ``binary`` payload format: records, concatenated.

    The feeder's own bytes, unmodified -- no envelope, no re-packing, and a
    whole number of records by construction. There is no room for ``rx`` here,
    which is why :attr:`TickFrame.hop` is ``None`` for binary frames.
    """
    return b"".join(records)


def stats_payload(stats: HubStats) -> dict[str, Any]:
    """The ``stats`` frame's fields, and the only list of them in the process.

    :func:`stats_frame` renders these to the wire; ``GET /api/v1/stats`` returns
    :class:`mt5_ws_stream.api.StatsResponse`, which is built from this dict --
    so the streamed frame and the REST body cannot drift apart, and
    ``tests/test_api.py`` asserts they are equal field for field.

    ``uptime_s`` and ``tick_rate`` are rounded because a dashboard shows them to
    a human and full float precision on a rate is noise, not information.
    """
    return {
        FRAME_TAG: FrameKind.STATS.value,
        "uptime_s": round(stats.uptime_s, 1),
        "ticks": stats.ticks,
        "tick_rate": round(stats.tick_rate, 1),
        "subscribers": stats.subscribers,
        "symbols": stats.symbols,
        "seq_gaps": stats.seq_gaps,
        "heartbeats": stats.heartbeats,
        "dropped": stats.dropped,
        "broker_lag_ms_p50": stats.broker_lag_ms_p50,
        "broker_lag_ms_p99": stats.broker_lag_ms_p99,
    }


def stats_frame(stats: HubStats) -> str:
    """*stats* as its wire frame, for both producers of one.

    The WebSocket ``stats`` control reply and the bridge's periodic broadcast
    both go through here. Unlike every other list in this module, ``symbols``
    is the bridge's catalogue rather than a subscription filter: a ``stats``
    frame reports the process, not a session.
    """
    return _dumps(stats_payload(stats))


def ack_frame(
    op: str, *, symbols: Collection[str] | None, payload_format: PayloadFormat
) -> str:
    """The reply to a control op that changed the subscription.

    One shape for every such op -- ``subscribe``, ``unsubscribe``, ``format`` --
    rather than the two the grammar used to have (``symbols`` for one pair of
    ops, ``value`` for the other). An ack answers one question, *what is my
    subscription now*, and answering it the same way regardless of which op
    asked is what lets a consumer keep its own copy in step with a single
    handler.

    ``backpressure`` is absent on purpose: no control op can change it, so
    echoing it would be noise.

    Args:
        op: The op being acknowledged, echoed back.
        symbols: The filter as it now stands, or ``None`` for every symbol.
        payload_format: The encoding as it now stands.
    """
    return _dumps(
        {
            FRAME_TAG: FrameKind.ACK.value,
            "op": op,
            "symbols": _symbol_filter(symbols),
            "format": payload_format.value,
        }
    )


def pong_frame(*, echo: Any, rx: float) -> str:
    """The reply to ``ping``, carrying the client's *echo* back untouched.

    *echo* is whatever the client sent -- the grammar does not interpret it, it
    is the client's own correlation token.
    """
    return _dumps({FRAME_TAG: FrameKind.PONG.value, "rx": rx, "echo": echo})


def error_frame(reason: str) -> str:
    """A rejected control frame, with *reason* in words.

    Sent instead of dropping the connection: a consumer that mistypes an op in
    a debugging console should lose the op, not the stream.
    """
    return _dumps({FRAME_TAG: FrameKind.ERROR.value, "reason": reason})


def _symbol_filter(symbols: Collection[str] | None) -> list[str] | None:
    """Render a subscription filter for the wire: ``None`` for "every symbol".

    Sorted, so a frame is a function of the subscription and not of set
    iteration order -- which is what makes the reference vectors reproducible.
    """
    return None if symbols is None else sorted(symbols)


def _dumps(payload: dict[str, Any]) -> str:
    """Compact JSON. Whitespace is bandwidth at a few thousand ticks per second."""
    return compact_json(payload)


# -- decode --------------------------------------------------------------


class FrameDecodeError(ProtocolError):
    """Raised when a WebSocket message is not a frame this grammar knows.

    A subclass of :class:`~mt5_ws_stream.protocol.ProtocolError` so one
    ``except`` covers both halves of the wire: a record that will not decode and
    a frame that will not decode are the same kind of problem to a consumer.
    """


def _hop(rx: float | None, received_at: float) -> float | None:
    if rx is None:
        return None
    return received_at - rx


@dataclass(frozen=True, slots=True)
class TickFrame:
    """The quotes one ``ticks`` frame carried."""

    ticks: tuple[Tick, ...]
    """Every tick in the frame, in wire order. May be empty."""

    received_at: float
    """Local clock when the frame came off the socket (epoch seconds)."""

    rx: float | None = None
    """The bridge's send timestamp (epoch seconds), or ``None`` if unknown.

    Binary frames are raw records with no room for a timestamp, so they always
    decode to ``None``; a JSON frame is only missing it if the bridge omitted it.
    """

    @property
    def hop(self) -> float | None:
        """Seconds from bridge send to local receive, or ``None`` without ``rx``.

        Meaningful only when both ends share a clock (same machine, or NTP
        synced); otherwise read it as a trend, not an absolute.
        """
        return _hop(self.rx, self.received_at)


@dataclass(frozen=True, slots=True)
class ControlFrame:
    """A non-tick frame: ``hello``, ``stats``, ``ack``, ``pong``, ``error``."""

    kind: str
    """The frame's ``t`` tag, verbatim -- including tags this version predates.

    A plain :class:`str` rather than a :class:`FrameKind`, because a client must
    survive a kind added after it was written; compare it against
    :class:`FrameKind` members, which are strings.
    """

    received_at: float
    """Local clock when the frame came off the socket (epoch seconds)."""

    payload: dict[str, Any] = field(default_factory=dict)
    """The decoded JSON object, ``t`` included."""

    rx: float | None = None
    """The bridge's send timestamp, for the frames that carry one (``hello``,
    ``pong``); ``None`` for the ones that do not."""

    @property
    def hop(self) -> float | None:
        """Seconds from bridge send to local receive, or ``None`` without ``rx``."""
        return _hop(self.rx, self.received_at)


#: What :func:`decode_frame` returns. One message in, one of these out.
DecodedFrame = TickFrame | ControlFrame


def decode_frame(
    message: str | bytes | bytearray, *, received_at: float | None = None
) -> DecodedFrame:
    """Decode one WebSocket message from the bridge.

    The inverse of the encoders above, and the reason they live in the same
    module: a key renamed on one side fails on the other in
    ``tests/test_frames.py`` rather than at a consumer's runtime.

    Args:
        message: The message exactly as the transport delivered it. ``bytes``
            means a binary ``ticks`` frame; ``str`` means a JSON frame whose
            ``t`` tag says which kind.
        received_at: Local receive time in epoch seconds. Defaults to
            :func:`time.time`; pass it explicitly to decode a captured frame
            without the clock moving underneath the result.

    Returns:
        A :class:`TickFrame` or a :class:`ControlFrame`.

    Raises:
        FrameDecodeError: the message is not valid JSON, is JSON that is not an
            object, carries no ``t`` tag, or is a binary payload that is not a
            whole number of records.
        ~mt5_ws_stream.protocol.ProtocolError: a binary record's header does not
            match this protocol version.
    """
    at = time.time() if received_at is None else received_at

    if isinstance(message, (bytes, bytearray)):
        if len(message) % RECORD_SIZE:
            raise FrameDecodeError(
                f"binary frame is {len(message)} bytes, not a multiple of {RECORD_SIZE}"
            )
        return TickFrame(ticks=tuple(iter_ticks(message)), received_at=at)

    try:
        payload = json.loads(message)
    except ValueError as exc:
        raise FrameDecodeError(f"frame is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise FrameDecodeError(f"frame is not a JSON object: {type(payload).__name__}")

    kind = payload.get(FRAME_TAG)
    if not isinstance(kind, str):
        raise FrameDecodeError(f"frame has no `{FRAME_TAG}` tag: {sorted(payload)}")

    rx = _timestamp(payload.get("rx"))
    if kind != FrameKind.TICKS:
        return ControlFrame(kind=kind, received_at=at, payload=payload, rx=rx)

    data = payload.get("d", [])
    if not isinstance(data, list):
        raise FrameDecodeError(f"`ticks` frame's `d` is {type(data).__name__}, not a list")
    ticks = []
    for item in data:
        if not isinstance(item, dict):
            raise FrameDecodeError(
                f"`ticks` frame holds a {type(item).__name__}, not an object"
            )
        ticks.append(Tick.from_dict(item))
    return TickFrame(ticks=tuple(ticks), received_at=at, rx=rx)


def _timestamp(value: Any) -> float | None:
    """A frame timestamp, or ``None`` when the field is absent or not a number."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)
