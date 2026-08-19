"""Frame-grammar tests: the JSON half of the wire, pinned as exact text.

``test_protocol.py`` pins the binary record as hex literals; this file does the
same job for :mod:`mt5_ws_stream.frames`. Every frame kind gets a reference
vector -- fixed inputs in, the exact string that goes on the socket out -- so a
renamed key, a reordered field or a stray space fails here rather than at a
consumer that was written against the old spelling. The bridge cannot be
diffed against its JavaScript readers (``web/dashboard.html``,
``examples/``) any other way.

Each vector is then decoded back, because encode and decode are two halves of
one definition: a key that only one side knows about is a bug the round-trip
catches even when both sides look right in isolation.

Everything here is a literal. No socket, no bridge, no clock -- the encoders
take their timestamps as arguments for exactly that reason.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from conftest import heartbeat, tick
from mt5_ws_stream import frames
from mt5_ws_stream.frames import (
    ControlFrame,
    FrameDecodeError,
    FrameKind,
    TickFrame,
    ack_frame,
    binary_ticks_frame,
    decode_frame,
    error_frame,
    hello_frame,
    pong_frame,
    stats_frame,
    stats_payload,
    ticks_frame,
)
from mt5_ws_stream.hub import HubStats
from mt5_ws_stream.protocol import (
    FLAG_HEARTBEAT,
    BackpressurePolicy,
    PayloadFormat,
    ProtocolError,
    Tick,
    pack_tick,
)

# -- fixed inputs --------------------------------------------------------

QUOTE = Tick(
    symbol="EURUSD",
    time_msc=1_700_000_000_123,
    bid=1.08501,
    ask=1.08504,
    last=0.0,
    volume=0.0,
    flags=6,
    seq=42,
)

STATS = HubStats(
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


# -- reference vectors ---------------------------------------------------
#
# Captured from the encoders and checked by hand against docs/protocol.md §2.
# A change here is a wire change: it belongs in the CHANGELOG.

REFERENCE_VECTORS: list[tuple[str, str, str]] = [
    (
        "hello",
        hello_frame(
            session_id=1,
            payload_format=PayloadFormat.JSON,
            backpressure=BackpressurePolicy.LOSSLESS,
            symbols={"EURUSD"},
            available=["EURUSD", "USDJPY"],
            snapshot=[QUOTE],
            rx=1_700_000_000.61,
        ),
        '{"t":"hello","id":1,"protocol":1,"format":"json","backpressure":"lossless",'
        '"record_size":64,"symbols":["EURUSD"],"available":["EURUSD","USDJPY"],'
        '"snapshot":[{"s":"EURUSD","ms":1700000000123,"b":1.08501,"a":1.08504,'
        '"l":0.0,"v":0.0,"f":6,"q":42}],"rx":1700000000.61}',
    ),
    (
        "ticks",
        ticks_frame([QUOTE], rx=1_700_000_000.612),
        '{"t":"ticks","rx":1700000000.612,"d":[{"s":"EURUSD","ms":1700000000123,'
        '"b":1.08501,"a":1.08504,"l":0.0,"v":0.0,"f":6,"q":42}]}',
    ),
    (
        "stats",
        stats_frame(STATS),
        '{"t":"stats","uptime_s":12.3,"ticks":9,"tick_rate":41.8,"subscribers":2,'
        '"symbols":["EURUSD"],"seq_gaps":1,"heartbeats":3,"dropped":4,'
        '"broker_lag_ms_p50":1.5,"broker_lag_ms_p99":9.5}',
    ),
    (
        "ack",
        ack_frame("subscribe", symbols={"USDJPY", "EURUSD"}, payload_format=PayloadFormat.JSON),
        '{"t":"ack","op":"subscribe","symbols":["EURUSD","USDJPY"],"format":"json"}',
    ),
    (
        "pong",
        pong_frame(echo=123, rx=1_700_000_000.7),
        '{"t":"pong","rx":1700000000.7,"echo":123}',
    ),
    (
        "error",
        error_frame("unknown op: 'nope'"),
        '{"t":"error","reason":"unknown op: \'nope\'"}',
    ),
]

VECTOR_IDS = [name for name, _, _ in REFERENCE_VECTORS]


@pytest.mark.parametrize(("name", "frame", "expected"), REFERENCE_VECTORS, ids=VECTOR_IDS)
def test_reference_vectors_are_stable(name: str, frame: str, expected: str) -> None:
    assert frame == expected


@pytest.mark.parametrize(("name", "frame", "expected"), REFERENCE_VECTORS, ids=VECTOR_IDS)
def test_every_kind_survives_a_round_trip(name: str, frame: str, expected: str) -> None:
    """Encode then decode: the two halves of one definition have to agree."""
    decoded = decode_frame(frame, received_at=1_700_000_001.0)

    if name == "ticks":
        assert isinstance(decoded, TickFrame)
        assert decoded.ticks == (QUOTE,)
    else:
        assert isinstance(decoded, ControlFrame)
        assert decoded.kind == name
        assert decoded.payload == json.loads(frame)

    # The kinds that carry `rx` are the ones a consumer can measure the hop on;
    # the rest report None rather than guessing zero.
    if "rx" in json.loads(frame):
        assert decoded.rx is not None
        assert decoded.hop == pytest.approx(1_700_000_001.0 - decoded.rx)
    else:
        assert decoded.rx is None
        assert decoded.hop is None


def test_every_frame_kind_has_a_reference_vector() -> None:
    """A kind added to the grammar without a vector is a kind nothing pins."""
    assert {name for name, _, _ in REFERENCE_VECTORS} == {k.value for k in FrameKind}


# -- the symbols convention (E9) -----------------------------------------


@pytest.mark.parametrize(
    ("symbols", "expected"),
    [
        (None, None),
        (frozenset(), []),
        (frozenset({"USDJPY", "EURUSD"}), ["EURUSD", "USDJPY"]),
    ],
    ids=["every-symbol", "no-symbol", "a-filter"],
)
def test_a_symbols_list_distinguishes_all_from_none(
    symbols: frozenset[str] | None, expected: list[str] | None
) -> None:
    """``null`` is "every symbol", ``[]`` is "no symbol". Both are reachable --
    an unfiltered session, and one that unsubscribed from everything it asked
    for -- and the old ack spelled them both ``[]``."""
    ack = json.loads(ack_frame("subscribe", symbols=symbols, payload_format=PayloadFormat.JSON))
    hello = json.loads(
        hello_frame(
            session_id=1,
            payload_format=PayloadFormat.JSON,
            backpressure=BackpressurePolicy.LOSSLESS,
            symbols=symbols,
            available=[],
            snapshot=[],
            rx=1.0,
        )
    )

    assert ack["symbols"] == expected
    assert hello["symbols"] == expected, "hello and ack say it the same way"


def test_hello_separates_the_subscription_from_the_catalogue() -> None:
    """``symbols`` is what this consumer gets; ``available`` is what exists."""
    hello = json.loads(
        hello_frame(
            session_id=7,
            payload_format=PayloadFormat.BINARY,
            backpressure=BackpressurePolicy.CONFLATE,
            symbols={"EURUSD"},
            available=["EURUSD", "USDJPY"],
            snapshot=[],
            rx=1.0,
        )
    )

    assert hello["symbols"] == ["EURUSD"]
    assert hello["available"] == ["EURUSD", "USDJPY"]
    assert hello["format"] == "binary"
    assert hello["backpressure"] == "conflate"


@pytest.mark.parametrize("op", ["subscribe", "unsubscribe", "format"])
def test_one_ack_shape_whatever_the_op(op: str) -> None:
    """``ack`` used to carry ``symbols`` for two ops and ``value`` for a third,
    so a consumer needed a branch per op to track its own subscription."""
    payload = json.loads(ack_frame(op, symbols={"EURUSD"}, payload_format=PayloadFormat.BINARY))

    assert payload == {
        "t": "ack",
        "op": op,
        "symbols": ["EURUSD"],
        "format": "binary",
    }


# -- ticks frames --------------------------------------------------------


def test_an_empty_ticks_frame_is_still_a_frame() -> None:
    """Frame boundaries survive the round trip -- that is what makes ticks/frame
    visible to a consumer measuring batching."""
    frame = ticks_frame([], rx=1.0)

    assert frame == '{"t":"ticks","rx":1.0,"d":[]}'
    decoded = decode_frame(frame, received_at=1.0)
    assert isinstance(decoded, TickFrame)
    assert decoded.ticks == ()


def test_the_binary_spelling_is_the_feeder_bytes_unchanged() -> None:
    """No envelope: a binary frame is records, concatenated, and nothing else."""
    records = [pack_tick(QUOTE), pack_tick(tick(symbol="USDJPY", seq=43))]

    frame = binary_ticks_frame(records)

    assert frame == b"".join(records)
    decoded = decode_frame(frame, received_at=1.0)
    assert isinstance(decoded, TickFrame)
    assert [t.symbol for t in decoded.ticks] == ["EURUSD", "USDJPY"]


def test_the_two_payload_formats_carry_the_same_ticks() -> None:
    """One grammar, two spellings: only the timestamp differs, because raw
    records have nowhere to put one."""
    batch = [QUOTE, tick(symbol="USDJPY", seq=43)]

    as_json = decode_frame(ticks_frame(batch, rx=1.0), received_at=1.5)
    as_binary = decode_frame(binary_ticks_frame(pack_tick(t) for t in batch), received_at=1.5)

    assert isinstance(as_json, TickFrame)
    assert isinstance(as_binary, TickFrame)
    assert as_json.ticks == as_binary.ticks == tuple(batch)
    assert as_json.hop == pytest.approx(0.5)
    assert as_binary.hop is None


def test_the_heartbeat_flag_survives_both_spellings() -> None:
    """A JSON consumer must be able to tell a keep-alive from a quote, like binary."""
    beat = heartbeat(seq=3)

    as_json = decode_frame(ticks_frame([beat], rx=1.0), received_at=1.0)
    as_binary = decode_frame(binary_ticks_frame([pack_tick(beat)]), received_at=1.0)

    assert isinstance(as_json, TickFrame)
    assert isinstance(as_binary, TickFrame)
    assert as_json.ticks[0].flags == FLAG_HEARTBEAT
    assert as_json.ticks[0].is_heartbeat
    assert as_binary.ticks[0].is_heartbeat


# -- stats ---------------------------------------------------------------


def test_stats_payload_rounds_what_a_human_reads() -> None:
    """Full float precision on an uptime or a rate is noise, not information."""
    payload = stats_payload(STATS)

    assert payload["uptime_s"] == 12.3
    assert payload["tick_rate"] == 41.8
    assert payload["ticks"] == 9, "counters are exact"


def test_stats_frame_is_stats_payload_on_the_wire() -> None:
    assert json.loads(stats_frame(STATS)) == stats_payload(STATS)


def test_stats_reports_a_quiet_interval_as_null_not_zero() -> None:
    """``None`` means *nothing measured*; ``0.0`` would mean *measured zero*."""
    quiet = HubStats(
        uptime_s=1.0,
        ticks=0,
        tick_rate=0.0,
        subscribers=0,
        symbols=[],
        seq_gaps=0,
        heartbeats=0,
        dropped=0,
        broker_lag_ms_p50=None,
        broker_lag_ms_p99=None,
    )

    assert '"broker_lag_ms_p50":null' in stats_frame(quiet)


# -- decode: timestamps --------------------------------------------------


def test_a_frame_without_rx_reports_an_unknown_hop() -> None:
    frame = decode_frame('{"t":"ticks","d":[]}', received_at=100.0)

    assert frame.rx is None
    assert frame.hop is None


@pytest.mark.parametrize("rx", ["nope", None, True])
def test_a_non_numeric_rx_is_treated_as_absent(rx: object) -> None:
    frame = decode_frame(json.dumps({"t": "ticks", "rx": rx, "d": []}), received_at=1.0)

    assert frame.rx is None


def test_decode_defaults_received_at_to_the_clock() -> None:
    """The parameter exists so a captured frame decodes without the clock moving;
    omitting it still has to work for a live consumer."""
    before = decode_frame('{"t":"ticks","rx":1.0,"d":[]}').received_at

    assert before > 0.0


# -- decode: control frames ----------------------------------------------


def test_an_unknown_tag_decodes_as_a_control_frame() -> None:
    """A frame kind added later must not stop an older client's tick stream."""
    frame = decode_frame('{"t":"weather","sunny":true}', received_at=1.0)

    assert isinstance(frame, ControlFrame)
    assert frame.kind == "weather"
    assert frame.payload["sunny"] is True


@pytest.mark.parametrize(
    ("message", "kind"),
    [
        ('{"t":"stats","uptime_s":120.0,"ticks":48120}', "stats"),
        ('{"t":"ack","op":"subscribe","symbols":null,"format":"json"}', "ack"),
        ('{"t":"error","reason":"unknown op: \'x\'"}', "error"),
    ],
)
def test_control_frames_decode_without_a_send_timestamp(message: str, kind: str) -> None:
    frame = decode_frame(message, received_at=1.0)

    assert isinstance(frame, ControlFrame)
    assert frame.kind == kind
    assert frame.rx is None
    assert frame.hop is None


def test_a_decoded_kind_compares_against_the_enum() -> None:
    """`FrameKind` is a str enum so a client can branch on it without unwrapping."""
    frame = decode_frame(pong_frame(echo=None, rx=1.0), received_at=1.0)

    assert isinstance(frame, ControlFrame)
    assert frame.kind == FrameKind.PONG


# -- decode: rejections --------------------------------------------------


@pytest.mark.parametrize(
    ("message", "reason"),
    [
        ("{not json", "not valid JSON"),
        ("[1, 2, 3]", "not a JSON object"),
        ('"a string"', "not a JSON object"),
        ('{"rx":1.0,"d":[]}', "no `t` tag"),
        ('{"t":7,"d":[]}', "no `t` tag"),
        ('{"t":"ticks","d":{"s":"EURUSD"}}', "not a list"),
        ('{"t":"ticks","d":[1,2]}', "not an object"),
    ],
)
def test_a_frame_that_is_not_this_grammar_is_rejected(message: str, reason: str) -> None:
    with pytest.raises(FrameDecodeError, match=reason):
        decode_frame(message, received_at=1.0)


def test_a_truncated_binary_frame_is_rejected() -> None:
    """A WebSocket message is a whole message; a partial record is a real error."""
    with pytest.raises(FrameDecodeError, match="not a multiple of 64"):
        decode_frame(pack_tick(QUOTE)[:-1], received_at=1.0)


def test_a_binary_frame_from_a_different_protocol_is_rejected() -> None:
    with pytest.raises(ProtocolError, match="bad header"):
        decode_frame(b"\x00" * 64, received_at=1.0)


def test_a_frame_decode_error_is_a_protocol_error() -> None:
    """One `except` covers both halves of the wire."""
    assert issubclass(FrameDecodeError, ProtocolError)


# -- the home itself -----------------------------------------------------


def test_the_decoder_module_is_the_same_grammar() -> None:
    """`mt5_ws_stream.decoder` is the consumer's view of this module, not a
    second implementation of it."""
    from mt5_ws_stream import decoder

    assert decoder.decode_frame is frames.decode_frame
    assert decoder.TickFrame is TickFrame
    assert decoder.ControlFrame is ControlFrame
    assert decoder.FrameDecodeError is FrameDecodeError


def test_frames_does_not_reach_for_a_clock_when_encoding() -> None:
    """Encoders take ``rx`` as an argument, which is what makes the vectors
    above exact -- and what lets one broadcast stamp every session alike."""
    payloads: list[Any] = [
        ticks_frame([QUOTE], rx=0.0),
        pong_frame(echo=None, rx=0.0),
        hello_frame(
            session_id=1,
            payload_format=PayloadFormat.JSON,
            backpressure=BackpressurePolicy.LOSSLESS,
            symbols=None,
            available=[],
            snapshot=[],
            rx=0.0,
        ),
    ]

    assert all('"rx":0.0' in payload for payload in payloads)
