"""Wire-format tests.

The protocol is the contract between MQL5 and Python, and only one side of it is
testable here -- MetaTrader is Windows-only and cannot run in CI. So these tests
pin the *bytes*: :func:`test_reference_vectors_are_stable` encodes known ticks and
compares against hex literals captured from a working end-to-end run. If someone
reorders a field, that test fails even though a pack/unpack round-trip still
passes.
"""

from __future__ import annotations

import random
import struct

import pytest

from mt5_ws_stream.protocol import (
    FLAG_HEARTBEAT,
    MAGIC,
    RECORD_SIZE,
    BackpressurePolicy,
    PayloadFormat,
    ProtocolError,
    Tick,
    compact_json,
    decode_records,
    iter_ticks,
    pack_tick,
    split_symbols,
    unpack_tick,
)


def make_tick(**overrides: object) -> Tick:
    base: dict[str, object] = {
        "symbol": "EURUSD",
        "time_msc": 1_700_000_000_123,
        "bid": 1.08501,
        "ask": 1.08504,
        "last": 0.0,
        "volume": 0.0,
        "flags": 6,
        "seq": 42,
    }
    base.update(overrides)
    return Tick(**base)  # type: ignore[arg-type]


def test_record_size_is_frozen() -> None:
    # The MQL5 side hard-codes 64. If this changes, TickStreamer.mq5 must too.
    assert RECORD_SIZE == 64
    assert len(pack_tick(make_tick())) == 64


def test_round_trip_preserves_every_field() -> None:
    tick = make_tick(last=1.085, volume=12.5, seq=7, flags=30)
    assert unpack_tick(pack_tick(tick)) == tick


@pytest.mark.parametrize(
    "tick",
    [
        make_tick(),
        make_tick(symbol="", flags=FLAG_HEARTBEAT, bid=0.0, ask=0.0),
        make_tick(symbol="USDJPY", bid=157.254, ask=157.257),
        make_tick(seq=0xFFFF_FFFF),
        make_tick(time_msc=0),
        make_tick(time_msc=-1),
        make_tick(bid=-1.5, ask=1e308, last=-0.0, volume=1e-308),
        make_tick(flags=0xFFFF_FFFF),
    ],
    ids=[
        "typical",
        "heartbeat",
        "jpy-pair",
        "seq-max",
        "epoch",
        "negative-time",
        "float-extremes",
        "all-flags",
    ],
)
def test_round_trip_edge_cases(tick: Tick) -> None:
    assert unpack_tick(pack_tick(tick)) == tick


def test_symbol_is_truncated_not_rejected() -> None:
    # Brokers append suffixes ("EURUSD.pro"); 12 bytes is the field width, and
    # silently truncating beats dropping the tick.
    tick = make_tick(symbol="VERYLONGSYMBOLNAME")
    assert unpack_tick(pack_tick(tick)).symbol == "VERYLONGSYMB"


def test_seq_wraps_instead_of_overflowing() -> None:
    assert unpack_tick(pack_tick(make_tick(seq=0x1_0000_0000))).seq == 0


def test_non_ascii_symbol_is_rejected() -> None:
    with pytest.raises(ProtocolError):
        pack_tick(make_tick(symbol="ユーロ"))


def test_heartbeat_flag_is_exposed() -> None:
    assert unpack_tick(pack_tick(make_tick(flags=FLAG_HEARTBEAT))).is_heartbeat
    assert not unpack_tick(pack_tick(make_tick(flags=6))).is_heartbeat


def test_spread() -> None:
    assert make_tick(bid=1.0, ask=1.5).spread == pytest.approx(0.5)


def test_as_dict_uses_the_short_json_keys() -> None:
    payload = make_tick().as_dict()
    assert set(payload) == {"s", "ms", "b", "a", "l", "v", "f", "q"}
    assert payload["s"] == "EURUSD"


def test_as_dict_keeps_the_heartbeat_bit() -> None:
    # BUG-2: "f" used to be masked to 16 bits, silently dropping FLAG_HEARTBEAT
    # and making Tick.is_heartbeat unrecoverable from the JSON payload.
    payload = make_tick(flags=FLAG_HEARTBEAT | 6).as_dict()
    assert payload["f"] == FLAG_HEARTBEAT | 6


def test_as_json_is_byte_identical_to_encoding_as_dict() -> None:
    """The hot path spells a tick's JSON object directly; this is the contract
    that lets it. Randomised rather than a fixed list because the interesting
    cases are the ones nobody thinks to write down: a price whose ``repr`` is
    17 significant digits, a symbol needing an escape, a flags word at the
    32-bit ceiling, and the non-finite doubles the wire can carry but JSON
    spells ``Infinity`` / ``NaN`` rather than ``inf`` / ``nan``.
    """
    rng = random.Random(20260817)
    # U+FFFD is what a non-ASCII byte on the wire decodes to; the rest are the
    # characters JSON has to escape.
    symbols = ["EURUSD", "", 'a"b', "US\\D", "��", "x" * 12, "\xe9uro", "\t\n"]
    numbers = [
        0.0,
        -0.0,
        1.08501,
        -1.5,
        0.1 + 0.2,
        1e-300,
        1e300,
        1.0000000000000002,
        float("inf"),
        float("-inf"),
        float("nan"),
    ]
    ints = [0, 1, 6, -5, FLAG_HEARTBEAT, 0xFFFF_FFFF, 2**63 - 1, -(2**63)]

    for _ in range(2_000):
        tick = Tick(
            symbol=rng.choice(symbols),
            time_msc=rng.choice(ints),
            bid=rng.choice(numbers),
            ask=rng.choice(numbers),
            last=rng.choice(numbers),
            volume=rng.choice(numbers),
            flags=rng.choice(ints),
            seq=rng.choice(ints),
        )
        assert tick.as_json() == compact_json(tick.as_dict()), tick


def test_as_json_survives_more_symbols_than_the_cache_holds() -> None:
    """The escaped-symbol cache is capped, so it clears and refills mid-stream.
    A tick encoded either side of that must read the same."""
    before = make_tick(symbol="EURUSD").as_json()
    for i in range(3_000):
        make_tick(symbol=f"S{i}").as_json()
    assert make_tick(symbol="EURUSD").as_json() == before


@pytest.mark.parametrize(
    "tick",
    [make_tick(), make_tick(symbol="", flags=FLAG_HEARTBEAT, bid=0.0, ask=0.0)],
    ids=["quote", "heartbeat"],
)
def test_from_dict_is_the_inverse_of_as_dict(tick: Tick) -> None:
    assert Tick.from_dict(tick.as_dict()) == tick
    assert Tick.from_dict(tick.as_dict()).is_heartbeat == tick.is_heartbeat


def test_from_dict_defaults_missing_keys() -> None:
    # Matches the .get(..., default) behaviour the old client._tick_from_dict had.
    assert Tick.from_dict({}) == Tick(
        symbol="", time_msc=0, bid=0.0, ask=0.0, last=0.0, volume=0.0, flags=0, seq=0
    )


def test_iter_ticks_decodes_a_batch() -> None:
    ticks = [make_tick(seq=i, bid=1.0 + i) for i in range(5)]
    blob = b"".join(pack_tick(t) for t in ticks)
    assert list(iter_ticks(blob)) == ticks


def test_iter_ticks_ignores_a_partial_tail() -> None:
    blob = pack_tick(make_tick()) + b"\x00" * 13
    assert len(list(iter_ticks(blob))) == 1


def test_decode_records_returns_each_tick_with_its_own_bytes() -> None:
    """The bridge needs both halves at once -- the value for JSON subscribers,
    the exact record for binary ones -- so the one decode loop hands back both,
    and the partial tail is ignored here exactly as it is for ticks alone."""
    records = [pack_tick(make_tick(seq=i, bid=1.0 + i)) for i in range(3)]
    whole = b"".join(records)

    decoded = decode_records(whole + b"\x00" * 13)

    assert [raw for _tick, raw in decoded] == records
    assert [t for t, _raw in decoded] == list(iter_ticks(whole))


def test_bad_magic_is_rejected() -> None:
    corrupt = bytearray(pack_tick(make_tick()))
    corrupt[0] ^= 0xFF
    with pytest.raises(ProtocolError, match="bad header"):
        unpack_tick(bytes(corrupt))


def test_wrong_record_size_is_rejected() -> None:
    corrupt = bytearray(pack_tick(make_tick()))
    corrupt[2:4] = struct.pack("<H", 128)
    with pytest.raises(ProtocolError, match="bad header"):
        unpack_tick(bytes(corrupt))


def test_truncated_buffer_is_rejected() -> None:
    with pytest.raises(ProtocolError, match="truncated"):
        unpack_tick(pack_tick(make_tick())[:40])


# Captured from a verified end-to-end run. Changing a field's offset or width
# breaks these, which is the point -- MQL5 cannot be tested from CI.
REFERENCE_VECTORS = [
    (
        make_tick(),
        "544b"  # magic 'TK'
        "4000"  # record_size 64
        "2a000000"  # seq 42
        "455552555344000000000000"  # 'EURUSD' NUL-padded to 12
        "7b68e5cf8b010000"  # time_msc 1700000000123
        "ce531d72335cf13f"  # bid 1.08501
        "23a12de7525cf13f"  # ask 1.08504
        "0000000000000000"  # last
        "0000000000000000"  # volume_real
        "06000000",  # flags
    ),
    (
        make_tick(symbol="", flags=FLAG_HEARTBEAT, bid=0.0, ask=0.0, seq=1),
        "544b"
        "4000"
        "01000000"
        "000000000000000000000000"  # empty symbol
        "7b68e5cf8b010000"
        "0000000000000000"
        "0000000000000000"
        "0000000000000000"
        "0000000000000000"
        "00000080",  # FLAG_HEARTBEAT
    ),
]


@pytest.mark.parametrize(
    ("tick", "expected_hex"), REFERENCE_VECTORS, ids=["quote", "heartbeat"]
)
def test_reference_vectors_are_stable(tick: Tick, expected_hex: str) -> None:
    assert pack_tick(tick).hex() == expected_hex.replace(" ", "")


def test_magic_is_ascii_tk() -> None:
    assert struct.pack("<H", MAGIC) == b"TK"


# -- PayloadFormat / BackpressurePolicy -----------------------------------
#
# The enums themselves live here because they are client/server vocabulary,
# not a hub concern (see mt5_ws_stream.hub's docstring). Lenient parsing of
# query strings is an HTTP concern and lives in mt5_ws_stream.api instead.


def test_payload_format_values() -> None:
    assert PayloadFormat.JSON.value == "json"
    assert PayloadFormat.BINARY.value == "binary"


def test_backpressure_policy_values() -> None:
    assert BackpressurePolicy.LOSSLESS.value == "lossless"
    assert BackpressurePolicy.CONFLATE.value == "conflate"


# -- split_symbols ----------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("EURUSD", ["EURUSD"]),
        ("EURUSD,USDJPY", ["EURUSD", "USDJPY"]),
        (" EURUSD , USDJPY ", ["EURUSD", "USDJPY"]),
        ("", []),
        (",,,", []),
        ("EURUSD,,USDJPY", ["EURUSD", "USDJPY"]),
    ],
)
def test_split_symbols(text: str, expected: list[str]) -> None:
    assert split_symbols(text) == expected
