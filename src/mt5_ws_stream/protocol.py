"""Binary wire protocol shared by every feeder and the bridge.

The MQL5 Expert Advisor (``mql5/Experts/TickStreamer/TickStreamer.mq5``) and every
Python feeder emit the *same* fixed-size little-endian record, so the bridge never
has to scan for delimiters or run a parser on the hot path -- one
:func:`struct.Struct.unpack_from` per tick is the whole decode step.

Record layout (64 bytes, little-endian, no padding)::

    offset  size  type      field
    ------  ----  --------  --------------------------------------------------
    0       2     uint16    magic        0x4B54 ('TK')
    2       2     uint16    record_size  64
    4       4     uint32    seq          per-feeder counter, wraps at 2**32
    8       12    char[12]  symbol       ASCII, NUL-padded (NOT NUL-terminated)
    20      8     int64     time_msc     UTC milliseconds (the EA normalises
                                         MqlTick.time_msc, which is broker
                                         server time, before sending)
    28      8     float64   bid
    36      8     float64   ask
    44      8     float64   last
    52      8     float64   volume_real
    60      4     uint32    flags        MqlTick.flags | FLAG_HEARTBEAT
    ------  ----  --------  --------------------------------------------------
    64                      total

This module is the binary half of the wire. The other half -- the JSON frame
grammar between bridge and consumer (`docs/protocol.md §2`) -- has its own one
home in :mod:`mt5_ws_stream.frames`, which builds on the :class:`Tick` defined
here.

Compatibility rules for anyone extending this:

* ``magic`` and ``record_size`` are the first four bytes on purpose -- a reader can
  reject a mismatched peer immediately instead of silently decoding garbage.
* Never change a field's offset or width. Add new fields by defining a new
  ``record_size`` and branching on it.
"""

from __future__ import annotations

import enum
import json
import math
import struct
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Final, NamedTuple

__all__ = [
    "FLAG_HEARTBEAT",
    "MAGIC",
    "RECORD_SIZE",
    "SYMBOL_FIELD_SIZE",
    "BackpressurePolicy",
    "PayloadFormat",
    "ProtocolError",
    "Tick",
    "compact_json",
    "decode_records",
    "iter_ticks",
    "pack_tick",
    "percentile",
    "split_symbols",
    "unpack_tick",
]

#: Magic number at the start of every record -- ASCII ``TK`` in little-endian order.
MAGIC: Final[int] = 0x4B54

#: Bit set in :attr:`Tick.flags` to mark a keep-alive record carrying no quote.
FLAG_HEARTBEAT: Final[int] = 0x8000_0000

#: Width of the fixed-size symbol field, in bytes.
SYMBOL_FIELD_SIZE: Final[int] = 12

_RECORD = struct.Struct("<HHI12sqddddI")

#: Size of one wire record, in bytes.
RECORD_SIZE: Final[int] = _RECORD.size

_UINT32_MASK: Final[int] = 0xFFFF_FFFF

if RECORD_SIZE != 64:  # pragma: no cover - a plain ``assert`` vanishes under ``python -O``
    raise RuntimeError("wire format changed without bumping RECORD_SIZE")

#: Decoded symbol per raw 12-byte field, so the same instrument is turned into a
#: ``str`` once instead of once per record. A feeder sends a handful of symbols
#: for the life of a connection, so the hit rate is effectively 100%; the cap
#: exists only so a peer that invents a new symbol every record cannot grow this
#: without bound. Clearing wholesale rather than evicting one entry keeps the
#: lookup on the hot path to a single ``dict.get``.
_SYMBOLS: dict[bytes, str] = {}

#: The same idea one step further along: a symbol as the JSON *string literal*
#: it becomes in a ``ticks`` frame, escaping included, so :meth:`Tick.as_json`
#: escapes each instrument once rather than once per quote.
_SYMBOL_LITERALS: dict[str, str] = {}

_SYMBOL_CACHE_CAP: Final[int] = 1024

#: One compact encoder for the whole process. ``json.dumps(obj, separators=...)``
#: builds a fresh :class:`~json.JSONEncoder` on every call, which is real time
#: when the call is per frame.
compact_json = json.JSONEncoder(separators=(",", ":")).encode

_isfinite = math.isfinite


class ProtocolError(ValueError):
    """Raised when a byte range cannot be decoded as a wire record."""


class Tick(NamedTuple):
    """One decoded quote.

    A :class:`~typing.NamedTuple` rather than a dataclass: construction is on the
    bridge's hot path, and tuple allocation is measurably cheaper.
    """

    symbol: str
    """Instrument name as reported by the terminal, e.g. ``"EURUSD"``."""

    time_msc: int
    """Broker *server* time in milliseconds since the Unix epoch (UTC).

    This is the broker's clock, not the local one. Comparing it against
    :func:`time.time` measures broker latency *plus* whatever skew exists between
    the two clocks -- useful as a trend, not as an absolute.
    """

    bid: float
    ask: float
    last: float
    volume: float

    flags: int
    """``MqlTick.flags`` from the terminal, OR-ed with :data:`FLAG_HEARTBEAT`."""

    seq: int
    """Per-feeder counter. A gap means the feeder dropped records."""

    @property
    def is_heartbeat(self) -> bool:
        """``True`` if this record is a keep-alive rather than a quote."""
        return bool(self.flags & FLAG_HEARTBEAT)

    @property
    def spread(self) -> float:
        """``ask - bid``. Meaningless on heartbeat records."""
        return self.ask - self.bid

    def as_dict(self) -> dict[str, float | int | str]:
        """Compact JSON representation used by the WebSocket ``json`` format.

        Keys are deliberately short -- at a few thousand ticks per second the
        difference between ``"symbol"`` and ``"s"`` is real bandwidth.

        ``"f"`` carries the *full* :attr:`flags` value, including
        :data:`FLAG_HEARTBEAT`, so JSON consumers can tell a keep-alive from a
        quote exactly like binary consumers do (see :attr:`is_heartbeat`).
        """
        return {
            "s": self.symbol,
            "ms": self.time_msc,
            "b": self.bid,
            "a": self.ask,
            "l": self.last,
            "v": self.volume,
            "f": self.flags,
            "q": self.seq,
        }

    def as_json(self) -> str:
        """This tick as the exact JSON text :meth:`as_dict` would encode to.

        The bridge's hot path: a ``ticks`` frame is a few of these joined by
        commas, and building the dict first is pure allocation -- eight keys and
        a hash table per quote, thousands of times a second -- for a shape that
        never varies. Spelling the object directly skips it.

        The two spellings sit next to each other because they must not drift: a
        renamed key belongs in both, ``tests/test_protocol.py`` asserts they are
        byte-identical over randomised ticks, and ``tests/test_frames.py``'s
        reference vectors pin the result as text.

        Non-finite prices fall back to :data:`compact_json`, because JSON spells
        them ``Infinity`` / ``NaN`` where :func:`repr` says ``inf`` / ``nan`` --
        and a ``float64`` off the wire can be either.
        """
        bid, ask, last, volume = self.bid, self.ask, self.last, self.volume
        if _isfinite(bid) and _isfinite(ask) and _isfinite(last) and _isfinite(volume):
            return (
                f'{{"s":{_symbol_literal(self.symbol)},"ms":{self.time_msc},'
                f'"b":{bid!r},"a":{ask!r},"l":{last!r},"v":{volume!r},'
                f'"f":{self.flags},"q":{self.seq}}}'
            )
        return compact_json(self.as_dict())

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> Tick:
        """Inverse of :meth:`as_dict`.

        Used to decode the WebSocket ``json`` format's tick objects. Missing
        keys default to the falsy value for their type -- ``""`` for the
        symbol, ``0``/``0.0`` for everything else -- so a partial payload
        decodes instead of raising.
        """
        return cls(
            symbol=str(d.get("s", "")),
            time_msc=int(d.get("ms", 0)),
            bid=float(d.get("b", 0.0)),
            ask=float(d.get("a", 0.0)),
            last=float(d.get("l", 0.0)),
            volume=float(d.get("v", 0.0)),
            flags=int(d.get("f", 0)),
            seq=int(d.get("q", 0)),
        )


def _symbol_literal(symbol: str) -> str:
    """*symbol* as a JSON string literal, escaped once per distinct symbol.

    Capped and cleared wholesale like :data:`_SYMBOLS`, for the same reason:
    the working set is a handful of instruments, and a peer that invents one
    per record must not be able to grow this.
    """
    literal = _SYMBOL_LITERALS.get(symbol)
    if literal is None:
        literal = compact_json(symbol)
        if len(_SYMBOL_LITERALS) >= _SYMBOL_CACHE_CAP:
            _SYMBOL_LITERALS.clear()
        _SYMBOL_LITERALS[symbol] = literal
    return literal


def pack_tick(tick: Tick) -> bytes:
    """Encode *tick* as a single :data:`RECORD_SIZE`-byte record.

    Symbols longer than :data:`SYMBOL_FIELD_SIZE` bytes are truncated, matching
    what the MQL5 side does.

    Raises:
        ProtocolError: if the symbol is not encodable as ASCII.
    """
    try:
        symbol = tick.symbol.encode("ascii")[:SYMBOL_FIELD_SIZE]
    except UnicodeEncodeError as exc:  # pragma: no cover - defensive
        raise ProtocolError(f"symbol must be ASCII: {tick.symbol!r}") from exc
    return _RECORD.pack(
        MAGIC,
        RECORD_SIZE,
        tick.seq & _UINT32_MASK,
        symbol,
        tick.time_msc,
        tick.bid,
        tick.ask,
        tick.last,
        tick.volume,
        tick.flags & _UINT32_MASK,
    )


def unpack_tick(buffer: bytes | bytearray | memoryview, offset: int = 0) -> Tick:
    """Decode the record starting at *offset*.

    The symbol is looked up in :data:`_SYMBOLS` rather than re-split and
    re-decoded per record: it is the only variable-length field in a fixed-size
    layout, and it repeats.

    Raises:
        ProtocolError: if the header does not match this protocol version, or the
            buffer is too short.
    """
    try:
        magic, size, seq, symbol, time_msc, bid, ask, last, volume, flags = _RECORD.unpack_from(
            buffer, offset
        )
    except struct.error as exc:
        raise ProtocolError(f"truncated record at offset {offset}") from exc

    if magic != MAGIC or size != RECORD_SIZE:
        raise ProtocolError(
            f"bad header at offset {offset}: magic=0x{magic:04x} size={size} "
            f"(expected magic=0x{MAGIC:04x} size={RECORD_SIZE})"
        )

    name = _SYMBOLS.get(symbol)
    if name is None:
        name = symbol.split(b"\x00", 1)[0].decode("ascii", "replace")
        if len(_SYMBOLS) >= _SYMBOL_CACHE_CAP:
            _SYMBOLS.clear()
        _SYMBOLS[symbol] = name

    return Tick(
        symbol=name,
        time_msc=time_msc,
        bid=bid,
        ask=ask,
        last=last,
        volume=volume,
        flags=flags,
        seq=seq,
    )


def decode_records(buffer: bytes | bytearray | memoryview) -> list[tuple[Tick, bytes]]:
    """Decode every whole record in *buffer*, each with the bytes it came from.

    The one loop over this format. Both halves of a record are wanted at once on
    the bridge's hot path -- the :class:`Tick` for JSON subscribers and the exact
    :data:`RECORD_SIZE` bytes for binary ones -- and slicing here costs less than
    the offset bookkeeping a caller would otherwise keep alongside this loop.

    A list rather than a generator, for two reasons that point the same way: the
    bridge has to know the *whole* buffer decodes before it publishes any of it
    (a published tick cannot be taken back), and a comprehension is measurably
    the cheapest way to walk a fixed-size format.

    Any trailing partial record is ignored -- callers reading from a stream are
    expected to keep it and prepend it to the next chunk. In this process that
    caller is :class:`~mt5_ws_stream.hub.FeederLink`.

    Raises:
        ProtocolError: on the first record whose header does not match.
    """
    # Normalised once rather than per record: slicing `bytes` already yields
    # `bytes`, so the per-record `bytes(...)` this used to do was a C call per
    # record buying nothing on the only input the bridge ever passes.
    data = buffer if type(buffer) is bytes else bytes(buffer)
    return [
        (unpack_tick(data, offset), data[offset : offset + RECORD_SIZE])
        for offset in range(0, len(data) - RECORD_SIZE + 1, RECORD_SIZE)
    ]


def iter_ticks(buffer: bytes | bytearray | memoryview) -> Iterator[Tick]:
    """Decode every whole record in *buffer*, values only.

    For consumers that have no use for the bytes -- the client-side frame
    decoder. Same framing rules as :func:`decode_records`, because it is the
    same loop.
    """
    return (tick for tick, _raw in decode_records(buffer))


def percentile(sorted_samples: Sequence[float], p: float) -> float | None:
    """Nearest-rank percentile of an *already sorted* sequence.

    Latency percentiles are computed in three places (the hub's stats, the CLI's
    benchmark summary, the dashboard) and every one of them wants the same cheap,
    dependency-free answer: index ``len * p``, clamped to the last element. No
    interpolation -- with the sample counts involved it would move the number by
    less than the measurement noise, and an exact observed value is easier to
    reason about when chasing a latency spike.

    The caller sorts, because callers typically want several percentiles from one
    sample set and sorting once is the whole point.

    Args:
        sorted_samples: Samples in ascending order. Not verified.
        p: Fraction in ``[0, 1]`` -- ``0.5`` for the median.

    Returns:
        The selected sample, or ``None`` when there are no samples. ``None`` and
        ``0.0`` mean different things here (*nothing measured* vs *measured zero*),
        so this deliberately does not collapse them.
    """
    if not sorted_samples:
        return None
    return sorted_samples[min(len(sorted_samples) - 1, int(len(sorted_samples) * p))]


class PayloadFormat(enum.StrEnum):
    """How a subscriber wants its ticks encoded.

    Shared vocabulary between client and server: the client sets it on the
    subscription request, the hub reads it to pick an encoding per subscriber.
    """

    JSON = "json"
    """UTF-8 JSON. Cheap to consume in a browser."""

    BINARY = "binary"
    """Raw wire records, concatenated. 5-10x cheaper to decode off-browser."""

    @classmethod
    def parse(cls, value: str | None, default: PayloadFormat | None = None) -> PayloadFormat:
        """Read a consumer's spelling of a format, leniently.

        One grammar for the two places a consumer names one: the ``format``
        query parameter at connect time and the ``format`` control op
        mid-stream. Anything starting with ``b`` means binary; anything else
        named -- including a typo -- means JSON, because a consumer that
        silently receives a format it cannot decode is worse off than one whose
        typo left it with the readable default. *default* covers the other
        case: nothing named at all, where the answer is "whatever you have".
        """
        if not value:
            return default if default is not None else cls.JSON
        return cls.BINARY if value.lower().startswith("b") else cls.JSON


class BackpressurePolicy(enum.StrEnum):
    """What to do when a subscriber cannot keep up.

    Shared vocabulary between client and server: the client requests a policy,
    the hub enforces it per subscriber.
    """

    LOSSLESS = "lossless"
    """Queue up to ``queue_limit`` ticks, then drop the oldest half.

    Right for consumers that must see every tick -- recorders, strategy engines.
    """

    CONFLATE = "conflate"
    """Keep only the newest tick per symbol.

    Right for consumers that only ever render the latest value -- dashboards,
    price tickers. Memory is bounded by the symbol count regardless of rate, and
    a slow consumer sees *current* prices rather than a backlog of stale ones.
    """


def split_symbols(text: str) -> list[str]:
    """Split a comma-separated symbol list into stripped, non-empty entries.

    Shared by the WS query string, the REST ``symbols`` filter and the CLI's
    ``--symbols`` flag -- everywhere a human types a comma-separated list.
    """
    return [item.strip() for item in text.split(",") if item.strip()]
