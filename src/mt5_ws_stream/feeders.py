"""Feeders: processes that push wire records into a bridge's TCP port.

The live feeder is the MQL5 Expert Advisor in ``mql5/``: it captures ticks in
``OnTick()`` and writes them straight to the bridge's TCP port. One feeder is
bundled here, in Python:

:class:`MockFeeder`
    Synthesises a random walk. Lets the whole pipeline -- bridge, dashboard,
    clients, CI -- be exercised on any OS with no MetaTrader 5 installed.

Anything that can open a TCP socket can be a feeder. The contract is only:
connect, then write whole :data:`~mt5_ws_stream.protocol.RECORD_SIZE`-byte records.
"""

from __future__ import annotations

import logging
import random
import socket
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

from .protocol import FLAG_HEARTBEAT, Tick, pack_tick

__all__ = ["FeederConnection", "MockFeeder"]

log = logging.getLogger("mt5_ws_stream.feeders")

TICK_FLAG_BID = 0x02
TICK_FLAG_ASK = 0x04

#: How far behind schedule a paced loop may fall before it stops trying to
#: repay the debt and re-anchors on the current time.
_MAX_LAG_S = 0.5


class _RecordSink(Protocol):
    """Anything a feeder can hand a batch of records to.

    The only requirement is ``send`` -- this is what lets tests inject a
    plain in-memory recorder instead of :class:`FeederConnection`, with no
    subclassing and no socket.
    """

    def send(self, ticks: Sequence[Tick]) -> None: ...


class _RecordSequencer:
    """Builds ticks and hands out a dense, wrapping sequence number.

    Transport-free -- nothing here touches a socket. Quotes and heartbeats
    share one counter so the wire's ``seq`` stays dense across both.
    """

    def __init__(self, start_seq: int = 0) -> None:
        self._seq = start_seq & 0xFFFF_FFFF

    def next_seq(self) -> int:
        seq = self._seq
        self._seq = (self._seq + 1) & 0xFFFF_FFFF
        return seq

    def make_tick(
        self,
        symbol: str,
        bid: float,
        ask: float,
        *,
        last: float = 0.0,
        volume: float = 0.0,
        flags: int = TICK_FLAG_BID | TICK_FLAG_ASK,
        time_msc: int | None = None,
    ) -> Tick:
        return Tick(
            symbol=symbol,
            time_msc=time_msc if time_msc is not None else int(time.time() * 1000),
            bid=bid,
            ask=ask,
            last=last,
            volume=volume,
            flags=flags,
            seq=self.next_seq(),
        )

    def make_heartbeat(self) -> Tick:
        return Tick(
            symbol="",
            time_msc=int(time.time() * 1000),
            bid=0.0,
            ask=0.0,
            last=0.0,
            volume=0.0,
            flags=FLAG_HEARTBEAT,
            seq=self.next_seq(),
        )


class FeederConnection:
    """A TCP connection to a bridge, with a sequence counter and heartbeats.

    Reconnects are the caller's business -- for the mock feeder a
    crash-and-restart is fine, and the MQL5 EA has its own retry loop.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9800,
        *,
        timeout: float = 5.0,
        start_seq: int = 0,
    ):
        """Args:
        host: Bridge address to connect to.
        port: Bridge TCP feeder port.
        timeout: Connect timeout, in seconds.
        start_seq: First sequence number to hand out. Only useful for
            resuming a counter (and for exercising the 2**32 wrap).
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self._socket: socket.socket | None = None
        self._sequencer = _RecordSequencer(start_seq)

    def connect(self) -> None:
        self._socket = socket.create_connection((self.host, self.port), timeout=self.timeout)
        # Small records at high frequency: Nagle would batch them into up to
        # 40 ms of added latency for no bandwidth benefit on loopback.
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        log.info("feeder connected to %s:%d", self.host, self.port)

    def close(self) -> None:
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    def __enter__(self) -> FeederConnection:
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def next_seq(self) -> int:
        return self._sequencer.next_seq()

    def send(self, ticks: Sequence[Tick]) -> None:
        """Write records in a single ``sendall`` -- one syscall per batch."""
        if not ticks or self._socket is None:
            return
        self._socket.sendall(b"".join(pack_tick(t) for t in ticks))

    def make_tick(
        self,
        symbol: str,
        bid: float,
        ask: float,
        *,
        last: float = 0.0,
        volume: float = 0.0,
        flags: int = TICK_FLAG_BID | TICK_FLAG_ASK,
        time_msc: int | None = None,
    ) -> Tick:
        return self._sequencer.make_tick(
            symbol, bid, ask, last=last, volume=volume, flags=flags, time_msc=time_msc
        )

    def make_heartbeat(self) -> Tick:
        return self._sequencer.make_heartbeat()


_DEFAULT_PRICES: dict[str, tuple[float, float]] = {
    # symbol: (starting price, tick size)
    "EURUSD": (1.0850, 0.00001),
    "USDJPY": (157.25, 0.001),
    "GBPUSD": (1.2710, 0.00001),
    "AUDUSD": (0.6620, 0.00001),
    "XAUUSD": (2385.0, 0.01),
    "BTCUSD": (68_000.0, 0.5),
}


@dataclass(slots=True)
class MockFeeder:
    """Synthetic tick source: a bounded random walk at a target rate.

    Used by the test suite and by anyone who wants to see the dashboard move
    before touching a trading terminal.
    """

    host: str = "127.0.0.1"
    port: int = 9800
    symbols: Sequence[str] = field(default_factory=lambda: ["EURUSD", "USDJPY", "GBPUSD"])
    rate: float = 50.0
    """Target ticks per second, across all symbols combined."""

    batch: int = 1
    """Records per ``sendall``. Raise it for load tests; leave at 1 for realism."""

    duration: float = 0.0
    """Seconds to run; ``0`` means forever."""

    heartbeat_interval: float = 1.0
    seed: int | None = None

    def run(self, link: _RecordSink | None = None) -> int:
        """Feed until *duration* elapses or the process is interrupted.

        Each iteration builds a batch of quotes, appends a heartbeat if one
        is due, and hands the whole batch to *link* in one call (one
        syscall, when *link* is a real socket). The cadence is drift-free:
        each slot is measured from the previous slot, not from when the
        iteration happened to start, so the configured rate is met on
        average. It re-anchors on the current time only once more than
        :data:`_MAX_LAG_S` behind, instead of spinning at 100% CPU trying to
        repay the debt.

        Args:
            link: Where records go. Only needs a ``send(ticks)`` method; the
                caller owns it and it is never closed here. ``None`` opens
                (and closes) a :class:`FeederConnection` to ``host``/``port``.

        Returns:
            Number of quote records sent. Heartbeats are not counted.
        """
        rng = random.Random(self.seed)
        prices = {s: _DEFAULT_PRICES.get(s, (100.0, 0.01))[0] for s in self.symbols}
        steps = {s: _DEFAULT_PRICES.get(s, (100.0, 0.01))[1] for s in self.symbols}
        records = _RecordSequencer()
        started = time.perf_counter()

        owned: FeederConnection | None = None
        sink: _RecordSink
        if link is not None:
            sink = link
        else:
            owned = FeederConnection(self.host, self.port)
            owned.connect()
            sink = owned

        def produce() -> list[Tick]:
            batch: list[Tick] = []
            for _ in range(self.batch):
                symbol = rng.choice(list(self.symbols))
                step = steps[symbol]
                prices[symbol] += rng.choice((-1, 1)) * step * rng.randint(0, 3)
                bid = round(prices[symbol], 6)
                ask = round(bid + step * rng.randint(1, 4), 6)
                batch.append(records.make_tick(symbol, bid, ask))
            return batch

        sent = 0
        try:
            log.info("mock feeder: %.0f ticks/s across %s", self.rate, ",".join(self.symbols))
            interval_s = self.batch / self.rate if self.rate > 0 else 0.0
            next_send = started
            next_heartbeat = started + self.heartbeat_interval

            try:
                while True:
                    now = time.perf_counter()
                    if self.duration and now - started >= self.duration:
                        break

                    batch = produce()
                    sent += len(batch)

                    if now >= next_heartbeat:
                        next_heartbeat = now + self.heartbeat_interval
                        batch.append(records.make_heartbeat())

                    sink.send(batch)

                    next_send += interval_s
                    sleep_for = next_send - time.perf_counter()
                    if sleep_for > 0:
                        time.sleep(sleep_for)
                    elif sleep_for < -_MAX_LAG_S:
                        next_send = time.perf_counter()
            except KeyboardInterrupt:
                pass
        finally:
            if owned is not None:
                owned.close()

        elapsed = time.perf_counter() - started
        log.info(
            "mock feeder: sent %d ticks in %.2fs (%.0f/s)",
            sent,
            elapsed,
            sent / max(elapsed, 1e-9),
        )
        return sent
