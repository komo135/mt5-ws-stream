"""Fan-out core: decode feeder bytes once, deliver to every subscriber.

The hub is deliberately free of any WebSocket types, and (ADR-0002) of any
tasks: it owns *delivery policy* -- who gets which tick, and what happens to a
consumer that falls behind -- and nothing else. :meth:`Hub.publish` appends to a
:class:`Subscriber`'s queue and sets its event; the awaiting, encoding and
sending are a :class:`~mt5_ws_stream.session.Session`'s job.

Two design decisions carry most of the latency behaviour:

**Drain-then-send, never a batch timer.** A session's writer loop wakes, takes
*everything* queued, and sends it as one frame. At low tick rates that is one
tick per frame with zero added delay; at high rates frames naturally coalesce. A
fixed batching interval would do the opposite -- it adds latency exactly when
there is nothing to gain.

**Slow subscribers degrade, they do not block.** :meth:`Hub.publish` never awaits
and never applies backpressure upstream, so one stalled browser cannot delay the
feeder or any other subscriber. What a stalled subscriber gets instead is chosen
by its :class:`BackpressurePolicy`.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from .protocol import (
    FLAG_HEARTBEAT,
    RECORD_SIZE,
    BackpressurePolicy,
    PayloadFormat,
    Tick,
    decode_records,
    percentile,
)

__all__ = [
    "FeederLink",
    "Hub",
    "HubStats",
    "Sink",
    "Subscriber",
    "SubscriptionOptions",
    "SymbolSnapshot",
]

log = logging.getLogger("mt5_ws_stream.hub")

_LATENCY_SAMPLE_CAP = 20_000


class Sink(Protocol):
    """Minimal transport a subscription's frames are written to.

    The one outward seam of the delivery path: ``RecordingSink`` in the tests
    and ``_WebSocketSink`` in :mod:`mt5_ws_stream.api` are its adapters, which
    is what lets the fan-out be exercised without a network. Defined here
    rather than next to the writer loop because "what a delivered frame goes
    to" belongs with the delivery policy it serves.
    """

    async def send(self, payload: str | bytes) -> None:  # pragma: no cover - protocol
        ...


@dataclass(slots=True)
class SubscriptionOptions:
    """Per-subscriber delivery settings."""

    symbols: frozenset[str] | None = None
    """Symbols to deliver, or ``None`` for everything."""

    payload_format: PayloadFormat = PayloadFormat.JSON
    backpressure: BackpressurePolicy = BackpressurePolicy.LOSSLESS
    include_heartbeats: bool = False
    """Heartbeats are link-liveness records; most consumers do not want them."""


@dataclass(slots=True)
class FeederLink:
    """One feeder connection's ingest state, owned by whoever reads the socket.

    A TCP read boundary has nothing to do with a record boundary, so *where a
    record starts* is this object's question to answer: it holds the bytes of a
    record that has not arrived in full yet, and hands :meth:`Hub.feed` the
    whole records the newest chunk completed. That is what lets the reader
    (:meth:`mt5_ws_stream.bridge.Bridge._handle_feeder`) be a loop over
    ``hub.feed(chunk, link)`` with no index arithmetic in it, and what lets
    packet-boundary behaviour be tested with byte strings instead of a socket.

    It also owns the continuity facts that only make sense per connection --
    the sequence counter, the counters ``/api/v1/feeders`` reports, the symbols
    this feeder has sent -- because those are mutated by the same records.
    """

    name: str = "feeder"
    connected_at: float = 0.0
    """Unix seconds when the connection was accepted; ``0`` if never set."""

    last_seq: int | None = None
    last_record_at: float = 0.0
    ticks: int = 0
    heartbeats: int = 0
    seq_gaps: int = 0
    symbols: set[str] = field(default_factory=set)
    """Symbols seen from this feeder connection, for the "new symbol" log."""

    _tail: bytearray = field(default_factory=bytearray, init=False, repr=False)

    @property
    def pending_bytes(self) -> int:
        """Bytes of a partial record held back for the next chunk.

        Always less than :data:`~mt5_ws_stream.protocol.RECORD_SIZE`: a record
        stops being pending the moment it is complete.
        """
        return len(self._tail)

    def take_records(self, chunk: bytes) -> bytes:
        """Append *chunk* to this link's stream and return its whole records.

        The returned bytes are a whole number of records, ready to decode; a
        trailing partial record stays here and is prepended to the next chunk.
        Returns ``b""`` when *chunk* did not complete one.

        The common case -- a chunk that is already a whole number of records,
        with nothing held over -- returns the caller's own bytes without
        copying them.
        """
        tail = self._tail
        if not tail:
            extra = len(chunk) % RECORD_SIZE
            if not extra:
                return chunk
            whole = len(chunk) - extra
            tail += chunk[whole:]
            return chunk[:whole]

        tail += chunk
        usable = len(tail) - len(tail) % RECORD_SIZE
        if not usable:
            return b""
        records = bytes(tail[:usable])
        del tail[:usable]
        return records

    def account(self, tick: Tick, now: float) -> bool:
        """Fold one decoded record into this link's counters.

        Lives here rather than in :meth:`Hub.feed` because every value it
        touches is per-connection: sequence continuity is only defined within
        one feeder's stream, and two feeders' counters must not interfere.

        Returns:
            ``True`` if *tick* did not continue the sequence -- the feeder
            dropped records between the previous one and this one. Wraparound
            past ``2**32`` is continuity, not a gap.
        """
        gap = self.last_seq is not None and tick.seq != (self.last_seq + 1) & 0xFFFF_FFFF
        if gap:
            self.seq_gaps += 1
        self.last_seq = tick.seq
        self.last_record_at = now

        if tick.is_heartbeat:
            self.heartbeats += 1
        else:
            self.ticks += 1
            if tick.symbol not in self.symbols:
                self.symbols.add(tick.symbol)
                log.info(
                    "feeder %s: new symbol %s (%d so far)",
                    self.name,
                    tick.symbol,
                    len(self.symbols),
                )
        return gap


@dataclass(slots=True)
class SymbolSnapshot:
    """Latest quote for one symbol plus the hub's per-symbol bookkeeping.

    A domain value, not a wire shape: how it is rendered for HTTP belongs to
    :meth:`mt5_ws_stream.api.SymbolResponse.from_snapshot`, so adding a field
    here is a two-line change there and nowhere else.
    """

    tick: Tick
    received_at: float
    """Unix seconds when the bridge decoded this tick."""

    ticks: int
    """Quotes seen for this symbol since the hub started."""


@dataclass(slots=True)
class HubStats:
    """Point-in-time snapshot. ``ticks``, ``heartbeats``, ``seq_gaps`` and
    ``dropped`` are cumulative; ``tick_rate`` and the latency percentiles cover
    the interval since the previous :meth:`Hub.consume_interval`.

    Like :class:`SymbolSnapshot` this is a domain value; its JSON form (REST
    ``/stats`` and the WebSocket ``stats`` frame alike) is defined once by
    :func:`mt5_ws_stream.frames.stats_payload`.
    """

    uptime_s: float
    ticks: int
    tick_rate: float
    subscribers: int
    symbols: list[str]
    seq_gaps: int
    heartbeats: int
    dropped: int
    broker_lag_ms_p50: float | None
    broker_lag_ms_p99: float | None


class Subscriber:
    """One consumer's queue and backpressure policy.

    Created by :meth:`Hub.subscribe`; not meant to be instantiated directly.
    Everything past the queue -- the transport, the encoding, the loop that
    drains it -- belongs to the :class:`~mt5_ws_stream.session.Session` that
    owns this subscriber.
    """

    __slots__ = (
        "_closed",
        "_conflated",
        "_id",
        "_options",
        "_pending",
        "_queue_limit",
        "_wakeup",
        "dropped",
    )

    def __init__(
        self,
        subscriber_id: int,
        options: SubscriptionOptions,
        queue_limit: int,
    ) -> None:
        self._id = subscriber_id
        self._options = options
        self._queue_limit = queue_limit
        self._pending: list[tuple[Tick, bytes]] = []
        self._conflated: dict[str, tuple[Tick, bytes]] = {}
        # asyncio.Event() binds to no loop until it is awaited, which is what
        # lets Hub.subscribe() be called from synchronous code.
        self._wakeup = asyncio.Event()
        self._closed = False
        self.dropped = 0

    @property
    def id(self) -> int:
        return self._id

    @property
    def options(self) -> SubscriptionOptions:
        return self._options

    @property
    def closed(self) -> bool:
        """``True`` once :meth:`close` ran; a closed queue takes no more ticks."""
        return self._closed

    @property
    def pending_count(self) -> int:
        """How many items are waiting for the writer loop to pick them up.

        The public value for observing the backpressure policy's memory bound:
        under :attr:`~mt5_ws_stream.protocol.BackpressurePolicy.CONFLATE` it is
        the number of *symbols* being held (bounded by the symbol count, however
        fast the feeder runs); under ``LOSSLESS`` it is the queue length
        (bounded by ``queue_limit``, past which the oldest half is shed).
        """
        if self._options.backpressure is BackpressurePolicy.CONFLATE:
            return len(self._conflated)
        return len(self._pending)

    def update_options(self, options: SubscriptionOptions) -> None:
        self._options = options

    def offer(self, items: Sequence[tuple[Tick, bytes]]) -> None:
        """Enqueue a batch of ticks, already filtered for this subscription.

        Called on the hot path -- never blocks, never awaits. A batch rather
        than a tick because everything here is per-call rather than per-item:
        one policy branch, one queue-limit test and one wakeup for the whole
        batch, which is what keeps the fan-out cost flat as the tick rate rises.
        """
        if self._closed or not items:
            return
        if self._options.backpressure is BackpressurePolicy.CONFLATE:
            conflated = self._conflated
            for item in items:
                conflated[item[0].symbol] = item
        else:
            pending = self._pending
            pending.extend(items)
            excess = len(pending) - self._queue_limit
            if excess > 0:
                # Shed the oldest half rather than exactly the overflow: shedding
                # the minimum leaves the queue permanently full and turns every
                # subsequent batch into a drop, which reads as "everything is broken".
                shed = max(excess, len(pending) // 2)
                del pending[:shed]
                self.dropped += shed
        self._wakeup.set()

    def drain(self) -> Sequence[tuple[Tick, bytes]]:
        """Take *everything* queued, leaving the queue empty.

        Drain-then-send is the whole batching strategy: one call per wakeup,
        one frame per call. See the module docstring for why there is no timer.
        """
        if self._options.backpressure is BackpressurePolicy.CONFLATE:
            items = list(self._conflated.values())
            self._conflated.clear()
            return items
        items = self._pending
        self._pending = []
        return items

    async def wait(self) -> bool:
        """Block until something is queued, or the queue closes.

        Returns ``False`` once closed -- which is how a session's writer loop
        ends without anyone cancelling a task.
        """
        if self._closed:
            return False
        await self._wakeup.wait()
        self._wakeup.clear()
        return not self._closed

    def close(self) -> None:
        """Close the queue and wake whoever is waiting on it. Idempotent."""
        self._closed = True
        self._pending = []
        self._conflated = {}
        self._wakeup.set()


class Hub:
    """Decodes feeder bytes once and fans the result out to all subscribers."""

    def __init__(
        self, *, queue_limit: int = 20_000, latency_sample_cap: int = _LATENCY_SAMPLE_CAP
    ) -> None:
        if queue_limit < 1:
            raise ValueError("queue_limit must be >= 1")
        if latency_sample_cap < 1:
            raise ValueError("latency_sample_cap must be >= 1")
        self._queue_limit = queue_limit
        self._subscribers: set[Subscriber] = set()
        self._latest: dict[str, Tick] = {}
        # Per-symbol bookkeeping for the REST API. Two extra dict writes per tick
        # on the hot path -- measured as noise next to the struct decode, and the
        # alternative (deriving counts on read) cannot recover them at all.
        self._symbol_ticks: dict[str, int] = {}
        self._symbol_received_at: dict[str, float] = {}
        self._next_id = 0
        self._started_at = time.monotonic()

        self._ticks = 0
        self._heartbeats = 0
        self._seq_gaps = 0
        # Drops belonging to subscribers that have since gone. A subscriber
        # owns its own `dropped` count, so summing the live set alone would
        # make a cumulative total *fall* every time a consumer disconnected --
        # which reads as "the shedding un-happened" and breaks any rate derived
        # from it. Folded in here at unsubscribe, when the count is final.
        self._dropped_gone = 0
        # Bounded: the cap evicts the oldest sample rather than refusing the
        # newest, so percentiles keep tracking the present even when nothing ever
        # calls consume_interval (stats_interval_s=0).
        self._lag_samples: deque[float] = deque(maxlen=latency_sample_cap)
        self._stats_marker = (time.monotonic(), 0)

    # -- subscribers -----------------------------------------------------

    @property
    def subscribers(self) -> frozenset[Subscriber]:
        return frozenset(self._subscribers)

    @property
    def symbols(self) -> list[str]:
        """Every symbol seen since start, sorted."""
        return sorted(self._latest)

    def latest(self, symbols: Iterable[str] | None = None) -> list[Tick]:
        """Most recent tick per symbol, for connect-time snapshots."""
        if symbols is None:
            return list(self._latest.values())
        wanted = set(symbols)
        return [t for s, t in self._latest.items() if s in wanted]

    def snapshot_symbol(self, symbol: str) -> SymbolSnapshot | None:
        """Latest quote plus counters for one symbol, or ``None`` if unseen."""
        tick = self._latest.get(symbol)
        if tick is None:
            return None
        return SymbolSnapshot(
            tick=tick,
            received_at=self._symbol_received_at.get(symbol, 0.0),
            ticks=self._symbol_ticks.get(symbol, 0),
        )

    def snapshot_symbols(self, symbols: Iterable[str] | None = None) -> list[SymbolSnapshot]:
        """Latest quote plus counters for every symbol seen, sorted by name."""
        wanted = None if symbols is None else set(symbols)
        out = []
        for symbol in sorted(self._latest):
            if wanted is not None and symbol not in wanted:
                continue
            snapshot = self.snapshot_symbol(symbol)
            if snapshot is not None:
                out.append(snapshot)
        return out

    def subscribe(self, options: SubscriptionOptions | None = None) -> Subscriber:
        """Open a queue on this hub. No task, no loop, no sink.

        Safe to call from synchronous code: what the returned queue is drained
        into, and by whom, is the caller's business -- in this process, a
        :class:`~mt5_ws_stream.session.Session`.
        """
        self._next_id += 1
        subscriber = Subscriber(
            self._next_id, options or SubscriptionOptions(), self._queue_limit
        )
        self._subscribers.add(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: Subscriber) -> None:
        """Stop delivering to *subscriber* and close its queue. Idempotent.

        The subscriber's drop count is folded into the hub's total on the way
        out -- once, guarded by membership, because this is idempotent and a
        second call must not count it twice.
        """
        if subscriber in self._subscribers:
            self._subscribers.discard(subscriber)
            self._dropped_gone += subscriber.dropped
        subscriber.close()

    async def aclose(self) -> None:
        """Close every queue and forget every subscriber.

        Sessions end on their own once their queue is closed -- there is no
        task here to cancel and no teardown order to get wrong. The yield gives
        any writer loop already scheduled a chance to notice before the caller
        moves on.
        """
        for subscriber in list(self._subscribers):
            self.unsubscribe(subscriber)
        await asyncio.sleep(0)

    # -- ingest ----------------------------------------------------------

    def feed(self, chunk: bytes, link: FeederLink) -> int:
        """Ingest one chunk of feeder bytes: frame it, decode it, publish it.

        *chunk* is whatever came off the socket -- any length, including one
        that cuts a record in half or does not complete a single one.
        :meth:`FeederLink.take_records` holds the remainder until the next
        call, so there is no length rule for a caller to honour.

        The records a chunk completes are one **batch**, and a batch is
        all-or-nothing: it is decoded in full before anything is published, so
        a bad record aborts the batch with no tick delivered and no counter
        moved -- rather than leaving subscribers with the half that decoded
        before the failure.

        Args:
            chunk: Bytes as read from the feeder's socket.
            link: The connection's :class:`FeederLink`, mutated in place.

        Returns:
            Number of records decoded and published from this chunk.

        Raises:
            ProtocolError: on a header mismatch. The caller should drop the
                connection: a bad header means the peer speaks a different
                protocol, and there is no safe resync point in a fixed-size stream.
        """
        data = link.take_records(chunk)
        if not data:
            return 0
        # Decoded in full first: this is where a bad record raises, and it has
        # to raise before the first publish for the batch to be atomic.
        records = decode_records(data)

        now = time.time()
        now_ms = now * 1000.0
        latest = self._latest
        symbol_ticks = self._symbol_ticks
        symbol_received_at = self._symbol_received_at
        lag_samples = self._lag_samples
        heartbeats = 0

        for tick, _raw in records:
            # The per-connection half of the accounting belongs to the link;
            # what comes back is whether this record broke its sequence, which
            # the hub mirrors into its own total.
            if link.account(tick, now):
                self._seq_gaps += 1

            if tick.flags & FLAG_HEARTBEAT:
                heartbeats += 1
            else:
                symbol = tick.symbol
                latest[symbol] = tick
                symbol_ticks[symbol] = symbol_ticks.get(symbol, 0) + 1
                symbol_received_at[symbol] = now
                lag_samples.append(now_ms - tick.time_msc)

        self._heartbeats += heartbeats
        self._ticks += len(records) - heartbeats
        self.publish(records, heartbeats=heartbeats > 0)

        return len(records)

    def publish(
        self, records: Sequence[tuple[Tick, bytes]], *, heartbeats: bool = True
    ) -> None:
        """Offer one decoded batch to every interested subscriber.

        A batch rather than a tick because the filtering is per *subscription*
        and not per record: a subscriber's symbol set and heartbeat flag are
        read once here and the whole batch is handed to
        :meth:`Subscriber.offer` in one call. The default subscription -- every
        symbol, no heartbeats -- then costs nothing per record at all: it gets
        the caller's own list.

        Args:
            records: The batch, as :func:`~mt5_ws_stream.protocol.decode_records`
                returned it.
            heartbeats: Whether *records* contains any heartbeat record. The
                caller usually knows -- :meth:`feed` counted them -- and the
                answer is usually ``False``, since a heartbeat arrives once every
                few seconds against thousands of quotes. ``True`` is the safe
                default: it only costs a scan.
        """
        if not records:
            return

        quotes = records
        if heartbeats:
            quotes = [item for item in records if not item[0].flags & FLAG_HEARTBEAT]

        for subscriber in self._subscribers:
            options = subscriber.options
            symbols = options.symbols
            # A heartbeat is link liveness rather than a quote about an
            # instrument, so it bypasses the symbol filter: a consumer that
            # asked for heartbeats wants them whatever it is subscribed to.
            if options.include_heartbeats:
                wanted = (
                    records
                    if symbols is None
                    else [
                        item
                        for item in records
                        if item[0].symbol in symbols or item[0].flags & FLAG_HEARTBEAT
                    ]
                )
            elif symbols is None:
                wanted = quotes
            else:
                wanted = [item for item in quotes if item[0].symbol in symbols]
            subscriber.offer(wanted)

    # -- stats -----------------------------------------------------------

    def snapshot_stats(self) -> HubStats:
        """Read the current stats without disturbing them.

        Safe to call from anywhere and as often as you like -- the REST endpoint,
        a health probe, a WebSocket ``stats`` frame. Reading is not consuming:
        :meth:`consume_interval` is the one call that closes the interval, and it
        belongs to the periodic reporter alone. A getter that silently stole the
        samples out from under every other observer is exactly the bug this
        split exists to prevent.

        The latency samples live in a ring buffer of ``_LATENCY_SAMPLE_CAP``
        entries, so on a long-running bridge with the periodic report disabled
        (``stats_interval_s=0``) the percentiles describe the most recent samples
        rather than freezing on the first ones ever seen.
        """
        return self._stats(time.monotonic())

    def consume_interval(self) -> HubStats:
        """Return the stats *and* close the interval: the rate marker moves to
        now and the latency samples are cleared, so the next interval measures
        the window that starts here.

        Exactly one caller is intended -- the bridge's periodic stats report. Any
        second consumer would halve both their windows without either noticing.
        """
        now = time.monotonic()
        stats = self._stats(now)
        self._stats_marker = (now, self._ticks)
        self._lag_samples.clear()
        return stats

    def _stats(self, now: float) -> HubStats:
        last_at, last_ticks = self._stats_marker
        elapsed = max(now - last_at, 1e-9)
        samples = sorted(self._lag_samples)

        def lag_pct(p: float) -> float | None:
            value = percentile(samples, p)
            return None if value is None else round(value, 2)

        return HubStats(
            uptime_s=now - self._started_at,
            ticks=self._ticks,
            tick_rate=(self._ticks - last_ticks) / elapsed,
            subscribers=len(self._subscribers),
            symbols=self.symbols,
            seq_gaps=self._seq_gaps,
            heartbeats=self._heartbeats,
            dropped=self._dropped_gone + sum(s.dropped for s in self._subscribers),
            broker_lag_ms_p50=lag_pct(0.50),
            broker_lag_ms_p99=lag_pct(0.99),
        )
