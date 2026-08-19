"""Hub tests: framing, fan-out, filtering, backpressure and sequence accounting.

These exercise the delivery policy through a :class:`RecordingSink` and a
:class:`~mt5_ws_stream.session.Session`, so nothing here touches a socket. The
point is that *who gets what, and what happens when they fall behind* is
verifiable without a network -- and, since
:class:`~mt5_ws_stream.hub.FeederLink` owns the ingest buffer, so is *where a
record starts*: a badly split TCP packet is a byte string sliced at 7.

Nothing waits on a background task either (ADR-0002): the hub owns no tasks, so
a test feeds records and then steps the writer itself with ``session.flush()``.
"Nothing was delivered" and "everything was" are both assertions about state,
not about how long the loop was left to run.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from conftest import RecordingSink, blob, heartbeat, tick, wait_until
from mt5_ws_stream.hub import FeederLink, Hub, SubscriptionOptions
from mt5_ws_stream.protocol import (
    RECORD_SIZE,
    BackpressurePolicy,
    PayloadFormat,
    ProtocolError,
    Tick,
)
from mt5_ws_stream.session import Session


async def test_delivers_to_every_subscriber() -> None:
    hub = Hub()
    sinks = [RecordingSink() for _ in range(3)]
    sessions = [Session(hub, sink) for sink in sinks]

    hub.feed(blob(tick(seq=0), tick(seq=1)), FeederLink())
    for session in sessions:
        await session.flush()

    for sink in sinks:
        assert len(sink.ticks()) == 2
    await hub.aclose()


async def test_symbol_filter_excludes_others() -> None:
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink, SubscriptionOptions(symbols=frozenset({"EURUSD"})))

    hub.feed(blob(tick("EURUSD", seq=0), tick("USDJPY", seq=1)), FeederLink())
    await session.flush()

    assert [t["s"] for t in sink.ticks()] == ["EURUSD"]
    await hub.aclose()


async def test_heartbeats_are_suppressed_by_default() -> None:
    hub = Hub()
    default, opted_in = RecordingSink(), RecordingSink()
    plain = Session(hub, default)
    beats = Session(hub, opted_in, SubscriptionOptions(include_heartbeats=True))

    hub.feed(blob(heartbeat(seq=0)), FeederLink())
    await plain.flush()
    await beats.flush()

    assert default.frames == []
    assert len(opted_in.ticks()) == 1
    await hub.aclose()


async def test_heartbeats_ignore_the_symbol_filter() -> None:
    """A heartbeat reports the *link*, not an instrument, so a subscriber that
    asked for one gets it whatever its symbol filter says -- the record's own
    symbol (empty, on the wire) is not a subscription."""
    hub = Hub()
    sink = RecordingSink()
    session = Session(
        hub,
        sink,
        SubscriptionOptions(symbols=frozenset({"EURUSD"}), include_heartbeats=True),
    )

    hub.feed(blob(heartbeat(seq=0), tick("USDJPY", seq=1), tick("EURUSD", seq=2)), FeederLink())
    await session.flush()

    assert [t["s"] for t in sink.ticks()] == ["", "EURUSD"]
    await hub.aclose()


async def test_binary_subscribers_get_the_original_bytes() -> None:
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink, SubscriptionOptions(payload_format=PayloadFormat.BINARY))

    payload = blob(tick(seq=0), tick(seq=1))
    hub.feed(payload, FeederLink())
    await session.flush()

    assert b"".join(f for f in sink.frames if isinstance(f, bytes)) == payload
    await hub.aclose()


async def test_a_drain_batches_everything_queued_into_one_frame() -> None:
    """Drain-then-send, not one frame per tick: whatever piled up while the
    previous send was in flight leaves as a single frame."""
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)

    hub.feed(blob(*(tick(seq=i) for i in range(50))), FeederLink())
    await session.flush()

    assert len(sink.frames) == 1
    assert len(sink.ticks()) == 50
    await hub.aclose()


async def test_flushing_an_empty_queue_sends_nothing() -> None:
    """A wakeup with nothing behind it must not put an empty frame on the wire."""
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)

    await session.flush()

    assert sink.frames == []
    await hub.aclose()


async def test_lossless_sheds_the_oldest_when_the_queue_is_full() -> None:
    hub = Hub(queue_limit=10)
    sink = RecordingSink()
    session = Session(hub, sink)

    # Nothing drains until flush(), so the whole burst meets the queue limit.
    hub.feed(blob(*(tick(seq=i, bid=1.0 + i) for i in range(40))), FeederLink())
    assert session.dropped > 0

    await session.flush()
    # Whatever survived must be the *newest* data, not a stale prefix.
    assert sink.ticks()[-1]["q"] == 39
    await hub.aclose()


async def test_dropped_total_survives_the_subscriber_that_earned_it() -> None:
    """``HubStats.dropped`` is documented cumulative, so it must not fall when
    the consumer that was shed disconnects: a total that goes backwards reads
    as "the shedding un-happened" and makes any rate derived from it negative.
    """
    hub = Hub(queue_limit=10)
    session = Session(hub, RecordingSink())

    hub.feed(blob(*(tick(seq=i) for i in range(40))), FeederLink())
    shed = session.dropped
    assert shed > 0
    assert hub.snapshot_stats().dropped == shed

    session.close()
    assert hub.snapshot_stats().dropped == shed, "the count outlives the subscriber"
    session.close()  # idempotent -- and must not count the same drops twice
    assert hub.snapshot_stats().dropped == shed

    await hub.aclose()


async def test_conflate_keeps_only_the_newest_per_symbol() -> None:
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink, SubscriptionOptions(backpressure=BackpressurePolicy.CONFLATE))

    hub.feed(blob(*(tick("EURUSD", seq=i, bid=1.0 + i) for i in range(100))), FeederLink())
    hub.feed(blob(tick("USDJPY", seq=100, bid=157.0)), FeederLink())
    await session.flush()

    delivered = sink.ticks()
    assert len(delivered) == 2, "one per symbol, not one per tick"
    by_symbol = {t["s"]: t for t in delivered}
    assert by_symbol["EURUSD"]["q"] == 99
    assert by_symbol["USDJPY"]["q"] == 100
    await hub.aclose()


async def test_conflate_memory_is_bounded_by_symbol_count() -> None:
    """Pure queue policy: no sink, no session, nothing draining -- the flood
    still has to stay bounded by the number of symbols in it."""
    hub = Hub()
    subscriber = hub.subscribe(SubscriptionOptions(backpressure=BackpressurePolicy.CONFLATE))

    hub.feed(blob(*(tick("EURUSD", seq=i) for i in range(5_000))), FeederLink())

    assert subscriber.pending_count == 1
    assert subscriber.dropped == 0, "conflation is not a drop; it is a merge"
    await hub.aclose()


def test_subscribing_needs_no_running_event_loop() -> None:
    """A synchronous caller can open a queue: the hub creates no task, so there
    is nothing that needs a loop to attach to (ADR-0002)."""
    hub = Hub()

    subscriber = hub.subscribe()

    assert subscriber.id == 1
    assert not subscriber.closed


async def test_a_running_session_ends_when_the_hub_closes() -> None:
    """``Hub.aclose()`` closes the queues; the writer loops return on their own.
    Nothing here cancels a task, and no task outlives the hub."""
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)
    writer = asyncio.create_task(session.run())

    hub.feed(blob(tick()), FeederLink())
    await wait_until(lambda: bool(sink.frames))
    await hub.aclose()

    await asyncio.wait_for(writer, timeout=5.0)
    assert len(sink.ticks()) == 1


# -- framing -------------------------------------------------------------
#
# A TCP read boundary has nothing to do with a record boundary, and the link is
# what reconciles the two. It needs no socket to say so: a chunk is a byte
# string, and "the packets fell badly" is a slice size.


@pytest.mark.parametrize("piece", [1, 7, 63, 64, 65, 100, 128, 1_000])
async def test_records_arrive_whole_however_the_chunks_fall(piece: int) -> None:
    """The same twenty records, sliced into chunks that variously cut a record
    in half, land exactly once each and in order."""
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)
    link = FeederLink()
    payload = blob(*(tick(seq=i, bid=1.0 + i) for i in range(20)))

    decoded = sum(
        hub.feed(payload[start : start + piece], link)
        for start in range(0, len(payload), piece)
    )
    await session.flush()

    assert decoded == 20
    assert [t["q"] for t in sink.ticks()] == list(range(20))
    assert link.pending_bytes == 0, "nothing may be left over from a whole payload"
    await hub.aclose()


async def test_a_partial_record_is_held_back_until_it_is_complete() -> None:
    """A partial tail is neither an error nor a loss: it waits. Nothing is
    published, and nothing is decoded, until the last byte of it arrives."""
    hub = Hub()
    subscriber = hub.subscribe()
    link = FeederLink()
    record = blob(tick())

    assert hub.feed(record[:20], link) == 0
    assert link.pending_bytes == 20
    assert hub.feed(record[20:50], link) == 0
    assert link.pending_bytes == 50
    assert subscriber.pending_count == 0, "half a record is not a tick"

    assert hub.feed(record[50:], link) == 1
    assert link.pending_bytes == 0
    assert subscriber.pending_count == 1
    await hub.aclose()


async def test_an_exact_multiple_chunk_leaves_nothing_pending() -> None:
    """The common case -- the feeder writes whole records and they arrive that
    way -- goes straight through with no tail held over."""
    hub = Hub()
    link = FeederLink()

    assert hub.feed(blob(tick(seq=0), tick(seq=1)), link) == 2
    assert link.pending_bytes == 0
    assert hub.feed(b"", link) == 0
    await hub.aclose()


async def test_each_link_buffers_its_own_stream() -> None:
    """Two feeders' half-records must never be spliced into one another, so the
    buffer belongs to the connection rather than to the hub."""
    hub = Hub()
    first, second = FeederLink(name="peer-1"), FeederLink(name="peer-2")
    eurusd, usdjpy = blob(tick("EURUSD", bid=1.0)), blob(tick("USDJPY", bid=157.0))

    assert hub.feed(eurusd[:40], first) == 0
    assert hub.feed(usdjpy[:40], second) == 0
    assert hub.feed(eurusd[40:], first) == 1
    assert hub.feed(usdjpy[40:], second) == 1

    assert {t.symbol: t.bid for t in hub.latest()} == {"EURUSD": 1.0, "USDJPY": 157.0}
    assert first.symbols == {"EURUSD"}
    assert second.symbols == {"USDJPY"}
    await hub.aclose()


async def test_feed_tracks_the_symbol_set_per_feeder() -> None:
    """Each feeder's own symbol set is what /api/v1/feeders reports, so it has to
    grow once per *new* symbol and stay put when a known one repeats."""
    hub = Hub()
    link = FeederLink(name="peer-1")

    hub.feed(blob(tick("EURUSD", seq=0), tick("USDJPY", seq=1)), link)
    assert link.symbols == {"EURUSD", "USDJPY"}

    # A repeat of a known symbol must not grow the set.
    hub.feed(blob(tick("EURUSD", seq=2)), link)
    assert link.symbols == {"EURUSD", "USDJPY"}
    await hub.aclose()


async def test_sequence_gaps_are_counted() -> None:
    hub = Hub()
    link = FeederLink()

    hub.feed(blob(tick(seq=0), tick(seq=1)), link)
    assert link.seq_gaps == 0

    hub.feed(blob(tick(seq=99)), link)
    assert link.seq_gaps == 1
    assert hub.snapshot_stats().seq_gaps == 1
    await hub.aclose()


async def test_sequence_wraparound_is_not_a_gap() -> None:
    hub = Hub()
    link = FeederLink()
    hub.feed(blob(tick(seq=0xFFFF_FFFF)), link)
    hub.feed(blob(tick(seq=0)), link)
    assert link.seq_gaps == 0
    await hub.aclose()


async def test_snapshot_returns_the_latest_per_symbol() -> None:
    hub = Hub()
    hub.feed(
        blob(tick("EURUSD", seq=0, bid=1.0), tick("EURUSD", seq=1, bid=2.0)),
        FeederLink(),
    )
    hub.feed(blob(tick("USDJPY", seq=2, bid=157.0)), FeederLink())

    latest = {t.symbol: t.bid for t in hub.latest()}
    assert latest == {"EURUSD": 2.0, "USDJPY": 157.0}
    assert [t.symbol for t in hub.latest(["USDJPY"])] == ["USDJPY"]
    await hub.aclose()


async def test_snapshot_stats_is_a_pure_read() -> None:
    """Reading stats must not be consuming them. Every observer -- the REST poll,
    a WebSocket ``stats`` frame, a health probe -- has to see the same open
    interval, not race the others for it."""
    hub = Hub()
    hub.feed(blob(*(tick(seq=i) for i in range(10))), FeederLink())

    reads = [hub.snapshot_stats() for _ in range(3)]
    assert [r.ticks for r in reads] == [10, 10, 10]
    assert all(r.tick_rate > 0 for r in reads), "the interval stays open"
    assert len({(r.broker_lag_ms_p50, r.broker_lag_ms_p99) for r in reads}) == 1

    # ...and the samples are still there for the one caller meant to take them.
    assert hub.consume_interval().broker_lag_ms_p50 == reads[0].broker_lag_ms_p50
    await hub.aclose()


async def test_consume_interval_closes_the_window() -> None:
    hub = Hub()
    hub.feed(blob(*(tick(seq=i) for i in range(10))), FeederLink())

    first = hub.consume_interval()
    assert first.ticks == 10
    assert first.tick_rate > 0
    assert first.broker_lag_ms_p50 is not None

    second = hub.consume_interval()
    assert second.ticks == 10, "cumulative count keeps counting"
    assert second.tick_rate == 0.0, "rate is per-interval and starts over"
    assert second.broker_lag_ms_p50 is None, "the latency samples were consumed"

    # Closing the window must not swallow what arrives after it: a tick fed
    # once the samples are cleared belongs to the *next* interval, not to
    # nobody. (Otherwise the empty percentiles seen on a sparse live feed would
    # mean lost samples rather than a genuinely quiet ten seconds.)
    hub.feed(blob(tick(seq=10)), FeederLink())
    third = hub.consume_interval()
    assert third.ticks == 11
    assert third.tick_rate > 0
    assert third.broker_lag_ms_p50 is not None, "a post-clear tick was dropped"
    await hub.aclose()


async def test_latency_samples_keep_moving_past_the_cap() -> None:
    """Past the cap the ring buffer evicts the oldest sample rather than refusing
    the newest -- otherwise a bridge run with ``stats_interval_s=0`` would report
    percentiles frozen on its first samples forever. The cap is a constructor
    argument so this can be shown with a hundred records instead of the twenty
    thousand a production hub keeps."""
    cap = 100
    hub = Hub(latency_sample_cap=cap)
    link = FeederLink()
    now_ms = int(time.time() * 1000)

    def aged(time_msc: int, seq: int) -> Tick:
        return Tick("EURUSD", time_msc, 1.0, 1.0001, 0.0, 0.0, 6, seq)

    hub.feed(blob(*(aged(now_ms, i) for i in range(cap))), link)
    fresh = hub.snapshot_stats().broker_lag_ms_p99
    assert fresh is not None
    assert fresh < 60_000

    # An hour of broker lag, well after the buffer is already full.
    hub.feed(blob(*(aged(now_ms - 3_600_000, cap + i) for i in range(cap))), link)
    assert link.seq_gaps == 0

    stale = hub.snapshot_stats().broker_lag_ms_p99
    assert stale is not None, "post-cap samples were dropped"
    assert stale > 3_000_000, "post-cap samples were dropped"
    await hub.aclose()


async def test_stats_is_a_plain_domain_value() -> None:
    """`HubStats` carries counters, not a wire shape.

    Its JSON form lives in `api.StatsResponse.from_stats`; see
    `test_api.py::test_stats_frame_is_the_one_stats_json_shape`.
    """
    hub = Hub()
    hub.feed(blob(tick()), FeederLink())
    stats = hub.snapshot_stats()
    assert stats.ticks == 1
    assert stats.symbols == ["EURUSD"]
    assert not hasattr(stats, "as_dict")
    await hub.aclose()


async def test_unsubscribe_stops_delivery_and_is_idempotent() -> None:
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)

    session.close()
    session.close()

    hub.feed(blob(tick()), FeederLink())
    await session.flush()
    assert sink.frames == []
    await hub.aclose()


async def test_feed_rejects_a_foreign_protocol() -> None:
    hub = Hub()
    with pytest.raises(ProtocolError):
        hub.feed(b"\x00" * RECORD_SIZE, FeederLink())
    await hub.aclose()


async def test_a_bad_record_rejects_the_whole_batch() -> None:
    """A batch is all-or-nothing. A chunk whose second record is garbage used to
    leave the first one already delivered -- so a consumer's stream ended on a
    tick the bridge went on to disown, and the counters said it had arrived.
    Now nothing from the batch is published and no counter moves; the caller
    drops the connection."""
    hub = Hub()
    sink = RecordingSink()
    session = Session(hub, sink)
    link = FeederLink()
    payload = blob(tick("EURUSD", seq=0)) + b"\x00" * RECORD_SIZE + blob(tick("EURUSD", seq=1))

    with pytest.raises(ProtocolError):
        hub.feed(payload, link)
    await session.flush()

    assert sink.frames == [], "half a batch must not reach a subscriber"
    assert (link.ticks, link.seq_gaps, link.last_seq) == (0, 0, None)
    assert link.symbols == set()
    assert hub.snapshot_stats().ticks == 0
    assert hub.symbols == []
    await hub.aclose()


def test_queue_limit_must_be_positive() -> None:
    with pytest.raises(ValueError, match="queue_limit"):
        Hub(queue_limit=0)


def test_latency_sample_cap_must_be_positive() -> None:
    with pytest.raises(ValueError, match="latency_sample_cap"):
        Hub(latency_sample_cap=0)
