"""Micro-benchmarks for the two CPU-bound halves of the bridge's hot path.

    python benchmarks/micro.py                    # both, default sizes
    python benchmarks/micro.py --subscribers 4
    python benchmarks/micro.py --only encode

`bench.py` measures the whole loopback path, which is the number that matters
but also the noisiest: it includes the event loop, uvicorn, the WebSocket
framing and the OS scheduler. This module measures only the code this package
runs per tick, with no loop and no socket, so a change of a few hundred
nanoseconds per record is visible instead of buried:

* **ingest** -- ``Hub.feed(chunk, link)`` for a chunk of whole records with N
  subscribers attached: framing, ``decode_records``, ``FeederLink.account``,
  the latest-price map, lag sampling and ``publish``. Subscribers are drained
  every block so the measurement is ingest, not queue shedding.
* **encode** -- one drained batch through ``frames.ticks_frame`` (the ``json``
  format) and ``frames.binary_ticks_frame`` (the ``binary`` one), called
  exactly the way ``Session.encode`` calls them.

Reported as the *median* of several repetitions plus the *minimum*: the median
is what a run costs, the minimum is what the code costs with the machine's
noise removed. Compare two trees by pointing ``PYTHONPATH`` at each in turn and
running this same file both times -- the header prints which
``mt5_ws_stream`` actually got imported, which is the only way to notice that a
``PYTHONPATH`` typo silently benchmarked the installed copy.
"""

from __future__ import annotations

import argparse
import platform
import statistics
import sys
from time import perf_counter

import mt5_ws_stream
from mt5_ws_stream import frames
from mt5_ws_stream.hub import FeederLink, Hub, SubscriptionOptions
from mt5_ws_stream.protocol import PayloadFormat, Tick, pack_tick

SYMBOLS = ("EURUSD", "USDJPY", "GBPUSD", "XAUUSD")

#: Records per feeder chunk -- what a 20,000 ticks/s MockFeeder sends per write.
CHUNK_RECORDS = 20

#: Chunks fed between drains. Large enough that the timer call around a block
#: is noise; small enough that the pending lists stay realistic.
BLOCK_CHUNKS = 50


def make_ticks(count: int, *, first_seq: int = 0) -> list[Tick]:
    """*count* plausible quotes with a continuous sequence, cycling SYMBOLS."""
    return [
        Tick(
            symbol=SYMBOLS[i % len(SYMBOLS)],
            time_msc=1_700_000_000_000 + i,
            bid=1.08512 + i * 1e-5,
            ask=1.08529 + i * 1e-5,
            last=0.0,
            volume=float(i % 97),
            flags=6,
            seq=first_seq + i,
        )
        for i in range(count)
    ]


def make_chunks(chunks: int) -> list[bytes]:
    """*chunks* wire chunks of :data:`CHUNK_RECORDS` records, sequence continuous."""
    ticks = make_ticks(chunks * CHUNK_RECORDS)
    packed = [pack_tick(tick) for tick in ticks]
    return [
        b"".join(packed[i : i + CHUNK_RECORDS]) for i in range(0, len(packed), CHUNK_RECORDS)
    ]


def bench_ingest(subscribers: int, reps: int) -> None:
    """Records per second through ``Hub.feed`` with *subscribers* attached."""
    chunks = make_chunks(BLOCK_CHUNKS)
    records = BLOCK_CHUNKS * CHUNK_RECORDS

    hub = Hub()
    subs = [hub.subscribe(SubscriptionOptions()) for _ in range(subscribers)]
    link = FeederLink(name="micro")

    per_record_us = []
    for rep in range(reps + 1):  # rep 0 is warmup
        start = perf_counter()
        for chunk in chunks:
            hub.feed(chunk, link)
        elapsed = perf_counter() - start
        for sub in subs:
            sub.drain()
        if rep:
            per_record_us.append(elapsed / records * 1e6)

    plural = "s" if subscribers != 1 else ""
    _report(
        f"ingest  ({subscribers} subscriber{plural})",
        per_record_us,
        "us/record",
        with_rate=True,
    )


def bench_encode(batch: int, reps: int, calls: int) -> None:
    """Cost of encoding one drained batch of *batch* ticks, per payload format."""
    ticks = make_ticks(batch)
    items = [(tick, pack_tick(tick)) for tick in ticks]
    rx = 1_700_000_000.125

    for fmt in (PayloadFormat.JSON, PayloadFormat.BINARY):
        per_batch_us = []
        for rep in range(reps + 1):  # rep 0 is warmup
            start = perf_counter()
            if fmt is PayloadFormat.JSON:
                for _ in range(calls):
                    frames.ticks_frame((tick for tick, _ in items), rx=rx)
            else:
                for _ in range(calls):
                    frames.binary_ticks_frame(raw for _, raw in items)
            elapsed = perf_counter() - start
            if rep:
                per_batch_us.append(elapsed / calls * 1e6)
        _report(f"encode  ({fmt.value}, {batch} ticks/batch)", per_batch_us, "us/batch")


def _report(label: str, samples: list[float], unit: str, *, with_rate: bool = False) -> None:
    median = statistics.median(samples)
    line = f"  {label:<34} median {median:8.3f} {unit}   min {min(samples):8.3f} {unit}"
    if with_rate:
        line += f"   -> {1e6 / median:,.0f} records/s"
    print(line)


def main() -> None:
    parser = argparse.ArgumentParser(description="mt5-ws-stream hot-path micro-benchmarks")
    parser.add_argument("--subscribers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--batch", type=int, default=20, help="ticks per encoded frame")
    parser.add_argument("--reps", type=int, default=7)
    parser.add_argument("--calls", type=int, default=2000, help="encode calls per rep")
    parser.add_argument("--only", choices=["ingest", "encode"], default=None)
    args = parser.parse_args()

    print("mt5-ws-stream micro-benchmarks")
    print(f"  python   {sys.version.split()[0]}  {platform.platform()}")
    print(f"  package  {mt5_ws_stream.__file__}")
    print()

    if args.only != "encode":
        for n in args.subscribers:
            bench_ingest(n, args.reps)
    if args.only != "ingest":
        bench_encode(args.batch, args.reps, args.calls)
    print()


if __name__ == "__main__":
    main()
