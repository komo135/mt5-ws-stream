"""End-to-end benchmark: feeder -> bridge -> WebSocket consumer, in one process.

    python benchmarks/bench.py                          # 200 and 20000 ticks/s
    python benchmarks/bench.py --rate 50000 --duration 10
    python benchmarks/bench.py --format binary

Everything runs in one process on loopback, so the numbers isolate *this
project's* contribution -- serialisation, fan-out, framing -- rather than
whatever a network is doing today. That is the number worth optimising, because
it is the only part the code controls.

The reported hop is bridge-send to client-receive. Both timestamps come from the
same clock, so unlike the broker-lag figure it needs no skew caveat.

Collection is :func:`mt5_ws_stream.cli.collect_bench` -- the same loop
``mt5-ws-stream client --bench`` uses -- so this module owns no frame-demuxing
loop of its own, only the reporting :func:`print_run` builds from what it
returns.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics

from mt5_ws_stream import (
    Bridge,
    BridgeConfig,
    HubStats,
    MockFeeder,
    PayloadFormat,
    TickStreamClient,
)
from mt5_ws_stream.cli import BenchResult, collect_bench
from mt5_ws_stream.protocol import percentile


def _pct_ms(values: list[float], p: float) -> float:
    """``percentile()`` in milliseconds; ``values`` is assumed non-empty here."""
    result = percentile(sorted(values), p)
    assert result is not None  # guarded by `if result.frame_latencies_ms:` at the call site
    return result


def print_run(
    rate: float, fmt: PayloadFormat, batch: int, result: BenchResult, stats: HubStats
) -> None:
    """Render one :func:`run_once` measurement.

    Pulled out of ``run_once`` so the formatting is testable against a canned
    :class:`~mt5_ws_stream.cli.BenchResult` and
    :class:`~mt5_ws_stream.hub.HubStats` -- no bridge, no feeder, no socket.

    The hop percentiles and the ``ticks/frame`` line read
    ``result.frame_latencies_ms`` -- one hop measurement per frame, credited
    once per frame rather than once per tick. That is the weighting this
    benchmark has always reported; ``collect_bench``'s per-tick
    ``latencies_ms`` is what ``mt5-ws-stream client --bench`` reports instead,
    and the two differ numerically whenever a run batches more than one tick
    per frame.
    """
    print(f"\n  target {rate:,.0f} ticks/s  ({fmt.value}, batch={batch})")
    print(
        f"    received : {result.ticks:,} ticks in {result.elapsed_s:.2f}s -> "
        f"{result.ticks / result.elapsed_s:,.0f}/s"
    )
    print(f"    integrity: {stats.seq_gaps} seq gaps, {stats.dropped} dropped")
    if result.frame_latencies_ms:
        print(
            f"    bridge->client: p50 {_pct_ms(result.frame_latencies_ms, 0.50):.3f} ms  "
            f"p99 {_pct_ms(result.frame_latencies_ms, 0.99):.3f} ms  "
            f"max {max(result.frame_latencies_ms):.3f} ms  "
            f"mean {statistics.fmean(result.frame_latencies_ms):.3f} ms"
        )
        print(f"    batching : {result.ticks / max(result.frames, 1):.1f} ticks/frame")
    else:
        print(
            "    bridge->client: binary frames carry no send timestamp; "
            "re-run with --format json to measure the hop"
        )


async def run_once(rate: float, duration: float, batch: int, fmt: PayloadFormat) -> None:
    config = BridgeConfig(tcp_port=0, http_port=0, stats_interval_s=0.0)
    async with Bridge(config) as bridge:
        feeder = MockFeeder(
            port=bridge.tcp_port,
            symbols=["EURUSD", "USDJPY", "GBPUSD", "XAUUSD"],
            rate=rate,
            batch=batch,
            duration=duration + 3,
            seed=1,
        )
        loop = asyncio.get_running_loop()
        feeder_task = loop.run_in_executor(None, feeder.run)
        await asyncio.sleep(0.5)

        url = f"ws://127.0.0.1:{bridge.http_port}/ws"
        async with TickStreamClient(url, payload_format=fmt) as stream:
            result = await collect_bench(stream, duration)

        stats = bridge.hub.snapshot_stats()

    print_run(rate, fmt, batch, result, stats)

    # The feeder stops on its own once its duration elapses; wait so the executor
    # thread does not outlive the benchmark and skew the next run.
    await feeder_task


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=float, default=None, help="ticks/second")
    parser.add_argument("--duration", type=float, default=8.0, help="seconds per run")
    parser.add_argument("--batch", type=int, default=None, help="records per send")
    parser.add_argument("--format", choices=["json", "binary"], default="json")
    args = parser.parse_args()

    fmt = PayloadFormat(args.format)
    print("mt5-ws-stream benchmark (loopback, single process)")

    if args.rate is not None:
        batch = args.batch if args.batch is not None else (20 if args.rate > 5000 else 1)
        await run_once(args.rate, args.duration, batch, fmt)
    else:
        await run_once(200, args.duration, 1, fmt)
        await run_once(20_000, args.duration, 20, fmt)

    print()


if __name__ == "__main__":
    asyncio.run(main())
