"""Tests for ``benchmarks/bench.py``.

``run_once`` itself needs a real bridge, feeder and socket, so it is exercised
only by hand (``python benchmarks/bench.py``) -- like
``benchmarks/symbol_scaling.py``'s ``_collect``, it is thin glue around
:func:`mt5_ws_stream.cli.collect_bench` and not worth a fake-socket test.
:func:`~benchmarks.bench.print_run` is where the formatting lives, and it
takes a plain :class:`~mt5_ws_stream.cli.BenchResult` and
:class:`~mt5_ws_stream.hub.HubStats`, so it is testable with neither.
"""

from __future__ import annotations

import pytest
from benchmarks.bench import print_run

from mt5_ws_stream import HubStats
from mt5_ws_stream.cli import BenchResult
from mt5_ws_stream.protocol import PayloadFormat


def _stats(*, seq_gaps: int = 0, dropped: int = 0) -> HubStats:
    return HubStats(
        uptime_s=0.0,
        ticks=0,
        tick_rate=0.0,
        subscribers=0,
        symbols=[],
        seq_gaps=seq_gaps,
        heartbeats=0,
        dropped=dropped,
        broker_lag_ms_p50=None,
        broker_lag_ms_p99=None,
    )


def test_print_run_reports_throughput_integrity_and_per_frame_hop(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = BenchResult(
        ticks=10,
        elapsed_s=2.0,
        frames=4,
        frame_latencies_ms=[1.0, 2.0, 3.0, 4.0, 5.0],
    )

    print_run(20_000, PayloadFormat.JSON, 20, result, _stats(seq_gaps=1, dropped=2))

    out = capsys.readouterr().out
    assert "target 20,000 ticks/s  (json, batch=20)" in out
    assert "received : 10 ticks in 2.00s -> 5/s" in out
    assert "integrity: 1 seq gaps, 2 dropped" in out
    assert "p50 3.000 ms" in out
    assert "p99 5.000 ms" in out
    assert "max 5.000 ms" in out
    assert "mean 3.000 ms" in out
    assert "batching : 2.5 ticks/frame" in out


def test_print_run_notes_binary_format_carries_no_hop(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = BenchResult(ticks=5, elapsed_s=1.0, frames=5, frame_latencies_ms=[])

    print_run(200, PayloadFormat.BINARY, 1, result, _stats())

    out = capsys.readouterr().out
    assert "received : 5 ticks in 1.00s -> 5/s" in out
    assert "binary frames carry no send timestamp" in out
    assert "batching" not in out
