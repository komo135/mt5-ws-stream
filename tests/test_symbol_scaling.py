"""Tests for ``benchmarks/symbol_scaling.py``.

Almost everything here feeds :class:`~symbol_scaling.Aggregator` decoded frame
dicts directly -- no socket, matching the split the module's docstring
describes: :func:`symbol_scaling._collect` is the only network-touching
function, and it is thin enough not to need its own unit test. One integration
test at the bottom exercises it against the real ``bridge`` fixture.
"""

from __future__ import annotations

import asyncio
import csv
import socket
from pathlib import Path

import pytest
from benchmarks.symbol_scaling import (
    Aggregator,
    RestStats,
    SymbolStats,
    _http_base,
    _stream_url,
    render_markdown,
    run,
    write_csv,
)

from mt5_ws_stream import Bridge
from mt5_ws_stream.protocol import FLAG_HEARTBEAT, pack_tick
from mt5_ws_stream.protocol import Tick as WireTick

# asyncio_mode = "auto" in pyproject.toml means async tests need no marker.


def _ticks_frame(rx: float, ticks: list[dict[str, object]]) -> dict[str, object]:
    return {"t": "ticks", "rx": rx, "d": ticks}


def _tick_dict(
    symbol: str, *, ms: int, bid: float = 1.1, seq: int = 0, flags: int = 6
) -> dict[str, object]:
    return {
        "s": symbol,
        "ms": ms,
        "b": bid,
        "a": bid + 0.0002,
        "l": 0.0,
        "v": 0.0,
        "f": flags,
        "q": seq,
    }


# -- Aggregator ------------------------------------------------------------


def test_on_frame_counts_ticks_per_symbol() -> None:
    agg = Aggregator()
    frame = _ticks_frame(
        rx=1_700_000_000.0,
        ticks=[
            _tick_dict("EURUSD", ms=1_700_000_000_000, seq=0),
            _tick_dict("USDJPY", ms=1_700_000_000_000, seq=1),
            _tick_dict("EURUSD", ms=1_700_000_000_050, seq=2),
        ],
    )
    agg.on_frame(frame, received_wall=1_700_000_000.1, received_perf=10.0)

    assert agg.frames == 1
    assert agg.ticks == 3
    assert agg.heartbeats == 0
    assert set(agg.symbols) == {"EURUSD", "USDJPY"}
    assert agg.symbols["EURUSD"].ticks == 2
    assert agg.symbols["USDJPY"].ticks == 1


def test_on_frame_counts_heartbeats_separately_and_not_as_ticks() -> None:
    agg = Aggregator()
    frame = _ticks_frame(
        rx=1_700_000_000.0,
        ticks=[_tick_dict("", ms=1_700_000_000_000, seq=0, flags=FLAG_HEARTBEAT)],
    )
    agg.on_frame(frame, received_wall=1_700_000_000.1, received_perf=10.0)

    assert agg.heartbeats == 1
    assert agg.ticks == 0
    assert agg.symbols == {}


def test_on_frame_ignores_non_ticks_frames() -> None:
    agg = Aggregator()
    agg.on_frame({"t": "hello", "symbols": ["EURUSD"]}, received_wall=1.0, received_perf=1.0)
    agg.on_frame({"t": "stats", "ticks": 999}, received_wall=1.0, received_perf=1.0)

    assert agg.frames == 0
    assert agg.ticks == 0
    assert agg.symbols == {}


def test_broker_lag_and_hop_are_computed_from_the_right_clocks() -> None:
    agg = Aggregator()
    # received_wall is epoch seconds; ms is epoch milliseconds.
    received_wall = 1_700_000_000.500
    rx = 1_700_000_000.400
    frame = _ticks_frame(rx=rx, ticks=[_tick_dict("EURUSD", ms=1_700_000_000_000, seq=0)])

    agg.on_frame(frame, received_wall=received_wall, received_perf=5.0)

    stats = agg.symbols["EURUSD"]
    # lag = received_wall*1000 - ms = 1_700_000_000_500 - 1_700_000_000_000 = 500
    assert stats.lag_p50 == pytest.approx(500.0)
    # hop = (received_wall - rx) * 1000 = 100 ms
    assert stats.hop_p50 == pytest.approx(100.0)


def test_max_gap_is_the_largest_monotonic_interval_between_ticks() -> None:
    agg = Aggregator()
    base_ms = 1_700_000_000_000
    agg.on_frame(
        _ticks_frame(rx=1.0, ticks=[_tick_dict("EURUSD", ms=base_ms, seq=0)]),
        received_wall=1.0,
        received_perf=10.0,
    )
    agg.on_frame(
        _ticks_frame(rx=1.0, ticks=[_tick_dict("EURUSD", ms=base_ms, seq=1)]),
        received_wall=1.0,
        received_perf=10.25,  # +250 ms
    )
    agg.on_frame(
        _ticks_frame(rx=1.0, ticks=[_tick_dict("EURUSD", ms=base_ms, seq=2)]),
        received_wall=1.0,
        received_perf=10.30,  # +50 ms
    )

    assert agg.symbols["EURUSD"].max_gap_ms == pytest.approx(250.0)


def test_percentiles_are_none_when_a_symbol_has_no_samples() -> None:
    stats = SymbolStats("EURUSD")
    assert stats.lag_p50 is None
    assert stats.lag_p99 is None
    assert stats.hop_p50 is None
    assert stats.hop_p99 is None
    assert stats.ticks_per_s(10.0) == 0.0


# -- rendering ---------------------------------------------------------------


def _sample_run() -> tuple[Aggregator, RestStats, RestStats]:
    agg = Aggregator()
    base_ms = 1_700_000_000_000
    rx = 1_700_000_000.0
    agg.on_frame(
        _ticks_frame(
            rx=rx,
            ticks=[
                _tick_dict("EURUSD", ms=base_ms, seq=0),
                _tick_dict("USDJPY", ms=base_ms, seq=1),
            ],
        ),
        received_wall=1_700_000_000.05,
        received_perf=100.0,
    )
    agg.on_frame(
        _ticks_frame(rx=rx, ticks=[_tick_dict("", ms=base_ms, seq=2, flags=FLAG_HEARTBEAT)]),
        received_wall=1_700_000_000.10,
        received_perf=100.1,
    )
    start = RestStats(seq_gaps=1, dropped=0, heartbeats=5, ticks=100)
    end = RestStats(seq_gaps=1, dropped=2, heartbeats=6, ticks=103)
    return agg, start, end


def test_render_markdown_includes_label_symbols_and_totals() -> None:
    agg, start, end = _sample_run()
    table = render_markdown(
        agg,
        elapsed_s=2.0,
        label="N=10",
        url="ws://127.0.0.1:8765/ws",
        start_stats=start,
        end_stats=end,
    )

    assert "N=10" in table
    assert "EURUSD" in table
    assert "USDJPY" in table
    assert "clock skew" in table
    assert "| Ticks | 2 |" in table
    assert "| Heartbeats | 1 |" in table
    assert "| seq_gaps (delta, REST) | 0 |" in table
    assert "| dropped (delta, REST) | 2 |" in table
    assert "EA timer callback" in table


def test_render_markdown_handles_no_ticks_received() -> None:
    agg = Aggregator()
    start = end = RestStats(seq_gaps=0, dropped=0, heartbeats=0, ticks=0)
    table = render_markdown(
        agg, elapsed_s=1.0, label="", url="ws://x/ws", start_stats=start, end_stats=end
    )
    assert "no ticks received" in table
    assert "| Ticks | 0 |" in table


def test_write_csv_round_trips_symbol_rows_and_totals(tmp_path: Path) -> None:
    agg, start, end = _sample_run()
    csv_path = tmp_path / "run.csv"
    write_csv(csv_path, agg, elapsed_s=2.0, start_stats=start, end_stats=end)

    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))

    header = rows[0]
    assert header[:2] == ["symbol", "ticks"]
    symbol_rows = {row[0]: row for row in rows if row and row[0] in ("EURUSD", "USDJPY")}
    assert symbol_rows["EURUSD"][1] == "1"
    assert symbol_rows["USDJPY"][1] == "1"
    total_row = next(row for row in rows if row and row[0] == "TOTAL")
    assert total_row[1] == "2"
    flat = {row[0]: row[1] for row in rows if len(row) == 2}
    assert flat["seq_gaps_delta"] == "0"
    assert flat["dropped_delta"] == "2"
    assert flat["heartbeats"] == "1"


# -- URL helpers --------------------------------------------------------


def test_stream_url_forces_json_and_heartbeats_and_keeps_symbols() -> None:
    built = _stream_url("ws://127.0.0.1:8765/ws?symbols=EURUSD&conflate=1")
    assert "format=json" in built
    assert "heartbeats=1" in built
    assert "symbols=EURUSD" in built
    assert "conflate=1" in built


def test_stream_url_forces_json_even_if_binary_was_requested() -> None:
    built = _stream_url("ws://127.0.0.1:8765/ws?format=binary")
    assert "format=json" in built
    assert "format=binary" not in built


def test_http_base_derives_rest_origin_from_ws_url() -> None:
    assert _http_base("ws://127.0.0.1:8765/ws") == "http://127.0.0.1:8765"
    assert _http_base("wss://example.com/ws?symbols=A") == "https://example.com"


# -- integration: real bridge, real socket -----------------------------


async def test_run_against_a_real_bridge(bridge: Bridge, feeder_socket: socket.socket) -> None:
    """One end-to-end check that ``run()`` talks to the real WS + REST surface."""

    async def feed_after_a_beat() -> None:
        await asyncio.sleep(0.05)  # let run()'s collector connect first
        feeder_socket.sendall(
            b"".join(
                pack_tick(t)
                for t in [
                    WireTick(
                        symbol="EURUSD",
                        time_msc=1_700_000_000_000,
                        bid=1.1,
                        ask=1.1002,
                        last=0.0,
                        volume=0.0,
                        flags=6,
                        seq=0,
                    ),
                    WireTick(
                        symbol="EURUSD",
                        time_msc=1_700_000_000_010,
                        bid=1.1001,
                        ask=1.1003,
                        last=0.0,
                        volume=0.0,
                        flags=6,
                        seq=1,
                    ),
                ]
            )
        )

    feeder_task = asyncio.create_task(feed_after_a_beat())
    url = f"ws://127.0.0.1:{bridge.http_port}/ws"
    aggregator, elapsed_s, start_stats, end_stats = await run(url, 0.4)
    await feeder_task

    assert elapsed_s > 0
    assert aggregator.ticks == 2
    assert aggregator.symbols["EURUSD"].ticks == 2
    assert isinstance(start_stats, RestStats)
    assert isinstance(end_stats, RestStats)
