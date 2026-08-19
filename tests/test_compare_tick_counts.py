"""Tests for ``benchmarks/compare_tick_counts.py``.

Both CSV readers and the join/render logic are exercised against small,
hand-written fake CSVs -- no real ``symbol_scaling.py`` or ``CountTicks.mq5``
run involved, matching the "decoder is transport/tool-free" split used
elsewhere in this repo (``tests/test_symbol_scaling.py``).
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest
from benchmarks.compare_tick_counts import (
    ComparisonRow,
    compare,
    main,
    read_terminal_csv,
    read_wire_csv,
    render_table,
)


def _write_csv(path: Path, rows: list[list[object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)


def _wire_csv_rows() -> list[list[object]]:
    # Shape of symbol_scaling.py's write_csv(): header, per-symbol rows,
    # TOTAL, blank separator, key/value summary block.
    return [
        [
            "symbol",
            "ticks",
            "ticks_per_s",
            "lag_p50_ms",
            "lag_p99_ms",
            "hop_p50_ms",
            "hop_p99_ms",
            "max_gap_ms",
        ],
        ["EURUSD", 120, "4.0", "1.200", "2.500", "0.150", "0.300", "50.0"],
        ["USDJPY", 80, "2.7", "1.100", "2.100", "0.140", "0.290", "60.0"],
        ["TOTAL", 200, "6.7", "", "", "", "", ""],
        [],
        ["symbols_seen", 2],
        ["frames", 40],
        ["ticks_per_frame", "5.00"],
        ["heartbeats", 3],
        ["seq_gaps_delta", 0],
        ["dropped_delta", 0],
        ["ea_timer_callback_us", "paste from EA stats log"],
    ]


def _terminal_csv_rows() -> list[list[object]]:
    # Shape of CountTicks.mq5's output: header, per-symbol rows (one an
    # "error" row for a symbol CopyTicksRange failed on), TOTAL, blank, then
    # from_msc/to_msc/flags/errors metadata rows.
    return [
        ["symbol", "count"],
        ["EURUSD", 121],
        ["USDJPY", 80],
        ["GBPUSD", "error"],
        ["TOTAL", 201],
        [],
        ["from_msc", 1700000000000],
        ["to_msc", 1700000030000],
        ["flags", 8],
        ["errors", 1],
    ]


def test_read_wire_csv_returns_symbol_to_ticks_and_skips_summary_rows(tmp_path: Path) -> None:
    path = tmp_path / "wire.csv"
    _write_csv(path, _wire_csv_rows())

    counts = read_wire_csv(path)

    assert counts == {"EURUSD": 120, "USDJPY": 80}


def test_read_terminal_csv_skips_error_rows_and_metadata(tmp_path: Path) -> None:
    path = tmp_path / "terminal.csv"
    _write_csv(path, _terminal_csv_rows())

    counts = read_terminal_csv(path)

    assert counts == {"EURUSD": 121, "USDJPY": 80}
    assert "GBPUSD" not in counts
    assert "from_msc" not in counts
    assert "TOTAL" not in counts


def test_compare_computes_lost_as_terminal_minus_wire() -> None:
    wire = {"EURUSD": 120, "USDJPY": 80}
    terminal = {"EURUSD": 121, "USDJPY": 80}

    rows = compare(wire, terminal)

    assert rows == [
        ComparisonRow(symbol="EURUSD", terminal=121, wire=120),
        ComparisonRow(symbol="USDJPY", terminal=80, wire=80),
    ]
    assert rows[0].lost == 1
    assert rows[1].lost == 0


def test_compare_treats_a_symbol_missing_on_one_side_as_zero() -> None:
    wire = {"EURUSD": 120}
    terminal = {"EURUSD": 121, "GBPUSD": 5}

    rows = compare(wire, terminal)

    assert rows == [
        ComparisonRow(symbol="EURUSD", terminal=121, wire=120),
        ComparisonRow(symbol="GBPUSD", terminal=5, wire=0),
    ]


def test_render_table_includes_header_rows_and_total() -> None:
    rows = [
        ComparisonRow(symbol="EURUSD", terminal=121, wire=120),
        ComparisonRow(symbol="USDJPY", terminal=80, wire=80),
    ]

    table = render_table(rows)

    assert "symbol" in table
    assert "terminal" in table
    assert "wire" in table
    assert "lost" in table
    assert "EURUSD" in table
    assert "USDJPY" in table
    lines = table.splitlines()
    total_line = next(line for line in lines if line.startswith("TOTAL"))
    # terminal total 201, wire total 200, lost total 1
    assert total_line.split() == ["TOTAL", "201", "200", "1"]


def test_main_reads_both_csvs_and_prints_table(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    wire_path = tmp_path / "wire.csv"
    terminal_path = tmp_path / "terminal.csv"
    _write_csv(wire_path, _wire_csv_rows())
    _write_csv(terminal_path, _terminal_csv_rows())

    rc = main(["--wire", str(wire_path), "--terminal", str(terminal_path)])

    assert rc == 0
    out = capsys.readouterr().out
    assert "EURUSD" in out
    assert "USDJPY" in out
    assert "TOTAL" in out
