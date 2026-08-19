"""Ground-truth tick-loss comparison for the EA capacity study (ADR-0004).

    python benchmarks/compare_tick_counts.py \\
        --wire benchmarks/results/after-20260817-N10-groundtruth.csv \\
        --terminal MQL5/Files/TickStreamer_counts.csv

Reads two CSVs produced independently over the same window and prints a
per-symbol ``ticks_lost = terminal - wire`` table plus a total, expected to be
0 after E2 (see ``docs/latency.md``, "Symbol scaling table" -- an
implementation cannot be its own witness, so the wire count and the terminal
count have to come from two places that do not share code):

* **wire**: ``benchmarks/symbol_scaling.py --csv PATH``, run against the
  running bridge during the measurement window (the "Ticks" column of its
  per-symbol table).
* **terminal**: ``mql5/Scripts/TickStreamer/CountTicks.mq5``, run from the
  terminal's Navigator over the same window via ``CopyTicksRange()``.

This module is deliberately thin and untested at its I/O edges (reading a CSV
file, writing to stdout) and fully tested at its logic (``compare()``) --
``tests/test_compare_tick_counts.py`` feeds it in-memory dicts, the same split
``symbol_scaling.py``/``CONTEXT.md`` use elsewhere in this repo.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

__all__ = [
    "ComparisonRow",
    "build_parser",
    "compare",
    "main",
    "read_terminal_csv",
    "read_wire_csv",
    "render_table",
]

#: Row labels that are not a symbol's tick count -- the trailing summary block
#: both `symbol_scaling.py`'s `write_csv()` and `CountTicks.mq5` append after
#: their per-symbol rows.
_NON_SYMBOL_ROWS = frozenset(
    {
        "",
        "TOTAL",
        "symbols_seen",
        "frames",
        "ticks_per_frame",
        "heartbeats",
        "seq_gaps_delta",
        "dropped_delta",
        "ea_timer_callback_us",
        "from_msc",
        "to_msc",
        "flags",
        "errors",
    }
)


def _read_symbol_counts(path: str | Path, *, count_column: int, source: str) -> dict[str, int]:
    """Shared CSV-walking core for :func:`read_wire_csv` / :func:`read_terminal_csv`.

    Both CSVs share the same shape: a header row, one row per symbol with the
    count in a fixed column, then a ``TOTAL`` row and a trailing key/value
    summary block. Skips blank rows, rows in :data:`_NON_SYMBOL_ROWS`, and any
    row whose count cell is not an integer (``CountTicks.mq5`` writes
    ``error`` for a symbol ``CopyTicksRange`` failed on) -- warning to stderr
    for the latter, since it silently removes that symbol from the comparison.
    """
    counts: dict[str, int] = {}
    with Path(path).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))

    for row in rows[1:]:  # rows[0] is the header
        if len(row) <= count_column:
            continue
        symbol = row[0].strip()
        if symbol in _NON_SYMBOL_ROWS:
            continue
        raw_count = row[count_column].strip()
        try:
            counts[symbol] = int(raw_count)
        except ValueError:
            print(
                f"compare_tick_counts: {source} {path}: skipping {symbol!r} "
                f"(non-integer count {raw_count!r})",
                file=sys.stderr,
            )
    return counts


def read_wire_csv(path: str | Path) -> dict[str, int]:
    """``{symbol: ticks}`` from a ``symbol_scaling.py --csv`` file."""
    return _read_symbol_counts(path, count_column=1, source="wire")


def read_terminal_csv(path: str | Path) -> dict[str, int]:
    """``{symbol: count}`` from a ``CountTicks.mq5`` CSV file."""
    return _read_symbol_counts(path, count_column=1, source="terminal")


@dataclass(frozen=True, slots=True)
class ComparisonRow:
    """One symbol's ground-truth comparison.

    ``lost`` is ``terminal - wire``: positive means the wire under-counted
    (E2's expectation, 0, holds only if every tick that reached the terminal
    also reached the bridge and the collector). A symbol missing from one side
    is treated as 0 on that side rather than dropped -- it is exactly the kind
    of mismatch this comparison exists to surface.
    """

    symbol: str
    terminal: int
    wire: int

    @property
    def lost(self) -> int:
        return self.terminal - self.wire


def compare(wire: dict[str, int], terminal: dict[str, int]) -> list[ComparisonRow]:
    """Join *wire* and *terminal* counts into one row per symbol, sorted by name."""
    symbols = sorted(set(wire) | set(terminal))
    return [
        ComparisonRow(symbol=symbol, terminal=terminal.get(symbol, 0), wire=wire.get(symbol, 0))
        for symbol in symbols
    ]


def render_table(rows: Sequence[ComparisonRow]) -> str:
    """Plain-text ``symbol, terminal, wire, lost`` table plus a totals line."""
    lines = [f"{'symbol':<12} {'terminal':>10} {'wire':>10} {'lost':>10}"]
    lines.append("-" * len(lines[0]))
    lines.extend(
        f"{row.symbol:<12} {row.terminal:>10} {row.wire:>10} {row.lost:>10}" for row in rows
    )
    total_terminal = sum(row.terminal for row in rows)
    total_wire = sum(row.wire for row in rows)
    lines.append("-" * len(lines[0]))
    lines.append(
        f"{'TOTAL':<12} {total_terminal:>10} {total_wire:>10} {total_terminal - total_wire:>10}"
    )
    return "\n".join(lines)


# -- CLI ----------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--wire",
        required=True,
        metavar="PATH",
        help="symbol_scaling.py --csv output for the ground-truth window",
    )
    parser.add_argument(
        "--terminal",
        required=True,
        metavar="PATH",
        help="CountTicks.mq5's InpCsvFile output (MQL5\\Files\\...)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    wire = read_wire_csv(args.wire)
    terminal = read_terminal_csv(args.terminal)
    rows = compare(wire, terminal)
    print(render_table(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
