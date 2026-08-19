"""Phase (b): the capacity sweep -- E0 versus HEAD, across symbol counts.

What this measures, and why it is shaped this way:

*Interleaved, not batched.* Every symbol count N runs all of its builds back to
back -- E0, then HEAD in ``EXTRA_POLL``, then HEAD in ``EXTRA_EVENT`` -- before
the sweep moves to the next N. Tick rates drift with the session, and
``docs/latency.md`` is explicit that a baseline and an after measured hours
apart compare the market rather than the code. Interleaving keeps the compared
runs minutes apart instead.

*The symbol sets are measured, not assumed.* A capacity study whose "10
symbols" are ten that never ticked measures nothing. The sweep opens with a
discovery pass over the broker's whole instrument list and ranks what actually
arrived; N=10 and N=50 are the busiest of those. How many *silent* symbols each
set still contains is recorded, because that number says how much of a row to
believe.

*The wire numbers come from ``symbol_scaling`` itself*, called in process
rather than as a subprocess: the same collector, the same percentiles, and the
CSV still written by ``symbol_scaling.write_csv`` so the artifacts match the
manual procedure. Calling it directly is what makes a *pooled* hop/lag
percentile possible -- the CSV is per symbol, and "the hop p50 of this run" is
a question about the run.

*Every window is bracketed by a restart.* Changing an EA input means closing
the terminal, rewriting the chart file and starting it again -- see
:mod:`.terminal` for why an edit to a running terminal is silently lost. Each
run therefore pays a restart plus a warm-up before its measurement window, and
the warm-up is not optional: the first ``CopyTicks`` for a symbol can block for
tens of seconds, and E0 pays that inside ``OnTimer``.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any

from benchmarks.symbol_scaling import Aggregator, RestStats, write_csv
from benchmarks.symbol_scaling import run as scaling_run

from mt5_ws_stream.protocol import percentile

from .ealog import StatsLine

__all__ = [
    "RunOutcome",
    "RunSpec",
    "WireSummary",
    "collect_window",
    "rank_symbols",
    "results_table",
    "summarise",
]


@dataclass(frozen=True)
class RunSpec:
    """One measurement window: which build, which symbols, which delivery mode."""

    label: str
    build: str
    """``"e0"`` or ``"head"``."""

    n_label: str
    """``"1"``, ``"10"``, ``"50"``, ``"all"`` -- or ``"discovery"``."""

    symbols: str
    """The ``InpSymbols`` value: a comma list, ``"*"``, or ``""`` (chart only)."""

    mode: str
    """``"poll"`` or ``"event"``. E0 has no modes; it is always ``"poll"``."""

    measure_tick_loss: bool = False
    """E0 only. Its ``CopyTicks``-per-poll diagnostic, which has no warm-up."""

    hard_timeout_s: float = 480.0
    """Whole-run deadline. E0 with the diagnostic on can wedge the terminal."""


@dataclass(frozen=True)
class WireSummary:
    """What the consumer side saw during one window."""

    elapsed_s: float
    ticks: int
    ticks_per_s: float
    symbols_with_ticks: int
    hop_p50: float | None
    hop_p99: float | None
    lag_p50: float | None
    lag_p99: float | None
    seq_gaps_delta: int
    dropped_delta: int
    heartbeats: int
    ranked: tuple[tuple[str, int, float], ...]
    """``(symbol, ticks, ticks/s)``, busiest first."""

    window_start: float = 0.0
    """Local epoch seconds when collection began. Zero when not recorded.

    Phase (c) needs this: ``CountTicks.mq5`` counts the terminal's own tick
    database over ``CopyTicksRange(from_msc, to_msc)``, and the two counts only
    compare if they cover the same window. Recorded by :func:`collect_window`
    at the moment the collector starts, so it is the *receive* window -- which
    is not the same clock as the broker timestamps ``CopyTicksRange`` filters
    on, and the edge ticks that straddle either end are the documented reason
    a ground-truth comparison is read as "near zero", not "exactly zero".
    """

    window_end: float = 0.0
    """Local epoch seconds when collection stopped. Zero when not recorded."""

    def as_dict(self) -> dict[str, Any]:
        return {
            "elapsed_s": round(self.elapsed_s, 1),
            "ticks": self.ticks,
            "ticks_per_s": round(self.ticks_per_s, 2),
            "symbols_with_ticks": self.symbols_with_ticks,
            "hop_p50_ms": self.hop_p50,
            "hop_p99_ms": self.hop_p99,
            "lag_p50_ms": self.lag_p50,
            "lag_p99_ms": self.lag_p99,
            "seq_gaps_delta": self.seq_gaps_delta,
            "dropped_delta": self.dropped_delta,
            "heartbeats": self.heartbeats,
            "window_start_msc": int(self.window_start * 1000),
            "window_end_msc": int(self.window_end * 1000),
            "top": [
                {"symbol": name, "ticks": ticks, "ticks_per_s": round(rate, 2)}
                for name, ticks, rate in self.ranked[:15]
            ],
        }


def summarise(
    aggregator: Aggregator,
    *,
    elapsed_s: float,
    start_stats: RestStats,
    end_stats: RestStats,
) -> WireSummary:
    """Fold one collection into the row the sweep table wants.

    The hop and lag percentiles pool every symbol's samples rather than
    averaging the per-symbol percentiles: a mean of medians weights a symbol
    that ticked twice the same as one that ticked two thousand times, and at
    N=50 the twice-ticking symbols are most of the table.
    """
    hop: list[float] = []
    lag: list[float] = []
    ranked: list[tuple[str, int, float]] = []
    for stats in aggregator.symbols.values():
        hop.extend(stats.hop_samples_ms)
        lag.extend(stats.lag_samples_ms)
        ranked.append((stats.symbol, stats.ticks, stats.ticks_per_s(elapsed_s)))
    ranked.sort(key=lambda row: (-row[1], row[0]))
    hop.sort()
    lag.sort()
    return WireSummary(
        elapsed_s=elapsed_s,
        ticks=aggregator.ticks,
        ticks_per_s=aggregator.ticks / elapsed_s if elapsed_s > 0 else 0.0,
        symbols_with_ticks=len(aggregator.symbols),
        hop_p50=percentile(hop, 0.50),
        hop_p99=percentile(hop, 0.99),
        lag_p50=percentile(lag, 0.50),
        lag_p99=percentile(lag, 0.99),
        seq_gaps_delta=end_stats.seq_gaps - start_stats.seq_gaps,
        dropped_delta=end_stats.dropped - start_stats.dropped,
        heartbeats=aggregator.heartbeats,
        ranked=tuple(ranked),
    )


def rank_symbols(
    summary: WireSummary, *, count: int, universe: list[str], exclude: str = ""
) -> tuple[list[str], int]:
    """The *count* busiest symbols, padded from *universe* if too few ticked.

    Returns ``(symbols, silent)`` where *silent* counts how many of the chosen
    names produced no tick during discovery. Padding rather than shrinking
    keeps N honest: the EA still collects *count* symbols, and the row says how
    many of them had anything to collect.

    *exclude* drops the chart symbol, which ``OnTick`` already delivers and
    which the EA therefore never collects -- listing it would spend one of the
    N slots on a symbol that is not on the path being measured.
    """
    ticked = {name for name, ticks, _ in summary.ranked if ticks > 0}
    busiest = [name for name, ticks, _ in summary.ranked if ticks > 0 and name != exclude]
    chosen = busiest[:count]
    seen = set(chosen)
    for name in universe:
        if len(chosen) >= count:
            break
        if name not in seen and name != exclude:
            chosen.append(name)
            seen.add(name)
    return chosen, sum(1 for name in chosen if name not in ticked)


@dataclass
class RunOutcome:
    """One run's result -- success or not -- with everything the report needs."""

    spec: RunSpec
    status: str = "ok"
    note: str = ""
    started_line: str | None = None
    warmup_lines: list[str] = field(default_factory=list)
    wire: WireSummary | None = None
    ea: StatsLine | None = None
    resources: dict[str, Any] = field(default_factory=dict)
    csv_path: Path | None = None
    at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))

    def as_dict(self) -> dict[str, Any]:
        return {
            "at": self.at,
            "label": self.spec.label,
            "build": self.spec.build,
            "n": self.spec.n_label,
            "mode": self.spec.mode,
            "measure_tick_loss": self.spec.measure_tick_loss,
            "status": self.status,
            "note": self.note,
            "started_line": self.started_line,
            "warmup_lines": self.warmup_lines,
            "wire": self.wire.as_dict() if self.wire else None,
            "ea_stats": self.ea.values if self.ea else None,
            "ea_stats_raw": self.ea.raw if self.ea else None,
            "resources": self.resources,
            "csv": str(self.csv_path) if self.csv_path else None,
            "symbols": self.spec.symbols,
        }


def _num(value: float | None, digits: int = 2) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


_COLUMNS = (
    "Run",
    "N",
    "ticked",
    "ticks",
    "ticks/s",
    "hop p50",
    "hop p99",
    "lag p50",
    "lag p99",
    "poll_us a/m/p99",
    "ct_us a/m",
    "ct_err",
    "cur_skip",
    "obs/sent",
    "evt n/late/bad",
    "drop",
    "gaps",
    "ticks_lost",
)


def results_table(outcomes: list[RunOutcome]) -> str:
    """The whole sweep as one Markdown table, one row per run."""
    lines = ["| " + " | ".join(_COLUMNS) + " |", "|" + "---|" * len(_COLUMNS)]
    lines.extend("| " + " | ".join(_row_cells(outcome)) + " |" for outcome in outcomes)
    return "\n".join(lines)


def _row_cells(outcome: RunOutcome) -> list[str]:
    spec, wire, ea = outcome.spec, outcome.wire, outcome.ea
    if outcome.status != "ok" or wire is None:
        head = [f"`{spec.label}`", spec.n_label, f"**{outcome.status.upper()}** {outcome.note}"]
        return head + [""] * (len(_COLUMNS) - len(head))

    def stat(key: str) -> str:
        return ea.values.get(key, "-") if ea else "-"

    return [
        f"`{spec.label}`",
        spec.n_label,
        str(wire.symbols_with_ticks),
        str(wire.ticks),
        f"{wire.ticks_per_s:.1f}",
        _num(wire.hop_p50, 3),
        _num(wire.hop_p99, 3),
        _num(wire.lag_p50, 0),
        _num(wire.lag_p99, 0),
        f"{stat('poll_us_avg')}/{stat('poll_us_max')}/{stat('poll_us_p99')}",
        f"{stat('ct_us_avg')}/{stat('ct_us_max')}",
        stat("ct_err"),
        stat("cursor_skip"),
        f"{stat('extra_obs')}/{stat('extra_sent')}",
        f"{stat('evt_n')}/{stat('evt_late')}/{stat('evt_bad')}",
        stat("dropped"),
        str(wire.seq_gaps_delta),
        stat("ticks_lost"),
    ]


def collect_window(url: str, seconds: float, csv_path: Path) -> WireSummary:
    """Run one measurement window and write its CSV.

    Wraps ``symbol_scaling`` rather than reimplementing it, so the numbers in
    the sweep table and the numbers in the per-run CSV are the same numbers,
    produced once.
    """
    started_at = time.time()
    aggregator, elapsed_s, start_stats, end_stats = asyncio.run(scaling_run(url, seconds))
    stopped_at = time.time()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_csv(
        csv_path,
        aggregator,
        elapsed_s=elapsed_s,
        start_stats=start_stats,
        end_stats=end_stats,
    )
    summary = summarise(
        aggregator, elapsed_s=elapsed_s, start_stats=start_stats, end_stats=end_stats
    )
    return replace(summary, window_start=started_at, window_end=stopped_at)


def warm_up(seconds: float) -> None:
    """Wait out the EA's symbol warm-up before measuring.

    A function rather than a bare ``sleep`` so the reason has somewhere to
    live: the EA seeds every extra symbol's cursor from the newest tick during
    ``OnInit`` and finishes the stragglers one per timer tick. Measuring before
    that settles would charge the run for a synchronisation cost that is paid
    once per start, not once per tick.
    """
    time.sleep(seconds)


def merge_discovery(summaries: list[WireSummary]) -> WireSummary:
    """Combine several discovery passes into one ranking.

    The terminal caps an ``<inputs>`` line at 255 characters (see
    :data:`benchmarks.live.profile.MAX_INPUT_LINE`), so no single run can
    collect more than about 28 symbols and a universe larger than that has to
    be discovered in chunks. The chunks run back to back in one session and for
    the same duration, so their ticks-per-second are comparable -- which is the
    only property the ranking needs.
    """
    ranked: list[tuple[str, int, float]] = []
    for summary in summaries:
        ranked.extend(summary.ranked)
    ranked.sort(key=lambda row: (-row[2], row[0]))
    first = summaries[0]
    return WireSummary(
        elapsed_s=sum(s.elapsed_s for s in summaries),
        ticks=sum(s.ticks for s in summaries),
        ticks_per_s=sum(s.ticks_per_s for s in summaries),
        symbols_with_ticks=sum(1 for _, ticks, _ in ranked if ticks > 0),
        hop_p50=first.hop_p50,
        hop_p99=first.hop_p99,
        lag_p50=first.lag_p50,
        lag_p99=first.lag_p99,
        seq_gaps_delta=sum(s.seq_gaps_delta for s in summaries),
        dropped_delta=sum(s.dropped_delta for s in summaries),
        heartbeats=sum(s.heartbeats for s in summaries),
        ranked=tuple(ranked),
    )
