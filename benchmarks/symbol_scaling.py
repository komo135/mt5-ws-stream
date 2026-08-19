"""Symbol-scaling measurement harness for the EA capacity study (ADR-0004).

    python benchmarks/symbol_scaling.py --seconds 60 --label "N=10"
    python benchmarks/symbol_scaling.py --url ws://127.0.0.1:8765/ws --csv N10.csv

Connects to a *running* bridge (start one with ``mt5-ws-stream bridge`` and feed
it from a live or demo MetaTrader terminal -- this script has no feeder of its
own), subscribes to every symbol in ``json`` with heartbeats on, and prints a
per-symbol + totals table once the collection window closes.

Unlike ``benchmarks/bench.py`` -- which runs a synthetic feeder and a bridge in
one process to isolate this project's own overhead -- this script is meant to
run against the real EA, so it measures the whole path including the broker and
the terminal. It deliberately does not use
:class:`~mt5_ws_stream.client.TickStreamClient`: a measurement harness that
changes shape when the client does cannot compare a run to an older one, so this
script speaks the wire directly with
``websockets`` + ``json``, and only reaches into :mod:`mt5_ws_stream.protocol`
(the *record* layer, which is stable) for ``Tick.from_dict``, ``percentile`` and
``FLAG_HEARTBEAT``.

The network I/O lives in exactly one function, :func:`_collect`, kept thin and
untested. Everything it feeds -- :class:`Aggregator` -- takes already-decoded
frame dicts and is exercised in ``tests/test_symbol_scaling.py`` with no socket
at all, the same "decoder is transport-free" split ``docs/protocol.md`` and
``CONTEXT.md`` use for the client side.

## The sweep procedure

The EA capacity study (ADR-0004) sweeps ``InpSymbols`` -- the EA's
"extra symbols" input (``CONTEXT.md`` "Chart symbol" / "Extra symbols") -- and
records one row of the scaling table per (delivery mode, N) point:

1. Same machine for the whole sweep (terminal, bridge and this script on one
   box) -- a network hop between EA and bridge would drown out what the sweep is
   trying to isolate.
2. Market open, so ticks actually arrive; a quiet market produces heartbeats,
   not data.
3. For N in 1, 10, 50, "all" (every symbol the terminal offers): set
   ``InpSymbols`` on the running EA to N extra symbols, wait for it to
   reconnect, then run

   .. code-block:: bash

       python benchmarks/symbol_scaling.py --seconds 60 --label "N=<N>"

   Measure each N under ``InpExtraMode=EXTRA_POLL`` and then immediately under
   ``EXTRA_EVENT``, same ``InpSymbols``: market tick rates differ enough between
   hours to swamp the difference the two modes make.
   ``benchmarks/wizard_baseline_sweep.sh --mode after`` drives exactly this.
4. Paste each run's Markdown table into ``docs/latency.md``'s scaling table.
   The **EA timer callback (microseconds)** column is *not* measured by this
   script -- it comes from the EA's own ``InpStatsSec`` line (``poll_us_*``) --
   copy that number in by hand; the printed table leaves the column as a
   placeholder for exactly that reason.
5. Because ``broker_lag_ms`` is local-clock-minus-broker-clock (see
   ``docs/latency.md`` "Reading the two latency numbers"), only compare it
   *between* runs of this sweep on the same machine, never against numbers from
   a different host.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
from websockets.asyncio.client import connect

from mt5_ws_stream.frames import FrameKind
from mt5_ws_stream.protocol import FLAG_HEARTBEAT, Tick, percentile

__all__ = [
    "Aggregator",
    "RestStats",
    "SymbolStats",
    "build_parser",
    "main",
    "render_markdown",
    "run",
    "write_csv",
]

#: Matches ``cli.py``'s ``_DEFAULT_WS`` -- the bridge's default consumer port/path.
_DEFAULT_URL = "ws://127.0.0.1:8765/ws"
_REST_STATS_PATH = "/api/v1/stats"


def _fmt(value: float | None, digits: int = 3) -> str:
    """Render one optional measurement for a human or a CSV cell.

    ``None`` means "no samples in this window" (a quiet symbol, or a run with
    binary framing) -- distinct from a measured zero, same convention as
    :func:`mt5_ws_stream.protocol.percentile` and
    :func:`mt5_ws_stream.api.lag_text`.
    """
    return "n/a" if value is None else f"{value:.{digits}f}"


# -- aggregation -----------------------------------------------------------


@dataclass(slots=True)
class SymbolStats:
    """Per-symbol accumulator: everything the table needs, nothing it doesn't."""

    symbol: str
    ticks: int = 0
    lag_samples_ms: list[float] = field(default_factory=list)
    hop_samples_ms: list[float] = field(default_factory=list)
    max_gap_ms: float = 0.0
    _last_perf: float | None = field(default=None, repr=False)

    def record(self, *, lag_ms: float, hop_ms: float | None, perf_now: float) -> None:
        """Fold in one quote tick.

        Args:
            lag_ms: ``broker_lag_ms`` for this tick (local wall clock - ``ms``).
            hop_ms: bridge-to-here hop for this tick's frame, or ``None`` when
                the frame carried no ``rx`` (should not happen over this
                script's own ``json`` subscription, but a stray frame from a
                bridge running an older protocol version must not crash it).
            perf_now: ``time.perf_counter()`` at receipt -- monotonic, so gaps
                are immune to wall-clock adjustments mid-run.
        """
        self.ticks += 1
        self.lag_samples_ms.append(lag_ms)
        if hop_ms is not None:
            self.hop_samples_ms.append(hop_ms)
        if self._last_perf is not None:
            gap_ms = (perf_now - self._last_perf) * 1000.0
            if gap_ms > self.max_gap_ms:
                self.max_gap_ms = gap_ms
        self._last_perf = perf_now

    @property
    def lag_p50(self) -> float | None:
        return percentile(sorted(self.lag_samples_ms), 0.50)

    @property
    def lag_p99(self) -> float | None:
        return percentile(sorted(self.lag_samples_ms), 0.99)

    @property
    def hop_p50(self) -> float | None:
        return percentile(sorted(self.hop_samples_ms), 0.50)

    @property
    def hop_p99(self) -> float | None:
        return percentile(sorted(self.hop_samples_ms), 0.99)

    def ticks_per_s(self, elapsed_s: float) -> float:
        return self.ticks / elapsed_s if elapsed_s > 0 else 0.0


@dataclass(slots=True)
class Aggregator:
    """Folds decoded WebSocket frames into per-symbol and total counters.

    Deliberately has no socket, no ``asyncio`` and no clock of its own beyond
    what callers pass in -- :meth:`on_frame` takes an already-json.loads'd frame
    plus the two timestamps the caller took at receipt, so it is testable with
    dict literals (:func:`_collect` is the only thing that owns a connection).
    """

    symbols: dict[str, SymbolStats] = field(default_factory=dict)
    frames: int = 0
    ticks: int = 0
    heartbeats: int = 0

    def on_frame(
        self, payload: Mapping[str, Any], *, received_wall: float, received_perf: float
    ) -> None:
        """Fold one decoded JSON frame into the running totals.

        Non-``ticks`` frames (``hello``, ``stats``, ``ack``, ``pong``, ``error``)
        are ignored here -- they carry no quotes, and ``hello``'s ``snapshot`` is
        a point-in-time convenience for chart-drawing, not part of the stream
        this harness measures.

        Args:
            payload: One frame, already ``json.loads``'d.
            received_wall: ``time.time()`` when the frame was received --
                compared against each tick's ``ms`` (broker UTC millis) for
                ``broker_lag_ms``, and against the frame's ``rx`` for hop.
            received_perf: ``time.perf_counter()`` at the same moment -- used
                for the monotonic inter-tick gap only.
        """
        if payload.get("t") != FrameKind.TICKS:
            return
        self.frames += 1
        rx = payload.get("rx")
        rx_f = rx if isinstance(rx, (int, float)) else None
        for item in payload.get("d", []):
            tick = Tick.from_dict(item)
            if tick.flags & FLAG_HEARTBEAT:
                self.heartbeats += 1
                continue
            self.ticks += 1
            stats = self.symbols.setdefault(tick.symbol, SymbolStats(tick.symbol))
            lag_ms = received_wall * 1000.0 - tick.time_msc
            hop_ms = (received_wall - rx_f) * 1000.0 if rx_f is not None else None
            stats.record(lag_ms=lag_ms, hop_ms=hop_ms, perf_now=received_perf)


# -- REST stats delta --------------------------------------------------------


@dataclass(slots=True)
class RestStats:
    """The subset of ``GET /api/v1/stats`` this harness deltas across the run."""

    seq_gaps: int
    dropped: int
    heartbeats: int
    ticks: int

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> RestStats:
        return cls(
            seq_gaps=int(payload.get("seq_gaps", 0)),
            dropped=int(payload.get("dropped", 0)),
            heartbeats=int(payload.get("heartbeats", 0)),
            ticks=int(payload.get("ticks", 0)),
        )


# -- rendering ---------------------------------------------------------------


def render_markdown(
    aggregator: Aggregator,
    *,
    elapsed_s: float,
    label: str,
    url: str,
    start_stats: RestStats,
    end_stats: RestStats,
) -> str:
    """Render the per-symbol + totals Markdown table for one run.

    The header states the clock-skew caveat every time on purpose: this table
    is meant to be pasted straight into ``docs/latency.md``, and a reader who
    only sees one pasted table should not have to go find the caveat elsewhere.
    """
    lines: list[str] = []
    title = "Symbol scaling run"
    if label:
        title += f" -- {label}"
    lines.append(f"### {title}")
    lines.append("")
    lines.append(f"- url: `{url}`")
    lines.append(f"- duration: {elapsed_s:.1f} s (requested {elapsed_s:.0f} s)")
    lines.append(
        "- `broker_lag_ms` = local UTC now - the tick's `ms` field. This includes "
        "clock skew between this machine and the broker server -- read it as a "
        "trend and compare only *between* runs made on this same machine, never "
        "as an absolute (see `docs/latency.md`)."
    )
    lines.append(
        "- `hop_ms` = local receive time - the frame's `rx`. Clean when both ends "
        "share a clock (true here: bridge and this script are the same machine)."
    )
    lines.append("")

    lines.append(
        "| Symbol | Ticks | Ticks/s | lag p50 (ms) | lag p99 (ms) | "
        "hop p50 (ms) | hop p99 (ms) | max gap (ms) |"
    )
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for symbol in sorted(aggregator.symbols):
        stats = aggregator.symbols[symbol]
        lines.append(
            f"| {symbol} | {stats.ticks} | {stats.ticks_per_s(elapsed_s):.1f} | "
            f"{_fmt(stats.lag_p50)} | {_fmt(stats.lag_p99)} | "
            f"{_fmt(stats.hop_p50)} | {_fmt(stats.hop_p99)} | "
            f"{stats.max_gap_ms:.1f} |"
        )
    if not aggregator.symbols:
        lines.append("| *(no ticks received)* | | | | | | | |")
    lines.append("")

    seq_gaps_delta = end_stats.seq_gaps - start_stats.seq_gaps
    dropped_delta = end_stats.dropped - start_stats.dropped
    ticks_per_frame = aggregator.ticks / aggregator.frames if aggregator.frames else 0.0

    lines.append("| Metric | Value |")
    lines.append("| --- | ---: |")
    lines.append(f"| Symbols seen | {len(aggregator.symbols)} |")
    lines.append(f"| Ticks | {aggregator.ticks} |")
    lines.append(f"| Frames | {aggregator.frames} |")
    lines.append(f"| Ticks/frame | {ticks_per_frame:.2f} |")
    lines.append(f"| Heartbeats | {aggregator.heartbeats} |")
    lines.append(f"| seq_gaps (delta, REST) | {seq_gaps_delta} |")
    lines.append(f"| dropped (delta, REST) | {dropped_delta} |")
    lines.append(
        "| EA timer callback (µs) | *paste from the EA's stats log line -- "
        "not measured by this script* |"
    )
    lines.append("")
    return "\n".join(lines)


def write_csv(
    path: str | Path,
    aggregator: Aggregator,
    *,
    elapsed_s: float,
    start_stats: RestStats,
    end_stats: RestStats,
) -> None:
    """Write the same numbers :func:`render_markdown` prints, as CSV.

    One row per symbol, plus a trailing ``TOTAL`` row -- the shape a
    spreadsheet-based scaling table wants, rather than the two-table split the
    Markdown output uses for readability.
    """
    fieldnames = [
        "symbol",
        "ticks",
        "ticks_per_s",
        "lag_p50_ms",
        "lag_p99_ms",
        "hop_p50_ms",
        "hop_p99_ms",
        "max_gap_ms",
    ]
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(fieldnames)
        for symbol in sorted(aggregator.symbols):
            stats = aggregator.symbols[symbol]
            writer.writerow(
                [
                    symbol,
                    stats.ticks,
                    f"{stats.ticks_per_s(elapsed_s):.1f}",
                    _fmt(stats.lag_p50),
                    _fmt(stats.lag_p99),
                    _fmt(stats.hop_p50),
                    _fmt(stats.hop_p99),
                    f"{stats.max_gap_ms:.1f}",
                ]
            )
        ticks_per_frame = aggregator.ticks / aggregator.frames if aggregator.frames else 0.0
        writer.writerow(
            [
                "TOTAL",
                aggregator.ticks,
                f"{aggregator.ticks / elapsed_s:.1f}" if elapsed_s > 0 else "0.0",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        writer.writerow([])
        writer.writerow(["symbols_seen", len(aggregator.symbols)])
        writer.writerow(["frames", aggregator.frames])
        writer.writerow(["ticks_per_frame", f"{ticks_per_frame:.2f}"])
        writer.writerow(["heartbeats", aggregator.heartbeats])
        writer.writerow(["seq_gaps_delta", end_stats.seq_gaps - start_stats.seq_gaps])
        writer.writerow(["dropped_delta", end_stats.dropped - start_stats.dropped])
        writer.writerow(["ea_timer_callback_us", "paste from EA stats log"])


# -- network I/O (thin, untested) -------------------------------------------


def _stream_url(url: str) -> str:
    """*url* with ``format=json&heartbeats=1`` forced on.

    Any ``symbols`` the caller already put in *url* is left alone -- omitted,
    per ``docs/protocol.md``, it means "all", which is what this harness wants
    by default. Forcing JSON matters even if the caller asked for binary:
    binary frames carry no ``rx`` (see ``bench.py``), and this script's whole
    point is the per-tick and per-frame latency numbers that come from it.
    """
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query))
    query["format"] = "json"
    query["heartbeats"] = "1"
    new_query = urlencode(query)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def _http_base(url: str) -> str:
    """The bridge's REST base URL, derived from its WebSocket *url*.

    Both live on the same port (``docs/protocol.md`` §2): only the scheme and
    path differ.
    """
    parts = urlsplit(url)
    scheme = "https" if parts.scheme == "wss" else "http"
    return urlunsplit((scheme, parts.netloc, "", "", ""))


async def _fetch_rest_stats(client: httpx.AsyncClient, base_url: str) -> RestStats:
    response = await client.get(f"{base_url}{_REST_STATS_PATH}", timeout=5.0)
    response.raise_for_status()
    return RestStats.from_json(response.json())


async def _collect(url: str, seconds: float) -> tuple[Aggregator, float]:
    """Connect, subscribe to everything in JSON with heartbeats, collect.

    The only function in this module that touches a socket. Kept deliberately
    thin -- one connection, one receive loop, one call into
    :meth:`Aggregator.on_frame` per frame -- so the logic worth testing lives in
    :class:`Aggregator` instead, where it is testable without a socket.
    """
    stream_url = _stream_url(url)
    aggregator = Aggregator()
    started = time.perf_counter()
    async with connect(stream_url, max_queue=None, compression=None) as connection:
        while True:
            remaining = seconds - (time.perf_counter() - started)
            if remaining <= 0:
                break
            try:
                message = await asyncio.wait_for(connection.recv(), timeout=remaining)
            except TimeoutError:
                break
            if isinstance(message, (bytes, bytearray)):
                continue  # json was requested; ignore a stray binary frame
            received_wall = time.time()
            received_perf = time.perf_counter()
            payload = json.loads(message)
            aggregator.on_frame(
                payload, received_wall=received_wall, received_perf=received_perf
            )
    return aggregator, time.perf_counter() - started


async def run(url: str, seconds: float) -> tuple[Aggregator, float, RestStats, RestStats]:
    """Fetch REST stats, collect for *seconds*, fetch REST stats again.

    Bracketing the WebSocket collection with the two REST reads means the delta
    covers (approximately) the same window the aggregator measured, without the
    REST calls themselves ever mutating hub state (`docs/protocol.md` "All GET,
    all read-only").
    """
    base_url = _http_base(url)
    async with httpx.AsyncClient() as client:
        start_stats = await _fetch_rest_stats(client, base_url)
        aggregator, elapsed_s = await _collect(url, seconds)
        end_stats = await _fetch_rest_stats(client, base_url)
    return aggregator, elapsed_s, start_stats, end_stats


# -- CLI ----------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url", default=_DEFAULT_URL, help=f"bridge WebSocket URL (default: {_DEFAULT_URL})"
    )
    parser.add_argument(
        "--seconds", type=float, default=60.0, help="collection window in seconds (default: 60)"
    )
    parser.add_argument(
        "--label",
        default="",
        metavar="TEXT",
        help='run label echoed in the header, e.g. "N=10"',
    )
    parser.add_argument(
        "--csv", dest="csv_path", default=None, metavar="PATH", help="also write CSV to PATH"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    print(
        f"connecting to {args.url} for {args.seconds:.0f}s ...",
        file=sys.stderr,
    )
    aggregator, elapsed_s, start_stats, end_stats = asyncio.run(run(args.url, args.seconds))

    table = render_markdown(
        aggregator,
        elapsed_s=elapsed_s,
        label=args.label,
        url=args.url,
        start_stats=start_stats,
        end_stats=end_stats,
    )
    print(table)

    if args.csv_path:
        write_csv(
            args.csv_path,
            aggregator,
            elapsed_s=elapsed_s,
            start_stats=start_stats,
            end_stats=end_stats,
        )
        print(f"wrote {args.csv_path}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
