"""The entry point: one command per phase of the capacity study.

    python -m benchmarks.live.run smoke
    python -m benchmarks.live.run sweep

``smoke`` is the whole cycle once, end to end, on the HEAD build with the chart
symbol only. It exists to prove the rig before a sweep spends an hour of live
market on it: if the profile mechanism, the compile, the install, the restart,
the log parsing, the bridge and the harness all work for one run, they work for
sixty.

``sweep`` is phase (b): a discovery pass that ranks the broker's instruments by
measured activity, then E0 / HEAD-POLL / HEAD-EVENT at each symbol count,
interleaved. :mod:`.sweep` holds the measurement vocabulary; this module holds
the sequencing and the restarts.

**Results are appended as they happen**, not written at the end. A live run can
be interrupted by anything -- a terminal that will not close, a market that
closes, a machine that reboots -- and a report that only exists in memory until
the last step is a report that the interesting failures destroy. Every step
appends a JSON record to ``benchmarks/results/live-<date>/steps.jsonl`` and a
section to ``RESULTS.md`` in the same directory.

The clock skew in the header is not decoration. ``broker_lag_ms`` is measured
as *broker timestamp minus local clock*, so a local clock running behind NTP
shows up as a negative broker lag and looks like the impossible -- see
``docs/troubleshooting.md``. Recording the skew once per run is what lets a
reader tell a clock problem from a latency one.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from . import builds, profile
from .bridge import BridgeProcess, http_get_json
from .config import LiveConfig
from .ealog import ExpertLog, classify, server_offset_ms
from .sweep import (
    RunOutcome,
    RunSpec,
    collect_window,
    merge_discovery,
    rank_symbols,
    results_table,
    warm_up,
)
from .terminal import Terminal, TerminalError
from .textfiles import write_terminal_text
from .verify_ws_rest import verify

__all__ = ["Recorder", "clock_skew", "main", "symbol_universe"]

BENCH_PROFILE = "TickBench"
#: An FX pair on this broker. The ``#`` suffix is XMTrading's, and a symbol
#: name that does not exist means a chart that never opens.
DEFAULT_SYMBOL = "EURUSD#"

#: The sweep's chart symbol, in preference order. The chart symbol is the one
#: instrument delivered through ``OnTick`` rather than collected, so it should
#: tick whatever the session: gold trades ~23 h a day, crypto 24/7. This broker
#: spells gold ``GOLD#`` and has no ``XAUUSD#``.
CHART_SYMBOL_PREFERENCE = ("XAUUSD#", "BTCUSD#", "ETHUSD#", "GOLD#", "EURUSD#")


def _say(message: str) -> None:
    print(message, flush=True)


# -- recording -------------------------------------------------------------


class Recorder:
    """Append-only run record: one JSONL line and one Markdown section a step."""

    def __init__(self, directory: Path, *, title: str) -> None:
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self.steps_path = directory / "steps.jsonl"
        self.markdown_path = directory / "RESULTS.md"
        if not self.markdown_path.exists():
            self.markdown_path.write_text(
                f"# {title}\n\nStarted {datetime.now():%Y-%m-%d %H:%M:%S} (local).\n",
                encoding="utf-8",
            )

    def step(self, name: str, payload: dict[str, Any]) -> None:
        record = {"at": datetime.now().isoformat(timespec="seconds"), "step": name, **payload}
        with self.steps_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, default=str) + "\n")

    def section(self, heading: str, body: str) -> None:
        with self.markdown_path.open("a", encoding="utf-8") as handle:
            handle.write(f"\n## {heading}\n\n{body.rstrip()}\n")
        _say(f"  -> {self.markdown_path}")

    def artifact(self, name: str, text: str) -> Path:
        path = self.directory / name
        path.write_text(text, encoding="utf-8")
        return path


def clock_skew(*, samples: int = 3, timeout: float = 60.0) -> dict[str, Any]:
    """Local clock versus an NTP server, via ``w32tm /stripchart``.

    Returned rather than asserted: a skewed clock does not invalidate a run,
    it explains the sign of ``broker_lag_ms``.
    """
    try:
        result = subprocess.run(
            [
                "w32tm",
                "/stripchart",
                "/computer:time.windows.com",
                f"/samples:{samples}",
                "/dataonly",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"available": False, "error": str(exc)}
    offsets: list[float] = []
    for line in result.stdout.split("\n"):
        _, _, tail = line.partition(",")
        token = tail.strip().rstrip("s")
        try:
            offsets.append(float(token))
        except ValueError:
            continue
    return {
        "available": bool(offsets),
        "offsets_s": offsets,
        "mean_offset_s": round(sum(offsets) / len(offsets), 4) if offsets else None,
        "raw": result.stdout.strip(),
    }


# -- the smoke cycle -------------------------------------------------------


def smoke(args: argparse.Namespace) -> int:
    """Install, attach, start, verify, measure, and check the watchdog. Once."""
    config = LiveConfig.detect()
    config.check()

    results_root = config.results_dir / f"live-{date.today():%Y%m%d}"
    recorder = Recorder(results_root, title=f"Live rig -- {date.today():%Y-%m-%d}")
    terminal = Terminal(config)

    skew = clock_skew()
    recorder.step("clock_skew", skew)
    recorder.section(
        "Environment",
        "\n".join(
            [
                f"- terminal: `{config.terminal_exe}`",
                f"- data folder: `{config.data_dir}`",
                f"- repo: `{config.repo_root}`",
                f"- local clock vs time.windows.com: "
                f"{skew.get('mean_offset_s')} s offset (`w32tm` reports "
                "*server minus local*, so **positive means this PC is behind**; a PC "
                "that is behind makes `broker_lag_ms` read negative by about the same "
                "amount -- see `docs/troubleshooting.md`)",
            ]
        ),
    )

    # 1. Compile and stage. The terminal may still be running -- MetaEditor is
    #    a separate process and staging touches nothing the terminal owns.
    _say("compiling the HEAD build (EA + TickSpy + CountTicks) ...")
    artifacts = builds.prepare(
        config,
        name="head",
        builds_dir=config.results_dir / "builds",
        with_tools=True,
    )
    recorder.step(
        "build",
        {
            "name": artifacts.name,
            "ok": artifacts.ok,
            "compiles": [
                {
                    "source": str(result.source),
                    "errors": result.errors,
                    "warnings": result.warnings,
                    "tail": result.tail,
                }
                for result in artifacts.compiles
            ],
        },
    )
    if not artifacts.ok:
        recorder.section(
            "Build FAILED",
            "\n".join(
                f"- `{r.source.name}`: {r.errors} error(s)\n\n```\n{r.tail}\n```"
                for r in artifacts.compiles
            ),
        )
        _say("compile failed; stopping.")
        return 1
    recorder.section(
        "Build (HEAD)",
        "\n".join(
            f"- `{result.source.name}` -> {result.errors} errors, {result.warnings} warnings"
            for result in artifacts.compiles
        ),
    )

    # 2. Everything that edits a file the terminal rewrites on exit happens in
    #    this window, with the terminal down.
    _say("closing the terminal (graceful) ...")
    terminal.close(timeout=args.close_timeout)
    installed = builds.install_artifacts(config, artifacts, backup_suffix=args.backup_suffix)
    _say(f"installed: {', '.join(installed)}")

    template_path = profile.find_expert_template(
        config.deleted_profiles_dir, config.profiles_dir
    )
    if template_path is None:
        recorder.section(
            "Profile FAILED",
            "No `.chr` carrying an `<expert>` block was found under "
            f"`{config.deleted_profiles_dir}` or `{config.profiles_dir}`. "
            "Bootstrap via `/config:` would be the fallback.",
        )
        _say("no chart template with an <expert> block; stopping.")
        return 1

    inputs = profile.head_inputs(
        symbols="",
        extra_mode=0,
        stats_sec=args.stats_sec,
        verbose=True,
    )
    chart = profile.chart_for_expert(
        profile.ChartFile.read(template_path),
        symbol=args.symbol,
        inputs=inputs,
    )
    written = profile.write_profile(config.profiles_dir / BENCH_PROFILE, [chart])
    previous_profile = terminal.profile_last()
    terminal.set_profile_last(BENCH_PROFILE)
    recorder.step(
        "profile",
        {
            "template": str(template_path),
            "written": [str(path) for path in written],
            "profile_last_was": previous_profile,
            "inputs": dict(inputs),
        },
    )
    recorder.section(
        "Profile",
        "\n".join(
            [
                f"- template: `{template_path}`",
                f"- chart: `{written[0]}` ({args.symbol}, M1, TickStreamer)",
                f"- `ProfileLast`: `{previous_profile}` -> `{BENCH_PROFILE}`",
                "- inputs: "
                + ", ".join(f"`{k}={v}`" for k, v in inputs if k.startswith("Inp")),
            ]
        ),
    )

    # 3. Bridge first, then the terminal: an EA that starts against a dead port
    #    spends its first seconds in the reconnect backoff.
    bridge = BridgeProcess(
        repo_root=config.repo_root,
        log_path=results_root / "bridge.log",
        stats_interval_s=args.bridge_stats_interval,
    )
    _say("starting the bridge ...")
    bridge.start()

    started_at = datetime.now()
    _say("starting the terminal ...")
    terminal.start()
    try:
        started = terminal.wait_for_expert(since=started_at, timeout=args.start_timeout)
        connected = terminal.wait_for_expert(
            since=started_at, timeout=args.start_timeout, want="connected"
        )
    except TerminalError as exc:
        recorder.section("EA start FAILED", f"```\n{exc}\n```")
        _say(str(exc))
        return 1
    recorder.step("ea_start", {"started": started.raw, "connected": connected.raw})
    recorder.section(
        "EA start",
        f"```\n{started.raw}\n{connected.raw}\n```",
    )

    # 4. Verification against the live feeder.
    _say("verifying REST + WebSocket against the live EA ...")
    report = asyncio.run(
        verify(
            http_base=bridge.http_base,
            ws_url=bridge.ws_url,
            compare_window_s=args.compare_window,
        )
    )
    recorder.artifact("verify.json", json.dumps(report.as_dict(), indent=2, default=str))
    recorder.step("verify", {"passed": report.passed, "checks": len(report.results)})
    recorder.section("WS/REST verification", report.to_markdown())
    _say(f"verification: {'PASS' if report.passed else 'FAIL'}")

    # 5. The harness, for one short window.
    _say(f"running symbol_scaling for {args.scaling_seconds:.0f}s ...")
    csv_path = results_root / "symbol_scaling-smoke.csv"
    scaling = subprocess.run(
        [
            sys.executable,
            str(config.repo_root / "benchmarks" / "symbol_scaling.py"),
            "--url",
            bridge.ws_url,
            "--seconds",
            str(args.scaling_seconds),
            "--label",
            "smoke",
            "--csv",
            str(csv_path),
        ],
        cwd=config.repo_root,
        capture_output=True,
        text=True,
        check=False,
        timeout=args.scaling_seconds + 120,
    )
    recorder.step(
        "symbol_scaling",
        {"returncode": scaling.returncode, "csv": str(csv_path)},
    )
    recorder.section(
        "symbol_scaling (30 s, chart symbol only)",
        scaling.stdout.strip() or f"```\n{scaling.stderr.strip()}\n```",
    )

    # 6. The EA's own view of the same window.
    log = ExpertLog(config.logs_dir)
    stats = log.latest_stats(since=started_at)
    recorder.step("ea_stats", {"line": stats.raw if stats else None})
    recorder.section(
        "EA stats line",
        f"```\n{stats.raw}\n```" if stats else "_no stats line yet_",
    )

    # 7. The watchdog: take the bridge away and watch the EA notice.
    #
    #    The window is *waited out and then read*, not polled. The terminal
    #    flushes the Experts log in batches, so a poll during the outage sees
    #    one line where the EA has already written six -- the first attempt at
    #    this reported a single reconnect for a 20 s outage that actually held
    #    six. Reading after the link is back gets the flushed truth.
    _say("stopping the bridge to exercise the EA reconnect watchdog ...")
    watchdog_from = datetime.now()
    bridge.stop()
    time.sleep(args.watchdog_seconds)

    reconnected: str | None = None
    if args.keep_bridge:
        _say("restarting the bridge ...")
        bridge.start()
        line = log.wait_for(
            lambda entry: classify(entry.message) == "connected",
            since=watchdog_from,
            timeout=60.0,
        )
        reconnected = line.raw if line else None

    attempts = [
        entry.raw
        for entry in log.read(since=watchdog_from)
        if classify(entry.message) in ("connect_failed", "reconnect", "socket_error")
    ]
    recorder.step("watchdog", {"attempts": attempts, "reconnected": reconnected})
    recorder.section(
        f"Reconnect watchdog (bridge down {args.watchdog_seconds:.0f} s)",
        ("```\n" + "\n".join(attempts[:3]) + "\n```")
        if attempts
        else "_no reconnect lines observed_",
    )
    if args.keep_bridge:
        recorder.section(
            "Reconnect",
            f"```\n{reconnected}\n```" if reconnected else "_EA did not reconnect within 60 s_",
        )

    final_stats = http_get_json(bridge.api("stats")) if args.keep_bridge else None
    recorder.step("done", {"stats": final_stats})
    recorder.section(
        "State left behind",
        "\n".join(
            [
                "- terminal: **running**, profile `TickBench`, HEAD build",
                f"- bridge: {'**running**' if args.keep_bridge else 'stopped'} "
                f"(log `{bridge.log_path}`)",
                f"- `ProfileLast` was `{previous_profile}` before this run",
                f"- backups: `*{args.backup_suffix}` next to each replaced file",
            ]
        ),
    )
    _say(f"done. results: {results_root}")
    return 0 if report.passed else 2


# -- the sweep -------------------------------------------------------------


def symbol_universe(config: LiveConfig) -> list[str]:
    """Every broker symbol that has a tick database, ``#``-suffixed, sorted.

    The terminal keeps one directory per instrument under
    ``bases\\<server>\\ticks``, so this is the broker's own answer to "what can
    be streamed" -- more reliable than Market Watch, which holds only what
    somebody happened to add. The EA calls ``SymbolSelect`` for anything it is
    asked to collect, so a name that is not in Market Watch yet still works,
    and after the discovery pass Market Watch holds them all -- which is what
    makes the later ``InpSymbols="*"`` run mean "all of these".
    """
    base = config.data_dir / "bases"
    names: set[str] = set()
    for server in base.iterdir() if base.exists() else []:
        ticks = server / "ticks"
        if not ticks.is_dir():
            continue
        names.update(path.name for path in ticks.iterdir() if path.is_dir())
    # The "#" suffix is the account's own instrument set. The 19 unsuffixed
    # names alongside them are NOT dead entries -- the N=all run, which streams
    # every Market Watch symbol, showed them ticking in parallel with their "#"
    # twins. They are excluded because a sweep wants one instrument per market,
    # not the same market twice under two names, which would let one busy pair
    # occupy two slots of a ranked set.
    return sorted(name for name in names if name.endswith("#"))


@dataclass
class _Sweep:
    """Everything one sweep run needs to restart the terminal and measure."""

    config: LiveConfig
    terminal: Terminal
    bridge: BridgeProcess
    recorder: Recorder
    log: ExpertLog
    template: profile.ChartFile
    chart_symbol: str
    results_root: Path
    artifacts: dict[str, builds.BuildArtifacts]
    backup_suffix: str
    stats_sec: int = 30
    warmup_s: float = 60.0
    measure_s: float = 60.0
    connect_timeout: float = 90.0
    spy_period: int = profile.PERIOD_MN1
    installed: str = ""
    baseline_resources: dict[str, float] = field(default_factory=dict)
    """The terminal's footprint before the EA attached anything, same process.

    The sweep samples memory only *after* a run, which compares runs but cannot
    say what a change cost -- each run restarts the terminal, so "before"
    belongs to the previous process. This is sampled as soon as the new process
    exists, which is before OnInit has attached any spy, so after-minus-baseline
    is a difference within one process.
    """


def _inputs_for(sweep_run: _Sweep, spec: RunSpec) -> list[tuple[str, str]]:
    """The chart's ``<inputs>`` for one run, per build.

    The two builds keep their own defaults -- E0's ``InpTimerMs=1`` and HEAD's
    ``InpPollMs=10``. Forcing them to match would measure a hypothetical E0
    rather than the one the baseline was, so the difference is left in and the
    ``timer=`` field of each ``started`` line records it.
    """
    if spec.build == "e0":
        return profile.e0_inputs(
            symbols=spec.symbols,
            stats_sec=sweep_run.stats_sec,
            measure_tick_loss=spec.measure_tick_loss,
        )
    return profile.head_inputs(
        symbols=spec.symbols,
        extra_mode=1 if spec.mode == "event" else 0,
        stats_sec=sweep_run.stats_sec,
        spy_period=sweep_run.spy_period,
    )


def _restart_with(sweep_run: _Sweep, spec: RunSpec, *, deadline: float) -> tuple[str, str]:
    """Close, install, rewrite the chart, start, and wait for the EA.

    Returns ``(started_line, connected_line)``. Raises :class:`TerminalError`
    if either never arrives, which the caller turns into a retry and then a
    FAIL row.
    """
    sweep_run.terminal.close(timeout=60.0)
    if sweep_run.installed != spec.build:
        builds.install_artifacts(
            sweep_run.config,
            sweep_run.artifacts[spec.build],
            backup_suffix=sweep_run.backup_suffix,
        )
        sweep_run.installed = spec.build
    chart = profile.chart_for_expert(
        sweep_run.template,
        symbol=sweep_run.chart_symbol,
        inputs=_inputs_for(sweep_run, spec),
    )
    profile.write_profile(sweep_run.config.profiles_dir / BENCH_PROFILE, [chart])

    since = datetime.now()
    sweep_run.terminal.start()
    for _ in range(40):
        usage = sweep_run.terminal.resource_usage()
        if usage:
            sweep_run.baseline_resources = usage
            break
        time.sleep(0.25)
    budget = max(5.0, min(sweep_run.connect_timeout, deadline - time.monotonic()))
    started = sweep_run.terminal.wait_for_expert(since=since, timeout=budget, want="started")
    budget = max(5.0, min(sweep_run.connect_timeout, deadline - time.monotonic()))
    connected = sweep_run.terminal.wait_for_expert(
        since=since, timeout=budget, want="connected"
    )
    return started.raw, connected.raw


def _execute_run(sweep_run: _Sweep, spec: RunSpec) -> RunOutcome:
    """One measurement: restart, warm up, measure, read the EA back.

    Retried once. A run that fails twice is recorded as FAIL and the sweep
    moves on -- one wedged window is worth less than the fourteen after it.
    """
    outcome = RunOutcome(spec)
    deadline = time.monotonic() + spec.hard_timeout_s
    attempts: list[str] = []
    for attempt in (1, 2):
        try:
            started, connected = _restart_with(sweep_run, spec, deadline=deadline)
        except TerminalError as exc:
            attempts.append(f"attempt {attempt}: {exc}")
            _say(f"  ! {spec.label} attempt {attempt} failed: {exc}")
            if time.monotonic() >= deadline:
                break
            continue
        outcome.started_line = started
        _say(f"  started: {started.split(chr(9))[-1]}")
        _say(f"  warm-up {sweep_run.warmup_s:.0f}s ...")
        warm_up(min(sweep_run.warmup_s, max(0.0, deadline - time.monotonic())))

        measure_start = datetime.now()
        csv_path = sweep_run.results_root / f"scaling-{_slug(spec.label)}.csv"
        _say(f"  measuring {sweep_run.measure_s:.0f}s ...")
        outcome.wire = collect_window(sweep_run.bridge.ws_url, sweep_run.measure_s, csv_path)
        outcome.csv_path = csv_path

        # The terminal flushes the Experts log in batches, so the stats line
        # covering this window may not be on disk the instant it ends.
        sweep_run.log.wait_for(
            lambda entry: classify(entry.message) == "stats",
            since=measure_start,
            timeout=90.0,
        )
        outcome.ea = sweep_run.log.latest_stats(since=measure_start)
        outcome.warmup_lines = [
            entry.text
            for entry in sweep_run.log.read(since=measure_start - timedelta(seconds=180))
            if classify(entry.message) in ("warmup", "spy")
        ][:6]
        outcome.note = _sanity(spec, outcome, connected=connected)
        return outcome

    outcome.status = "fail"
    outcome.note = "; ".join(attempts) or "no attempt completed"
    return outcome


def _sanity(spec: RunSpec, outcome: RunOutcome, *, connected: str) -> str:
    """Everything about a completed run that the table cannot show.

    The EVENT check is the important one: ``InpExtraMode=EXTRA_EVENT`` with no
    ``TickSpy.ex5`` degrades to polling *while still reporting* ``mode=event``,
    so a row that says event and has ``evt_n=0`` is a POLL measurement wearing
    the wrong label. That is a note on the row, not a silent pass.
    """
    notes: list[str] = []
    ea = outcome.ea
    if ea is None:
        return "no EA stats line for this window"
    if spec.mode == "event":
        if ea.mode != "event":
            notes.append(f"**mode={ea.mode!r}, expected event**")
        evt_n = ea.as_int("evt_n")
        if ea.as_int("symbols") == 0:
            # N=1 is the chart symbol alone, which OnTick delivers. There are
            # no extra symbols to spy on, so evt_n=0 is the correct answer
            # rather than the "spies are not running" failure below.
            notes.append("no extra symbols: EVENT and POLL are the same run here")
        elif evt_n <= 0:
            notes.append("**evt_n=0 -- spies not running; this is a POLL measurement**")
        else:
            late = ea.as_int("evt_late")
            notes.append(f"evt_late/evt_n = {late}/{evt_n} ({100.0 * late / evt_n:.0f}%)")
    if ea.as_int("ct_err") > 0:
        notes.append(f"ct_err={ea.as_int('ct_err')}")
    if ea.as_int("cursor_skip") > 0:
        notes.append(f"**cursor_skip={ea.as_int('cursor_skip')} -- raise EXTRA_MAX_TICKS**")
    if outcome.wire is not None and outcome.wire.seq_gaps_delta:
        notes.append(f"**seq_gaps={outcome.wire.seq_gaps_delta}**")
    if "reconnect" in connected:  # pragma: no cover - defensive
        notes.append("reconnected during start")
    return "; ".join(notes)


def _slug(label: str) -> str:
    return "".join(char if char.isalnum() else "-" for char in label).strip("-").lower()


def sweep(args: argparse.Namespace) -> int:
    """Phase (b): discovery, then E0 / HEAD-POLL / HEAD-EVENT interleaved by N."""
    config = LiveConfig.detect()
    config.check()
    results_root = config.results_dir / f"live-{date.today():%Y%m%d}"
    recorder = Recorder(results_root, title=f"Live rig -- {date.today():%Y-%m-%d}")
    terminal = Terminal(config)
    log = ExpertLog(config.logs_dir)

    universe = symbol_universe(config)
    chart_symbol = next(
        (name for name in CHART_SYMBOL_PREFERENCE if name in universe), DEFAULT_SYMBOL
    )
    skew = clock_skew()
    recorder.step(
        "sweep_start",
        {"universe": universe, "chart_symbol": chart_symbol, "skew": skew},
    )
    recorder.section(
        "Phase (b) sweep -- setup",
        "\n".join(
            [
                f"- chart symbol: **{chart_symbol}** "
                f"(preference {', '.join(CHART_SYMBOL_PREFERENCE)}; "
                "the chart symbol is delivered by `OnTick`, never collected)",
                f"- instrument universe: {len(universe)} `#`-suffixed symbols with a tick "
                "database on this broker",
                f"- clock skew: {skew.get('mean_offset_s')} s "
                "(`w32tm` server-minus-local; positive = this PC is behind, which makes "
                "`broker_lag_ms` read negative by the same amount)",
                f"- warm-up {args.warmup:.0f} s + measurement {args.measure:.0f} s per run, "
                f"`InpStatsSec={args.stats_sec}`",
            ]
        ),
    )

    _say("compiling both builds ...")
    artifacts = {
        "head": builds.prepare(
            config, name="head", builds_dir=config.results_dir / "builds", with_tools=True
        ),
        "e0": builds.prepare(
            config, name="e0", builds_dir=config.results_dir / "builds", with_tools=False
        ),
    }
    for name, artifact in artifacts.items():
        if not artifact.ok:
            recorder.section("Build FAILED", f"`{name}` did not compile; stopping.")
            return 1
    recorder.section(
        "Builds",
        "\n".join(
            f"- `{name}`: "
            + ", ".join(f"{r.source.name} {r.errors} errors" for r in artifact.compiles)
            for name, artifact in artifacts.items()
        ),
    )

    template_path = profile.find_expert_template(
        config.deleted_profiles_dir, config.profiles_dir
    )
    if template_path is None:
        recorder.section("Profile FAILED", "no `.chr` with an `<expert>` block to clone")
        return 1

    bridge = BridgeProcess(
        repo_root=config.repo_root,
        log_path=results_root / "bridge.log",
        stats_interval_s=args.bridge_stats_interval,
    )
    bridge.start()

    sweep_run = _Sweep(
        config=config,
        terminal=terminal,
        bridge=bridge,
        recorder=recorder,
        log=log,
        template=profile.ChartFile.read(template_path),
        chart_symbol=chart_symbol,
        results_root=results_root,
        artifacts=artifacts,
        backup_suffix=args.backup_suffix,
        stats_sec=args.stats_sec,
        warmup_s=args.warmup,
        measure_s=args.measure,
    )

    outcomes: list[RunOutcome] = []

    def record(outcome: RunOutcome) -> None:
        outcomes.append(outcome)
        recorder.step("run", outcome.as_dict())
        body = results_table([outcome])
        if outcome.ea is not None:
            body += f"\n\n```\n{outcome.ea.raw}\n```"
        if outcome.note:
            body += f"\n\n{outcome.note}"
        recorder.section(f"Run: {outcome.spec.label}", body)

    # -- discovery -------------------------------------------------------
    #
    # In chunks, because one chart input line holds about 28 symbol names --
    # see profile.MAX_INPUT_LINE, measured on this terminal. Each chunk also
    # leaves its symbols in Market Watch, which is what later makes
    # InpSymbols="*" mean "the whole universe" rather than "whatever was open".
    extras = [name for name in universe if name != chart_symbol]
    chunks: list[list[str]] = []
    remaining = extras
    while remaining:
        kept, remaining = profile.fit_symbols(remaining)
        chunks.append(kept)
    _say(f"discovery: {len(extras)} symbols in {len(chunks)} chunk(s) ...")

    summaries = []
    for index, chunk in enumerate(chunks, start=1):
        sweep_run.measure_s = args.discovery
        outcome = _execute_run(
            sweep_run,
            RunSpec(
                label=f"discovery {index}/{len(chunks)} HEAD-POLL",
                build="head",
                n_label="disc",
                symbols=",".join(chunk),
                mode="poll",
            ),
        )
        sweep_run.measure_s = args.measure
        record(outcome)
        if outcome.wire is not None:
            summaries.append(outcome.wire)
    if not summaries:
        recorder.section("Discovery FAILED", "no ranking; stopping the sweep.")
        bridge.stop()
        return 1
    ranking = merge_discovery(summaries)

    top10, silent10 = rank_symbols(ranking, count=10, universe=extras, exclude=chart_symbol)
    # The largest N a chart input can express. Not 50: the line cap decides it.
    big_candidates, _ = rank_symbols(
        ranking, count=len(extras), universe=extras, exclude=chart_symbol
    )
    big, big_dropped = profile.fit_symbols(big_candidates)
    ticked = {name for name, ticks, _ in ranking.ranked if ticks > 0}
    silent_big = sum(1 for name in big if name not in ticked)
    big_label = str(len(big))

    recorder.step(
        "symbol_sets",
        {
            "top10": top10,
            "big": big,
            "silent10": silent10,
            "silent_big": silent_big,
            "dropped_by_input_cap": big_dropped,
        },
    )
    recorder.section(
        "Symbol sets (ranked by measured ticks/s during discovery)",
        "\n".join(
            [
                f"- symbols that ticked at all: **{ranking.symbols_with_ticks}** of "
                f"{len(extras)} collected, over {len(chunks)} chunk(s) x "
                f"{args.discovery:.0f} s",
                f"- N=10: {', '.join(f'`{s}`' for s in top10)} "
                f"({silent10} silent during discovery)",
                f"- N={big_label}: the largest set a chart `<inputs>` line can carry "
                f"({len(','.join(big))} of {profile.input_value_budget('InpSymbols')} "
                f"characters); {silent_big} of them were silent. "
                f"**N=50 is not expressible** -- the terminal truncates the line at "
                f"{profile.MAX_INPUT_LINE} characters including the key, silently and "
                f"mid-name, so {len(big_dropped)} ranked symbols could not be asked for.",
                '- N=all: `InpSymbols="*"`, i.e. every Market Watch symbol -- which the '
                "discovery chunks have just populated with the whole universe.",
                "",
                "| # | symbol | ticks | ticks/s |",
                "|---|---|---:|---:|",
            ]
            + [
                f"| {index} | `{name}` | {ticks} | {rate:.2f} |"
                for index, (name, ticks, rate) in enumerate(ranking.ranked[:15], start=1)
            ]
        ),
    )

    # -- the run list ----------------------------------------------------
    plan: list[RunSpec] = []
    for n_label, symbols in (
        ("1", ""),
        ("10", ",".join(top10)),
        (big_label, ",".join(big)),
        ("all", "*"),
    ):
        plan.append(RunSpec(f"E0 N={n_label} loss=off", "e0", n_label, symbols, "poll"))
        if n_label in ("1", "10"):
            plan.append(
                RunSpec(
                    f"E0 N={n_label} loss=on",
                    "e0",
                    n_label,
                    symbols,
                    "poll",
                    measure_tick_loss=True,
                    hard_timeout_s=300.0,
                )
            )
        plan.append(RunSpec(f"HEAD N={n_label} POLL", "head", n_label, symbols, "poll"))
        plan.append(RunSpec(f"HEAD N={n_label} EVENT", "head", n_label, symbols, "event"))

    for spec in plan:
        _say(f"== {spec.label} ==")
        before = terminal.resource_usage() if spec.n_label == "all" else {}
        outcome = _execute_run(sweep_run, spec)
        if spec.n_label == "all":
            outcome.resources = {"before": before, "after": terminal.resource_usage()}
        record(outcome)

        if spec.label == "HEAD N=10 POLL" and outcome.status == "ok":
            _checkpoints(sweep_run, results_root)

    recorder.section("Phase (b) results", results_table(outcomes))

    # -- leave the bench in the state phase (c) wants ---------------------
    _say("leaving the terminal on HEAD-POLL N=10 ...")
    final = RunSpec("final HEAD N=10 POLL", "head", "10", ",".join(top10), "poll")
    left_ok = True
    try:
        _restart_with(sweep_run, final, deadline=time.monotonic() + 300.0)
    except TerminalError as exc:
        left_ok = False
        recorder.section("Final state WARNING", f"```\n{exc}\n```")
    bridge.stop()
    recorder.section(
        "State left behind",
        "\n".join(
            [
                f"- terminal: {'**running**' if left_ok else '**check it**'} on profile "
                f"`{BENCH_PROFILE}`, HEAD build, `InpSymbols` = the N=10 set, "
                f"chart `{chart_symbol}`",
                "- bridge: **stopped** (phase (c) starts its own; the EA is retrying "
                "127.0.0.1:9800 every ~3 s until it does)",
                f"- N=10 set: {', '.join(top10)}",
                f"- largest expressible N on this platform: {big_label} "
                "(chart `<inputs>` line cap)",
            ]
        ),
    )
    _say(f"sweep done. results: {results_root}")
    return 0


def _checkpoints(sweep_run: _Sweep, results_root: Path) -> None:
    """Between-window checks at N=10: the protocol under real load, and the watchdog.

    Between windows, never inside one: a verification opens and closes several
    subscriptions and takes the bridge away, which would show up in the
    measurement it interrupted.
    """
    _say("  checkpoint: verification under N=10 load ...")
    report = asyncio.run(
        verify(http_base=sweep_run.bridge.http_base, ws_url=sweep_run.bridge.ws_url)
    )
    (results_root / "verify-n10.json").write_text(
        json.dumps(report.as_dict(), indent=2, default=str), encoding="utf-8"
    )
    sweep_run.recorder.step("checkpoint_verify", {"passed": report.passed})
    sweep_run.recorder.section(
        "Checkpoint: WS/REST verification at N=10 (real load, multiple symbols)",
        report.to_markdown(),
    )

    _say("  checkpoint: bridge kill/restart watchdog ...")
    watchdog_from = datetime.now()
    sweep_run.bridge.stop()
    time.sleep(20.0)
    sweep_run.bridge.start()
    reconnected = sweep_run.log.wait_for(
        lambda entry: classify(entry.message) == "connected",
        since=watchdog_from,
        timeout=90.0,
    )
    attempts = [
        entry.raw
        for entry in sweep_run.log.read(since=watchdog_from)
        if classify(entry.message) in ("connect_failed", "reconnect", "socket_error")
    ]
    sweep_run.recorder.step(
        "checkpoint_watchdog",
        {"attempts": attempts, "reconnected": reconnected.raw if reconnected else None},
    )
    sweep_run.recorder.section(
        "Checkpoint: reconnect watchdog at N=10 (bridge down 20 s)",
        "```\n"
        + "\n".join(attempts[:3])
        + f"\n{reconnected.raw if reconnected else 'DID NOT RECONNECT'}\n```",
    )


def remeasure(args: argparse.Namespace) -> int:
    """Re-run N=all windows after a code change, with memory as a real delta.

    Phase (b) found EXTRA_EVENT driving the terminal to 16.8 GB at 72 symbols.
    This is the A/B that says whether a fix worked: same rung, same session,
    same harness, and a baseline taken inside each run's own process so the
    number reported is what the EA and its spies added rather than what two
    different terminal processes happened to weigh.
    """
    config = LiveConfig.detect()
    config.check()
    results_root = config.results_dir / f"live-{date.today():%Y%m%d}"
    recorder = Recorder(results_root, title=f"Live rig -- {date.today():%Y-%m-%d}")
    terminal = Terminal(config)
    log = ExpertLog(config.logs_dir)

    _say("compiling HEAD ...")
    artifacts = builds.prepare(
        config, name="head", builds_dir=config.results_dir / "builds", with_tools=True
    )
    if not artifacts.ok:
        recorder.section(
            "Rebuild FAILED",
            "\n".join(
                f"- `{r.source.name}`: {r.errors} errors\n```\n{r.tail}\n```"
                for r in artifacts.compiles
            ),
        )
        return 1

    template_path = profile.find_expert_template(
        config.deleted_profiles_dir, config.profiles_dir
    )
    if template_path is None:
        recorder.section("Profile FAILED", "no chart template with an <expert> block")
        return 1

    bridge = BridgeProcess(
        repo_root=config.repo_root,
        log_path=results_root / "bridge.log",
        stats_interval_s=10.0,
    )
    bridge.start()

    sweep_run = _Sweep(
        config=config,
        terminal=terminal,
        bridge=bridge,
        recorder=recorder,
        log=log,
        template=profile.ChartFile.read(template_path),
        chart_symbol=args.symbol,
        results_root=results_root,
        artifacts={"head": artifacts},
        backup_suffix=args.backup_suffix,
        stats_sec=30,
        warmup_s=args.warmup,
        measure_s=args.measure,
        spy_period=profile.PERIOD_M1 if args.spy_period == "M1" else profile.PERIOD_MN1,
    )

    outcomes: list[RunOutcome] = []
    for mode in args.modes.split(","):
        spec = RunSpec(
            label=f"{args.label} N=all {mode.upper()}",
            build="head",
            n_label="all",
            symbols="*",
            mode=mode,
        )
        _say(f"== {spec.label} ==")
        outcome = _execute_run(sweep_run, spec)
        outcome.resources = {
            "baseline": dict(sweep_run.baseline_resources),
            "after": terminal.resource_usage(),
            "spy_period": args.spy_period,
        }
        outcomes.append(outcome)
        recorder.step("run", outcome.as_dict())
        parts = [results_table([outcome])]
        if outcome.ea is not None:
            parts.append("```\n" + outcome.ea.raw + "\n```")
        parts.append(f"resources: {outcome.resources}")
        if outcome.warmup_lines:
            parts.append("\n".join(f"- {line}" for line in outcome.warmup_lines))
        if outcome.note:
            parts.append(outcome.note)
        recorder.section(f"Run: {spec.label}", "\n\n".join(parts))

    recorder.section(f"{args.label} (spy period {args.spy_period})", results_table(outcomes))
    bridge.stop()
    _say(f"done. results: {results_root}")
    return 0


#: The N=10 set phase (b)'s discovery pass ranked as the most active.
GROUND_TRUTH_SYMBOLS = [
    "US100Cash#",
    "GOLD#",
    "USDNOK#",
    "GBPJPY#",
    "SILVER#",
    "AUDJPY#",
    "GBPAUD#",
    "USDSEK#",
    "EURAUD#",
    "CADJPY#",
]


def count_ticks_headless(
    sweep_run: _Sweep,
    *,
    symbols: list[str],
    from_msc: int,
    to_msc: int,
    csv_name: str,
    timeout: float = 300.0,
) -> Path | None:
    r"""Run ``CountTicks.mq5`` over a window without anyone touching the GUI.

    The terminal runs a script named in a startup configuration file:
    ``[StartUp] Script=`` plus ``ScriptParameters=<file>.set`` under
    ``MQL5\Presets``, launched as ``terminal64.exe /config:<file>``
    (MetaTrader 5 help, "Configuration at Startup"). ``script_show_inputs`` on
    the script does *not* raise a dialog when the parameters come from a file
    -- verified on this terminal -- so phase (c) needs no human.

    The symbol list goes in the same 244-character budget as everything else
    (:data:`benchmarks.live.profile.MAX_INPUT_LINE`): a ``.set`` value is
    truncated at exactly the same point a chart input is, measured, so a list
    that does not fit is refused here rather than silently cut.

    Returns the CSV path, or ``None`` if the script never reported ``done``.
    """
    joined = ",".join(symbols)
    budget = profile.input_value_budget("InpSymbols")
    if len(joined) > budget:
        raise ValueError(f"symbol list is {len(joined)} characters; a .set holds {budget}")

    preset_name = "countticks.set"
    write_terminal_text(
        sweep_run.config.data_dir / "MQL5" / "Presets" / preset_name,
        "\n".join(
            [
                f"InpFromMsc={from_msc}",
                f"InpToMsc={to_msc}",
                f"InpSymbols={joined}",
                f"InpCsvFile={csv_name}",
                "",
            ]
        ),
    )
    ini = sweep_run.results_root / "countticks.ini"
    write_terminal_text(
        ini,
        "\n".join(
            [
                "[StartUp]",
                f"Symbol={sweep_run.chart_symbol}",
                "Period=M1",
                "Script=TickStreamer\\CountTicks",
                f"ScriptParameters={preset_name}",
                "ShutdownTerminal=0",
                "",
            ]
        ),
    )

    csv_path = sweep_run.config.files_dir / csv_name
    if csv_path.exists():
        csv_path.unlink()

    # Twice, if the first pass comes back empty. CopyTicksRange reads the
    # terminal's tick *database*, and a symbol whose database has never been
    # synchronised returns nothing rather than an error -- the first call is
    # what triggers the sync. This bites exactly where it is least expected:
    # E0 with the diagnostic off never calls CopyTicks at all, so an E0 window
    # counted straight after its own run reported zero for every symbol, while
    # the identical HEAD window (HEAD polls with CopyTicks) counted fine.
    for attempt in (1, 2):
        sweep_run.terminal.close(timeout=90.0)
        since = datetime.now()
        sweep_run.terminal.start(config_ini=ini)
        done = sweep_run.log.wait_for(
            lambda entry: classify(entry.message) == "counted", since=since, timeout=timeout
        )
        if done is not None:
            _say(f"  {done.text}")
        for text in [
            entry.text
            for entry in sweep_run.log.read(since=since)
            if classify(entry.message) == "count_error"
        ][:3]:
            _say(f"  ! {text}")
        if done is not None and "total=0 " not in f"{done.text} ":
            break
        if attempt == 1:
            _say("  empty count -- retrying once, the first pass synchronises")
    return csv_path if csv_path.exists() else None


def groundtruth(args: argparse.Namespace) -> int:
    """Phase (c): the terminal's own tick count against the wire's, per build.

    Two independent witnesses, which is the whole point -- an implementation
    cannot be its own witness, so the wire count comes from the bridge and the
    terminal count from ``CopyTicksRange`` over the same window.

    The windows are not measured the same way and cannot be: the wire window is
    *receive* time on this machine, ``CopyTicksRange`` filters on the broker's
    ``time_msc``. A tick landing within a few hundred milliseconds of either
    edge can fall inside one window and outside the other, so a small non-zero
    difference is the measurement's own slop. What would be a *finding* is a
    one-sided deficit that scales with symbol activity: that is coalescing.
    """
    config = LiveConfig.detect()
    config.check()
    results_root = config.results_dir / f"live-{date.today():%Y%m%d}"
    recorder = Recorder(results_root, title=f"Live rig -- {date.today():%Y-%m-%d}")
    terminal = Terminal(config)
    log = ExpertLog(config.logs_dir)

    _say("compiling both builds ...")
    artifacts = {
        "head": builds.prepare(
            config, name="head", builds_dir=config.results_dir / "builds", with_tools=True
        ),
        "e0": builds.prepare(
            config, name="e0", builds_dir=config.results_dir / "builds", with_tools=False
        ),
    }
    for name, artifact in artifacts.items():
        if not artifact.ok:
            recorder.section("Build FAILED", f"`{name}` did not compile")
            return 1

    template_path = profile.find_expert_template(
        config.deleted_profiles_dir, config.profiles_dir
    )
    if template_path is None:
        return 1

    bridge = BridgeProcess(
        repo_root=config.repo_root,
        log_path=results_root / "bridge.log",
        stats_interval_s=10.0,
    )
    bridge.start()

    sweep_run = _Sweep(
        config=config,
        terminal=terminal,
        bridge=bridge,
        recorder=recorder,
        log=log,
        template=profile.ChartFile.read(template_path),
        chart_symbol=args.symbol,
        results_root=results_root,
        artifacts=artifacts,
        backup_suffix=args.backup_suffix,
        stats_sec=30,
        warmup_s=args.warmup,
        measure_s=args.measure,
    )

    symbols = list(GROUND_TRUTH_SYMBOLS)
    # The chart symbol is on the wire too (OnTick delivers it), so it has to be
    # counted on both sides or it reads as a symbol the terminal never saw.
    counted = [args.symbol, *symbols]

    for build in args.builds.split(","):
        spec = RunSpec(
            label=f"ground truth {build.upper()} N=10",
            build=build,
            n_label="10",
            symbols=",".join(symbols),
            mode="poll",
        )
        _say(f"== {spec.label} ==")
        outcome = _execute_run(sweep_run, spec)
        recorder.step("run", outcome.as_dict())
        if outcome.wire is None:
            recorder.section(f"Ground truth {build}: FAILED", outcome.note)
            continue

        # CopyTicksRange filters on MqlTick.time_msc, which is broker server
        # time; the collector's window is UTC. Without this shift the script
        # counts a window hours away from the one that was measured.
        offset_ms = server_offset_ms(outcome.started_line or "")
        from_msc = int(outcome.wire.window_start * 1000) + offset_ms
        to_msc = int(outcome.wire.window_end * 1000) + offset_ms
        csv_name = f"TickStreamer_counts_{build}.csv"
        _say("  counting the terminal's own database ...")
        terminal_csv = count_ticks_headless(
            sweep_run,
            symbols=counted,
            from_msc=from_msc,
            to_msc=to_msc,
            csv_name=csv_name,
        )
        if terminal_csv is None:
            recorder.section(
                f"Ground truth {build}: no terminal count",
                "`CountTicks` produced no CSV -- the comparison cannot be made.",
            )
            continue

        kept = results_root / csv_name
        shutil.copy2(terminal_csv, kept)
        compared = subprocess.run(
            [
                sys.executable,
                str(config.repo_root / "benchmarks" / "compare_tick_counts.py"),
                "--wire",
                str(outcome.csv_path),
                "--terminal",
                str(kept),
            ],
            cwd=config.repo_root,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        recorder.step(
            "groundtruth",
            {
                "build": build,
                "from_msc": from_msc,
                "to_msc": to_msc,
                "wire_csv": str(outcome.csv_path),
                "terminal_csv": str(kept),
                "table": compared.stdout,
            },
        )
        recorder.section(
            f"Ground truth: {build.upper()} N=10 ({args.measure:.0f} s window)",
            "\n".join(
                [
                    f"- window: `{from_msc}` .. `{to_msc}` (broker server-time ms; "
                    f"the collector's UTC receive window shifted by the EA's reported "
                    f"server_utc_offset of {offset_ms / 3600000:+.1f} h, because "
                    f"`CopyTicksRange` filters on `MqlTick.time_msc`)",
                    f"- wire: `{outcome.csv_path}`",
                    f"- terminal: `{kept}`",
                    "",
                    "```",
                    compared.stdout.strip() or compared.stderr.strip(),
                    "```",
                    "",
                    "`lost` = terminal - wire. The two windows are bounded by"
                    " different clocks (receive time vs the broker's `time_msc`), so a"
                    " few ticks either way at the edges are the measurement's slop, not"
                    " loss.",
                    "",
                    f"The chart symbol (`{sweep_run.chart_symbol}`) is expected to read"
                    " terminal=0: it reaches the wire through `OnTick`, so nothing ever"
                    " calls `CopyTicks` on it and its tick database is never"
                    " synchronised. The ground truth is about the *extra* symbols --"
                    " they are the ones on the polled path where coalescing could occur.",
                ]
            ),
        )
        _say(compared.stdout.strip()[-400:])

    bridge.stop()
    _say(f"done. results: {results_root}")
    return 0


def _not_yet(args: argparse.Namespace) -> int:
    _say(f"`{args.command}` arrives with phase (b)/(c); the rig it needs is already here.")
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    smoke_cmd = sub.add_parser("smoke", help="one full cycle, HEAD build, chart symbol only")
    smoke_cmd.add_argument("--symbol", default=DEFAULT_SYMBOL)
    smoke_cmd.add_argument("--stats-sec", type=int, default=30)
    smoke_cmd.add_argument("--scaling-seconds", type=float, default=30.0)
    smoke_cmd.add_argument("--compare-window", type=float, default=10.0)
    smoke_cmd.add_argument("--watchdog-seconds", type=float, default=20.0)
    smoke_cmd.add_argument("--start-timeout", type=float, default=180.0)
    smoke_cmd.add_argument("--close-timeout", type=float, default=60.0)
    smoke_cmd.add_argument("--bridge-stats-interval", type=float, default=10.0)
    smoke_cmd.add_argument("--backup-suffix", default=".bak-live")
    smoke_cmd.add_argument(
        "--no-keep-bridge",
        dest="keep_bridge",
        action="store_false",
        help="leave the bridge down at the end instead of restarting it",
    )
    smoke_cmd.set_defaults(func=smoke, keep_bridge=True)

    sweep_cmd = sub.add_parser("sweep", help="phase (b): E0 / HEAD-POLL / HEAD-EVENT across N")
    sweep_cmd.add_argument("--warmup", type=float, default=60.0)
    sweep_cmd.add_argument("--measure", type=float, default=60.0)
    sweep_cmd.add_argument("--discovery", type=float, default=45.0)
    sweep_cmd.add_argument("--stats-sec", type=int, default=30)
    sweep_cmd.add_argument("--bridge-stats-interval", type=float, default=10.0)
    sweep_cmd.add_argument("--backup-suffix", default=".bak-live")
    sweep_cmd.set_defaults(func=sweep)

    re_cmd = sub.add_parser("remeasure", help="re-run N=all windows after a code change")
    re_cmd.add_argument("--label", default="remeasure")
    re_cmd.add_argument("--modes", default="poll,event")
    re_cmd.add_argument("--symbol", default="BTCUSD#")
    re_cmd.add_argument("--spy-period", choices=["M1", "MN1"], default="MN1")
    re_cmd.add_argument("--warmup", type=float, default=60.0)
    re_cmd.add_argument("--measure", type=float, default=60.0)
    re_cmd.add_argument("--backup-suffix", default=".bak-live")
    re_cmd.set_defaults(func=remeasure)

    ground = sub.add_parser("groundtruth", help="phase (c): CountTicks vs the wire")
    ground.add_argument("--builds", default="e0,head")
    ground.add_argument("--symbol", default="BTCUSD#")
    ground.add_argument("--warmup", type=float, default=60.0)
    ground.add_argument("--measure", type=float, default=120.0)
    ground.add_argument("--backup-suffix", default=".bak-live")
    ground.set_defaults(func=groundtruth)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handler: Any = args.func
    return int(handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
