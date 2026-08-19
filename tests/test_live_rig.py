"""Tests for the pure half of ``benchmarks/live`` -- the live rig.

What is tested here is every transform the rig performs on a file the terminal
owns, plus the parsers that read the terminal back. That is the half where a
bug is silent: a chart file written with the wrong encoding still *looks* like
a chart, a stats line parsed with the wrong field names still produces a
report, and both would corrupt a measurement rather than fail it.

What is deliberately *not* tested is the half that drives processes --
``Terminal.close``, ``Compiler.compile``, ``BridgeProcess.start`` -- because a
test double for those would only assert that we typed the right command line,
which reading the code already shows. The smoke run is their test.
"""

from __future__ import annotations

import codecs
import json
from datetime import date, datetime
from pathlib import Path

import pytest
from benchmarks.live import profile
from benchmarks.live.builds import parse_compile_log
from benchmarks.live.config import LiveConfig
from benchmarks.live.ealog import (
    ExpertLog,
    classify,
    log_path_for,
    parse_log_text,
    parse_stats_line,
    server_offset_ms,
    tickstreamer_lines,
)
from benchmarks.live.profile import (
    MAX_INPUT_LINE,
    ChartFile,
    ExpertBlock,
    chart_for_expert,
    e0_inputs,
    find_expert_template,
    fit_symbols,
    format_input_value,
    head_inputs,
    input_value_budget,
    write_profile,
)
from benchmarks.live.run import Recorder, clock_skew, symbol_universe
from benchmarks.live.sweep import (
    RunOutcome,
    RunSpec,
    WireSummary,
    merge_discovery,
    rank_symbols,
    results_table,
    summarise,
)
from benchmarks.live.terminal import set_ini_value
from benchmarks.live.textfiles import (
    backup_once,
    decode_terminal_text,
    encode_terminal_text,
    read_terminal_text,
    write_terminal_text,
)
from benchmarks.live.verify_ws_rest import CheckResult, Verification, _describe, _ticks_equal
from benchmarks.symbol_scaling import Aggregator, RestStats

from mt5_ws_stream.protocol import Tick

# -- fixtures: the shapes the terminal actually writes ---------------------

#: A chart carrying an EA, trimmed from the real
#: ``MQL5\\Profiles\\deleted\\13.chr`` this terminal left behind.
TEMPLATE_CHART = """<chart>
id=18496769904100
symbol=ETHUSD#
description=Ethereum vs US Dollar
period_type=0
period_size=1
digits=2
scale_fix=0
windows_total=1

<expert>
name=TickStreamer
path=Experts\\TickStreamer.ex5
expertmode=1
<inputs>
Connection=
InpHost=127.0.0.1
InpPort=9800
Symbols=
InpSymbols=
Timestamps=
InpUtcTimestamps=true
</inputs>
</expert>

<window>
height=100.000000
objects=0

<indicator>
name=Main
expertmode=0
</indicator>
</window>
</chart>
"""

#: The same file with no EA on it -- the Default profile's chart.
PLAIN_CHART = """<chart>
id=18520796638037
symbol=US100Cash#
description=US 100 Index Cash
period_type=0
period_size=5
digits=2
scale_fix=0
windows_total=1

<window>
height=100.000000
objects=0

<indicator>
name=Main
expertmode=0
</indicator>
</window>
</chart>
"""

COMMON_INI = """
[Common]
Login=75537514
Server=XMTrading-MT5 3
[Charts]
ProfileLast=Default
MaxBars=100000000
[Experts]
Enabled=1
Profile=1
"""

#: HEAD's stats line, verbatim from the EA's PrintFormat.
HEAD_STATS = (
    "[TickStreamer] last 30s: ticks=1234 (41.1/s) dropped=0 send_us avg=12 max=310 "
    "reconnects=0 total_sent=98765 symbols=10 mode=event poll_n=300 poll_us_avg=40 "
    "poll_us_max=900 poll_us_p99=800 ping_us=7000 extra_obs=2000 extra_sent=1900 "
    "ct_n=3000 ct_us_avg=30 ct_us_max=700 ct_err=0 cursor_skip=0 "
    "evt_n=1500 evt_us_avg=25 evt_us_max=400 evt_late=120 evt_bad=0"
)

#: E0's shorter line, verbatim from this terminal's 20260817 log.
E0_STATS = (
    "[TickStreamer] last 60s: ticks=0 (0.0/s) dropped=30 send_us avg=0 max=0 "
    "reconnects=0 total_sent=0"
)


def _log_text(*messages: str) -> str:
    """Wrap *messages* in the tab-separated shape the Experts log uses."""
    return "".join(
        f"XY\t0\t07:5{index}:06.212\tTickStreamer (EURUSD#,M1)\t{message}\n"
        for index, message in enumerate(messages)
    )


# -- textfiles -------------------------------------------------------------


def test_terminal_text_round_trips_through_the_terminals_own_format() -> None:
    encoded = encode_terminal_text("a\nb\n")
    assert encoded.startswith(codecs.BOM_UTF16_LE)
    assert b"\r\x00\n\x00" in encoded
    assert decode_terminal_text(encoded) == "a\nb\n"


def test_decode_accepts_utf8_and_normalises_crlf() -> None:
    assert decode_terminal_text(b"x\r\ny\r\n") == "x\ny\n"
    assert decode_terminal_text(codecs.BOM_UTF8 + b"x\r\n") == "x\n"


def test_decode_survives_a_file_being_appended_to() -> None:
    """A read that lands mid-code-unit must not raise: the log is live."""
    truncated = encode_terminal_text("hello\n")[:-1]
    assert decode_terminal_text(truncated, errors="replace").startswith("hello")


def test_read_terminal_text_treats_a_missing_file_as_empty(tmp_path: Path) -> None:
    assert read_terminal_text(tmp_path / "nope.log") == ""


def test_backup_once_keeps_the_first_copy(tmp_path: Path) -> None:
    path = tmp_path / "common.ini"
    write_terminal_text(path, "original\n")
    first = backup_once(path, suffix=".bak-live")
    assert first is not None
    write_terminal_text(path, "rig wrote this\n")
    assert backup_once(path, suffix=".bak-live") is None
    assert read_terminal_text(first) == "original\n"


def test_backup_once_on_a_missing_file_is_a_no_op(tmp_path: Path) -> None:
    assert backup_once(tmp_path / "absent", suffix=".bak") is None


# -- chart files -----------------------------------------------------------


def test_chart_reads_and_writes_top_level_keys() -> None:
    chart = ChartFile.parse(TEMPLATE_CHART)
    assert chart.get("symbol") == "ETHUSD#"
    chart.set("symbol", "EURUSD#")
    assert ChartFile.parse(chart.render()).get("symbol") == "EURUSD#"


def test_chart_set_refuses_a_key_the_terminal_never_wrote() -> None:
    chart = ChartFile.parse(TEMPLATE_CHART)
    with pytest.raises(KeyError):
        chart.set("smybol", "EURUSD#")


def test_chart_top_level_lookup_stops_at_the_first_block() -> None:
    """``expertmode`` exists inside blocks too; the top level has no such key."""
    assert ChartFile.parse(TEMPLATE_CHART).get("expertmode") is None


def test_chart_parse_rejects_a_file_that_is_not_a_chart() -> None:
    with pytest.raises(ValueError, match="not a chart file"):
        ChartFile.parse("hello\n")


def test_expert_block_is_read_back_whole() -> None:
    expert = ChartFile.parse(TEMPLATE_CHART).expert()
    assert expert is not None
    assert (expert.name, expert.path, expert.expertmode) == (
        "TickStreamer",
        "Experts\\TickStreamer.ex5",
        1,
    )
    assert expert.input_map()["InpHost"] == "127.0.0.1"
    # Group headers survive as valueless entries: the dialog shows them.
    assert expert.input_map()["Connection"] == ""


def test_a_chart_with_no_expert_reports_none() -> None:
    assert ChartFile.parse(PLAIN_CHART).expert() is None


def test_set_expert_replaces_an_existing_block_without_duplicating_it() -> None:
    chart = ChartFile.parse(TEMPLATE_CHART)
    chart.set_expert(ExpertBlock(inputs=[("InpStatsSec", "30")]))
    rendered = chart.render()
    assert rendered.count("<expert>") == 1
    expert = ChartFile.parse(rendered).expert()
    assert expert is not None
    assert expert.inputs == [("InpStatsSec", "30")]


def test_set_expert_attaches_one_to_a_chart_that_had_none() -> None:
    chart = ChartFile.parse(PLAIN_CHART)
    assert chart.expert() is None
    chart.set_expert(ExpertBlock(inputs=[("InpPort", "9800")]))
    reparsed = ChartFile.parse(chart.render())
    expert = reparsed.expert()
    assert expert is not None
    assert expert.name == "TickStreamer"
    # The block goes before the window, where the terminal writes it.
    lines = chart.render().split("\n")
    assert lines.index("<expert>") < lines.index("<window>")


def test_remove_expert_leaves_a_plain_chart() -> None:
    chart = ChartFile.parse(TEMPLATE_CHART)
    assert chart.remove_expert() is True
    assert chart.expert() is None
    assert chart.remove_expert() is False
    assert "<window>" in chart.render()


@pytest.mark.parametrize(
    ("value", "expected"),
    [(True, "true"), (False, "false"), (10, "10"), ("", ""), ("EURUSD#", "EURUSD#")],
)
def test_input_values_are_rendered_the_way_the_terminal_writes_them(
    value: profile.InputValue, expected: str
) -> None:
    assert format_input_value(value) == expected


def test_chart_for_expert_rebuilds_the_template_for_a_new_symbol() -> None:
    chart = chart_for_expert(
        ChartFile.parse(TEMPLATE_CHART),
        symbol="EURUSD#",
        inputs=head_inputs(symbols="", extra_mode=1, stats_sec=30),
    )
    assert chart.get("symbol") == "EURUSD#"
    # M1: period_type 0 is minutes, period_size 1.
    assert (chart.get("period_type"), chart.get("period_size")) == ("0", "1")
    expert = chart.expert()
    assert expert is not None
    inputs = expert.input_map()
    assert inputs["InpExtraMode"] == "1"
    assert inputs["InpStatsSec"] == "30"
    assert inputs["InpSymbols"] == ""


def test_the_two_builds_get_the_parameters_they_actually_have() -> None:
    head = dict(head_inputs())
    e0 = dict(e0_inputs(measure_tick_loss=True))
    assert "InpPollMs" in head
    assert "InpTimerMs" not in head
    assert "InpTimerMs" in e0
    assert "InpPollMs" not in e0
    assert e0["InpMeasureTickLoss"] == "true"
    assert "InpMeasureTickLoss" not in head


def test_write_profile_writes_the_charts_and_the_order_index(tmp_path: Path) -> None:
    chart = ChartFile.parse(TEMPLATE_CHART)
    written = write_profile(tmp_path / "TickBench", [chart])
    assert [path.name for path in written] == ["chart01.chr"]
    order = tmp_path / "TickBench" / "order.wnd"
    assert read_terminal_text(order) == "chart01.chr\n"
    # The terminal will only read it if it is in the terminal's own encoding.
    assert order.read_bytes().startswith(codecs.BOM_UTF16_LE)


def test_write_profile_removes_charts_a_previous_run_left(tmp_path: Path) -> None:
    directory = tmp_path / "TickBench"
    write_profile(directory, [ChartFile.parse(TEMPLATE_CHART)] * 2)
    assert (directory / "chart02.chr").exists()
    write_profile(directory, [ChartFile.parse(TEMPLATE_CHART)])
    assert not (directory / "chart02.chr").exists()
    assert read_terminal_text(directory / "order.wnd") == "chart01.chr\n"


def test_find_expert_template_picks_a_chart_that_carries_an_ea(tmp_path: Path) -> None:
    write_terminal_text(tmp_path / "plain" / "chart01.chr", PLAIN_CHART)
    write_terminal_text(tmp_path / "deleted" / "13.chr", TEMPLATE_CHART)
    found = find_expert_template(tmp_path)
    assert found is not None
    assert found.name == "13.chr"


def test_find_expert_template_returns_none_when_no_chart_ever_ran_one(tmp_path: Path) -> None:
    write_terminal_text(tmp_path / "chart01.chr", PLAIN_CHART)
    assert find_expert_template(tmp_path, tmp_path / "missing") is None


# -- common.ini ------------------------------------------------------------


def test_set_ini_value_edits_only_the_named_section() -> None:
    updated = set_ini_value(COMMON_INI, "Charts", "ProfileLast", "TickBench")
    assert "ProfileLast=TickBench" in updated
    # [Experts] has a Profile= key; the [Charts] edit must not reach it.
    assert "Profile=1" in updated
    assert updated.count("ProfileLast=") == 1
    assert "Login=75537514" in updated


def test_set_ini_value_rejects_a_key_outside_its_section() -> None:
    with pytest.raises(KeyError):
        set_ini_value(COMMON_INI, "Charts", "Enabled", "0")


def test_set_ini_value_rejects_a_missing_section() -> None:
    with pytest.raises(KeyError):
        set_ini_value(COMMON_INI, "Nope", "ProfileLast", "TickBench")


# -- the Experts log -------------------------------------------------------


def test_log_lines_take_their_date_from_the_file_and_their_time_from_the_line() -> None:
    text = _log_text("[TickStreamer] connected to 127.0.0.1:9800")
    lines = parse_log_text(text, on=date(2026, 8, 18))
    assert len(lines) == 1
    assert lines[0].timestamp == datetime(2026, 8, 18, 7, 50, 6, 212_000)
    assert lines[0].source == "TickStreamer (EURUSD#,M1)"
    assert lines[0].symbol == "EURUSD#"
    assert lines[0].text == "connected to 127.0.0.1:9800"


def test_only_the_eas_own_lines_are_kept() -> None:
    text = _log_text("[TickStreamer] connected to 127.0.0.1:9800", "Terminal: something else")
    kept = list(tickstreamer_lines(parse_log_text(text, on=date(2026, 8, 18))))
    assert [line.text for line in kept] == ["connected to 127.0.0.1:9800"]


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("[TickStreamer] started chart=EURUSD# extra_symbols=0 mode=poll", "started"),
        ("[TickStreamer] connected to 127.0.0.1:9800", "connected"),
        ("[TickStreamer] stopped (reason 4, sent 0, dropped 4227)", "stopped"),
        ("[TickStreamer] connect to 127.0.0.1:9800 timed out after 1000 ms", "connect_failed"),
        ("[TickStreamer] connection lost; reconnecting in 2000 ms", "reconnect"),
        ("[TickStreamer] warmed up 10 of 10 extra symbols in 900 ms", "warmup"),
        ("[TickStreamer] attached 10 of 10 tick spies in 30 ms", "spy"),
        (HEAD_STATS, "stats"),
        (E0_STATS, "stats"),
        ("[TickStreamer] something nobody has written yet", "other"),
    ],
)
def test_classify_names_the_lines_the_rig_waits_on(message: str, expected: str) -> None:
    assert classify(message) == expected


def test_head_stats_line_parses_every_field() -> None:
    stats = parse_stats_line(HEAD_STATS)
    assert stats is not None
    assert stats.interval_s == 30
    assert stats.rate_per_s == 41.1
    assert stats.as_int("ticks") == 1234
    assert stats.as_int("dropped") == 0
    assert stats.mode == "event"
    assert stats.as_int("evt_late") == 120
    assert stats.as_int("cursor_skip") == 0


def test_the_bare_send_us_label_becomes_the_prefix_of_its_two_fields() -> None:
    """``send_us avg=12 max=310`` is the one field pair not spelled key=value."""
    stats = parse_stats_line(HEAD_STATS)
    assert stats is not None
    assert stats.as_int("send_us_avg") == 12
    assert stats.as_int("send_us_max") == 310
    # ...and it must not swallow the fields that already carry their own name.
    assert stats.as_int("ct_us_avg") == 30
    assert stats.as_int("poll_us_p99") == 800


def test_the_e0_stats_line_parses_with_no_head_only_fields_invented() -> None:
    stats = parse_stats_line(E0_STATS)
    assert stats is not None
    assert stats.interval_s == 60
    assert stats.as_int("dropped") == 30
    assert stats.mode == ""
    assert "poll_n" not in stats.values
    assert stats.as_int("poll_n", default=-1) == -1


def test_a_line_that_is_not_a_stats_line_parses_as_none() -> None:
    assert parse_stats_line("[TickStreamer] connected to 127.0.0.1:9800") is None


def test_expert_log_reads_the_file_for_the_day_and_windows_it(tmp_path: Path) -> None:
    day = date(2026, 8, 18)
    write_terminal_text(
        log_path_for(tmp_path, day),
        _log_text(
            "[TickStreamer] started chart=EURUSD# extra_symbols=0 mode=poll",
            "[TickStreamer] connected to 127.0.0.1:9800",
        ),
    )
    log = ExpertLog(tmp_path)
    assert len(log.read(on=day)) == 2
    since = datetime(2026, 8, 18, 7, 51, 0)
    assert [line.text for line in log.read(since=since, on=day)] == [
        "connected to 127.0.0.1:9800"
    ]


def test_expert_log_on_a_day_the_terminal_has_not_written_is_empty(tmp_path: Path) -> None:
    assert ExpertLog(tmp_path).read(on=date(2026, 1, 1)) == []


def test_latest_stats_returns_the_most_recent_line(tmp_path: Path) -> None:
    day = date(2026, 8, 18)
    write_terminal_text(log_path_for(tmp_path, day), _log_text(E0_STATS, HEAD_STATS))
    stats = ExpertLog(tmp_path).latest_stats(since=datetime(2026, 8, 18, 0, 0))
    assert stats is not None
    assert stats.interval_s == 30


# -- compile logs ----------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("compiling...\nResult: 0 errors, 0 warnings, 1234 msec elapsed\n", (0, 0)),
        ("Result: 2 errors, 3 warnings, 900 msec elapsed\n", (2, 3)),
        ("Result: 0 error(s), 0 warning(s)\n", (0, 0)),
    ],
)
def test_compile_log_reports_what_metaeditor_said(text: str, expected: tuple[int, int]) -> None:
    assert parse_compile_log(text) == expected


def test_a_compile_log_with_no_result_line_counts_as_a_failure() -> None:
    """MetaEditor that never got as far as compiling is not a clean build."""
    assert parse_compile_log("MetaEditor could not open the file\n") == (1, 0)


# -- verification reporting ------------------------------------------------


def _tick(**overrides: object) -> Tick:
    fields: dict[str, object] = {
        "symbol": "EURUSD#",
        "time_msc": 1_700_000_000_123,
        "bid": 1.08501,
        "ask": 1.08504,
        "last": 0.0,
        "volume": 0.0,
        "flags": 6,
        "seq": 42,
    }
    fields.update(overrides)
    return Tick(**fields)  # type: ignore[arg-type]


def test_binary_and_json_records_compare_equal_when_they_are_the_same_record() -> None:
    assert _ticks_equal(_tick(), _tick())
    assert not _ticks_equal(_tick(), _tick(seq=43))
    assert not _ticks_equal(_tick(), _tick(bid=1.08502))


def test_a_record_renders_as_plain_json_for_the_evidence_block() -> None:
    described = _describe(_tick())
    assert described["symbol"] == "EURUSD#"
    assert json.loads(json.dumps(described))["seq"] == 42


def test_the_verification_table_marks_failures_and_counts_them() -> None:
    report = Verification(
        results=[
            CheckResult("rest_health", True, "status=ok"),
            CheckResult("ws_conflate", False, "0 frames | 0 duplicates"),
        ]
    )
    assert report.passed is False
    table = report.to_markdown()
    assert "**FAIL**" in table
    assert "1/2 checks passed." in table
    # A detail containing a pipe must not break the table.
    assert "0 frames \\| 0 duplicates" in table
    assert report.as_dict()["checks"][0]["status"] == "PASS"


def test_an_empty_verification_passes_vacuously_but_says_so() -> None:
    report = Verification()
    assert report.passed is True
    assert "0/0 checks passed." in report.to_markdown()


# -- the run record --------------------------------------------------------


def test_the_recorder_appends_rather_than_rewrites(tmp_path: Path) -> None:
    recorder = Recorder(tmp_path / "live-20260818", title="Live rig")
    recorder.step("build", {"ok": True})
    recorder.step("profile", {"template": tmp_path / "13.chr"})
    recorder.section("Build", "- ok")
    recorder.section("Profile", "- written")

    lines = recorder.steps_path.read_text(encoding="utf-8").strip().split("\n")
    assert [json.loads(line)["step"] for line in lines] == ["build", "profile"]
    # Paths are not JSON; ``default=str`` is what keeps a step from being lost.
    assert json.loads(lines[1])["template"].endswith("13.chr")

    markdown = recorder.markdown_path.read_text(encoding="utf-8")
    order = [markdown.index(part) for part in ("# Live rig", "## Build", "## Profile")]
    assert order == sorted(order)


def test_the_recorder_keeps_an_existing_report_when_a_run_is_resumed(tmp_path: Path) -> None:
    directory = tmp_path / "live-20260818"
    Recorder(directory, title="Live rig").section("First", "- one")
    Recorder(directory, title="Live rig").section("Second", "- two")
    markdown = (directory / "RESULTS.md").read_text(encoding="utf-8")
    assert markdown.count("# Live rig") == 1
    assert "## First" in markdown
    assert "## Second" in markdown


def test_clock_skew_reports_unavailability_instead_of_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*args: object, **kwargs: object) -> object:
        raise OSError("w32tm is not on this machine")

    monkeypatch.setattr("benchmarks.live.run.subprocess.run", boom)
    result = clock_skew()
    assert result["available"] is False
    assert "w32tm" in result["error"]


# -- the sweep's measurement vocabulary ------------------------------------


def _frame(rx: float, ticks: list[tuple[str, int]]) -> dict[str, object]:
    """A ``ticks`` frame carrying ``(symbol, seq)`` quotes."""
    return {
        "t": "ticks",
        "rx": rx,
        "d": [
            {
                "s": symbol,
                "ms": 1_700_000_000_000 + seq,
                "b": 1.1,
                "a": 1.1001,
                "l": 0.0,
                "v": 0.0,
                "f": 6,
                "q": seq,
            }
            for symbol, seq in ticks
        ],
    }


def _summary(*, counts: dict[str, int], elapsed_s: float = 10.0) -> WireSummary:
    aggregator = Aggregator()
    seq = 0
    for symbol, ticks in counts.items():
        for _ in range(ticks):
            seq += 1
            aggregator.on_frame(
                _frame(1_700_000_000.0, [(symbol, seq)]),
                received_wall=1_700_000_000.5,
                received_perf=float(seq),
            )
    stats = RestStats(seq_gaps=0, dropped=0, heartbeats=0, ticks=0)
    return summarise(
        aggregator,
        elapsed_s=elapsed_s,
        start_stats=stats,
        end_stats=RestStats(seq_gaps=2, dropped=1, heartbeats=0, ticks=0),
    )


def test_summarise_ranks_by_ticks_and_deltas_the_rest_counters() -> None:
    summary = _summary(counts={"QUIET#": 1, "BUSY#": 5, "MID#": 3})
    assert [name for name, _, _ in summary.ranked] == ["BUSY#", "MID#", "QUIET#"]
    assert summary.ticks == 9
    assert summary.symbols_with_ticks == 3
    assert summary.ticks_per_s == pytest.approx(0.9)
    assert (summary.seq_gaps_delta, summary.dropped_delta) == (2, 1)


def test_summarise_pools_samples_rather_than_averaging_per_symbol_medians() -> None:
    """A symbol with one tick must not weigh as much as one with hundreds."""
    summary = _summary(counts={"BUSY#": 100, "QUIET#": 1})
    assert summary.hop_p50 is not None
    assert summary.lag_p50 is not None
    # 101 pooled samples, all built the same way, so the pooled p50 is theirs.
    assert summary.hop_p50 == pytest.approx(500.0, abs=1.0)


def test_summarise_of_a_silent_window_reports_no_percentiles() -> None:
    summary = _summary(counts={})
    assert summary.ticks == 0
    assert summary.hop_p50 is None
    assert summary.lag_p99 is None


def test_rank_symbols_takes_the_busiest_and_skips_the_chart_symbol() -> None:
    summary = _summary(counts={"A#": 9, "CHART#": 8, "B#": 7, "C#": 1})
    chosen, silent = rank_symbols(
        summary, count=2, universe=["A#", "B#", "C#", "CHART#"], exclude="CHART#"
    )
    assert chosen == ["A#", "B#"]
    assert silent == 0


def test_rank_symbols_pads_from_the_universe_and_counts_the_silent_ones() -> None:
    summary = _summary(counts={"A#": 3})
    chosen, silent = rank_symbols(summary, count=3, universe=["A#", "M#", "Z#"], exclude="")
    assert chosen == ["A#", "M#", "Z#"]
    # Padding keeps N honest: the EA still collects 3, and 2 of them are silent.
    assert silent == 2


def test_rank_symbols_never_returns_more_than_asked_for() -> None:
    summary = _summary(counts={"A#": 3, "B#": 2, "C#": 1})
    chosen, _ = rank_symbols(summary, count=2, universe=["A#", "B#", "C#"], exclude="")
    assert len(chosen) == 2


def _outcome(**overrides: object) -> RunOutcome:
    spec = RunSpec(
        label="HEAD N=10 EVENT", build="head", n_label="10", symbols="A#", mode="event"
    )
    outcome = RunOutcome(spec=spec, wire=_summary(counts={"A#": 4}))
    outcome.ea = parse_stats_line(HEAD_STATS)
    for key, value in overrides.items():
        setattr(outcome, key, value)
    return outcome


def test_the_sweep_table_pulls_wire_and_ea_numbers_into_one_row() -> None:
    table = results_table([_outcome()])
    assert "`HEAD N=10 EVENT`" in table
    assert "1500/120/0" in table  # evt_n / evt_late / evt_bad
    assert "30/700" in table  # ct_us avg/max
    body = table.split("\n")[-1]
    assert body.count("|") == len(table.split("\n")[0].split("|")) - 1


def test_a_failed_run_still_gets_a_row_that_says_so() -> None:
    table = results_table(
        [_outcome(status="fail", note="terminal did not come back", wire=None)]
    )
    assert "**FAIL**" in table
    assert "terminal did not come back" in table


def test_symbol_universe_reads_the_brokers_own_tick_databases(tmp_path: Path) -> None:
    ticks = tmp_path / "bases" / "Broker-MT5 3" / "ticks"
    for name in ("EURUSD#", "GOLD#", "EURUSD", "BTCUSD#"):
        (ticks / name).mkdir(parents=True)
    config = LiveConfig(install_dir=tmp_path, data_dir=tmp_path, repo_root=tmp_path)
    # The unsuffixed duplicates are legacy entries with no live feed.
    assert symbol_universe(config) == ["BTCUSD#", "EURUSD#", "GOLD#"]


def test_symbol_universe_of_a_terminal_that_has_never_connected_is_empty(
    tmp_path: Path,
) -> None:
    config = LiveConfig(install_dir=tmp_path, data_dir=tmp_path, repo_root=tmp_path)
    assert symbol_universe(config) == []


# -- the chart input-line cap ---------------------------------------------


def test_the_input_value_budget_is_the_line_cap_minus_the_key() -> None:
    """255 including ``key=``; measured on a live terminal, not assumed."""
    assert MAX_INPUT_LINE == 255
    assert input_value_budget("InpSymbols") == 244
    assert input_value_budget("X") == 253


def test_fit_symbols_keeps_what_the_line_holds_and_hands_back_the_rest() -> None:
    names = [f"SYM{index:03d}#" for index in range(40)]  # 8 chars each
    kept, dropped = fit_symbols(names)
    assert len(kept) + len(dropped) == len(names)
    assert len(",".join(kept)) <= input_value_budget("InpSymbols")
    # One more would not fit -- that is what makes this the *largest* set.
    assert len(",".join([*kept, dropped[0]])) > input_value_budget("InpSymbols")


def test_fit_symbols_leaves_a_short_list_alone() -> None:
    kept, dropped = fit_symbols(["EURUSD#", "GBPUSD#"])
    assert kept == ["EURUSD#", "GBPUSD#"]
    assert dropped == []


def test_an_oversized_symbol_list_is_refused_rather_than_silently_cut() -> None:
    """The terminal's own answer is to truncate mid-name and say nothing."""
    too_many = ",".join(f"SYM{index:03d}#" for index in range(40))
    with pytest.raises(ValueError, match="truncates it at 244"):
        head_inputs(symbols=too_many)
    with pytest.raises(ValueError, match="truncates it at 244"):
        e0_inputs(symbols=too_many)


def test_every_market_watch_symbol_is_always_expressible() -> None:
    assert dict(head_inputs(symbols="*"))["InpSymbols"] == "*"


def test_merge_discovery_ranks_across_chunks_by_rate() -> None:
    first = _summary(counts={"SLOW#": 2, "FAST#": 9}, elapsed_s=10.0)
    second = _summary(counts={"MID#": 5}, elapsed_s=10.0)
    merged = merge_discovery([first, second])
    assert [name for name, _, _ in merged.ranked] == ["FAST#", "MID#", "SLOW#"]
    assert merged.ticks == 16
    assert merged.symbols_with_ticks == 3


# -- the ground-truth script's lines --------------------------------------


def test_countticks_lines_are_not_excluded_by_the_prefix() -> None:
    """``[TickStreamer][CountTicks]`` has no space after the tag.

    A prefix of ``"[TickStreamer] "`` silently dropped every line the ground
    truth produces, which is how a probe that had already run the script still
    reported finding none of its output.
    """
    text = _log_text(
        "[TickStreamer][CountTicks] done: symbols=30 total=4808 errors=1 csv=x.csv",
        "[TickStreamer][CountTicks] JP22: CopyTicksRange failed (error 4401)",
        "[TickStreamer] connected to 127.0.0.1:9800",
    )
    kept = list(tickstreamer_lines(parse_log_text(text, on=date(2026, 8, 18))))
    assert [classify(line.message) for line in kept] == ["counted", "count_error", "connected"]
    assert kept[0].text.startswith("[CountTicks] done:")
    # The EA's own lines still lose the tag *and* its separating space.
    assert kept[2].text == "connected to 127.0.0.1:9800"


def test_the_server_offset_is_read_from_the_line_the_ea_prints() -> None:
    """The ground truth needs it: CopyTicksRange filters on broker time."""
    started = (
        "[TickStreamer] started chart=BTCUSD# extra_symbols=10 mode=poll "
        "timer=10ms stats=30s server_utc_offset=+3.0h"
    )
    assert server_offset_ms(started) == 3 * 3600 * 1000
    assert server_offset_ms("server_utc_offset=-4.5h") == -4 * 3600 * 1000 - 1800 * 1000


def test_a_build_that_prints_no_offset_shifts_nothing() -> None:
    assert server_offset_ms("[TickStreamer] started chart=X extra_symbols=0") == 0
