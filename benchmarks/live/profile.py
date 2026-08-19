"""The chart file: how the EA gets onto a chart without a human dragging it.

A MetaTrader *profile* is a directory under
``MQL5\\Profiles\\Charts\\<name>``. It holds one ``chartNN.chr`` per open chart
plus an ``order.wnd`` listing them in z-order. ``common.ini``'s
``[Charts] ProfileLast`` names the profile the terminal opens on start, so
"restart the terminal with this EA, on this symbol, with these inputs" reduces
to: write one ``.chr``, point ``ProfileLast`` at its profile, start.

**The format is not guessed.** A chart that carried an EA leaves an
``<expert>`` block behind, and this terminal has one in
``MQL5\\Profiles\\deleted\\13.chr`` -- a TickStreamer chart the operator closed
on 2026-08-17. :func:`find_expert_template` finds such a file and
:class:`ChartFile` clones it. The block looks like::

    <expert>
    name=TickStreamer
    path=Experts\\TickStreamer.ex5
    expertmode=1
    <inputs>
    Connection=
    InpHost=127.0.0.1
    ...
    </inputs>
    </expert>

Two things about ``<inputs>`` that the shape does not make obvious:

* ``input group`` headers appear as a bare ``Name=`` line with no value. They
  are cosmetic, and kept only so a human opening the dialog sees the familiar
  grouping.
* The terminal matches inputs **by name**. A name the build does not have is
  ignored and one that is absent takes the source default -- which is why the
  E0 and HEAD input lists can be written by the same code without either build
  having to know about the other's parameters.

``expertmode`` is the per-chart "Algo Trading" allow flag; ``1`` means the EA
may run. (The global switch is ``[Experts] Enabled`` in ``common.ini``.)

Everything in this module except :func:`find_expert_template` and
:func:`write_profile` is a pure string transform, and that is what the tests
exercise.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from .textfiles import read_terminal_text, write_terminal_text

__all__ = [
    "MAX_INPUT_LINE",
    "PERIOD_M1",
    "PERIOD_MN1",
    "ChartFile",
    "ExpertBlock",
    "InputValue",
    "chart_for_expert",
    "e0_inputs",
    "find_expert_template",
    "fit_symbols",
    "format_input_value",
    "head_inputs",
    "input_value_budget",
    "write_profile",
]

#: What an input's value may be before it is rendered for the file.
InputValue = str | int | float | bool

_EA_PATH = "Experts\\TickStreamer.ex5"
_EA_NAME = "TickStreamer"

#: The terminal truncates an ``<inputs>`` line at 255 characters, **including
#: the ``key=`` prefix**, and it does so silently -- no log line, no error.
#:
#: Measured, not assumed: a run given all 54 of this broker's symbols came up
#: with ``extra_symbols=29`` and one complaint about a symbol named ``N``. The
#: terminal had rewritten the chart file with ``InpSymbols`` cut to 244
#: characters, ending mid-name at ``...,JP225Cash#,N`` -- and 244 + len
#: ``"InpSymbols="`` is exactly 255.
#:
#: The consequence is a real bound on the EA as deployed from a chart: with
#: eight-character symbol names, ``InpSymbols`` holds about 28 of them. Past
#: that, ``"*"`` (everything in Market Watch) is the only expressible answer.
MAX_INPUT_LINE = 255

#: ``ENUM_TIMEFRAMES`` as the terminal stores it in a parameter file. The
#: encoding packs hours and minutes, so the values are not consecutive; only
#: the two this study uses are named here, and the EA's
#: ``attached ... tick spies on <period>`` line reports back which one it got.
PERIOD_M1 = 1
PERIOD_MN1 = 49153


def input_value_budget(key: str) -> int:
    """How many characters *key*'s value may hold before the terminal cuts it."""
    return MAX_INPUT_LINE - len(key) - 1


def fit_symbols(
    names: Sequence[str], *, key: str = "InpSymbols"
) -> tuple[list[str], list[str]]:
    """Split *names* into the ones that fit one input line and the ones that do not.

    Returns ``(kept, dropped)``. Used instead of writing the whole list and
    hoping, because the truncation is silent and lands *mid-name*: the run
    would proceed with one fewer symbol than requested plus one garbage entry,
    and the row would claim an N it never measured.
    """
    budget = input_value_budget(key)
    kept: list[str] = []
    length = 0
    for index, name in enumerate(names):
        addition = len(name) + (1 if index else 0)
        if length + addition > budget:
            return kept, list(names[index:])
        kept.append(name)
        length += addition
    return kept, []


def format_input_value(value: InputValue) -> str:
    """Render one input the way the terminal writes it into ``<inputs>``.

    Booleans are ``true``/``false`` (not Python's capitalised form), and an
    ``enum`` input is its integer -- ``InpExtraMode=0`` is ``EXTRA_POLL``,
    ``1`` is ``EXTRA_EVENT``. Strings pass through, including the empty string
    that means "chart symbol only" for ``InpSymbols``.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


@dataclass
class ExpertBlock:
    """The ``<expert>`` section of a chart file."""

    name: str = _EA_NAME
    path: str = _EA_PATH
    expertmode: int = 1
    inputs: list[tuple[str, str]] = field(default_factory=list)

    def render(self) -> list[str]:
        """The block's lines, without a trailing blank."""
        lines = [
            "<expert>",
            f"name={self.name}",
            f"path={self.path}",
            f"expertmode={self.expertmode}",
            "<inputs>",
        ]
        lines.extend(f"{key}={value}" for key, value in self.inputs)
        lines.append("</inputs>")
        lines.append("</expert>")
        return lines

    def input_map(self) -> dict[str, str]:
        """The inputs as a mapping, group headers included (value ``""``)."""
        return dict(self.inputs)


class ChartFile:
    """One ``chartNN.chr``, editable without understanding all of it.

    The file is kept as its lines and edited in place. That is deliberate: a
    chart carries dozens of keys (colours, scales, window geometry, drawn
    objects) that the rig has no opinion about, and a model that only
    understood the handful it cares about would drop the rest on the way out.
    So the class exposes exactly three edits -- a top-level scalar, the expert
    block, and reading either back -- and everything else survives untouched.
    """

    def __init__(self, lines: list[str]) -> None:
        self._lines = lines

    @classmethod
    def parse(cls, text: str) -> ChartFile:
        """Parse chart text (``\\n`` line endings, as :mod:`.textfiles` returns)."""
        lines = text.split("\n")
        # A file read from disk ends with a newline; drop the empty tail so the
        # line list is the file's lines and render() puts the newline back.
        if lines and lines[-1] == "":
            lines.pop()
        if not lines or lines[0].strip() != "<chart>":
            raise ValueError("not a chart file: first line is not <chart>")
        return cls(lines)

    @classmethod
    def read(cls, path: Path) -> ChartFile:
        return cls.parse(read_terminal_text(path))

    def render(self) -> str:
        """The file's text, newline-terminated."""
        return "\n".join(self._lines) + "\n"

    def write(self, path: Path) -> None:
        write_terminal_text(path, self.render())

    # -- top-level scalars ------------------------------------------------

    def _scalar_span(self) -> int:
        """Index of the first nested block, i.e. the end of the header keys."""
        for index, line in enumerate(self._lines[1:], start=1):
            if line.startswith("<"):
                return index
        return len(self._lines)

    def get(self, key: str) -> str | None:
        """One top-level ``key=value``, or ``None`` if the chart has no such key."""
        prefix = f"{key}="
        for line in self._lines[1 : self._scalar_span()]:
            if line.startswith(prefix):
                return line[len(prefix) :]
        return None

    def set(self, key: str, value: InputValue) -> None:
        """Replace one top-level ``key=value``. The key must already exist.

        Requiring it to exist is the guard that catches a typo: a chart the
        terminal wrote has every key it understands, so a name it does not
        contain is a name the terminal will not read either.
        """
        prefix = f"{key}="
        rendered = format_input_value(value)
        for index in range(1, self._scalar_span()):
            if self._lines[index].startswith(prefix):
                self._lines[index] = prefix + rendered
                return
        raise KeyError(f"chart has no top-level key {key!r}")

    # -- the expert block -------------------------------------------------

    def _expert_span(self) -> tuple[int, int] | None:
        try:
            start = self._lines.index("<expert>")
        except ValueError:
            return None
        for index in range(start, len(self._lines)):
            if self._lines[index] == "</expert>":
                return start, index + 1
        raise ValueError("chart has <expert> with no closing </expert>")

    def expert(self) -> ExpertBlock | None:
        """The chart's expert, or ``None`` if no EA is attached."""
        span = self._expert_span()
        if span is None:
            return None
        start, end = span
        block = ExpertBlock(name="", path="", expertmode=0)
        in_inputs = False
        for line in self._lines[start + 1 : end - 1]:
            if line == "<inputs>":
                in_inputs = True
                continue
            if line == "</inputs>":
                in_inputs = False
                continue
            key, _, value = line.partition("=")
            if in_inputs:
                block.inputs.append((key, value))
            elif key == "name":
                block.name = value
            elif key == "path":
                block.path = value
            elif key == "expertmode":
                block.expertmode = int(value or 0)
        return block

    def set_expert(self, block: ExpertBlock) -> None:
        """Attach *block*, replacing any expert the chart already had.

        A new block goes immediately before the first ``<window>`` -- where the
        terminal writes it -- with a blank line after it, matching the file's
        own spacing.
        """
        span = self._expert_span()
        if span is not None:
            start, end = span
            self._lines[start:end] = block.render()
            return
        try:
            anchor = self._lines.index("<window>")
        except ValueError as exc:  # pragma: no cover - a chart always has a window
            raise ValueError("chart has no <window> to insert the expert before") from exc
        # The blank line the terminal leaves between the header and the block.
        insert = anchor - 1 if anchor > 0 and self._lines[anchor - 1] == "" else anchor
        self._lines[insert:insert] = ["", *block.render()]

    def remove_expert(self) -> bool:
        """Detach the EA. Returns whether there was one."""
        span = self._expert_span()
        if span is None:
            return False
        start, end = span
        if end < len(self._lines) and self._lines[end] == "":
            end += 1
        del self._lines[start:end]
        return True


# -- input lists -----------------------------------------------------------
#
# One function per build, because the two builds genuinely have different
# parameters -- E0 has InpTimerMs and InpMeasureTickLoss, HEAD has InpPollMs,
# InpExtraMode and InpEventBackstopMs -- and a single "inputs" dict with
# optional keys would let a caller silently set a parameter the build ignores.


def _checked_symbols(value: str) -> str:
    """``InpSymbols``, refused rather than silently truncated if it is too long.

    Loud is the whole point: the terminal's own answer to an over-long input is
    to cut it mid-name and say nothing, which turns a wrong measurement into a
    plausible-looking row. :func:`fit_symbols` is how a caller chooses what to
    drop; this is what happens when nobody chose.
    """
    budget = input_value_budget("InpSymbols")
    if len(value) > budget:
        raise ValueError(
            f"InpSymbols is {len(value)} characters; the terminal truncates it at "
            f"{budget} (a chart <inputs> line is capped at {MAX_INPUT_LINE} including "
            'the key). Use fit_symbols(), or "*" for every Market Watch symbol.'
        )
    return value


def head_inputs(
    *,
    host: str = "127.0.0.1",
    port: int = 9800,
    reconnect_ms: int = 2000,
    heartbeat_ms: int = 1000,
    symbols: str = "",
    extra_mode: int = 0,
    poll_ms: int = 10,
    event_backstop_ms: int = 100,
    spy_period: int = PERIOD_MN1,
    utc_timestamps: bool = True,
    verbose: bool = True,
    stats_sec: int = 60,
) -> list[tuple[str, str]]:
    """Inputs for the HEAD build (E1+E2+E3).

    ``extra_mode`` is the ``InpExtraMode`` enum as its integer: 0 =
    ``EXTRA_POLL``, 1 = ``EXTRA_EVENT``. The EA's ``started ... mode=`` log line
    reports which one it actually came up in, so a wrong encoding is visible
    rather than silent. ``spy_period`` is likewise ``ENUM_TIMEFRAMES`` as its
    integer, and the EA echoes it through ``EnumToString`` in the
    ``attached N of M tick spies on <period>`` line for the same reason.
    """
    values: list[tuple[str, InputValue]] = [
        ("Connection", ""),
        ("InpHost", host),
        ("InpPort", port),
        ("InpReconnectMs", reconnect_ms),
        ("InpHeartbeatMs", heartbeat_ms),
        ("Symbols", ""),
        ("InpSymbols", _checked_symbols(symbols)),
        ("InpExtraMode", extra_mode),
        ("InpPollMs", poll_ms),
        ("InpEventBackstopMs", event_backstop_ms),
        ("InpSpyPeriod", spy_period),
        ("Timestamps", ""),
        ("InpUtcTimestamps", utc_timestamps),
        ("Diagnostics", ""),
        ("InpVerbose", verbose),
        ("InpStatsSec", stats_sec),
    ]
    return [(key, format_input_value(value)) for key, value in values]


def e0_inputs(
    *,
    host: str = "127.0.0.1",
    port: int = 9800,
    reconnect_ms: int = 2000,
    heartbeat_ms: int = 1000,
    symbols: str = "",
    timer_ms: int = 1,
    utc_timestamps: bool = True,
    verbose: bool = True,
    stats_sec: int = 60,
    measure_tick_loss: bool = False,
) -> list[tuple[str, str]]:
    """Inputs for the E0 baseline build (commit ``760f2c3``).

    ``measure_tick_loss`` is E0's diagnostic mode: it runs a ``CopyTicks`` per
    symbol per poll from inside ``OnTimer``, with no warm-up, so at large
    symbol counts the first pass can block the terminal for minutes. Phase (b)
    only turns it on at N=1 and N=10, and with a timeout.
    """
    values: list[tuple[str, InputValue]] = [
        ("Connection", ""),
        ("InpHost", host),
        ("InpPort", port),
        ("InpReconnectMs", reconnect_ms),
        ("InpHeartbeatMs", heartbeat_ms),
        ("Symbols", ""),
        ("InpSymbols", _checked_symbols(symbols)),
        ("InpTimerMs", timer_ms),
        ("Timestamps", ""),
        ("InpUtcTimestamps", utc_timestamps),
        ("Diagnostics", ""),
        ("InpVerbose", verbose),
        ("InpStatsSec", stats_sec),
        ("InpMeasureTickLoss", measure_tick_loss),
    ]
    return [(key, format_input_value(value)) for key, value in values]


# -- building the bench chart ----------------------------------------------

_PERIOD_M1 = (0, 1)  # (period_type, period_size): 0 = minutes


def chart_for_expert(
    template: ChartFile,
    *,
    symbol: str,
    inputs: list[tuple[str, str]],
    description: str = "",
    chart_id: int | None = None,
) -> ChartFile:
    """Clone *template* into an M1 chart on *symbol* running TickStreamer.

    The template supplies every key the rig has no opinion about. What is
    overwritten is only what identifies the chart (symbol, timeframe, id) and
    the expert block. Price-scale keys are left alone: the template's are for
    whatever symbol it was captured on, and the terminal recomputes them from
    the symbol on load because ``scale_fix=0``.
    """
    chart = ChartFile.parse(template.render())
    chart.set("symbol", symbol)
    if chart.get("description") is not None:
        chart.set("description", description)
    period_type, period_size = _PERIOD_M1
    chart.set("period_type", period_type)
    chart.set("period_size", period_size)
    if chart_id is not None:
        chart.set("id", chart_id)
    chart.set_expert(ExpertBlock(name=_EA_NAME, path=_EA_PATH, expertmode=1, inputs=inputs))
    return chart


_CHART_FILE_RE = re.compile(r"^chart\d+\.chr$", re.IGNORECASE)


def find_expert_template(*roots: Path) -> Path | None:
    """The newest ``*.chr`` under *roots* that carries an ``<expert>`` block.

    Searched rather than hard-coded so the rig still works on a machine whose
    ``deleted`` folder has been cleaned: any chart that ever ran an EA is a
    valid template, and the newest one is the most likely to match the current
    terminal build's key set.
    """
    candidates: list[tuple[float, Path]] = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*.chr"):
            text = read_terminal_text(path, errors="replace")
            if "\n<expert>\n" in text:
                candidates.append((path.stat().st_mtime, path))
    if not candidates:
        return None
    return max(candidates)[1]


def write_profile(profile_dir: Path, charts: list[ChartFile]) -> list[Path]:
    """Write *charts* as ``chart01.chr`` ... plus the ``order.wnd`` index.

    Any ``chartNN.chr`` already in the directory that this call does not write
    is removed, so a profile written twice with fewer charts does not keep the
    stale ones -- the terminal would open them.
    """
    profile_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for index, chart in enumerate(charts, start=1):
        path = profile_dir / f"chart{index:02d}.chr"
        chart.write(path)
        written.append(path)
    keep = {path.name for path in written}
    for path in profile_dir.iterdir():
        if _CHART_FILE_RE.match(path.name) and path.name not in keep:
            path.unlink()
    # order.wnd is the z-order, front to back: same format, one file name a line.
    write_terminal_text(
        profile_dir / "order.wnd", "".join(f"{path.name}\n" for path in written)
    )
    return written
