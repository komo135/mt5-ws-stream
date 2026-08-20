"""Reading what the EA said.

The terminal's Experts log is the EA's only channel back to us: whether it
started, which mode it came up in, whether it reached the bridge, what the
periodic stats line says, and whether the reconnect watchdog is firing. The
rig treats it as the measurement instrument it is, so this module is written
around what makes that instrument awkward:

* **The file is UTF-16LE and is being appended to while we read it.** Handled
  in :mod:`.textfiles`; every read here is ``errors="replace"``.
* **Lines carry a time but no date.** The date is the file name
  (``MQL5\\Logs\\YYYYMMDD.log``), so a :class:`LogLine` timestamp is only
  meaningful when combined with the file it came from -- which is why
  :func:`parse_log_text` takes the date rather than inventing one.
* **Two stats-line formats exist.** E0 (commit ``760f2c3``) prints a short line
  ending at ``total_sent=``; HEAD prints that plus the symbol, poll, CopyTicks
  and spy-event fields. Phase (b) reads both, so :func:`parse_stats_line`
  never assumes a field list -- it scans ``key=value`` and returns what it
  found.
* **Not every field is spelled ``key=value``.** ``send_us avg=0 max=0`` is a
  bare token followed by two generic keys. The scan carries the bare token
  forward as a prefix, so those land as ``send_us_avg`` and ``send_us_max`` --
  the names every other timing field already uses (``ct_us_avg``,
  ``evt_us_avg``), which is what lets a report print one field list.

Local times: the terminal writes the log in *local* time, so
:class:`ExpertLog` windows are compared against :func:`datetime.now`, not UTC.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from .textfiles import read_terminal_text

__all__ = [
    "EA_PREFIX",
    "ExpertLog",
    "LogLine",
    "StatsLine",
    "classify",
    "log_path_for",
    "parse_log_text",
    "parse_stats_line",
    "server_offset_ms",
    "tickstreamer_lines",
]

#: Every line this project's MQL5 programs print starts with this.
#:
#: Deliberately without a trailing space. The EA writes ``[TickStreamer] ...``
#: but ``CountTicks.mq5`` writes ``[TickStreamer][CountTicks] ...``, and a
#: prefix that included the space silently excluded every line the ground-truth
#: script produces -- which is how a probe that had *already run* the script
#: still reported "no CountTicks lines".
EA_PREFIX = "[TickStreamer]"

_TIME_RE = re.compile(r"^(\d{2}):(\d{2}):(\d{2})\.(\d{3})$")


@dataclass(frozen=True)
class LogLine:
    """One parsed Experts-log line."""

    timestamp: datetime
    """Local time, the log file's date plus the line's clock."""

    source: str
    """The program and chart, e.g. ``TickStreamer (EURUSD#,M1)``."""

    message: str
    """Everything after the source, verbatim."""

    raw: str
    """The whole line, for evidence in a report."""

    @property
    def symbol(self) -> str | None:
        """The chart symbol from ``source``, if it names one."""
        match = re.search(r"\(([^,()]+),", self.source)
        return match.group(1) if match else None

    @property
    def text(self) -> str:
        """The message with the ``[TickStreamer] `` prefix stripped."""
        if self.message.startswith(EA_PREFIX):
            return self.message[len(EA_PREFIX) :].lstrip(" ")
        return self.message


def log_path_for(logs_dir: Path, when: date) -> Path:
    """The Experts log file for *when*. It may not exist yet."""
    return logs_dir / f"{when:%Y%m%d}.log"


def parse_log_text(text: str, *, on: date) -> list[LogLine]:
    """Parse a whole Experts log written on *on*.

    Lines the terminal wrote in some other shape (there are a few, mostly from
    the core) are skipped rather than guessed at: this is a filter for EA
    output, not a general log reader.
    """
    out: list[LogLine] = []
    for raw in text.split("\n"):
        line = _parse_line(raw, on=on)
        if line is not None:
            out.append(line)
    return out


def _parse_line(raw: str, *, on: date) -> LogLine | None:
    if not raw.strip():
        return None
    parts = raw.split("\t")
    for index, part in enumerate(parts):
        match = _TIME_RE.match(part)
        if match is None:
            continue
        if index + 2 > len(parts) - 1:
            return None
        hour, minute, second, milli = (int(group) for group in match.groups())
        stamp = datetime(on.year, on.month, on.day, hour, minute, second, milli * 1000)
        return LogLine(
            timestamp=stamp,
            source=parts[index + 1],
            message="\t".join(parts[index + 2 :]),
            raw=raw,
        )
    return None


def tickstreamer_lines(lines: Iterable[LogLine]) -> Iterator[LogLine]:
    """Only the lines the EA itself printed."""
    return (line for line in lines if line.message.startswith(EA_PREFIX))


# -- what a line means -----------------------------------------------------

_CLASSIFIERS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("started", re.compile(r"^started chart=")),
    ("stopped", re.compile(r"^stopped \(reason ")),
    ("connected", re.compile(r"^connected to ")),
    ("stats", re.compile(r"^last \d+s: ")),
    ("connect_failed", re.compile(r"^(connect to |cannot reach |connection to )")),
    ("reconnect", re.compile(r"^connection (lost|dropped right after)")),
    ("socket_error", re.compile(r"^(SocketCreate failed|SocketSend sent )")),
    ("warmup", re.compile(r"^(warm-up |warmed up )")),
    ("spy", re.compile(r"^(no tick spy for |attached \d+ of )")),
    ("clock", re.compile(r"^server-UTC offset changed ")),
    ("cursor_skip", re.compile(r"ticks or more at time_msc=")),
    ("heartbeat_cadence", re.compile(r"^heartbeats are claimed on the timer")),
    # CountTicks.mq5 -- the ground truth, phase (c).
    ("counted", re.compile(r"^\[CountTicks\] done:")),
    ("count_error", re.compile(r"^\[CountTicks\].*(failed|error)")),
)


def classify(message: str) -> str:
    """Name what an EA line is, for filtering and for the report.

    Returns ``"other"`` for anything unrecognised rather than raising: a build
    is allowed to print a line this rig has not met, and losing it from a
    category is better than losing the run.
    """
    text = message[len(EA_PREFIX) :].lstrip(" ") if message.startswith(EA_PREFIX) else message
    for name, pattern in _CLASSIFIERS:
        if pattern.search(text):
            return name
    return "other"


# -- the periodic stats line ----------------------------------------------

_STATS_HEAD_RE = re.compile(r"^last (\d+)s: ")
_RATE_RE = re.compile(r"^\(([\d.]+)/s\)$")


@dataclass(frozen=True)
class StatsLine:
    """One ``last Ns: ...`` summary, as key/value pairs.

    ``values`` holds the fields verbatim, so a build that prints a field this
    rig predates still reaches the report. :meth:`as_int` is the accessor for
    the counters a caller wants to compare.
    """

    interval_s: int
    rate_per_s: float | None
    values: dict[str, str]
    raw: str

    def as_int(self, key: str, default: int = 0) -> int:
        """One field as an integer, or *default* if absent or not a number."""
        try:
            return int(self.values[key])
        except KeyError, ValueError:
            return default

    @property
    def mode(self) -> str:
        """``poll`` or ``event``; ``""`` on a build that predates the field."""
        return self.values.get("mode", "")


def parse_stats_line(message: str) -> StatsLine | None:
    """Parse an EA stats line, or return ``None`` if *message* is not one.

    Works on both the E0 and the HEAD shape because it scans rather than
    matches: every ``key=value`` token becomes a field, a bare token becomes
    the prefix for the generic ``avg=``/``max=`` keys that follow it, and the
    ``(N.N/s)`` token becomes :attr:`StatsLine.rate_per_s`.
    """
    text = message[len(EA_PREFIX) :].lstrip(" ") if message.startswith(EA_PREFIX) else message
    head = _STATS_HEAD_RE.match(text)
    if head is None:
        return None
    body = text[head.end() :]

    values: dict[str, str] = {}
    rate: float | None = None
    prefix = ""
    for token in body.split():
        rate_match = _RATE_RE.match(token)
        if rate_match is not None:
            rate = float(rate_match.group(1))
            continue
        key, sep, value = token.partition("=")
        if not sep:
            # A bare word: the label for the generic keys that follow it.
            prefix = token
            continue
        if key in ("avg", "max") and prefix:
            key = f"{prefix}_{key}"
        else:
            prefix = ""
        values[key] = value
    return StatsLine(interval_s=int(head.group(1)), rate_per_s=rate, values=values, raw=text)


# -- reading the live file -------------------------------------------------


class ExpertLog:
    """The Experts log as a live source, windowed by time.

    Every read re-resolves the file name from the date, so a run that crosses
    midnight follows the terminal into the next day's file, and a run that
    starts before the terminal has written anything today sees an empty log
    rather than an error.
    """

    def __init__(self, logs_dir: Path) -> None:
        self.logs_dir = logs_dir

    def read(self, *, since: datetime | None = None, on: date | None = None) -> list[LogLine]:
        """Every EA line at or after *since*, from *on*'s file (default: today).

        When *since* falls on an earlier day than *on*, that day's file is read
        too -- a session started at 23:59 is still one session.
        """
        day = on if on is not None else date.today()
        days = [day]
        if since is not None and since.date() < day:
            days.insert(0, since.date())
        out: list[LogLine] = []
        for each in days:
            text = read_terminal_text(log_path_for(self.logs_dir, each), errors="replace")
            out.extend(tickstreamer_lines(parse_log_text(text, on=each)))
        if since is not None:
            out = [line for line in out if line.timestamp >= since]
        return out

    def wait_for(
        self,
        predicate: Callable[[LogLine], bool],
        *,
        since: datetime,
        timeout: float,
        poll_s: float = 0.5,
    ) -> LogLine | None:
        """Poll until an EA line at or after *since* satisfies *predicate*.

        Returns the line, or ``None`` on timeout -- a timeout is a result the
        caller reports, not an exception it has to catch.
        """
        deadline = time.monotonic() + timeout
        while True:
            for line in self.read(since=since):
                if predicate(line):
                    return line
            if time.monotonic() >= deadline:
                return None
            time.sleep(poll_s)

    def latest_stats(self, *, since: datetime) -> StatsLine | None:
        """The most recent stats line since *since*, if the EA has printed one."""
        found: StatsLine | None = None
        for line in self.read(since=since):
            parsed = parse_stats_line(line.message)
            if parsed is not None:
                found = parsed
        return found


_OFFSET_RE = re.compile(r"server_utc_offset=([+-]?[\d.]+)h")


def server_offset_ms(started_line: str) -> int:
    """The broker-server-time minus UTC offset, in milliseconds, from a started line.

    Needed because the two sides of the ground truth speak different clocks.
    The wire's window is UTC (the bridge normalises, and this machine's clock is
    UTC-based), but ``CopyTicksRange`` filters on ``MqlTick.time_msc``, which is
    **broker server time** -- the MQL5 reference does not say so, but the EA's
    own header does, and the measurement settles it: a 2-minute window passed as
    UTC to a UTC+3 broker returned zero ticks for every metal and index and a
    burst on the exotic FX pairs, which is exactly the 21:00 UTC rollover break
    three hours earlier, not the window that was asked for.

    The EA prints the offset it is currently using on every start, re-estimated
    once a minute, so it is read from the log rather than configured -- a DST
    change on the broker side then needs nothing from anyone.

    Returns 0 if the line carries no offset (an older build).
    """
    match = _OFFSET_RE.search(started_line)
    if match is None:
        return 0
    return round(float(match.group(1)) * 3600.0 * 1000.0)


def recent_window(minutes: float) -> datetime:
    """``now`` minus *minutes*, for reading back over a step just run."""
    return datetime.now() - timedelta(minutes=minutes)
