"""Starting and stopping the terminal, and editing what it reads on start.

Two rules shape this module, and both come from the same fact: **the terminal
rewrites its own configuration and profiles when it exits.**

1. ``common.ini`` and every ``*.chr`` may only be edited while the terminal is
   closed. An edit made to a running terminal is overwritten on exit, silently
   and with no error anywhere -- the worst possible failure for a measurement
   rig, because the run proceeds with the *previous* settings.
   :meth:`Terminal.set_profile_last` refuses to run if the process is up.
2. The close must be graceful. ``CloseMainWindow()`` is the programmatic
   equivalent of clicking the window's close button, so the terminal saves its
   state, flushes its logs and closes its broker connection. Killing it instead
   would leave the profile unwritten -- and on a bootstrap, would throw away
   the very chart file we were trying to harvest.

If a graceful close does not complete inside the timeout the rig **stops**. It
does not escalate to ``Stop-Process``: something is holding the terminal open
(a modal dialog, a pending order confirmation) and a human should see what.
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .config import LiveConfig
from .ealog import ExpertLog, LogLine, classify
from .textfiles import backup_once, read_terminal_text, write_terminal_text

__all__ = ["Terminal", "TerminalError", "set_ini_value"]

_PROCESS_NAME = "terminal64"


class TerminalError(RuntimeError):
    """The terminal did not do what was asked -- always a stop-and-report."""


def set_ini_value(text: str, section: str, key: str, value: str) -> str:
    """Set ``key=value`` inside ``[section]`` of an INI file's *text*.

    A pure transform over the whole file so nothing else moves: the terminal's
    ``common.ini`` holds encrypted blobs (``Environment=``, ``WebRequestUrl=``)
    that a re-serialising INI parser would be free to reformat, and comments
    and ordering are load-bearing for a file the terminal reads back.

    Raises:
        KeyError: the section, or the key inside it, does not exist. Both are
            typos rather than states worth creating -- the terminal writes
            every key it understands.
    """
    lines = text.split("\n")
    header = f"[{section}]"
    try:
        start = next(i for i, line in enumerate(lines) if line.strip() == header)
    except StopIteration as exc:
        raise KeyError(f"no [{section}] section") from exc
    prefix = f"{key}="
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line.startswith("[") and line.strip().endswith("]"):
            break
        if line.startswith(prefix):
            lines[index] = prefix + value
            return "\n".join(lines)
    raise KeyError(f"no {key} in [{section}]")


@dataclass
class Terminal:
    """One MetaTrader 5 terminal, driven from outside."""

    config: LiveConfig
    backup_suffix: str = ".bak-live"

    # -- process ---------------------------------------------------------

    def pids(self) -> list[int]:
        """PIDs of every running ``terminal64.exe``. Empty when it is not up.

        ``try``/``catch`` rather than ``-ErrorAction SilentlyContinue``: that
        switch silences the *message* but the cmdlet still fails, and PowerShell
        exits 1, so "the terminal is not running" would raise instead of
        answering. Promoting the error to terminating and swallowing it is the
        only spelling that makes an empty result a normal answer.
        """
        out = _powershell(
            f"try {{ Get-Process -Name {_PROCESS_NAME} -ErrorAction Stop "
            "| ForEach-Object { $_.Id } } catch { }"
        )
        return [int(token) for token in out.split() if token.strip().isdigit()]

    def is_running(self) -> bool:
        return bool(self.pids())

    def resource_usage(self) -> dict[str, float]:
        """Working set (MB) and total CPU seconds of the terminal process.

        A point sample, not a rate: the interesting reading is the *pair* taken
        before and after a heavy run, because CPU here is cumulative since the
        process started.
        """
        out = _powershell(
            f"try {{ Get-Process -Name {_PROCESS_NAME} -ErrorAction Stop "
            "| ForEach-Object { '{0} {1}' -f $_.WorkingSet64, $_.CPU } } catch { }"
        )
        fields = out.split()
        if len(fields) < 2:
            return {}
        try:
            return {
                "working_set_mb": round(float(fields[0]) / (1024 * 1024), 1),
                "cpu_seconds": round(float(fields[1]), 1),
            }
        except ValueError:
            return {}

    def close(self, *, timeout: float = 60.0) -> bool:
        """Ask every terminal window to close, and wait for the processes to go.

        Returns:
            ``True`` if the terminal exited (or was not running).

        Raises:
            TerminalError: it was still running after *timeout*. Deliberate:
                see the module docstring -- the rig stops rather than forcing.
        """
        if not self.is_running():
            return True
        _powershell(
            f"try {{ Get-Process -Name {_PROCESS_NAME} -ErrorAction Stop "
            "| ForEach-Object { $_.CloseMainWindow() | Out-Null } } catch { }"
        )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.is_running():
                # The profile and common.ini are written during shutdown; give
                # the last flush a beat before anyone reads or edits them.
                time.sleep(1.0)
                return True
            time.sleep(1.0)
        raise TerminalError(
            f"terminal64.exe did not exit within {timeout:.0f}s of CloseMainWindow() "
            f"(pids {self.pids()}). Something is holding it open -- stopping rather "
            "than forcing it."
        )

    def start(self, *, config_ini: Path | None = None) -> None:
        """Launch the terminal, optionally with a ``/config:`` startup file."""
        if self.is_running():
            raise TerminalError("terminal64.exe is already running")
        script = f"Start-Process -FilePath '{self.config.terminal_exe}'"
        if config_ini is not None:
            script += f" -ArgumentList '/config:{config_ini}'"
        _powershell(script)

    # -- configuration ---------------------------------------------------

    def profile_last(self) -> str | None:
        """The profile the terminal will open on its next start."""
        text = read_terminal_text(self.config.common_ini)
        for line in text.split("\n"):
            if line.startswith("ProfileLast="):
                return line[len("ProfileLast=") :]
        return None

    def set_profile_last(self, profile: str) -> None:
        """Point ``[Charts] ProfileLast`` at *profile*. Terminal must be closed."""
        self.require_closed("edit common.ini")
        path = self.config.common_ini
        backup_once(path, suffix=self.backup_suffix)
        text = read_terminal_text(path)
        write_terminal_text(path, set_ini_value(text, "Charts", "ProfileLast", profile))

    def require_closed(self, what: str) -> None:
        """Guard for anything that edits a file the terminal rewrites on exit."""
        if self.is_running():
            raise TerminalError(
                f"refusing to {what} while terminal64.exe is running "
                f"(pids {self.pids()}): the terminal rewrites it on exit."
            )

    # -- coming up -------------------------------------------------------

    def wait_for_expert(
        self,
        *,
        since: datetime,
        timeout: float = 180.0,
        want: str = "started",
    ) -> LogLine:
        """Block until the EA logs a line of category *want*.

        The Experts log is the only honest "the EA is running" signal: the
        process being up says nothing about whether the profile loaded, the
        chart opened, or Algo Trading is on.

        Raises:
            TerminalError: nothing matched inside *timeout*.
        """
        log = ExpertLog(self.config.logs_dir)
        line = log.wait_for(
            lambda entry: classify(entry.message) == want, since=since, timeout=timeout
        )
        if line is None:
            raise TerminalError(
                f"no {want!r} line from TickStreamer within {timeout:.0f}s "
                f"(log {self.config.logs_dir}). Check Algo Trading is enabled and the "
                "profile carries the chart."
            )
        return line


def _powershell(script: str) -> str:
    """Run one PowerShell command and return its stdout.

    ``-NonInteractive`` matters: a prompt from a cmdlet would hang a sweep that
    is meant to run unattended.

    **Failure is judged by stderr, not by the exit code.** ``powershell.exe
    -Command`` exits 1 whenever ``$?`` is false at the end of the script, and
    ``$?`` is false after a cmdlet that simply found nothing -- so
    ``Get-Process -Name terminal64`` with the terminal closed exits 1 with no
    error text. Treating that as a failure would mean "the terminal is not
    running" raises instead of answering, which is precisely the state
    :meth:`Terminal.close` starts by checking. A script that genuinely broke
    writes to stderr, and that is what is escalated.
    """
    result = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            script,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if result.stderr.strip():
        raise TerminalError(f"powershell failed ({result.returncode}): {result.stderr.strip()}")
    return result.stdout
