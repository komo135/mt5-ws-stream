"""Compiling MQL5 sources and installing the results into the terminal.

MetaEditor compiles headlessly:

    MetaEditor64.exe /compile:"<source.mq5>" /log:"<log>"

It writes the ``.ex5`` **next to the source**, and the log -- UTF-16LE, like
everything else the platform writes -- ends with a ``Result: N errors, M
warnings`` line. Its exit code is not a reliable success signal, so
:func:`parse_compile_log` reads that line instead.

Two builds are measured:

* ``head`` -- the working tree's EA, plus ``TickSpy.ex5`` and
  ``CountTicks.ex5``. **TickSpy is not optional**: without
  ``MQL5\\Indicators\\TickStreamer\\TickSpy.ex5`` every ``iCustom()`` returns
  ``INVALID_HANDLE`` and ``InpExtraMode=EXTRA_EVENT`` degrades to polling while
  still *reporting* ``mode=event``. Installing it up front is what stops a
  whole EVENT sweep from silently measuring POLL.
* ``e0`` -- the EA source at commit ``760f2c3``, extracted with ``git show``
  into a scratch directory and compiled there. The operator never checks out an
  old commit, and the working tree is never dirty.

Installing is a file copy while the terminal is closed, with the pre-existing
``.ex5`` backed up once. The terminal loads an EA when a chart opens, so a copy
over a running terminal's file may or may not take effect -- hence the caller
holds the terminal closed around it.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .config import LiveConfig
from .textfiles import backup_once, read_terminal_text

__all__ = [
    "E0_COMMIT",
    "BuildArtifacts",
    "CompileResult",
    "Compiler",
    "extract_e0_source",
    "install",
    "parse_compile_log",
]

#: The pre-refactor EA the capacity study measures against.
E0_COMMIT = "760f2c3"

_RESULT_RE = re.compile(r"^Result:\s*(\d+)\s*error", re.MULTILINE)
_WARN_RE = re.compile(r"(\d+)\s*warning", re.IGNORECASE)


@dataclass(frozen=True)
class CompileResult:
    """The outcome of one MetaEditor run."""

    source: Path
    output: Path
    errors: int
    warnings: int
    log_text: str

    @property
    def ok(self) -> bool:
        return self.errors == 0 and self.output.exists()

    @property
    def tail(self) -> str:
        """The last few log lines, for a report."""
        return "\n".join(self.log_text.strip().split("\n")[-4:])


def parse_compile_log(text: str) -> tuple[int, int]:
    """``(errors, warnings)`` from a MetaEditor compile log.

    A log with no ``Result:`` line at all counts as one error: MetaEditor did
    not get far enough to compile, and reporting "0 errors" for that would turn
    a broken toolchain into a passing build.
    """
    match = _RESULT_RE.search(text)
    if match is None:
        return 1, 0
    errors = int(match.group(1))
    end = text.find("\n", match.start())
    line = text[match.start() :] if end == -1 else text[match.start() : end]
    warn = _WARN_RE.search(line)
    return errors, int(warn.group(1)) if warn else 0


@dataclass
class Compiler:
    """MetaEditor, run headlessly."""

    metaeditor_exe: Path

    def compile(self, source: Path, *, log_path: Path) -> CompileResult:
        """Compile *source*, writing MetaEditor's log to *log_path*."""
        log_path.parent.mkdir(parents=True, exist_ok=True)
        if log_path.exists():
            log_path.unlink()
        subprocess.run(
            [
                str(self.metaeditor_exe),
                f"/compile:{source}",
                f"/log:{log_path}",
            ],
            capture_output=True,
            check=False,
            timeout=600,
        )
        text = read_terminal_text(log_path, errors="replace")
        errors, warnings = parse_compile_log(text)
        return CompileResult(
            source=source,
            output=source.with_suffix(".ex5"),
            errors=errors,
            warnings=warnings,
            log_text=text,
        )


def install(built: Path, destination: Path, *, backup_suffix: str) -> Path | None:
    """Copy *built* to *destination*, backing the existing file up once.

    Returns the backup path if one was taken. The caller is responsible for the
    terminal being closed -- see the module docstring.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    backup = backup_once(destination, suffix=backup_suffix)
    shutil.copy2(built, destination)
    return backup


def extract_e0_source(repo_root: Path, target: Path, *, commit: str = E0_COMMIT) -> Path:
    """Write the EA source at *commit* to *target* and return it.

    ``git show`` rather than a checkout: the sweep alternates E0 and HEAD runs
    within one market session, and switching the working tree between them
    would make every other tool in the rig depend on which build was last
    measured.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["git", "show", f"{commit}:mql5/Experts/TickStreamer/TickStreamer.mq5"],
        cwd=repo_root,
        capture_output=True,
        check=True,
        timeout=60,
    )
    target.write_bytes(result.stdout)
    return target


@dataclass(frozen=True)
class BuildArtifacts:
    """Everything one build produced, already installed in the terminal."""

    name: str
    ea: Path
    spy: Path | None
    count_ticks: Path | None
    compiles: tuple[CompileResult, ...]

    @property
    def ok(self) -> bool:
        return all(result.ok for result in self.compiles)


def prepare(
    config: LiveConfig,
    *,
    name: str,
    builds_dir: Path,
    with_tools: bool,
) -> BuildArtifacts:
    """Compile build *name* (``head`` or ``e0``) and stage its ``.ex5`` files.

    Staging (a copy into ``builds_dir/<name>/``) is separate from installing --
    :func:`install_artifacts` does that, and only while the terminal is closed.
    The split is what makes a build switch mid-sweep a file copy rather than a
    recompile, so the two builds being compared stay byte-identical across
    every N, and it keeps the slow step (MetaEditor) outside the window where
    the terminal is down.

    Args:
        with_tools: also build ``TickSpy.ex5`` and ``CountTicks.ex5``. Only
            meaningful for ``head``; E0 predates both.
    """
    compiler = Compiler(config.metaeditor_exe)
    stage = builds_dir / name
    stage.mkdir(parents=True, exist_ok=True)
    results: list[CompileResult] = []

    if name == "e0":
        source = extract_e0_source(config.repo_root, stage / "TickStreamer.mq5")
    elif name == "head":
        source = config.ea_source
    else:  # pragma: no cover - guarded at the CLI
        raise ValueError(f"unknown build {name!r}")

    ea_result = compiler.compile(source, log_path=stage / "compile-TickStreamer.log")
    results.append(ea_result)
    ea_staged = stage / "TickStreamer.ex5"
    if ea_result.ok and ea_result.output != ea_staged:
        shutil.copy2(ea_result.output, ea_staged)

    spy_staged: Path | None = None
    count_staged: Path | None = None
    if with_tools:
        spy_result = compiler.compile(config.spy_source, log_path=stage / "compile-TickSpy.log")
        results.append(spy_result)
        if spy_result.ok:
            spy_staged = stage / "TickSpy.ex5"
            shutil.copy2(spy_result.output, spy_staged)
        count_result = compiler.compile(
            config.count_ticks_source, log_path=stage / "compile-CountTicks.log"
        )
        results.append(count_result)
        if count_result.ok:
            count_staged = stage / "CountTicks.ex5"
            shutil.copy2(count_result.output, count_staged)

    return BuildArtifacts(
        name=name,
        ea=ea_staged,
        spy=spy_staged,
        count_ticks=count_staged,
        compiles=tuple(results),
    )


def install_artifacts(
    config: LiveConfig, artifacts: BuildArtifacts, *, backup_suffix: str
) -> dict[str, str]:
    """Copy a staged build into the terminal. Terminal must be closed."""
    installed: dict[str, str] = {}
    target = config.experts_dir / "TickStreamer.ex5"
    install(artifacts.ea, target, backup_suffix=backup_suffix)
    installed["TickStreamer.ex5"] = str(target)
    if artifacts.spy is not None:
        spy_target = config.indicators_dir / "TickStreamer" / "TickSpy.ex5"
        install(artifacts.spy, spy_target, backup_suffix=backup_suffix)
        installed["TickSpy.ex5"] = str(spy_target)
    if artifacts.count_ticks is not None:
        count_target = config.scripts_dir / "TickStreamer" / "CountTicks.ex5"
        install(artifacts.count_ticks, count_target, backup_suffix=backup_suffix)
        installed["CountTicks.ex5"] = str(count_target)
    return installed
