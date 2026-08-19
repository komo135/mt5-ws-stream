"""The live rig: drive a real MetaTrader 5 terminal from Python.

Everything the manual wizard (``benchmarks/wizard_baseline_sweep.sh``) asks a
human to do -- compile a build, install it, put the EA on a chart with a given
set of inputs, restart the terminal, read the Experts log, run the bridge and
the harness -- expressed as code so a sweep can run unattended.

The split every module keeps: *pure* functions that transform text (the chart
file, the ``common.ini`` section, the Experts log, the compile log) are
importable and unit tested; the process and filesystem work sits behind thin
classes that the tests do not touch. That is why the parsers take strings
rather than paths.

Nothing here is part of the published package -- it is a benchmark harness that
happens to need a terminal.
"""

from __future__ import annotations

__all__: list[str] = []
