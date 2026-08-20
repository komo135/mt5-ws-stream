"""The bridge as a supervised subprocess.

``python -m mt5_ws_stream bridge`` in its own process, with stdout and stderr
tee'd to a file the report can quote. Two things the rig needs from it that a
bare ``Popen`` does not give:

* **Readiness that means something.** ``Popen`` returns as soon as the process
  exists; the feeder port and the HTTP server come up later. :meth:`start`
  waits for ``GET /api/v1/health`` to answer, so the terminal is never started
  against a bridge that is not listening yet. (When it is, the EA logs the
  "connect ... timed out" line and backs off -- recoverable, but it costs the
  first seconds of every run.)
* **A restart that the EA's watchdog can be measured against.** :meth:`stop`
  followed by :meth:`start` is the reconnect checkpoint: the EA should log
  reconnect attempts while the bridge is down and reconnect when it returns.

Stopping is ``terminate()`` then, only if that is ignored, ``kill()``. The
bridge's own shutdown path closes sockets and drains subscribers; skipping it
would make every "dropped=" number at the end of a run meaningless.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import IO, Any

__all__ = ["BridgeProcess", "http_get_json"]


def http_get_json(url: str, *, timeout: float = 5.0) -> Any:
    """GET *url* and parse the JSON body."""
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


class BridgeProcess:
    """One ``mt5-ws-stream bridge`` process."""

    def __init__(
        self,
        *,
        repo_root: Path,
        log_path: Path,
        tcp_port: int = 9800,
        http_port: int = 8765,
        stats_interval_s: float = 10.0,
    ) -> None:
        self.repo_root = repo_root
        self.log_path = log_path
        self.tcp_port = tcp_port
        self.http_port = http_port
        self.stats_interval_s = stats_interval_s
        self._process: subprocess.Popen[bytes] | None = None
        self._log: IO[bytes] | None = None

    # -- addresses -------------------------------------------------------

    @property
    def http_base(self) -> str:
        return f"http://127.0.0.1:{self.http_port}"

    @property
    def ws_url(self) -> str:
        return f"ws://127.0.0.1:{self.http_port}/ws"

    def api(self, path: str) -> str:
        return f"{self.http_base}/api/v1/{path.lstrip('/')}"

    # -- lifecycle -------------------------------------------------------

    @property
    def running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def start(self, *, timeout: float = 30.0) -> None:
        """Launch the bridge and wait until ``/api/v1/health`` answers."""
        if self.running:
            return
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.log_path.open("ab")
        handle.write(f"\n=== bridge start {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n".encode())
        handle.flush()
        self._log = handle
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "mt5_ws_stream",
                "bridge",
                "--tcp-port",
                str(self.tcp_port),
                "--http-port",
                str(self.http_port),
                "--stats-interval",
                str(self.stats_interval_s),
            ],
            cwd=self.repo_root,
            stdout=handle,
            stderr=subprocess.STDOUT,
        )
        self._process = process
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"bridge exited at once; see {self.log_path}")
            try:
                http_get_json(self.api("health"), timeout=2.0)
            except urllib.error.URLError, OSError, ValueError:
                time.sleep(0.25)
            else:
                # A health check can be answered by *someone else's* bridge: a
                # leftover process still holding the port makes ours exit on
                # bind and the probe succeed anyway, and the run would then
                # measure a bridge this object cannot stop or restart.
                if process.poll() is not None:
                    raise RuntimeError(
                        f"another process is already serving {self.http_base}; ours exited "
                        f"on start (see {self.log_path}). Stop it and re-run."
                    )
                return
        self.stop()
        raise RuntimeError(f"bridge did not answer /api/v1/health within {timeout:.0f}s")

    def stop(self, *, timeout: float = 15.0) -> None:
        """Ask the bridge to shut down, then insist if it does not."""
        process = self._process
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=timeout)
        self._process = None
        if self._log is not None:
            self._log.close()
            self._log = None

    def restart(self, *, down_for_s: float = 0.0, timeout: float = 30.0) -> None:
        """Stop, optionally stay down for *down_for_s*, start again.

        The pause is the point at the reconnect checkpoint: the EA backs off
        ``InpReconnectMs`` between attempts, so the bridge must be gone long
        enough for several attempts to be logged.
        """
        self.stop()
        if down_for_s > 0:
            time.sleep(down_for_s)
        self.start(timeout=timeout)

    def __enter__(self) -> BridgeProcess:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()
