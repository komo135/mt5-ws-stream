"""Smoke test of the shipped entry point, run as real subprocesses.

Everything else imports the package. This test does what a user does -- runs
``mt5-ws-stream`` from a shell -- so packaging mistakes (a missing console script,
an import that only works from the source tree, a Windows-only event-loop bug)
show up here rather than in someone's first issue.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time

import pytest

pytestmark = pytest.mark.slow


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def wait_for_port(port: int, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise AssertionError(f"port {port} never opened")


def test_end_to_end_through_the_cli() -> None:
    tcp_port, http_port = free_port(), free_port()
    module = [sys.executable, "-m", "mt5_ws_stream"]

    # DEVNULL rather than PIPE: nothing reads these, and an unread pipe both
    # leaks a file descriptor and can deadlock a chatty child.
    bridge_cmd = [
        *module,
        "bridge",
        "--tcp-port",
        str(tcp_port),
        "--http-port",
        str(http_port),
        "--stats-interval",
        "0",
    ]
    feeder_cmd = [
        *module,
        "mock",
        "--port",
        str(tcp_port),
        "--rate",
        "300",
        "--duration",
        "8",
        "--symbols",
        "EURUSD,USDJPY",
    ]

    with subprocess.Popen(
        bridge_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    ) as bridge:
        try:
            wait_for_port(tcp_port)
            wait_for_port(http_port)

            with subprocess.Popen(
                feeder_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            ) as feeder:
                try:
                    result = subprocess.run(
                        [
                            *module,
                            "client",
                            "--url",
                            f"ws://127.0.0.1:{http_port}/ws",
                            "--bench",
                            "2",
                        ],
                        capture_output=True,
                        text=True,
                        timeout=60,
                        check=False,
                    )
                finally:
                    feeder.terminate()
        finally:
            bridge.terminate()

    assert result.returncode == 0, result.stdout + result.stderr
    assert "connected:" in result.stdout
    assert "ticks=" in result.stdout
    assert "bridge->client:" in result.stdout, "latency line missing from bench output"
