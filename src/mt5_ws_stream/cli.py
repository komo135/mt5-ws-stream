"""Command-line interface.

::

    mt5-ws-stream bridge          # run the bridge
    mt5-ws-stream client          # print ticks / measure latency
    mt5-ws-stream dashboard       # open the bundled dashboard in a browser
    mt5-ws-stream mock            # synthetic ticks for tests/CI (not market data)

``bridge`` serves everything on one port: the tick stream at ``/ws``, the REST
API under ``/api/v1``, the dashboard at ``/dashboard`` and OpenAPI at ``/docs``.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import statistics
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Protocol

from . import __version__
from .api import API_PREFIX, lag_text
from .bridge import Bridge, BridgeConfig
from .client import TickStreamClient
from .decoder import ControlFrame, DecodedFrame, FrameKind
from .feeders import MockFeeder
from .protocol import BackpressurePolicy, PayloadFormat, Tick, percentile, split_symbols

__all__ = [
    "BenchResult",
    "ClientRun",
    "TickSource",
    "bridge_config",
    "bridge_is_up",
    "build_parser",
    "client_hooks",
    "client_run",
    "collect_bench",
    "compact_stats",
    "consume_stream",
    "dashboard_path",
    "is_loopback_host",
    "main",
    "mock_feeder",
    "print_bench",
    "print_tick",
]

_DEFAULT_WS = "ws://127.0.0.1:8765/ws"
_DEFAULT_HTTP = "http://127.0.0.1:8765"
_HEALTH_TIMEOUT_S = 0.5
_LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mt5-ws-stream",
        description="Stream MetaTrader 5 ticks over WebSocket, with low latency.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    # -- bridge ----------------------------------------------------------
    bridge = sub.add_parser("bridge", help="run the TCP -> HTTP/WebSocket bridge")
    bridge.add_argument("--tcp-host", default="127.0.0.1", help="feeder bind address")
    bridge.add_argument("--tcp-port", type=int, default=9800)
    bridge.add_argument(
        "--ws-host",
        default="127.0.0.1",
        help="consumer bind address (HTTP + WebSocket); "
        "0.0.0.0 exposes an UNAUTHENTICATED feed",
    )
    bridge.add_argument(
        "--http-port", type=int, default=8765, help="consumer port; serves HTTP and WebSocket"
    )
    bridge.add_argument(
        "--queue-limit",
        type=int,
        default=20_000,
        help="ticks buffered per lossless consumer before shedding (default: 20000)",
    )
    bridge.add_argument("--stats-interval", type=float, default=10.0, metavar="SECONDS")
    bridge.add_argument(
        "--allow-origin",
        action="append",
        default=None,
        metavar="ORIGIN",
        help="restrict browser Origins; repeatable",
    )
    bridge.set_defaults(func=_cmd_bridge)

    # -- mock ------------------------------------------------------------
    mock = sub.add_parser("mock", help="synthetic ticks for tests/CI (not market data)")
    mock.add_argument("--host", default="127.0.0.1")
    mock.add_argument("--port", type=int, default=9800)
    mock.add_argument("--symbols", default="EURUSD,USDJPY,GBPUSD,XAUUSD")
    mock.add_argument("--rate", type=float, default=50.0, help="ticks/second, all symbols")
    mock.add_argument("--batch", type=int, default=1, help="records per send")
    mock.add_argument("--duration", type=float, default=0.0, help="seconds; 0 = forever")
    mock.add_argument("--seed", type=int, default=None)
    mock.set_defaults(func=_cmd_mock)

    # -- client ----------------------------------------------------------
    client = sub.add_parser("client", help="consume a bridge; print ticks or benchmark")
    client.add_argument("--url", default=_DEFAULT_WS)
    client.add_argument("--symbols", default="", help="comma-separated filter")
    client.add_argument("--format", choices=["json", "binary"], default="json")
    client.add_argument("--conflate", action="store_true", help="keep newest per symbol")
    client.add_argument("--print", dest="print_ticks", action="store_true")
    client.add_argument(
        "--bench",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="collect for N seconds, then print throughput and latency percentiles",
    )
    client.set_defaults(func=_cmd_client)

    # -- dashboard -------------------------------------------------------
    dashboard = sub.add_parser("dashboard", help="open the bundled dashboard")
    dashboard.add_argument(
        "--print-path", action="store_true", help="print the file path instead of opening"
    )
    dashboard.add_argument(
        "--url",
        default=_DEFAULT_HTTP,
        metavar="URL",
        help=f"base URL of a running bridge (default: {_DEFAULT_HTTP})",
    )
    dashboard.set_defaults(func=_cmd_dashboard)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        return int(args.func(args) or 0)
    except KeyboardInterrupt:
        return 130
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


# -- commands ------------------------------------------------------------


def bridge_config(args: argparse.Namespace) -> BridgeConfig:
    """Map parsed ``bridge`` arguments onto a :class:`~mt5_ws_stream.bridge.BridgeConfig`.

    The one non-trivial transform is ``--allow-origin``: argparse's
    ``action="append"`` yields a list of raw flags or ``None`` if the flag was
    never given, and :class:`BridgeConfig` wants a ``frozenset`` or ``None``.
    """
    return BridgeConfig(
        tcp_host=args.tcp_host,
        tcp_port=args.tcp_port,
        ws_host=args.ws_host,
        http_port=args.http_port,
        queue_limit=args.queue_limit,
        stats_interval_s=args.stats_interval,
        allowed_origins=frozenset(args.allow_origin) if args.allow_origin else None,
    )


def is_loopback_host(host: str) -> bool:
    """``True`` if *host* is a loopback address a bridge may bind without a warning."""
    return host in _LOOPBACK_HOSTS


def _cmd_bridge(args: argparse.Namespace) -> int:
    config = bridge_config(args)
    if not is_loopback_host(config.ws_host):
        logging.getLogger("mt5_ws_stream.bridge").warning(
            "consumer server bound to %s (not loopback)",
            config.ws_host,
        )

    async def run() -> None:
        async with Bridge(config):
            await _wait_for_shutdown()

    _run(run())
    return 0


async def _wait_for_shutdown() -> None:
    """Block until SIGINT/SIGTERM, falling back to KeyboardInterrupt on Windows."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        # Windows event loops do not implement add_signal_handler; there,
        # KeyboardInterrupt propagates out of asyncio.run instead.
        with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
            loop.add_signal_handler(sig, stop.set)
    await stop.wait()


def mock_feeder(args: argparse.Namespace) -> MockFeeder:
    """Map parsed ``mock`` arguments onto a :class:`~mt5_ws_stream.feeders.MockFeeder`.

    ``MockFeeder`` is itself the plain-data description of a mock run -- its
    fields are public -- so this is the whole mapping; the command's I/O shell
    only has to call :meth:`~mt5_ws_stream.feeders.MockFeeder.run` on the result.
    """
    return MockFeeder(
        host=args.host,
        port=args.port,
        symbols=split_symbols(args.symbols),
        rate=args.rate,
        batch=args.batch,
        duration=args.duration,
        seed=args.seed,
    )


def _cmd_mock(args: argparse.Namespace) -> int:
    mock_feeder(args).run()
    return 0


@dataclass(slots=True, frozen=True)
class ClientRun:
    """What the ``client`` subcommand asked for, parsed from argv."""

    url: str
    symbols: tuple[str, ...] | None
    payload_format: PayloadFormat
    backpressure: BackpressurePolicy
    print_ticks: bool
    bench_seconds: float


def client_run(args: argparse.Namespace) -> ClientRun:
    """Map parsed ``client`` arguments onto a :class:`ClientRun`.

    ``--symbols`` is a comma-separated allow-list where an empty string means
    "no filter"; :class:`~mt5_ws_stream.client.TickStreamClient` wants that as
    ``None`` rather than an empty tuple, so the transform happens here.
    """
    return ClientRun(
        url=args.url,
        symbols=tuple(split_symbols(args.symbols)) or None,
        payload_format=PayloadFormat(args.format),
        backpressure=(
            BackpressurePolicy.CONFLATE if args.conflate else BackpressurePolicy.LOSSLESS
        ),
        print_ticks=args.print_ticks,
        bench_seconds=args.bench,
    )


class TickSource(Protocol):
    """What :func:`consume_stream` and :func:`collect_bench` need from a tick
    source.

    One member, so :class:`~mt5_ws_stream.client.TickStreamClient` satisfies it
    structurally, as does any fake with a matching ``stream()`` -- which is
    what makes both loops testable with no socket.
    """

    def stream(self) -> AsyncIterator[DecodedFrame]:
        """Yield decoded frames, in arrival order."""
        ...


async def consume_stream(
    source: TickSource,
    *,
    on_tick: Callable[[Tick], None],
    on_frame: Callable[[ControlFrame], None],
) -> None:
    """Demux every frame from *source* to the matching hook, forever.

    A :class:`~mt5_ws_stream.decoder.TickFrame` fans out to one *on_tick* call
    per tick it carried; anything else goes to *on_frame* whole. This is the
    ``client`` subcommand's non-bench loop (``--bench`` unset), the same shape
    as :func:`collect_bench` minus the collection window and the measuring.
    Pulling it out of ``_cmd_client`` -- and taking *source* as a
    :class:`TickSource` rather than a live
    :class:`~mt5_ws_stream.client.TickStreamClient` -- is what makes the
    ``--print`` path runnable against a fake stream, no socket or server
    required.
    """
    async for frame in source.stream():
        if isinstance(frame, ControlFrame):
            on_frame(frame)
            continue
        for tick in frame.ticks:
            on_tick(tick)


def client_hooks(
    run: ClientRun,
) -> tuple[Callable[[Tick], None], Callable[[ControlFrame], None]]:
    """Build the ``on_tick``/``on_frame`` callbacks *run* describes.

    ``on_tick`` prints only when ``run.print_ticks`` is set, and threads the
    previous call's timing through a closure rather than a module global (see
    :func:`print_tick`). ``on_frame`` echoes ``stats`` frames. Both callbacks
    are plain stdout side effects with no dependency on a live connection,
    which is what lets :func:`consume_stream` and :func:`collect_bench` be
    driven with them against a fake source in tests.
    """
    prev_print_at: float | None = None

    def on_tick(tick: Tick) -> None:
        nonlocal prev_print_at
        if run.print_ticks:
            prev_print_at = print_tick(tick, prev_print_at)

    def on_frame(frame: ControlFrame) -> None:
        if frame.kind == FrameKind.STATS:
            print("stats:", compact_stats(frame.payload))

    return on_tick, on_frame


def _cmd_client(args: argparse.Namespace) -> int:
    run = client_run(args)

    async def go() -> None:
        client = TickStreamClient(
            run.url,
            symbols=run.symbols,
            payload_format=run.payload_format,
            backpressure=run.backpressure,
        )
        async with client as stream:
            print(f"connected: {stream.url}")
            if stream.hello:
                known = ",".join(stream.hello.get("available", [])) or "(none yet)"
                print(f"server symbols: {known}")

            on_tick, on_frame = client_hooks(run)

            if run.bench_seconds:
                result = await collect_bench(
                    stream, run.bench_seconds, on_tick=on_tick, on_frame=on_frame
                )
                print_bench(result)
                return

            await consume_stream(stream, on_tick=on_tick, on_frame=on_frame)

    _run(go())
    return 0


def _cmd_dashboard(args: argparse.Namespace) -> int:
    path = dashboard_path()
    if args.print_path or not path.exists():
        print(path)
        return 0 if path.exists() else 1

    # Prefer the copy a running bridge serves: it is same-origin with the
    # WebSocket, so the browser has no mixed-content or file:// quirks to trip
    # over, and its default URL already points at the right port.
    base = args.url.rstrip("/")
    url = f"{base}/dashboard" if bridge_is_up(base) else path.as_uri()
    print(f"opening {url}")
    webbrowser.open(url)
    return 0


def bridge_is_up(base_url: str) -> bool:
    """``True`` if a bridge answers its health endpoint at *base_url*.

    Public because it is the ``dashboard`` subcommand's blocking probe and a
    legitimate seam for tests to drive directly -- exercising it through
    ``main(["dashboard"])`` would open a real browser.
    """
    request = urllib.request.Request(f"{base_url}{API_PREFIX}/health", method="GET")
    try:
        with urllib.request.urlopen(request, timeout=_HEALTH_TIMEOUT_S) as response:
            return bool(response.status == 200)
    except (urllib.error.URLError, OSError, ValueError):
        return False


def dashboard_path() -> Path:
    """Filesystem path to the bundled single-file dashboard."""
    return Path(str(resources.files("mt5_ws_stream") / "web" / "dashboard.html"))


# -- helpers -------------------------------------------------------------


def print_tick(tick: Tick, prev_perf: float | None) -> float:
    """Print one tick, plus enough timing to tell "quiet market" from "slow pipeline".

    Public because it is the ``client --print`` command's rendering, reachable
    in production only from a loop that streams forever; calling it directly
    is what makes the formatting testable at all.

    Args:
        prev_perf: ``time.perf_counter()`` value returned by the previous call,
            or ``None`` for the first tick. Kept by the caller's loop rather than
            a module global so nothing here is process-wide state.

    Returns:
        The ``perf_counter()`` value to pass back in as *prev_perf* next time.
    """
    now = time.perf_counter()
    interval_ms = 0.0 if prev_perf is None else (now - prev_perf) * 1000.0
    # Includes clock skew between this host and the broker server, hence "lag"
    # rather than "latency".
    lag_ms = int(time.time() * 1000 - tick.time_msc)
    print(
        f"{tick.symbol:<10} bid={tick.bid:<12.5f} ask={tick.ask:<12.5f} "
        f"spread={tick.spread:.5f} seq={tick.seq} "
        f"+{interval_ms:.0f}ms lag={lag_ms}ms"
    )
    return now


def compact_stats(payload: dict[str, object]) -> str:
    """The interesting few fields of a ``stats`` frame, on one line.

    Public because it is the ``client`` command's ``stats`` echo, reachable in
    production only from inside a streaming loop; calling it directly is what
    makes the formatting testable at all.

    Reads the wire payload key by key rather than validating it into
    :class:`~mt5_ws_stream.api.StatsResponse`: this is a client talking to a
    bridge it did not ship with, and a field the server added or dropped must
    not raise inside the print callback of a streaming loop.

    The two latency figures go through :func:`~mt5_ws_stream.api.lag_text`, the
    same helper the bridge's own log line uses, so ``null`` percentiles read as
    ``n/a`` on both sides of the socket instead of as ``None``.
    """
    keys = (
        "tick_rate",
        "subscribers",
        "broker_lag_ms_p50",
        "broker_lag_ms_p99",
        "seq_gaps",
        "dropped",
    )
    lag_keys = ("broker_lag_ms_p50", "broker_lag_ms_p99")
    parts = []
    for key in keys:
        if key not in payload:
            continue
        value = payload[key]
        if key in lag_keys:
            number = value if isinstance(value, (int, float)) else None
            parts.append(f"{key}={lag_text(number)}")
        else:
            parts.append(f"{key}={value}")
    return " ".join(parts)


@dataclass(slots=True)
class BenchResult:
    """What :func:`collect_bench` measured over its collection window."""

    ticks: int
    """Ticks received (control frames are not counted)."""

    elapsed_s: float
    """Wall-clock seconds actually spent collecting."""

    frames: int = 0
    """``ticks`` frames received, counted once each regardless of how many
    ticks each one carried -- a frame with zero ticks still counts.

    What ``ticks / frames`` needs for a batching figure
    (`benchmarks/bench.py`'s ``ticks/frame`` line); ``mt5-ws-stream client
    --bench`` does not print one and ignores this field.
    """

    latencies_ms: list[float] = field(default_factory=list)
    """Bridge->client hop latency, one entry per tick.

    A frame's hop is credited to every tick it carried, so this is weighted by
    tick, not by frame -- what ``mt5-ws-stream client --bench`` reports. Empty
    when the stream is binary-framed: that format carries no send timestamp,
    so there is nothing to measure.
    """

    frame_latencies_ms: list[float] = field(default_factory=list)
    """The same hop measurements as :attr:`latencies_ms`, one entry per frame
    instead of one per tick.

    What `benchmarks/bench.py` reports: it predates per-tick weighting and its
    numbers should not move when this collector gains a second consumer.
    Numerically identical to :attr:`latencies_ms` only when every frame
    carries exactly one tick.
    """


async def collect_bench(
    source: TickSource,
    seconds: float,
    *,
    on_tick: Callable[[Tick], None] | None = None,
    on_frame: Callable[[ControlFrame], None] | None = None,
    clock: Callable[[], float] = time.perf_counter,
) -> BenchResult:
    """Consume *source* for *seconds* and measure throughput and hop latency.

    This is the measurement half of ``mt5-ws-stream client --bench`` and of
    `benchmarks/bench.py`'s per-run report: it owns no I/O of its own.
    *on_tick* / *on_frame* are optional hooks a caller can use to keep printing
    ticks/stats while collection runs (that is how ``--print --bench``
    compose); the default (``None``) makes this a pure collector for testing.

    A frame's hop is measured once, from the frame itself. It is recorded two
    ways: once per frame (:attr:`BenchResult.frame_latencies_ms`) and once per
    tick the frame carried (:attr:`BenchResult.latencies_ms`) -- two different
    printers weight the same measurement differently, so both are kept rather
    than picking one and losing the other.

    Args:
        source: Where decoded frames come from.
        seconds: Stop once this many seconds have elapsed, checked once per
            frame. ``0`` means "collect everything until the source is
            exhausted" -- it never sleeps or times out on its own.
        on_tick: Called with each :class:`~mt5_ws_stream.protocol.Tick`.
        on_frame: Called with each
            :class:`~mt5_ws_stream.decoder.ControlFrame`.
        clock: Timer used for the collection window; overridable for tests.

    Returns:
        Everything :func:`print_bench` (or `benchmarks/bench.py`'s own
        printer) needs to report on the run.
    """
    ticks = 0
    frames = 0
    latencies: list[float] = []
    frame_latencies: list[float] = []
    started = clock()
    elapsed = 0.0

    async for frame in source.stream():
        if isinstance(frame, ControlFrame):
            if on_frame is not None:
                on_frame(frame)
            continue
        frames += 1
        ticks += len(frame.ticks)
        if on_tick is not None:
            for tick in frame.ticks:
                on_tick(tick)
        hop = frame.hop
        if hop is not None:
            hop_ms = hop * 1000.0
            latencies.extend([hop_ms] * len(frame.ticks))
            frame_latencies.append(hop_ms)
        elapsed = clock() - started
        if seconds and elapsed >= seconds:
            break

    return BenchResult(
        ticks=ticks,
        elapsed_s=elapsed,
        frames=frames,
        latencies_ms=latencies,
        frame_latencies_ms=frame_latencies,
    )


def print_bench(result: BenchResult) -> None:
    """Print the throughput/latency summary :func:`collect_bench` measured.

    Public for the same reason as :func:`print_tick`: it is the ``--bench``
    rendering, otherwise reachable only at the end of a real collection run.
    """
    print(
        f"\n{result.elapsed_s:.2f}s  ticks={result.ticks} "
        f"({result.ticks / max(result.elapsed_s, 1e-9):.0f}/s)"
    )
    if not result.latencies_ms:
        print("  (binary format carries no send timestamp; use --format json to measure)")
        return
    ordered = sorted(result.latencies_ms)
    p50 = percentile(ordered, 0.50)
    p99 = percentile(ordered, 0.99)
    assert p50 is not None  # `ordered` is non-empty here
    assert p99 is not None  # `ordered` is non-empty here
    print(
        f"  bridge->client: p50={p50:.3f}ms  p99={p99:.3f}ms  "
        f"max={ordered[-1]:.3f}ms  mean={statistics.fmean(ordered):.3f}ms"
    )


def _run(coro: object) -> None:
    if os.name == "nt":
        # The Proactor loop does not support add_signal_handler, and the selector
        # loop is the better fit for many small sockets anyway.
        asyncio.set_event_loop_policy(
            asyncio.WindowsSelectorEventLoopPolicy()  # type: ignore[attr-defined,unused-ignore]
        )
    asyncio.run(coro)  # type: ignore[arg-type,unused-ignore]


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
