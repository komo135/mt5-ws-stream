"""Feeder and CLI tests.

The mock feeder is not just a toy: it is how the rest of the project is exercised
without MetaTrader, so its output has to be genuinely protocol-correct.
"""

from __future__ import annotations

import argparse
import asyncio
import time
from collections.abc import AsyncIterator, Callable, Iterator, Sequence

import pytest

from mt5_ws_stream import Bridge, BridgeConfig, MockFeeder
from mt5_ws_stream.cli import (
    BenchResult,
    ClientRun,
    bridge_config,
    build_parser,
    client_hooks,
    client_run,
    collect_bench,
    compact_stats,
    consume_stream,
    dashboard_path,
    is_loopback_host,
    main,
    mock_feeder,
    print_bench,
    print_tick,
)
from mt5_ws_stream.decoder import ControlFrame, DecodedFrame, TickFrame
from mt5_ws_stream.feeders import FeederConnection
from mt5_ws_stream.protocol import (
    MAGIC,
    RECORD_SIZE,
    BackpressurePolicy,
    PayloadFormat,
    Tick,
    iter_ticks,
    pack_tick,
)


def test_feeder_connection_sequence_wraps() -> None:
    link = FeederConnection(start_seq=0xFFFF_FFFF)
    assert link.next_seq() == 0xFFFF_FFFF
    assert link.next_seq() == 0


# -- the mock feeder without a socket ------------------------------------


class RecordingLink:
    """A link that keeps the bytes instead of writing them to a socket.

    The only thing :meth:`MockFeeder.run` needs from *link* is ``send`` --
    this is a plain object, not a :class:`FeederConnection` subclass, which
    is what proves the mock feeder does not require a socket-owning class to
    inject a fake.
    """

    def __init__(self) -> None:
        self.batches: list[bytes] = []
        self.closed = False

    def send(self, ticks: Sequence[Tick]) -> None:
        if not ticks:
            return
        self.batches.append(b"".join(pack_tick(t) for t in ticks))

    def close(self) -> None:
        self.closed = True

    @property
    def blob(self) -> bytes:
        return b"".join(self.batches)


def test_mock_feeder_writes_whole_records_into_an_injected_link() -> None:
    link = RecordingLink()
    sent = MockFeeder(
        symbols=["EURUSD", "USDJPY"],
        rate=2000,
        duration=0.25,
        heartbeat_interval=0.05,
        seed=11,
    ).run(link=link)

    blob = link.blob
    assert blob, "expected the feeder to write something"
    assert len(blob) % RECORD_SIZE == 0, "feeders must emit whole records"
    assert blob[:2] == MAGIC.to_bytes(2, "little"), "every record starts with the magic"

    ticks = list(iter_ticks(blob))
    assert {t.symbol for t in ticks if not t.is_heartbeat} <= {"EURUSD", "USDJPY"}
    assert [t.seq for t in ticks] == list(range(len(ticks))), "sequence must be dense"

    quotes = [t for t in ticks if not t.is_heartbeat]
    assert all(t.ask > t.bid for t in quotes)
    assert sent == len(quotes), "the return value counts quotes, not heartbeats"


def test_mock_feeder_emits_a_heartbeat_per_heartbeat_interval() -> None:
    link = RecordingLink()
    MockFeeder(
        symbols=["EURUSD"], rate=1000, duration=0.25, heartbeat_interval=0.05, seed=2
    ).run(link=link)

    heartbeats = [t for t in iter_ticks(link.blob) if t.is_heartbeat]
    # 0.25s / 0.05s = 5 in theory; sleep granularity makes the exact count
    # platform-dependent, so only assert that the timer really is periodic.
    assert len(heartbeats) >= 2


def test_an_injected_link_is_not_closed_by_the_feeder() -> None:
    link = RecordingLink()
    MockFeeder(symbols=["EURUSD"], rate=500, duration=0.05, seed=1).run(link=link)
    assert not link.closed, "the caller owns an injected link"


def test_mock_feeder_is_deterministic_with_a_seed() -> None:
    first = RecordingLink()
    MockFeeder(symbols=["EURUSD"], rate=2000, duration=0.05, seed=7).run(link=first)
    second = RecordingLink()
    MockFeeder(symbols=["EURUSD"], rate=2000, duration=0.05, seed=7).run(link=second)

    first_bids = [t.bid for t in iter_ticks(first.blob) if not t.is_heartbeat]
    second_bids = [t.bid for t in iter_ticks(second.blob) if not t.is_heartbeat]

    shared = min(len(first_bids), len(second_bids))
    assert shared > 5
    assert first_bids[:shared] == second_bids[:shared]


async def test_mock_feeder_reaches_a_bridge() -> None:
    """The advertised zero-MetaTrader smoke test actually works."""
    async with Bridge(BridgeConfig(tcp_port=0, http_port=0, stats_interval_s=0.0)) as bridge:
        feeder = MockFeeder(
            port=bridge.tcp_port, symbols=["EURUSD"], rate=300, duration=0.3, seed=5
        )
        await asyncio.get_running_loop().run_in_executor(None, feeder.run)
        await asyncio.sleep(0.1)

        stats = bridge.hub.snapshot_stats()
        assert stats.ticks > 0
        assert bridge.hub.symbols == ["EURUSD"]


# -- CLI -----------------------------------------------------------------


def test_parser_exposes_every_subcommand() -> None:
    parser = build_parser()
    for command in ("bridge", "mock", "client", "dashboard"):
        assert parser.parse_args([command]).command == command


def test_parser_defaults_are_loopback() -> None:
    args = build_parser().parse_args(["bridge"])
    assert args.tcp_host == "127.0.0.1"
    assert args.ws_host == "127.0.0.1", "must not bind a public interface by default"


def test_parser_rejects_an_unknown_subcommand() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["teleport"])


# -- Namespace -> config/description mapping ------------------------------


def _parse(*argv: str) -> argparse.Namespace:
    return build_parser().parse_args(list(argv))


def test_bridge_config_maps_every_argument() -> None:
    args = _parse(
        "bridge",
        "--tcp-host",
        "0.0.0.0",
        "--tcp-port",
        "1234",
        "--ws-host",
        "192.168.1.1",
        "--http-port",
        "4321",
        "--queue-limit",
        "500",
        "--stats-interval",
        "5.0",
    )
    config = bridge_config(args)
    assert config.tcp_host == "0.0.0.0"
    assert config.tcp_port == 1234
    assert config.ws_host == "192.168.1.1"
    assert config.http_port == 4321
    assert config.queue_limit == 500
    assert config.stats_interval_s == 5.0


def test_bridge_config_allow_origin_defaults_to_none() -> None:
    config = bridge_config(_parse("bridge"))
    assert config.allowed_origins is None


def test_bridge_config_allow_origin_becomes_a_frozenset() -> None:
    args = _parse(
        "bridge", "--allow-origin", "https://a.example", "--allow-origin", "https://b.example"
    )
    config = bridge_config(args)
    assert config.allowed_origins == frozenset({"https://a.example", "https://b.example"})


def test_is_loopback_host_recognizes_loopback_addresses() -> None:
    assert is_loopback_host("127.0.0.1")
    assert is_loopback_host("localhost")
    assert is_loopback_host("::1")
    assert not is_loopback_host("0.0.0.0")
    assert not is_loopback_host("192.168.1.1")


def test_mock_feeder_maps_every_argument() -> None:
    args = _parse(
        "mock",
        "--host",
        "0.0.0.0",
        "--port",
        "1234",
        "--symbols",
        "EURUSD,USDJPY",
        "--rate",
        "10.0",
        "--batch",
        "5",
        "--duration",
        "2.5",
        "--seed",
        "7",
    )
    feeder = mock_feeder(args)
    assert feeder.host == "0.0.0.0"
    assert feeder.port == 1234
    assert feeder.symbols == ["EURUSD", "USDJPY"]
    assert feeder.rate == 10.0
    assert feeder.batch == 5
    assert feeder.duration == 2.5
    assert feeder.seed == 7


def test_client_run_maps_every_argument() -> None:
    args = _parse(
        "client",
        "--url",
        "ws://example/ws",
        "--symbols",
        "EURUSD, USDJPY",
        "--format",
        "binary",
        "--conflate",
        "--print",
        "--bench",
        "3.0",
    )
    run = client_run(args)
    assert run == ClientRun(
        url="ws://example/ws",
        symbols=("EURUSD", "USDJPY"),
        payload_format=PayloadFormat.BINARY,
        backpressure=BackpressurePolicy.CONFLATE,
        print_ticks=True,
        bench_seconds=3.0,
    )


def test_client_run_defaults() -> None:
    run = client_run(_parse("client"))
    assert run.symbols is None, "an empty --symbols filter means no filter, not an empty tuple"
    assert run.payload_format is PayloadFormat.JSON
    assert run.backpressure is BackpressurePolicy.LOSSLESS
    assert run.print_ticks is False
    assert run.bench_seconds == 0.0


# -- the --print path, without a socket ------------------------------------


class _FakeTickSource:
    """A :class:`~mt5_ws_stream.cli.TickSource` with a canned frame script."""

    def __init__(self, frames: Sequence[DecodedFrame]) -> None:
        self._frames = frames

    async def stream(self) -> AsyncIterator[DecodedFrame]:
        for frame in self._frames:
            yield frame


def _now_tick_frame() -> TickFrame:
    """A one-tick `ticks` frame quoted just now, so `print_tick`'s lag stays small."""
    return TickFrame(ticks=(_sample_tick(time_msc=int(time.time() * 1000)),), received_at=0.0)


async def test_print_path_is_driven_through_client_hooks_and_consume_stream(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Exercises the same wiring ``_cmd_client`` uses for ``--print``, minus the
    socket: ``client_run`` -> ``client_hooks`` -> ``consume_stream``."""
    run = client_run(_parse("client", "--print"))
    source = _FakeTickSource(
        [
            _now_tick_frame(),
            ControlFrame(
                kind="stats",
                received_at=0.0,
                payload={"t": "stats", "tick_rate": 1.0},
            ),
            _now_tick_frame(),
        ]
    )
    on_tick, on_frame = client_hooks(run)

    await consume_stream(source, on_tick=on_tick, on_frame=on_frame)

    lines = capsys.readouterr().out.splitlines()
    tick_lines = [line for line in lines if line.startswith("EURUSD")]
    stats_lines = [line for line in lines if line.startswith("stats:")]
    assert len(tick_lines) == 2
    assert len(stats_lines) == 1
    assert "tick_rate=1.0" in stats_lines[0]


async def test_print_path_stays_silent_when_print_ticks_is_off(
    capsys: pytest.CaptureFixture[str],
) -> None:
    run = client_run(_parse("client"))
    source = _FakeTickSource([_now_tick_frame()])
    on_tick, on_frame = client_hooks(run)

    await consume_stream(source, on_tick=on_tick, on_frame=on_frame)

    assert capsys.readouterr().out == ""


def test_dashboard_is_packaged() -> None:
    path = dashboard_path()
    assert path.exists(), "dashboard.html must ship inside the wheel"
    assert "WebSocket" in path.read_text(encoding="utf-8")


def test_dashboard_command_prints_the_path(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["dashboard", "--print-path"]) == 0
    assert "dashboard.html" in capsys.readouterr().out


def _sample_tick(*, time_msc: int) -> Tick:
    return Tick(
        symbol="EURUSD",
        time_msc=time_msc,
        bid=1.1,
        ask=1.10012,
        last=0.0,
        volume=0.0,
        flags=6,
        seq=0,
    )


def test_print_tick_shows_zero_interval_on_the_first_tick(
    capsys: pytest.CaptureFixture[str],
) -> None:
    now_msc = int(time.time() * 1000)
    print_tick(_sample_tick(time_msc=now_msc), None)
    out = capsys.readouterr().out
    assert "+0ms" in out
    assert "lag=" in out


def test_print_tick_reports_the_gap_since_the_previous_tick(
    capsys: pytest.CaptureFixture[str],
) -> None:
    now_msc = int(time.time() * 1000)
    prev = time.perf_counter() - 0.25  # pretend the previous tick was ~250ms ago
    print_tick(_sample_tick(time_msc=now_msc), prev)
    out = capsys.readouterr().out
    assert "ms" in out
    assert "+0ms" not in out


def test_print_tick_lag_reflects_broker_clock_skew(
    capsys: pytest.CaptureFixture[str],
) -> None:
    stale_msc = int(time.time() * 1000) - 5_000
    print_tick(_sample_tick(time_msc=stale_msc), None)
    out = capsys.readouterr().out
    lag_str = out.split("lag=")[1].split("ms")[0]
    assert int(lag_str) >= 4_500


def test_compact_stats_says_n_a_when_no_quote_arrived_in_the_interval() -> None:
    """`null` percentiles are what the wire carries for a quiet interval; the
    client's stats echo must not surface that as a bare Python ``None``."""
    payload: dict[str, object] = {
        "t": "stats",
        "tick_rate": 0.0,
        "subscribers": 1,
        "broker_lag_ms_p50": None,
        "broker_lag_ms_p99": None,
        "seq_gaps": 0,
        "dropped": 0,
    }

    rendered = compact_stats(payload)

    assert "None" not in rendered
    assert "broker_lag_ms_p50=n/a" in rendered
    assert "dropped=0" in rendered


def test_compact_stats_keeps_measured_percentiles() -> None:
    rendered = compact_stats({"broker_lag_ms_p50": 1.5, "tick_rate": 12.0})

    assert "broker_lag_ms_p50=1.5" in rendered
    assert "tick_rate=12.0" in rendered


def test_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert "mt5-ws-stream" in capsys.readouterr().out


# -- collect_bench / print_bench -----------------------------------------


def _bench_tick(seq: int = 0) -> Tick:
    return Tick(
        symbol="EURUSD",
        time_msc=int(time.time() * 1000),
        bid=1.1,
        ask=1.10012,
        last=0.0,
        volume=0.0,
        flags=6,
        seq=seq,
    )


def _tick_frame(*ticks: Tick, hop: float | None) -> TickFrame:
    """One `ticks` frame whose hop is *hop* seconds, or unknown when ``None``."""
    # rx=0 keeps `received_at - rx` exact, so the assertions can spell the hop out.
    return TickFrame(ticks=ticks, received_at=hop or 0.0, rx=None if hop is None else 0.0)


def _fake_clock(values: Iterator[float]) -> Callable[[], float]:
    return lambda: next(values)


async def test_collect_bench_counts_ticks_and_latencies_over_the_whole_source() -> None:
    source = _FakeTickSource(
        [
            _tick_frame(_bench_tick(seq=0), hop=0.001),
            _tick_frame(_bench_tick(seq=1), hop=0.002),
            _tick_frame(_bench_tick(seq=2), hop=None),
        ]
    )
    # started, then one reading per frame: 0, 1, 2, 3.
    clock = _fake_clock(iter([0.0, 1.0, 2.0, 3.0]))

    result = await collect_bench(source, seconds=100.0, clock=clock)

    assert result.ticks == 3
    assert result.elapsed_s == 3.0
    assert result.latencies_ms == [1.0, 2.0]


async def test_collect_bench_credits_a_frames_hop_to_every_tick_it_carried() -> None:
    """One measurement per frame; the percentiles still weight it per tick."""
    source = _FakeTickSource([_tick_frame(*[_bench_tick(seq=i) for i in range(3)], hop=0.001)])
    clock = _fake_clock(iter([0.0, 1.0]))

    result = await collect_bench(source, seconds=100.0, clock=clock)

    assert result.ticks == 3
    assert result.latencies_ms == [1.0, 1.0, 1.0]


async def test_collect_bench_counts_frames_and_keeps_a_per_frame_hop_too() -> None:
    """`frames`/`frame_latencies_ms` are what `benchmarks/bench.py` needs for its
    ticks/frame line and its (per-frame, not per-tick) hop percentiles."""
    source = _FakeTickSource(
        [
            _tick_frame(*[_bench_tick(seq=i) for i in range(3)], hop=0.001),
            _tick_frame(_bench_tick(seq=3), hop=0.002),
            _tick_frame(_bench_tick(seq=4), hop=None),
        ]
    )
    clock = _fake_clock(iter([0.0, 1.0, 2.0, 3.0]))

    result = await collect_bench(source, seconds=100.0, clock=clock)

    assert result.frames == 3
    assert result.ticks == 5
    assert result.frame_latencies_ms == [1.0, 2.0]
    # Per-tick weighting is unaffected: the 3-tick frame still yields 3 entries.
    assert result.latencies_ms == [1.0, 1.0, 1.0, 2.0]


async def test_collect_bench_stops_once_the_window_elapses() -> None:
    source = _FakeTickSource([_tick_frame(_bench_tick(seq=i), hop=0.001) for i in range(5)])
    # started=0, then 1, 2 (>= seconds=2 -> stop before the remaining frames).
    clock = _fake_clock(iter([0.0, 1.0, 2.0, 3.0, 4.0, 5.0]))

    result = await collect_bench(source, seconds=2.0, clock=clock)

    assert result.ticks == 2
    assert result.elapsed_s == 2.0
    assert result.latencies_ms == [1.0, 1.0]


async def test_collect_bench_zero_seconds_never_stops_on_the_deadline() -> None:
    """``seconds=0`` mirrors ``--bench`` unset: run until the source ends."""
    source = _FakeTickSource([_tick_frame(_bench_tick(seq=i), hop=None) for i in range(3)])
    clock = _fake_clock(iter([0.0, 0.0, 0.0, 0.0]))

    result = await collect_bench(source, seconds=0.0, clock=clock)

    assert result.ticks == 3
    assert result.latencies_ms == []


async def test_collect_bench_invokes_hooks_for_ticks_and_frames() -> None:
    stats_frame = ControlFrame(kind="stats", received_at=1_000.0, payload={"t": "stats"})
    source = _FakeTickSource([_tick_frame(_bench_tick(), hop=0.001), stats_frame])
    clock = _fake_clock(iter([0.0, 1.0]))

    seen_ticks: list[Tick] = []
    seen_frames: list[ControlFrame] = []

    result = await collect_bench(
        source,
        seconds=100.0,
        on_tick=seen_ticks.append,
        on_frame=seen_frames.append,
        clock=clock,
    )

    assert result.ticks == 1
    assert [t.seq for t in seen_ticks] == [0]
    assert seen_frames == [stats_frame]


def test_print_bench_reports_throughput_and_percentiles(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = BenchResult(ticks=10, elapsed_s=2.0, latencies_ms=[1.0, 2.0, 3.0, 4.0, 5.0])

    print_bench(result)

    out = capsys.readouterr().out
    assert "2.00s  ticks=10 (5/s)" in out
    assert "p50=3.000ms" in out
    assert "p99=5.000ms" in out
    assert "max=5.000ms" in out
    assert "mean=3.000ms" in out


def test_print_bench_notes_binary_format_carries_no_latency(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = BenchResult(ticks=5, elapsed_s=1.0, latencies_ms=[])

    print_bench(result)

    out = capsys.readouterr().out
    assert "ticks=5" in out
    assert "binary format carries no send timestamp" in out
    assert "bridge->client:" not in out
