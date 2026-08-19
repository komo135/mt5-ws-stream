# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-08-20

First public release.

### Added

- **Wire protocol** — 64-byte fixed-size little-endian records (`mt5_ws_stream.protocol`), with
  reference vectors pinning the byte layout.
- **MQL5 Expert Advisor** (`mql5/Experts/TickStreamer/TickStreamer.mq5`) — event-driven `OnTick()`
  capture, automatic reconnect with backoff, heartbeats, and optional extra-symbol streaming.
- **Bridge** (`mt5_ws_stream.bridge`) — TCP listener for feeders, WebSocket server for consumers,
  per-connection subscriptions via query string, JSON control protocol, and periodic stats.
- **Hub** (`mt5_ws_stream.hub`) — transport-agnostic fan-out with per-subscriber `lossless` /
  `conflate` backpressure policies and sequence-gap accounting.
- **Client library** (`mt5_ws_stream.TickStreamClient`) — async iterator over `Tick` objects,
  identical across the JSON and binary payload formats.
- **MockFeeder** for running the whole pipeline without MetaTrader.
- **CLI** — `mt5-ws-stream bridge | mock | client | dashboard`.
- **Dashboard** — single-file HTML live monitor with bid/ask tiles, sparklines, tick-rate and
  delivery-latency charts, a colour-independent table view, and light/dark themes.
- **Docs** — protocol reference, architecture notes, latency analysis and troubleshooting guide,
  in English and Japanese.
- **REST API** under `/api/v1`, on the same port as the stream: `GET /` (route index),
  `/api/v1/health`, `/api/v1/symbols` (`?symbols=A,B` filters), `/api/v1/symbols/{symbol}`
  (404 if unknown), `/api/v1/stats`, `/api/v1/feeders`.
- **Dashboard at `GET /dashboard`**, same-origin with the WebSocket, and **OpenAPI docs** at
  `GET /docs` and `/openapi.json`. `mt5-ws-stream dashboard --url` opens the bridge-served copy
  when a bridge answers `/api/v1/health` there, and the bundled HTML file otherwise.
- **`InpSpyPeriod`** (EA, `EXTRA_EVENT` only, default `PERIOD_MN1`) — the timeframe spy handles
  open on. A coarser timeframe costs less memory and attaches faster; delivery is identical.
- **`mt5_ws_stream.frames`** — the JSON frame grammar in one module: `FrameKind`, `hello_frame`,
  `ticks_frame`, `binary_ticks_frame`, `stats_frame`, `stats_payload`, `ack_frame`, `pong_frame`,
  `error_frame`, `decode_frame`.
- **`mt5_ws_stream.decoder`** — `decode_frame(message, received_at=...)` returns a `TickFrame`
  (ticks, `rx`, receive time, `hop`) or a `ControlFrame` (kind, payload), else raises
  `FrameDecodeError`.
- **`mt5_ws_stream.session.Session`** (exported as `mt5_ws_stream.Session`) — one consumer
  connection: options, `hello`, control ops, `run()` / `flush()`, `send_stats()`, close counters.
- **`mt5_ws_stream.subscription`** — `SubscriptionRequest.to_query()` / `.to_query_string()`
  render the canonical query, `.from_query()` is the bridge's lenient reader, and
  `normalize_symbols()` the shared symbol-set normaliser.
- **`hub.FeederLink`** — one feeder connection's ingest state: `take_records()`, `pending_bytes`,
  `account()`.
- `protocol.decode_records(buffer)` pairs each `Tick` with the bytes it came from, and
  `iter_ticks` is expressed over it. Also `Tick.as_json()`, `Tick.from_dict()`,
  `protocol.compact_json`, `protocol.split_symbols()`, `protocol.percentile()`,
  `PayloadFormat.parse()`.
- `Hub.consume_interval()`, `Hub(latency_sample_cap=...)`, `Bridge.report_once()`,
  `create_app(..., sessions=...)`, and `Subscriber.drain()` / `.wait()` / `.close()` / `.closed`
  / `.pending_count`.
- `TickStreamClient(connect_fn=...)` and the `client.Connection` protocol;
  `client.STREAM_PATH` names `/ws`. `TickStreamClient(handshake_timeout=...)` is seconds to wait
  for `hello` once the socket is up (default `10.0`; `None` waits forever).
- `MockFeeder.run(link=None)` takes an already-connected object with `send(ticks)`, does not
  close it, and returns the quote records sent; `FeederConnection(start_seq=)` seeds the sequence
  counter.
- **CLI internals made public:** `cli.print_tick`, `cli.print_bench`, `cli.compact_stats`,
  `cli.bridge_is_up`, `cli.bridge_config`, `cli.is_loopback_host`, `cli.mock_feeder`,
  `cli.ClientRun`, `cli.consume_stream`, `cli.TickSource`, `cli.client_hooks`,
  `cli.collect_bench`, `cli.BenchResult`.
- **`benchmarks/micro.py`** — hot-path micro-benchmarks: ingest through `Hub.feed` with N
  subscribers, and one drained batch through the frame encoders.
- **`benchmarks/symbol_scaling.py`**, **`compare_tick_counts.py`** and
  **`wizard_baseline_sweep.sh`** — a wire-side per-symbol collector, the ticks-lost join, and an
  operator wizard sweeping symbol counts under both delivery modes.
- **`benchmarks/live/`** — the live rig driving a real terminal: chart-file authoring, headless
  MetaEditor compiles, Experts-log parsing, bridge supervision. Subcommands `smoke`, `sweep`,
  `remeasure` and `groundtruth`.
- **`docs/measurements/2026-08-18-live-sweep.md`**, the record behind the symbol scaling table in
  `docs/latency.md`, and **`docs/adr/`** plus `CONTEXT.md`, the domain vocabulary.
- Bridge logs `feeder <peer>: new symbol XYZ (n so far)` at INFO on each symbol's first arrival;
  `mt5-ws-stream -v bridge` logs every chunk (bytes, records, ms since the previous chunk).
- `mt5-ws-stream client --print` shows `+Nms` (interval since the previous tick) and `lag=Nms`
  (local clock minus `time_msc`, clock skew included).

### Changed

- **Consumer side rewritten on FastAPI + uvicorn**, one port for the stream, REST API, dashboard
  and OpenAPI schema. New runtime dependencies `fastapi` and `uvicorn` alongside `websockets`;
  `BridgeConfig.extra_serve_kwargs` passes through to `uvicorn.Config`.
- **BREAKING:** Python 3.11 is the minimum.
- **BREAKING:** the stream is served at `/ws` only; `/` is the HTTP route index.
  `TickStreamClient` appends `/ws` to a URL that names no path.
- **BREAKING:** the hub owns no tasks. `Hub.subscribe(options=None)` takes no `Sink` and creates
  no writer task, so it works with no running loop; `unsubscribe()` / `aclose()` close queues
  instead of cancelling tasks. `.sink`, `.encode()`, `.run()`, `.sent_frames` and `.sent_ticks`
  moved from `Subscriber` to `Session`; `dropped` and `pending_count` stayed. The WebSocket
  handler runs `Session.run()` beside the receive loop in an `asyncio.TaskGroup`.
- **BREAKING:** `Hub.publish(records, *, heartbeats=True)` takes a whole decoded batch and
  `Subscriber.offer(items)` a filtered list; `SubscriptionOptions.wants(symbol)` is gone, the
  filter being resolved once per batch inside `publish`.
- `Hub.feed(chunk, link)` accepts an **arbitrary** chunk, buffering a partial trailing record on
  the link, and is **atomic per batch**.
- `Hub.snapshot_stats()` is a pure read; `Hub.consume_interval()` closes an interval and only the
  periodic reporter calls it, so `GET /api/v1/stats` and `{"op":"stats"}` report the window since
  the last periodic report.
- **BREAKING (wire):** the `ack` frame has one shape for `subscribe`, `unsubscribe` and `format`
  alike — `{"t","op","symbols","format"}`. The `value` key is gone.
- **BREAKING (wire):** a `symbols` list in `hello` and `ack` is the connection's filter, `null`
  meaning *every symbol* and `[]` *none*. The bridge's catalogue is `hello.available`
  (`stats.symbols` is unchanged and is still the catalogue).
- **BREAKING:** `TickStreamClient.stream()` yields one decoded frame per WebSocket message
  (`TickFrame` or `ControlFrame`); `ticks()` and `async for tick in client` are unchanged.
- **BREAKING:** `TickStreamClient.last_hop_latency_s` is gone — read `TickFrame.hop` on the frame
  that carried the ticks, and `len(frame.ticks)` for ticks per frame.
- **BREAKING:** `TickStreamClient.connect()` fails loudly on a bad handshake. A first frame
  that is not `hello` raises `HandshakeError`, an undecodable one `FrameDecodeError`, and
  transport errors propagate; the connection is closed first.
- **BREAKING:** `PayloadFormat` and `BackpressurePolicy` live in `mt5_ws_stream.protocol`, not
  `mt5_ws_stream.hub` (`from mt5_ws_stream import PayloadFormat` is unchanged), and
  `BackpressurePolicy.parse()` is gone.
- **BREAKING:** `SymbolSnapshot.as_dict()` and `HubStats.as_dict()` are gone; the shapes come from
  the pydantic models in `mt5_ws_stream.api` and from `frames.stats_payload(stats)`. The emitted
  JSON is unchanged.
- **BREAKING:** `Bridge.http_port`, `BridgeConfig(http_port=...)` and `--http-port` are the one
  spelling for the bound consumer port.
- `create_app(hub, *, allowed_origins=None, feeders=None, sessions=None, version=__version__)`
  takes a `Hub`, not the `Bridge`, and can be driven over `httpx.ASGITransport` with no server,
  socket or bridge; routes, JSON shapes and OpenAPI operationIds are unchanged. Import
  `parse_subscription` from `mt5_ws_stream.api`.
- `mt5_ws_stream` exports `create_app`, `Bridge` and `BridgeConfig` lazily via PEP 562, so
  importing `TickStreamClient` does not load FastAPI, pydantic or uvicorn.
- **Internal:** `Bridge.start` / `aclose` share one teardown mechanism, per-stage `_close_*`
  callables pushed as each resource starts and popped in reverse. `benchmarks/bench.py` collects
  through `cli.collect_bench`, the collector `mt5-ws-stream client --bench` uses.
- **`docs/latency.md`'s symbol scaling table carries measured numbers** at N = 1, 10, 29 and `*`.
  The terminal truncates a parameter line at 255 characters including the `key=`, silently and
  mid-name, capping `InpSymbols` at 244 characters (~28 symbols) on a chart's `<inputs>` and a
  `.set` file alike; use `"*"` or a second EA instance to go past it.
- **`CountTicks.mq5`'s window is in the broker's server clock:** `InpFromMsc` / `InpToMsc` are
  compared against `MqlTick.time_msc`, not UTC. Its header says so.
- **The EA logs the spy timeframe it got** — `attached 72 of 72 tick spies on PERIOD_MN1 in
  343 ms`.
- README covers install, the chart-symbol stream, and the API. Extra-symbol
  latency (`EXTRA_EVENT` / TickSpy) is in `docs/latency.md`. The synthetic feeder
  and `CountTicks.mq5` are in `CONTRIBUTING.md`.

### Removed

- **`SECURITY.md`** and the README security section.
- **BREAKING:** `MetaTraderPollingFeeder`, the `mt5-ws-stream mt5-feeder` command,
  `--server-utc-offset`, the `mt5` extra and the `Feeder` protocol. The EA is the feeder; the
  Python side depends on the terminal in no configuration.
- **BREAKING:** the `/` WebSocket route, and `Bridge.ws_port` / `BridgeConfig.ws_port` /
  `--ws-port`, with no alias — use `http_port` (see Changed).
- **BREAKING:** `ServerFrame`, replaced by the frozen dataclass
  `decoder.ControlFrame(kind, received_at, payload, rx)`.
- **BREAKING:** `api.handle_control()` and `api.send_json()` — the control protocol is
  `Session.handle()`; `api.stats_frame()` — the frame is `frames.stats_frame()`, its field dict
  `frames.stats_payload()`.

### EA (`TickStreamer.mq5`)

- **Extra symbols (`InpSymbols`) stream every tick.** A collection asks
  `CopyTicks(symbol, ..., COPY_TICKS_ALL, from, 256)` for everything after the last record sent
  for that symbol and emits all of it, sequence advancing per record, tracked by a per-symbol
  cursor. A failed `CopyTicks` leaves the cursor untouched, costing latency rather than data.
- **`InpExtraMode` — extra symbols can be event-driven.** `EXTRA_POLL` (the default) collects them
  from the terminal timer; `EXTRA_EVENT` attaches one spy indicator per extra symbol via
  `iCustom()` — the new `mql5/Indicators/TickStreamer/TickSpy.mq5` — and `OnChartEvent()` runs
  that symbol's collection at once. Both modes deliver the same ticks. `EXTRA_EVENT` needs
  `MQL5\Indicators\TickStreamer\TickSpy.ex5` compiled in the terminal; the default does not.
- **New input `InpEventBackstopMs`** (default 100 ms, `<=0` = use `InpPollMs`) — how often the
  timer sweeps up what the spy events missed. A dropped event costs latency, never a tick.
- **A missing spy is contained, not fatal.** An `iCustom()` returning `INVALID_HANDLE` logs
  `no tick spy for XYZ ...` once and leaves that symbol on the timer; while any symbol is in that
  state the timer runs at `InpPollMs` rather than `InpEventBackstopMs`. Handles are released in
  `OnDeinit`; an event with an unrecognised symbol index or name is ignored and counted.
- **Start-up warms extra symbols up.** The first `CopyTicks` per symbol can block up to 45 s, so
  it runs in `OnInit` within a 5 s budget (`warmed up N of M extra symbols in X ms`, plus a line
  per symbol taking over a second); the rest are warmed one per timer tick and are not collected
  until they have been. Cursors are seeded from the newest tick, so no history streams on start.
- **BREAKING (input): `InpTimerMs` renamed to `InpPollMs`, default `1` → `10`.** The terminal's
  timer fires at most every 10–16 ms on Windows however low the input is set.
- **`InpSymbols=*`** — stream every symbol currently visible in Market Watch. The chart's own
  symbol is excluded from the extra list automatically, so listing it changes nothing.
- **`InpUtcTimestamps`** (default `true`) — normalises `time_msc` from broker server time to UTC
  by subtracting `TimeTradeServer() − TimeGMT()`, rounded to the nearest 30 minutes and
  re-estimated every 60 s. Logs `server_utc_offset=+3.0h` on start.
- **`InpStatsSec`** (default 60 s, `0` = off) — a periodic one-line Experts-tab summary: `ticks`,
  `dropped`, `send_us` avg/max, `reconnects`, `total_sent`, `symbols`, `mode=poll|event`,
  `poll_n`, `poll_us_*`, `ping_us`, `extra_obs` / `extra_sent`, `ct_n` / `ct_us_*` / `ct_err`,
  `cursor_skip`, `evt_n` / `evt_us_*` / `evt_late` / `evt_bad`. One shape in both modes.
- **Connection watchdog.** Each connect attempt is timed and classified live, timed out, dropped
  or unreachable, then reported with elapsed time and error code. A disconnect logs
  `connection lost; reconnecting in N ms` and backs off; three connections in a row dying within
  3 s of being accepted print a hint that no bridge may be listening.
- **Internals restructured.** File-scope globals are grouped into `ServerClock`, `SendBuffer`,
  `IntervalStats`, `Link`, `Heartbeat`, and `SymbolFeed` / `ExtraSymbolList` (one struct per
  symbol, replacing parallel arrays). `OnTimer` is three named steps: `TimerAlways`,
  `TimerServiceLink`, `TimerWhenConnected`.
- **`mql5/Scripts/TickStreamer/CountTicks.mq5`** — the offline ground-truth script:
  `CopyTicksRange()` over a given window, one `symbol,count` row per symbol to the Experts log and
  to a CSV under `MQL5\Files\`.

### Performance

Measured on Windows 11, Python 3.13.14, Intel Core i9-14900KF; three interleaved rounds, median
of per-round medians; noise floor ±3%. Full tables and method in `docs/latency.md`.

- Ingest through `Hub.feed`: **1.502 → 0.999 µs/record** with one subscriber (−33%) and
  **2.296 → 1.018** with four (−56%), now nearly flat in the subscriber count.
- JSON encode of a drained batch: **26.7 → 18.6 µs** for 20 ticks (−30%), **3.43 → 1.25 µs** for
  one (−64%). End to end on loopback, the bridge → consumer hop drops **~12%** at both 200 and
  20,000 ticks/s.
- Four measured changes, no wire bytes altered: a decoded-symbol cache in `unpack_tick`, one
  buffer normalisation per batch in `decode_records`, batch fan-out in `Hub.publish`, and a
  `ticks` frame written directly rather than through a dict per tick.
- EA at 72 extra symbols, "Max bars in charts" at 100 000 000: `EXTRA_EVENT` on `PERIOD_MN1` uses
  a **554 MB** working set against **16.8 GB** on `PERIOD_M1` (**431 MB** for `EXTRA_POLL`, which
  opens no handles), and spies attach in **343 ms** instead of **5 125 ms**. Delivery is
  unchanged.

### Fixed

- **The EA sent `time_msc` as broker server time**, so `broker_lag_ms` was off by the server's UTC
  offset. Fixed by `InpUtcTimestamps`.
- **The chart's own symbol was streamed twice** when it was also listed in `InpSymbols`.
- **`Tick.as_dict()` masked `flags` to 16 bits**, dropping `FLAG_HEARTBEAT`. The JSON path carries
  the full value, matching binary and REST.
- **A `{"op":"stats"}` frame stole the stats interval**, blanking the percentiles for the periodic
  log and for every other subscriber.
- **Broker-lag percentiles froze after 20,000 samples** with the periodic report disabled
  (`stats_interval_s=0`); the sample buffer is a ring that evicts the oldest.
- **A consumer whose socket died stayed subscribed.** The failed send reaches the session's task
  group, which unwinds it.
- **On Windows the feeder listener was bound with `SO_REUSEADDR`**, letting a second bridge bind
  the same port and silently split feeder connections. `reuse_address` is POSIX-only, so a second
  bridge fails loudly on every OS.
- **The dashboard read a non-existent `client_drops` key** from `stats` frames and showed
  `undefined` in the gaps/drops tile. It reads `dropped`.
- **`stats.dropped` fell when a shed consumer disconnected**, sending a cumulative counter
  backwards. A subscriber's drop count is folded into the hub's total when it unsubscribes.
- **`TickStreamClient.connect()` could block forever on a live port that never sent `hello`.** The
  wait for the first frame is bounded by `handshake_timeout` and raises `HandshakeError` on expiry.
- **A repeated query parameter meant two different things** depending on whether
  `SubscriptionRequest.from_query()` got a path string or Starlette's `query_params`. Both take
  the last value.
- **A millisecond holding 256 ticks or more stalled that symbol's cursor for good**, stopping the
  symbol silently. The EA forces the cursor past that millisecond, losing its remainder;
  `cursor_skip=` counts the occurrences and a `256 ticks or more at time_msc=…` line names the
  symbol. Non-zero means raise `EXTRA_MAX_TICKS`.
- **A failed `Flush()` counted the whole buffer as `dropped`.** The EA counts `(len - sent)`
  rounded up to whole records as dropped and the rest as sent, reports `L of R records lost` on
  the reconnect line, and tears the socket down in the same breath.
- **`InpEventBackstopMs` silently stretched the heartbeat.** The effective interval is
  `max(InpHeartbeatMs, timer period)`; `OnInit` prints it when the two disagree.
- **The periodic stats log printed `broker_lag_p50=None ms`** in an interval with no quotes.
  `api.stats_line()` renders `n/a` for a missing percentile and groups fields by what they measure
  (`interval:` / `total:` / `now:`).
- **A teardown step that raised left the HTTP server, HTTP socket and feeder listener bound.**
  Every `Bridge._unwind` step runs regardless; escapes are logged and the first is re-raised
  once the stack is empty.
- **The periodic stats broadcast could hang on one dead consumer.** Sends are fanned out
  concurrently, each capped by the new `BridgeConfig.stats_send_timeout_s` (default `1.0` s); a
  timed-out or failing session is skipped and logged at debug.

### Known

- uvicorn's WebSocket implementations send no server-initiated keepalive pings, so
  `ws_ping_interval_s` / `ws_ping_timeout_s` are advisory. Dead peers are still caught by TCP and
  by the feeder's own heartbeat records, but not by a ping timeout on the consumer side.

[Unreleased]: https://github.com/komo135/mt5-ws-stream/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/komo135/mt5-ws-stream/releases/tag/v0.1.0
