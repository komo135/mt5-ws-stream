# CONTEXT — domain language for mt5-ws-stream

The words the code, docs and tests use, and where each one lives. When a module
is named after one of these, it owns that concept. Architecture vocabulary
(module, interface, seam, adapter, depth, leverage, locality) is the
`codebase-design` vocabulary and is not redefined here.

## Wire

- **Record** — one fixed-size 64-byte binary message on the feeder wire, either
  a **quote** or a **heartbeat**. `protocol.py` is its one definition
  (`pack_tick` / `unpack_tick` / `decode_records`, and the layout table in the
  module docstring); the same layout is written up in `docs/protocol.md §1`.
- **Tick** — the decoded, transport-free value of a record. The same object
  whether it arrived as binary or as JSON. `protocol.Tick`, a `NamedTuple`.
- **Frame** — one WebSocket message from bridge to consumer. The kinds are
  `hello`, `ticks`, `stats`, `ack`, `pong` and `error` (`frames.FrameKind`,
  written up in `docs/protocol.md §2`).
- **Frame grammar** — the set of frame kinds and their shapes, encoders and
  decoder together. `frames.py` is its one home, as `protocol.py` is the one
  home of the record layout.
- **Subscription request** — the query-string vocabulary a consumer sends when
  connecting (`symbols`, `format`, `conflate`, `heartbeats`), plus the
  `subscribe` / `unsubscribe` / `format` control ops that mutate it afterwards.
  Rendering and parsing are two halves of one concept: `subscription.py`.
- **Filter** vs **catalogue** — the two symbol lists a frame can carry. A
  `symbols` list (in `hello` and `ack`) is *this connection's filter*: `null`
  means every symbol, `[]` means none. `hello.available` and the `stats`
  frame's `symbols` are the *catalogue*: every symbol the bridge has seen since
  it started. `hub.symbols` produces the catalogue; `SubscriptionOptions.symbols`
  holds the filter.
- **Payload format** — `json` or `binary`, per connection and switchable
  mid-stream (`protocol.PayloadFormat`).
- **Backpressure policy** — `lossless` or `conflate`, per connection and fixed
  for its life (`protocol.BackpressurePolicy`, applied in `hub.Subscriber.offer`).

## Feeder side

- **Feeder** — any process that writes records to the bridge's TCP port. Two
  adapters ship: the **EA** (`mql5/Experts/TickStreamer/TickStreamer.mq5`, the
  live one) and **MockFeeder** (`feeders.py`, for tests and benchmarks). The
  seam is the wire, not a Python protocol class.
- **Feeder link** — the bridge's per-connection ingest state: partial-record
  tail buffer, sequence continuity, counters and symbols seen. It owns framing —
  where a record starts. `hub.FeederLink`, fed an arbitrary chunk through
  `Hub.feed(chunk, link)`.
- **Batch** — the records one chunk completes. `Hub.feed` decodes a batch in
  full before publishing any of it, so a bad record rejects the whole batch.
- **Chart symbol** — the symbol of the chart the EA runs on, delivered by
  `OnTick` and never collected by the extra-symbol path
  (`TickStreamer.mq5`, `OnTick`).
- **Extra symbols** — everything else the one EA instance streams, named by
  `InpSymbols` (`*` means every symbol in Market Watch). They stream *every*
  tick: each collection asks `CopyTicks` for everything after the feed's cursor
  (`ExtraSymbolList` in `TickStreamer.mq5`).
- **Delivery mode** — what *wakes* an extra symbol's collection up
  (`InpExtraMode`), never what the collection does, which is one `Poll(feed)`
  either way. `EXTRA_POLL` is the `OnTimer` sweep, costing latency (the poll
  period plus the terminal's 10–16 ms timer floor) rather than data;
  `EXTRA_EVENT` is the spy indicators, with the timer demoted to a backstop.
- **Spy indicator** — `mql5/Indicators/TickStreamer/TickSpy.mq5`: one
  buffer-less, plot-less indicator per extra symbol, created with `iCustom` and
  never drawn. Indicators get `OnCalculate` on every tick but cannot open
  sockets, so a spy's whole job is one `EventChartCustom` carrying no price —
  an alarm clock the EA answers in `OnChartEvent` with that symbol's ordinary
  collection. The cursor decides what a collection returns, so a coalesced or
  discarded event costs latency and never a tick.
- **Feed cursor** — a symbol's position in its own tick stream: `last_msc` plus
  `seen_at_last_msc`, the number of ticks already delivered out of that
  millisecond. Two fields because `CopyTicks(from)` is inclusive and one
  millisecond can hold several ticks. `SymbolFeed` in `TickStreamer.mq5`.
- **Warm-up** — the first `CopyTicks` per extra symbol, which synchronises that
  symbol's tick database and may block for up to 45 s. It runs in `OnInit`
  under a 5 s budget and, for whatever the budget did not reach, one symbol per
  timer tick (`WarmUpNext`) — never in the poll loop.
- **Heartbeat** — a record with `FLAG_HEARTBEAT` set, sent by the feeder on an
  interval so a consumer can tell "quiet market" from "dead link". It carries an
  empty symbol and zero prices (`protocol.FLAG_HEARTBEAT`,
  `feeders.FeederConnection.make_heartbeat`).

## Bridge side

- **Hub** — decode once, fan out to N subscribers, enforce each subscriber's
  backpressure policy. It knows nothing about WebSocket and owns no tasks.
  `hub.Hub`.
- **Subscriber** — the hub's per-consumer queue plus policy, and the `dropped`
  counter that policy moves. `hub.Subscriber`.
- **Sink** — anything with an awaitable `send(payload)`; the hub's only outward
  seam. `hub.Sink`, with `_WebSocketSink` (`api.py`) and `RecordingSink`
  (`tests/conftest.py`) as its adapters.
- **Session** — one consumer's connection: its options, the `hello` it is told,
  the control ops that change those options, its writer loop
  (drain → encode → `sink.send`) and the summary it reports on close. Built from
  a Sink and a Hub; the WebSocket handler in `api.py` is a thin adapter around
  it. `session.Session`.
- **Bridge** — process lifecycle: the feeder TCP listener, one embedded uvicorn
  server, and the periodic **stats report**. `bridge.Bridge`; `report_once` is
  one report and `_stats_loop` is a loop over it.
- **Consumer** / **client** — anything speaking the WebSocket side.
  `client.TickStreamClient` is the Python adapter; the bundled browser dashboard
  (`web/dashboard.html`) is another.
- **Decoder** — the client-side half of the frame grammar: raw text or bytes in,
  `Tick` or control frame out, with the frame's `rx` timestamp attached.
  Defined in `frames.py` with the encoders it inverts; `decoder.py` re-exports
  it under the name consumers know.

## Measurement

- **Broker lag** — local clock minus a tick's `time_msc` (UTC), in
  milliseconds. A trend indicator: it includes clock skew between this host and
  the broker. Sampled per quote in `Hub.feed`, reported as
  `broker_lag_ms_p50` / `broker_lag_ms_p99`.
- **Hop** — client receive time minus the frame's `rx`. Clean when both ends
  share a clock; `None` for binary frames, which carry no `rx`.
  `frames.TickFrame.hop`.
- **Symbol scaling table** — one row per (delivery mode, N extra symbols) point
  of the sweep, N ∈ {1, 10, 29, all}: ticks/s, hop p50/p99, timer callback µs
  (`poll_us`), the `extra_obs`/`extra_sent` pair, the `evt_late`/`evt_n` ratio,
  and dropped/gaps. It lives in
  [docs/latency.md](docs/latency.md#symbol-scaling) and is filled in from a
  sweep — `benchmarks/live/run.py sweep`, or the operator-driven
  `benchmarks/wizard_baseline_sweep.sh`. A rung at 50 is not expressible: the
  terminal caps a parameter line at 255 characters, which caps `InpSymbols` at
  244 and so at roughly 28 symbols.
- **Ground truth** — the ticks-lost column, taken outside the EA: the
  terminal's own `CopyTicksRange()` count (`mql5/Scripts/TickStreamer/CountTicks.mq5`)
  against the wire count (`benchmarks/symbol_scaling.py`) over the same window,
  joined by `benchmarks/compare_tick_counts.py`.
