# Latency

## Where the time goes

```
broker  ──────────────────────────────►  MetaTrader 5 terminal      1–50 ms
terminal ─────────────────────────────►  OnTick() invoked         < 0.1 ms
OnTick  ──────────────────────────────►  SocketSend returns       ~10–50 µs
TCP loopback ─────────────────────────►  bridge decodes           ~50–100 µs
bridge  ──────────────────────────────►  WebSocket consumer      0.09–0.19 ms
```

Loopback hop, JSON: p50 0.091 ms / p99 0.176 ms at 200 ticks/s. Live hop p50
0.16–0.18 ms. Tables below.

## How the chart symbol is captured

The chart symbol is sent from `OnTick()`. The terminal calls the EA when a quote
arrives; the handler packs 64 bytes and writes them.

`OnTick()` fires for the chart symbol only. Extra symbols:
[Extra symbols: polling or events](#extra-symbols-polling-or-events).

## What keeps the hop small

**No batching timer.** Each subscriber's writer drains its queue into one frame
and repeats. Quiet market: one tick per frame. Busy market: ticks share a frame.

**Per-batch fan-out.** A chunk off the feeder socket is decoded as one batch and
fanned out as one: each subscription's symbol filter is resolved once for the
batch, its writer is woken once, and the `ticks` frame is written as text rather
than as a dict per quote handed to `json`. Ingest costs about the same with four
subscribers as with one.

**`TCP_NODELAY` on every socket**, on the feeder side and on the bridge's
accepted sockets. Without it Nagle's algorithm can hold a small record for up
to 40 ms waiting for a companion packet.

**No WebSocket compression.** Tick payloads are small; compression adds CPU and
latency.

**No server-initiated keepalive pings.** The bridge's consumer side is an
embedded uvicorn server pinned to the `websockets-sansio` implementation, which
sends no keepalive pings of its own, so `ws_ping_interval_s` and
`ws_ping_timeout_s` have no effect. A dead peer is caught by TCP and by the
feeder's own heartbeat records.

**A short `OnTick()`.** MetaTrader coalesces ticks that arrive while the handler
is still running, so time spent there costs data, not only latency.

**Send timeouts of 200 ms in the EA.** `SocketSend` runs inside `OnTick()`. If
the bridge stalls, the EA gives up and reconnects rather than block the
terminal's tick delivery.

## Extra symbols: polling or events

Empty `InpSymbols` streams the chart symbol. Adding more:

| What you want | What you do | What you pay |
| --- | --- | --- |
| Chart symbol, event-driven | Leave `InpSymbols` empty | Nothing extra |
| More symbols from one chart | Set `InpSymbols=A,B` or `*` | Timer wait on those symbols (10–16 ms on Windows). Every tick still arrives |
| That wait gone | `InpExtraMode=EXTRA_EVENT` and compile TickSpy | One indicator per extra symbol, and the terminal memory that handle holds |
| Event-driven on every symbol, no TickSpy | One EA instance per chart | One chart and one socket per symbol |

`InpSymbols` lists symbols other than the chart's. They have no `OnTick()` of
their own, so the EA collects them with `CopyTicks()` from a per-symbol cursor.
The chart symbol is never collected — listing it changes nothing — and duplicates
are skipped. `InpSymbols="*"` means every symbol currently in Market Watch.

Each collection asks for everything that arrived after the last record the EA
sent for that symbol. `CopyTicks()` is inclusive at its lower bound and one
millisecond can hold several ticks, so the cursor stores both the last
millisecond delivered and how many ticks were already taken out of it, and skips
exactly that many. Both modes run this identical collection and both deliver
every tick; they differ in how long a tick waits first.

| | `EXTRA_POLL` *(default)* | `EXTRA_EVENT` |
| --- | --- | --- |
| **Wake-up** | Terminal timer, every `InpPollMs` (default 10 ms) | A spy indicator per symbol → `EventChartCustom` → `OnChartEvent`; the timer runs underneath as a backstop |
| **Added latency** | Up to one effective timer period — `max(InpPollMs, 10–16 ms)`, half of it on average — **+** `CopyTicks()` | Event dispatch **+** `CopyTicks()`. No timer floor |
| **Cost** | One `CopyTicks()` per symbol per poll, whether or not anything ticked | One indicator calculation per symbol per tick, and one timeseries handle per symbol in terminal memory |
| **Requires** | Nothing | `MQL5\Indicators\TickStreamer\TickSpy.ex5` compiled in this terminal |

### What the spies do

Needed when `InpExtraMode=EXTRA_EVENT`.

1. Copy `mql5/Indicators/TickStreamer/TickSpy.mq5` into
   `MQL5/Indicators/TickStreamer/` in the terminal's data folder.
2. Compile it in MetaEditor (<kbd>F7</kbd>).
3. Set the EA's `InpExtraMode` to `EXTRA_EVENT`.

The EA asks for it by the relative name `TickStreamer\TickSpy`, so the
subfolder is part of the name. Do not drop it on a chart by hand.

The spy is an indicator with no buffers and no plots, created per extra symbol
with `iCustom()` and never drawn. An indicator has no timer floor — *"In
indicators, the `OnCalculate()` function is called after the arrival of each
tick"* — and it cannot open a socket, since `SocketCreate` returns error 4014
when called from one. So the indicator only raises the alarm: it sends a custom
chart event carrying the symbol's index and name, and the EA does the
collecting.

The timer keeps running as a backstop at `InpEventBackstopMs` (default 100 ms),
which also becomes the timer period once every symbol has a spy. If any spy fails
to attach, the timer period falls back to `InpPollMs` and the failing symbol
stays on timer polling; the EA names that symbol in the log.

Chart events can be dropped:

> If the ChartEvent is already in an mql5 program queue or such an event is
> being handled, then a new event of this type is not placed into a queue.
> […] Event queues have a limited but sufficient size, so the queue overflow is
> unlikely for a correctly developed program. When the queue overflows, new
> events are discarded without being set into a queue.
> — [Event Handling](https://www.mql5.com/en/docs/event_handlers)

The event carries no price — only a symbol index and a name — and the EA answers
it with `CopyTicks(from = its own cursor)`. A coalesced or discarded event
therefore costs latency, never a tick: the backstop, or the next event, collects
from the same cursor. `evt_late` in the EA's stats line counts the events whose
collection produced no record. `evt_n = 0` while `mode=event` means the spies are
not calculating.

Events that arrive while the socket is down are ignored and not counted at all.
The cursor stays put and the first collection after the reconnect picks those
ticks up.

### `InpSpyPeriod` and terminal memory

`iCustom(symbol, period, …)` binds the handle to a *(symbol, period)* timeseries
that the terminal must build and hold. With "Max bars in charts" at 100 000 000,
a `PERIOD_M1` series per spied symbol is deep: 72 spied symbols measured a
**16.8 GB** terminal working set, about 233 MB each. `InpSpyPeriod` defaults to
`PERIOD_MN1`, the coarsest timeframe, and the same 72 symbols measured **554 MB**
there. `EXTRA_POLL` attaches no spies at all and measured **423–431 MB**.

The handle's timeframe never affects *when* a spy fires. Lowering `InpSpyPeriod`
towards `PERIOD_M1` brings the memory back.

Attaching the spies costs time at start-up: **343 ms** for 72 symbols on
`PERIOD_MN1`. On `PERIOD_M1` the same step took 1 172 ms at 10 symbols, 2 547 ms
at 29 and 5 125 ms at 72. The EA logs what it got, e.g.
`[TickStreamer] attached 72 of 72 tick spies on PERIOD_MN1 in 343 ms`.

### Timer period, heartbeats and warm-up

The timer period is 200 ms with no extra symbols, `InpPollMs` with extra symbols
under `EXTRA_POLL`, and `InpEventBackstopMs` under `EXTRA_EVENT` when every
symbol got a spy.

Heartbeats are claimed on that same timer, so the **effective heartbeat interval
is `max(InpHeartbeatMs, timer period)`**: a timer period of 500 ms with
`InpHeartbeatMs = 100` gives beats every 500 ms. The EA prints the effective
interval at start-up when the two disagree.

The first `CopyTicks()` for a symbol synchronises that symbol's tick database and
can block the calling thread for **up to 45 s**. The EA does that work up front
in `OnInit` under a **5 s budget**, and warms whatever the budget does not reach
one symbol per timer tick; a symbol is not collected until it has been warmed.
Warm-up asks for the newest ticks, so the cursor lands on the present and no
history is streamed at start.

### The limits of "every tick"

One collection takes at most **256 ticks** per symbol (`EXTRA_MAX_TICKS`);
anything beyond that waits for the next collection. Ticks accumulated deeper than
the terminal's 4096-tick memory cache are served from the on-disk tick database:
slower, not lost.

The cap has one hard edge. A *single millisecond* holding 256 ticks or more
cannot be drained by asking again: the cursor sits on that millisecond and every
subsequent `CopyTicks(from = it)` returns the same capped batch. The EA detects
that case — a full batch that produced no record — and forces the cursor to
`last_msc + 1`, losing that millisecond's remainder so the symbol keeps streaming
instead of stalling for ever. `cursor_skip` counts the occurrences, not the
records: what sat past the cap was never returned. Any non-zero value is a signal
to raise `EXTRA_MAX_TICKS`.

"Every tick" means every tick *while the link is up*. Records that a full buffer
or a failed send discards are counted in `dropped` and are not replayed after a
reconnect: the cursor has moved on, and re-sending stale quotes would corrupt
every latency number downstream.

## Measured numbers

Loopback, one process, synthetic feeder: Windows 11 (26200), Python 3.13.14,
Intel Core i9-14900KF. Each figure is the median of three interleaved 8-second
runs; percentiles are per frame.

**End to end — `python benchmarks/bench.py`:**

| Load | Format | Result |
| --- | --- | --- |
| 200 ticks/s | JSON | p50 **0.091 ms**, p99 0.176 ms |
| 20,000 ticks/s | JSON | **0 dropped, 0 gaps**; p50 **0.101 ms**, p99 0.186 ms |
| 20,000 ticks/s | binary | 20,006 ticks/s, same throughput as JSON; ~5× less client CPU (percentiles are not published for this format — a binary frame carries no send timestamp) |

Live FX is typically 10–500 ticks/s across all symbols. 20,000/s is about 40×
that. A single-run max is usually a GC pause or a scheduler hiccup. At
200 ticks/s the hop is mostly a wakeup and a loopback write (one tick per frame).

**Per-tick CPU — `python benchmarks/micro.py`:**

`bench.py` measures everything, including the event loop, uvicorn, the WebSocket
framing and the OS scheduler, which swamp a change of a few hundred nanoseconds.
`micro.py` runs the same code with no loop and no socket.

| Measurement | Result |
| --- | --- |
| Ingest, 1 subscriber (`Hub.feed`: frame, decode, account, fan out) | **0.999 µs/record** — 1.0 M records/s |
| Ingest, 4 subscribers | **1.018 µs/record** — 0.98 M records/s |
| Encode, `json`, 20 ticks/frame | 18.6 µs/batch (0.93 µs/tick) |
| Encode, `json`, 1 tick/frame | 1.25 µs/batch |
| Encode, `binary`, 20 ticks/frame | 0.79 µs/batch |

Ingest barely moves from one subscriber to four, because fan-out resolves a
subscription's filter once per *batch*: adding consumers costs a list append, not
a pass over every tick. `binary` encode is ~20× cheaper than `json`.

**Reproduce:**

```bash
python benchmarks/bench.py --rate 20000 --duration 8
python benchmarks/micro.py
```

**Comparing two versions.** Point `PYTHONPATH` at each tree in turn and run the
*same* benchmark file against both, alternating (A, B, A, B, …) rather than all
of A then all of B — a machine drifts, and interleaving stops the drift from
looking like a result. `micro.py` prints the `mt5_ws_stream` package it actually
imported, which is how you notice that a `PYTHONPATH` typo measured the installed
copy twice. Run the same commit on both sides once first: whatever spread that
shows is your noise floor, and on this machine it is ±3 % on the medians. Nothing
smaller than that is a result.

## Symbol scaling

How the EA behaves at N extra symbols, measured **2026-08-18** on a live
XMTrading demo terminal driven by `benchmarks/live/` (`run.py sweep`). Raw
record, including the baseline comparisons:
[`measurements/2026-08-18-live-sweep.md`](measurements/2026-08-18-live-sweep.md).

Read the table with the session in mind. It was taken in the **Asian session,
crossing the 09:00 JST Tokyo open**, at 0.1–1.7 ticks/s per symbol — thin. Tick
rates therefore rise down the table for reasons that have nothing to do with the
code, and **tick counts across rows are not comparable**. The cost columns
(`poll_us`, `extra_obs` / `extra_sent`) are.

| N | Mode | ticks/s | hop p50 / p99 (ms) | `poll_us` avg/max/p99 | `extra_obs` / `extra_sent` | `evt_late` / `evt_n` | dropped / gaps |
| ---: | --- | ---: | --- | --- | --- | --- | --- |
| 1 | `poll` | 0.3 | 0.183 / 0.221 | 0 / 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| 1 | `event` | 0.6 | 0.177 / 0.243 | 0 / 0 / 0 | 0 / 0 | 0 / 0 † | 0 / 0 |
| 10 | `poll` | 11.1 | 0.174 / 0.229 | 12 / 163 / 61 | 19 085 / 335 | 0 / 0 | 0 / 0 |
| 10 | `event` ‡ | 16.4 | 0.167 / 0.250 | 14 / 99 / 45 | 3 760 / 380 | 56 / 380 (15 %) | 0 / 0 |
| 29 | `poll` | 22.4 | 0.175 / 0.285 | 23 / 210 / 71 | 55 479 / 698 | 0 / 0 | 0 / 0 |
| 29 | `event` ‡ | 26.5 | 0.167 / 0.260 | 29 / 148 / 82 | 10 098 / 703 | 48 / 695 (7 %) | 0 / 0 |
| all (72) | `poll` | 64.9 | 0.172 / 0.277 | 42 / 157 / 114 | 142 755 / 2 038 | 0 / 0 | 0 / 0 |
| all (72) | `event` | 83.6 | 0.160 / 0.243 | 48 / 137 / 89 | 27 160 / 2 618 | 267 / 2 607 (10 %) | 0 / 0 |

† At N=1 `InpSymbols` is empty, so there is nothing to spy on and `EXTRA_EVENT`
runs the same path as `EXTRA_POLL`. `evt_n = 0` is the correct answer there, not
a sign the spies failed. The 0.3 → 0.6 ticks/s difference between those two rows
is market variance on the chart symbol over 60 s.

‡ Measured with `PERIOD_M1` spies. Only the N=all `event` row was taken on the
shipped `InpSpyPeriod = PERIOD_MN1`. The delivery numbers of the two spy periods
are equivalent — only the memory differs, by 30×.

**The rungs are 1, 10, 29 and `*`.** The terminal truncates a parameter line at
**255 characters including the `key=`**, silently and mid-name, which
caps `InpSymbols` at 244 characters — about 28 symbols. N=29 is the largest
expressible set (240 of 244 characters). Past that cap, `InpSymbols="*"` is the
only expressible answer, or run **two EA instances on two charts** with ~25
symbols each — a different deployment, with two sockets, two `seq` spaces and
per-feeder rather than per-process counters. See
[Troubleshooting](troubleshooting.md#inpsymbols-is-truncated-at-244-characters).

`N = all` is 72 extra symbols: every instrument in Market Watch on that account.

`poll_us`, `extra_obs`, `extra_sent`, `evt_*`, `ct_*` and `cursor_skip` come from
the EA's own `InpStatsSec` stats line; ticks/s and the hop come from the bridge
and the client (`benchmarks/symbol_scaling.py`).

### What the table says

* **Poll cost grows with N and stays small.** Mean poll-loop duration is 12 µs at
  10 symbols, 23 µs at 29 and 42 µs at 72 — against a 10 ms timer period, 0.4 %
  of one timer tick at the largest N.
* **Event mode does about 5× fewer `CopyTicks` reads.** `extra_obs` counts what
  `CopyTicks` handed back, most of it the same last tick re-read on a poll that
  had nothing new; `extra_sent` counts records actually produced. `extra_sent`
  follows the market's own tick count, `extra_obs` follows the mode.
* **`evt_late` runs 7–15 %**, so most spy events beat the backstop to the tick.
  `evt_bad = 0` throughout.
* **No loss anywhere.** Every row: `dropped = 0`, `seq_gaps = 0`, `ct_err = 0`,
  `cursor_skip = 0`, at any N, in either mode.

### Memory and CPU at 72 extra symbols

Working set was measured only at N=all; there is no figure at N=1, 10 or 29.

| N = 72 extra symbols | working set | vs `poll` | CPU-s | spy attach |
| --- | ---: | ---: | ---: | ---: |
| `poll` (no spies) | 423 MB | 1.0× | 24.6 | — |
| `event`, `PERIOD_M1` | **16 783 MB** | **39.7×** | 100.0 | 5 125 ms |
| `poll`, re-measured | 431 MB | 1.0× | 18.3 | — |
| `event`, `PERIOD_MN1` *(shipped)* | **554 MB** | **1.29×** | 33.9 | 343 ms |

The two `PERIOD_MN1`-era rows use same-process baselines (sampled before the EA
attached anything): `poll` 59.0 → 431.0 MB, `event` 58.5 → 554.0 MB. The
`PERIOD_M1` row comes from a cross-run snapshot, so treat 16.8 GB as an
order-of-magnitude figure.

### Ticks lost (ground truth, N=10, 120 s window)

Counted in the terminal with `CopyTicksRange` against the records the wire
carried over the same window, in `EXTRA_POLL` at 10 extra symbols:

| | terminal | wire | difference |
| --- | ---: | ---: | ---: |
| extras total | 1 792 | 1 797 | −5 (−0.28 %) |

**What this shows:** no coalescing signature. The total sits inside ±0.5 %, and
the per-symbol differences are 0–5 ticks with no relationship to symbol activity
— the busiest symbol and the quietest differ by the same handful. That is the
receive-time-versus-`time_msc` edge slop described below, not loss. A coalescing
signature would be one-sided and would scale with activity.

**What it cannot show:** anything outside its own window. Ground truth was run at
N=10, in `EXTRA_POLL`, in a session too thin to stress the collection, so the
other rows carry no ticks-lost figure rather than a guessed one. The chart symbol
reads `terminal = 0` by construction: it reaches the wire through `OnTick`, so
nothing calls `CopyTicks` on it and its tick database is never synchronised.

## How ticks lost is measured

`CountTicks.mq5` counts ticks in the terminal's database so you can compare them
to the wire. The EA does not call it.

Ticks lost is not an EA counter: the wire count and the terminal count come from
two places that share no code.

* **`mql5/Scripts/TickStreamer/CountTicks.mq5`** — point it at a symbol list and
  a `[from_msc, to_msc]` window and it calls `CopyTicksRange()` once per symbol,
  printing `symbol,count` to the Experts log and to a CSV under `MQL5\Files\`.
  **`InpFromMsc` / `InpToMsc` are in the broker's server clock, not UTC**, since
  `CopyTicksRange` filters on `MqlTick.time_msc` — shift a UTC window by the
  offset the EA prints on every start (`server_utc_offset=+3.0h`). Passing UTC to
  a UTC+3 broker counts a window three hours away and reads as catastrophic loss.
  A second trap: a symbol whose tick database has never been synchronised returns
  *nothing* rather than an error, and the first `CopyTicks` is what triggers the
  sync — so run the count twice.
* **`benchmarks/compare_tick_counts.py`** — joins that CSV against a
  `symbol_scaling.py --csv` run over the same window and prints
  `ticks_lost = terminal_count - wire_count` per symbol plus a total.
* **`benchmarks/live/run.py groundtruth`** — drives all of it unattended: it
  measures the window, runs `CountTicks.mq5` through a startup configuration file
  (`[StartUp] Script=` plus `ScriptParameters=`, launched as
  `terminal64.exe /config:…`), then runs the comparison.
* **`benchmarks/wizard_baseline_sweep.sh --mode after`, stage 9** — the manual
  equivalent: it records the wire-side window and count, compiles and installs
  `CountTicks.ex5`, walks the operator through running it from the Navigator with
  the matching `InpFromMsc` / `InpToMsc`, and appends the resulting table to the
  results file.

The wire window is bounded by *receive* time and the terminal count by the
broker's `time_msc`, so a tick landing near either edge falls inside one window
and outside the other. A few ticks either way is the measurement's slop.

Interleave the configurations you are comparing within one session: market tick
rates differ enough between hours to swamp the effect being measured. The sweep
already interleaves `EXTRA_POLL` and `EXTRA_EVENT` at each N with the same
`InpSymbols`.

## Reading the two latency numbers

The bridge reports **`broker_lag_ms`** and a client can compute a
**bridge → consumer hop**. They measure different things and only one is a clean
measurement.

* **`broker_lag_ms` = local clock − `time_msc`.** The wire carries UTC — the EA
  normalises `MqlTick.time_msc`, which is broker server time, before sending
  (`InpUtcTimestamps`, default on) — so a suspiciously round offset (whole hours)
  means that normalisation is missing upstream. What remains is broker latency
  *plus* whatever clock skew exists between the broker's server and yours: a
  trend and a way to spot a feed falling behind, not a calibrated latency. In the
  sweep it read about **−0.80 s** on every row because that machine's clock was
  0.918 s behind NTP, which is a clock artefact rather than a negative latency.
* **The hop = client receive time − the frame's `rx`.** Clean when both ends
  share a clock (same machine, or NTP-synced). Binary frames carry no `rx`, so
  the hop is measurable only in `json`.

## Tuning, in order of effect

| Change | Effect |
| --- | --- |
| Run the terminal on a VPS near the broker's data centre | **Tens of ms** |
| `InpExtraMode=EXTRA_EVENT` instead of `EXTRA_POLL` | Removes the timer wait on every extra symbol — up to one effective timer period, `max(InpPollMs, 10–16 ms)`. Costs one spy indicator per symbol and needs `TickSpy.ex5`; at 72 symbols on `PERIOD_MN1` the terminal's working set goes 431 → 554 MB and hop p50 0.172 → 0.160 ms |
| One EA instance per chart instead of `InpSymbols` | The same saving as the row above, at one chart per symbol. Applies where `EXTRA_EVENT` cannot run — a symbol that failed to get a spy stays on the timer |
| Run the bridge on the same host as the terminal | A network round-trip |
| `format=binary` for non-browser consumers | 5–10× less consumer CPU |
| `conflate=1` for display consumers | Removes queueing delay under load |
| Windows power plan set to High Performance | Steadier timer resolution and scheduling |
