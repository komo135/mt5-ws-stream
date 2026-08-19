# Troubleshooting

Each entry is a symptom, its cause, what to check, and what to do about it.

EA lines are printed to the terminal's **Experts** tab and all begin `[TickStreamer]`.
Bridge lines go to the console running `mt5-ws-stream bridge`.

---

## `SocketCreate()` fails / error 4014

**Cause:** MQL5 socket functions are available to Expert Advisors, scripts and services
only. Error 4014 means the call came from an indicator. The same call also fails when the
host is missing from the terminal's allow-list, with a different error code:

```
[TickStreamer] SocketCreate failed (error 4014). Add 127.0.0.1 to Tools > Options > Expert Advisors > allowed URLs.
```

Check, in order:

1. **What is calling `SocketCreate`.** An indicator cannot open a socket. The bundled
   `TickSpy` indicator only raises a custom chart event for this reason; the EA does the
   sending.
2. **The allow-list.** MQL5 socket functions share the WebRequest allow-list:
   **Tools → Options → Expert Advisors → "Allow WebRequest for listed URL"**.

**Fix:** add `InpHost` (`127.0.0.1` by default) to the allow-list, and run socket code
from an EA, script or service.

## A recompiled EA still behaves like the previous version

**Cause:** an EA already attached to a chart does not always pick up a fresh `.ex5`.

**Fix:** open MetaEditor from the terminal and recompile there (<kbd>F7</kbd>), or remove
the EA from the chart and drag it back on.

## The EA runs but no ticks arrive

**Cause:** in almost every case the stream is empty at the source rather than lost in
transit.

Check, in order:

1. **Algo Trading is enabled.** The toolbar button must be on and the chart must show a
   smiling face, not a sad one.
2. **The market is open.** Forex is quiet at weekends. To test the Python side
   on its own:
   [CONTRIBUTING.md](../CONTRIBUTING.md#running-the-python-side-without-metatrader).
3. **The symbol is in Market Watch.** The EA calls `SymbolSelect` for extra symbols, but a
   symbol your broker does not offer never ticks. A symbol it could not select is named
   once:

   ```
   [TickStreamer] cannot add NZDCAD to Market Watch; skipping
   ```

4. **`InpSymbols` covers what you expect.** With `InpSymbols` empty the EA streams the
   chart's own symbol and nothing else. Set `InpSymbols=EURUSD,USDJPY,...` for a specific
   list, `InpSymbols=*` for every symbol in Market Watch, or attach one EA instance per
   chart.
5. **Warm-up has finished.** The start-up line reports it:

   ```
   [TickStreamer] warmed up 12 of 40 extra symbols in 5013 ms; the rest are warmed one per timer tick
   ```

   Symbols that are not warm yet do not stream; see "The EA pauses at start-up with many
   extra symbols" below.

**Fix:** enable Algo Trading, set `InpSymbols`, and confirm the symbol count the EA
resolved in its `started` line:

```
[TickStreamer] started chart=BTCUSD# extra_symbols=29 mode=poll timer=10ms stats=60s server_utc_offset=+3.0h
```

## The EA pauses at start-up with many extra symbols

**Cause:** the first `CopyTicks()` for a symbol synchronises that symbol's tick database,
downloading whatever the terminal is missing, and the MQL5 documentation allows it to
block for up to 45 s. The EA spends that cost in `OnInit()` — before the timer starts and
before the socket opens — for as many symbols as fit in a 5 s budget, keeping the
synchronisation out of the timer, where it would stall `OnTick()` and the chart symbol
would lose ticks.

Check the Experts tab for:

* `warmed up %d of %d extra symbols in %d ms` — printed at every start. The trailing
  `; the rest are warmed one per timer tick` appears when fewer than all were warmed.
* `warm-up EURUSD took 1234 ms (tick database synchronised)` — printed only for symbols
  whose call took a second or more, which are the ones whose ticks had to be fetched.
  Needs `InpVerbose`.
* `warm-up XYZ returned 0 (error N); its cursor is anchored from Market Watch on the first
  poll instead` — that symbol's database had nothing to give. It streams from the Market
  Watch quote on its first poll. Common at weekends and for symbols only just added to
  Market Watch. Needs `InpVerbose`.

**Fix:** none required. The remaining symbols are warmed one per timer tick, and each can
still cost that tick up to 45 s once before the symbol goes live; symbols already warm
keep streaming throughout. Shorten `InpSymbols` if the start-up cost is unacceptable.

## The EA cannot connect: which of the three messages you get

**Cause:** one of three distinct failures. `SocketConnect()` returns `true` even when it
burned its whole 1000 ms timeout without connecting, so the EA times each attempt and
classifies it before reporting. All three need `InpVerbose`.

```
[TickStreamer] cannot reach 127.0.0.1:9800 after 2 ms (error 5273). Is the bridge running?
```

Nothing is listening and the host refused the connection promptly — the ordinary case.

```
[TickStreamer] connect to 127.0.0.1:9800 timed out after 1000 ms (error 5273): nothing is listening there, or this host does not refuse closed loopback ports (WSL2 mirrored networking, firewall). Start the bridge with 'mt5-ws-stream bridge'.
```

Same cause, different host behaviour: the SYN was swallowed rather than refused, so the
attempt burned the full 1000 ms budget. Observed on Windows with WSL2's **mirrored
networking** mode. A timed-out connect does not count toward the hint below — nothing
accepted anything.

```
[TickStreamer] connection to 127.0.0.1:9800 was accepted but dropped at once after 8 ms (error 5273). Is the bridge running?
```

Something accepted the connection and then hung up. Three such connections in a row, each
dying within 3 s of being accepted, add one further line, printed once per EA run:

```
[TickStreamer] connection dropped right after connecting 3 times in a row: something is listening on 127.0.0.1:9800 but hanging up. Is it really the bridge, and does it stay up? Start it with 'mt5-ws-stream bridge'.
```

Check, in order:

1. **The bridge is running.** Start it before attaching the EA.
2. **`InpHost` / `InpPort` match the bridge's `--tcp-host` / `--tcp-port`.**
3. **The allow-list**, if `SocketCreate` itself is failing — see "`SocketCreate()` fails /
   error 4014" above.

**Fix:**

```bash
mt5-ws-stream bridge                     # start it, then attach the EA
mt5-ws-stream bridge --tcp-port 9801     # if 9800 is taken (set InpPort to match)
```

The EA retries every `InpReconnectMs` (2000 ms by default), so starting the bridge
afterwards recovers on its own; on the way down it logs
`[TickStreamer] connection lost; reconnecting in 2000 ms`. While it is reconnecting it
spends most of each attempt blocked inside `SocketConnect`, and ticks arriving in that
window are counted in `dropped=` and lost — a reconnect loop makes the stream look sparse
as well as broken.

## `InpExtraMode=EXTRA_EVENT` but the Experts tab says `no tick spy for ...`

The full message:

```
[TickStreamer] no tick spy for EURUSD (iCustom "TickStreamer\TickSpy" failed, error 4802); that symbol falls back to timer polling. Compile MQL5\Indicators\TickStreamer\TickSpy.mq5 in this terminal.
```

**Cause:** the spy indicator is not installed, or not compiled, in *this* terminal.
`EXTRA_EVENT` needs it; `EXTRA_POLL` does not, so it is a separate install step.

Check:

1. `MQL5\Indicators\TickStreamer\TickSpy.ex5` exists in the terminal's data folder
   (**File → Open Data Folder**). The subfolder is part of the name the EA asks for:
   `iCustom()` resolves the relative name `TickStreamer\TickSpy` under `MQL5\Indicators`,
   so `MQL5\Indicators\TickSpy.ex5` is not found.
2. The attach summary, printed once:

   ```
   [TickStreamer] attached 28 of 29 tick spies on PERIOD_MN1 in 412 ms
   ```

**Fix:** copy `mql5/Indicators/TickStreamer/TickSpy.mq5` into
`MQL5\Indicators\TickStreamer\`, open it in MetaEditor and compile (<kbd>F7</kbd>).

Nothing breaks meanwhile: a symbol without a spy stays on the timer, and while any symbol
is in that state the EA runs the timer at `InpPollMs` rather than the slower
`InpEventBackstopMs`, so the fallback matches `EXTRA_POLL` behaviour.

## The EA says heartbeats are slower than `InpHeartbeatMs`

The message, printed once at start-up:

```
[TickStreamer] heartbeats are claimed on the timer, so the effective interval is 500 ms (the timer period), not InpHeartbeatMs=100 ms
```

**Cause:** heartbeats are emitted from `OnTimer`, so the effective interval is
`max(InpHeartbeatMs, timer period)`. `InpHeartbeatMs` is a floor, not a period.

Check which timer period applies:

| Configuration | Timer period |
| --- | --- |
| No extra symbols (chart only) | 200 ms |
| Extra symbols, `EXTRA_POLL` | `InpPollMs` (default 10 ms), floored at 1 |
| Extra symbols, `EXTRA_EVENT`, every symbol got a spy | `InpEventBackstopMs` (default 100 ms) |
| Extra symbols, `EXTRA_EVENT`, any spy failed | `InpPollMs` |

**Fix:** nothing, unless the effective interval is too close to the idle timeout on
whatever watches the feeder link. If it is, lower `InpEventBackstopMs` (or `InpPollMs`).
The EA never shortens the timer period on its own to meet `InpHeartbeatMs`.

## The EA's stats line shows `mode=event` but `evt_n=0`

**Cause:** the spy indicators loaded (no `no tick spy for ...` line) but none of them is
firing, so every extra symbol is collected by the backstop timer at `InpEventBackstopMs`
— slower than `EXTRA_POLL` would be.

Check, in order:

1. **Is anything ticking at all?** `evt_n=0` alongside `extra_sent=0` is a quiet market,
   not a broken spy.
2. **Was `TickSpy.ex5` recompiled after an edit?** A stale `.ex5` still loads.
3. **Does an indicator created by `iCustom()` on a symbol with no open chart get
   calculated on this terminal build?** The MQL5 documentation states that
   `OnCalculate()` runs on the arrival of each tick but says nothing about whether a chart
   must exist. If one must on your setup, `EXTRA_EVENT` is not usable for symbols without
   charts.

**Fix:** set `InpExtraMode=EXTRA_POLL`. It is the default and depends on none of this.
See [docs/latency.md](latency.md#extra-symbols-polling-or-events) for how the two modes
differ.

## The EA's stats line shows `evt_late` close to `evt_n`

**Cause:** `evt_late` counts spy events whose collection produced no record — the backstop
timer, or an earlier event, had already taken that tick. A ratio close to 1 means the
events arrive too late to save any latency, so `EXTRA_EVENT` costs one indicator per
symbol and delivers what `EXTRA_POLL` would.

Some ratio is expected, and it is not data loss: an event carries no price, only "this
symbol ticked". Whatever an event misses, the next collection picks up from the same
cursor. Chart events are droppable: the MQL5 documentation says a `ChartEvent` is not
queued while one is already queued or being handled, and that a full queue discards new
events. The backstop timer collects whatever the events miss.

Check `evt_bad` on the same line: it counts events whose symbol index or name the EA does
not recognise. A few right after re-attaching the EA are stale events from the previous
symbol list and are harmless. A steady stream means another program on the same chart is
sending `CHARTEVENT_CUSTOM+1`.

**Fix:** switch to `InpExtraMode=EXTRA_POLL` if the ratio stays near 1; it delivers the
same ticks on the timer without the spy indicators' memory cost.

## The EA's stats line shows `ct_err` climbing

**Cause:** `ct_err` counts `CopyTicks()` calls that returned `-1` while collecting extra
symbols. A failed call costs latency, never data: the feed's cursor is left where it was,
so the next poll asks for exactly the same span again. A steady non-zero count means the
terminal cannot serve that symbol's ticks.

Check, in order:

1. **The `warm-up` lines at start** — a symbol whose database never synchronised.
2. **Market Watch** — a symbol removed from it while the EA was running.

**Fix:** re-select the symbol in Market Watch, or drop it from `InpSymbols`.

`extra_obs` minus `extra_sent` on the same line is not an error: `CopyTicks(from)` is
inclusive at its lower bound, so every poll that returned anything hands back the ticks
already sent from the cursor's millisecond and the EA skips them. Expect roughly one
skipped tick per symbol per productive poll.

## The EA's stats line shows `cursor_skip` non-zero

**Cause:** the cursor names a position as "millisecond *M*, and *n* ticks of it already
delivered". Once `EXTRA_MAX_TICKS` (256) or more ticks share *M*, every
`CopyTicks(from = M)` returns the same capped 256 — all already delivered — so the cursor
can never leave *M* and that symbol stops streaming. The EA steps the cursor to *M + 1*,
counts the occurrence, and logs it under `InpVerbose`:

```
[TickStreamer] EURUSD: 256 ticks or more at time_msc=1755400000123, all already sent; cursor forced past that millisecond, anything past the cap is lost
```

Whatever sat past the cap in that millisecond is lost so that everything after it still
arrives. `cursor_skip` is an occurrence count, not a record count: `CopyTicks()` never
returned the lost ticks, so there is nothing to count them by. At exactly 256 there is no
remainder and the advance loses nothing; the counter cannot distinguish the two cases.

Check: `cursor_skip` should be `0` on any live feed. A synthetic feed or a tester run
replaying compressed history is the realistic way to reach the cap; a live FX symbol is
orders of magnitude below it.

**Fix:** raise `EXTRA_MAX_TICKS` in `mql5/Experts/TickStreamer/TickStreamer.mq5` and
recompile. It is a compile-time constant, not an input: it also sizes the tick buffer the
poll body reuses, which is allocated once at start-up.

## `InpSymbols` is truncated at 244 characters

**Cause:** the terminal cuts a parameter line at **255 characters including the `key=`
prefix**, so `InpSymbols` holds at most 244 characters — roughly 28 eight-character names.
It does this silently and mid-name: the EA starts, resolves fewer symbols than you asked
for, and complains once about a symbol with a nonsense name. A list of 54 symbols produces:

```
[TickStreamer] cannot add N to Market Watch; skipping
[TickStreamer] started chart=BTCUSD# extra_symbols=29 mode=poll timer=10ms stats=60s server_utc_offset=+3.0h
```

`N` is the first letter of `NZDCAD#`, the 30th name, cut in half: the terminal rewrites
the chart file with `InpSymbols` ending `...,JP225Cash#,N`.

Check **`extra_symbols=` in the `started` line** against the number you asked for. It is
the only place the truncation is visible. The cap applies to every file-based route: a
chart's `<inputs>` block and a `.set` file passed as `ExpertParameters` or
`ScriptParameters` truncate at the same character.

**Fix:** one of two options.

* **`InpSymbols="*"`** — every symbol in Market Watch, whatever the list length. The usual
  answer.
* **Two EA instances on two charts**, splitting the list. This is also the lowest-latency
  shape, since each chart symbol gets its own `OnTick`, at the cost of two feeder
  connections with independent `seq` numbering — see `GET /api/v1/feeders` in
  [docs/protocol.md](protocol.md#rest-endpoints).

---

## Updates arrive only every few seconds

**Cause:** almost always the broker's real tick cadence for that symbol, not pipeline
latency. Some symbols do not trade often: on a Sunday, one broker's `BTCUSD#` delivered
one tick every 5–6 s with unchanged bid/ask, so the stream showed one update every 4–6 s
while the pipeline's own hop stayed well under a millisecond
([docs/latency.md](latency.md#measured-numbers)).

Check, in order:

1. **The terminal itself.** Market Watch → right-click the symbol → **Ticks**, or the
   chart's tick timeline, at the same moment. A matching cadence means there is nothing to
   fix.
2. **`mt5-ws-stream client --print`.** It shows `+Nms` (the interval since the previously
   printed tick) and `lag=Nms` (local clock minus `time_msc`, which includes clock skew).
   A large `+Nms` with a small `lag=` means slow market, not slow pipeline.
3. **`mt5-ws-stream -v bridge`.** At DEBUG it logs every chunk received from the feeder:
   `feeder %s: %d bytes -> %d records, +%.0f ms since previous chunk`. Gaps here mirror
   gaps at the source.
4. **The EA's `InpStatsSec` summary** (needs `InpVerbose`): ticks seen in the interval,
   the rate, drops, and `SocketSend` timing. Few ticks there means the terminal saw few
   ticks.
5. **The synthetic feeder** (`mt5-ws-stream mock --rate 200`; see
   [CONTRIBUTING.md](../CONTRIBUTING.md#running-the-python-side-without-metatrader)).
   Isolates the Python side: if this is fast, the cadence is on the MetaTrader
   side.
6. **Weekends and holidays.** FX is closed; only crypto and a handful of indices tick,
   often sparsely.
7. **`InpSymbols` and single-chart behaviour.** With `InpSymbols` empty only the chart's
   own symbol streams — see
   [the EA runs but no ticks arrive](#the-ea-runs-but-no-ticks-arrive).

**Fix:** none, when the terminal shows the same cadence. **Update interval** is
how often the source ticks. **Latency** is hop time. A large `+Nms` with a small
`lag=` is a quiet market.

## The dashboard shows "disconnected"

**Cause:** the browser cannot open a WebSocket to the bridge — the bridge is down, the URL
is wrong, the bind address is loopback-only, or the page is on `https://`.

Check, in order:

1. **The bridge is up:** `curl http://127.0.0.1:8765/api/v1/health`, or
   `mt5-ws-stream client --print` in a terminal.
2. **The URL field points at `/ws`, not the bare host.** `mt5-ws-stream dashboard` opens
   `http://127.0.0.1:8765/dashboard` (the copy the bridge serves) when a bridge is
   reachable, and that page's URL field defaults to `ws://127.0.0.1:8765/ws`. Only a
   hand-edited URL or an old bookmark is missing the path.
3. **Which host the bridge is bound to.** It binds to loopback by default, so another
   machine cannot reach it.
4. **The page's scheme.** A page loaded over `https://` cannot open a plain `ws://`
   connection.

**Fix:** start the bridge; restore `/ws` in the URL field; for another machine
use `--ws-host 0.0.0.0`; for HTTPS, terminate TLS at a proxy and use `wss://`.

## Prices lag behind and never catch up

**Cause:** the consumer is slower than the feed, so its `lossless` queue grows and it works
through stale ticks.

**Fix:** add `conflate=1`:

```
ws://127.0.0.1:8765/ws?conflate=1
```

It keeps only the newest tick per symbol, so a slow consumer always sees current prices.
Correct for anything that renders; wrong for anything that records.

## `seq_gaps` keeps climbing

**Cause:** `seq_gaps` counts sequence discontinuities **within one feeder connection**. A
climbing count means a feeder emitted non-contiguous `seq` values while the link stayed up.
Wraparound past 2³² is treated as continuity, not a gap.

Check, in order:

1. **The bridge's periodic line**, for the cumulative totals:

   ```
   interval: tick_rate=201.4/s broker_lag_p50=1.5 ms broker_lag_p99=9.5 ms | total: ticks=9812 heartbeats=20 gaps=0 dropped=0 | now: symbols=4 consumers=2
   ```

2. **`GET /api/v1/feeders`**, which reports `seq_gaps` per connection, to identify which
   feeder is producing them.
3. **The feeder's own sequence numbering**, if it is not the bundled EA. Every record must
   carry the next `seq` on that connection.

**The bridge cannot see loss across an EA reconnect.** Sequence continuity is per
connection: the first record over a fresh link starts the count again, so `seq_gaps` stays
flat however many records the old link lost on its way out. When a `SocketSend` comes up
short the EA tears the link down in the same breath, which puts the burned sequence numbers
at a connection boundary where the bridge never checks. The EA's own `dropped=` and
`reconnects=` in its stats line are the counters that answer "did that reconnect cost
anything", and the short send itself is logged under `InpVerbose`:

```
[TickStreamer] SocketSend sent 384 of 640 bytes (error 5273); 4 of 10 records lost; reconnecting
```

Only the records past the byte `SocketSend` stopped at are lost; the records that did go
out are counted as sent, because the bridge received them.

**Fix:** gaps are a diagnostic, not a retransmission protocol — the data is gone. If you
need guaranteed delivery, record at the feeder.

## `dropped` is non-zero

**Cause:** a `lossless` consumer hit its queue limit and the hub shed the oldest half of
its queue (at minimum the overflow). Either the consumer is too slow, or `--queue-limit`
(default 20000) is too small for your burst profile.

Check the bridge's disconnect line for that consumer, which reports its final counters:

```
consumer #3 disconnected (frames=1204 ticks=9812 dropped=0)
```

**Fix:** raise `--queue-limit` to trade memory for tolerance of longer stalls, or switch
the consumer to `conflate` if it only renders prices — conflate has no queue limit and no
`dropped` accounting, because memory is bounded by the symbol count regardless of rate.

## Broker lag percentiles look enormous

**Cause:** `broker_lag_ms` is local clock − `time_msc`, so it includes clock skew between
your machine and the broker's server. Two different problems show up here.

* **A suspiciously round value — e.g. exactly -10,800,000 ms (-3 h).** The feeder is
  sending `time_msc` in the broker's **server time** instead of UTC. `MqlTick.time_msc` is
  server time (XM, for example, runs UTC+3); the wire carries UTC.
* **A few hundred milliseconds, possibly negative, after that.** Ordinary clock skew plus
  real broker latency.

Check, in order:

1. **The EA's `started` line** for `server_utc_offset=+3.0h`. The EA normalises `time_msc`
   to UTC when `InpUtcTimestamps=true` (the default) and prints the offset it is applying.
   For a feeder you wrote yourself, normalise there rather than correcting downstream.
2. **Your PC's clock**, for a steady negative value (e.g. -670 ms), which means the PC is
   behind the broker's server:

   ```
   w32tm /stripchart /computer:time.windows.com /samples:3
   w32tm /resync
   ```

**Fix:** set `InpUtcTimestamps=true` and resync the clock. Even after syncing, read the
*change* over time rather than the absolute value — it is a trend, not a calibrated
measurement.

## Symbol names are truncated

**Cause:** the wire format's symbol field is 12 bytes, ASCII, NUL-padded. `EURUSD.pro` (10
characters) fits; a 14-character name is cut to 12.

**Fix:** none available in configuration. If your broker uses longer names, open an issue
— extending the format is a versioned change.

## A symbol whose name contains `#` gives 404, or filters to the wrong symbol

**Cause:** `#` starts a *fragment* in a URL. It never reaches the server, so a broker
suffix like XMTrading's `EURUSD#` silently becomes `EURUSD`:

```
GET /api/v1/symbols/EURUSD#      -> 404: the bridge was asked for "EURUSD"
ws://.../ws?symbols=EURUSD#      -> subscribes to "EURUSD", not "EURUSD#"
```

Check `GET /api/v1/symbols`: the bridge stores and reports the name exactly as the
terminal gave it, `#` and all, so the listing shows `EURUSD#` while a hand-typed URL for it
does not work.

**Fix:** percent-encode it — `#` is `%23` — in both the path and the query string:

```
GET /api/v1/symbols/EURUSD%23
ws://.../ws?symbols=EURUSD%23
```

Any HTTP client's own quoting does it: `urllib.parse.quote(name, safe="")` in Python,
`encodeURIComponent(name)` in JavaScript. This is a consumer-side concern only.

---

## `CountTicks.mq5` reports zero, or counts a window you did not ask for

**Cause:** one of two traps in `CopyTicksRange`.

* **Wrong clock.** `InpFromMsc` / `InpToMsc` are compared against `MqlTick.time_msc`,
  which is the **broker's server clock**, not UTC — the MQL5 reference says only
  "milliseconds since 1970.01.01" and does not name the base. A wire-side window is UTC,
  because the EA normalises `time_msc` on the way out, so it must be shifted by the server
  offset before being passed in. Passing a UTC window straight to a UTC+3 broker counts a
  window three hours earlier. The symptom is distinctive: zero ticks for every metal and
  index, and a *burst* on exotic FX pairs — the 21:00 UTC rollover break, where instruments
  halt and spreads churn.
* **Unsynchronised tick database.** `CopyTicksRange` reads the terminal's tick database,
  and a symbol whose database has never been synchronised returns nothing rather than an
  error; the first `CopyTicks` call is what triggers the sync. A count taken right after a
  run that never called `CopyTicks` reads zero for every symbol.

Check, in order:

1. **The offset the EA printed at start**, and add it to the window:

   ```
   [TickStreamer] started chart=BTCUSD# extra_symbols=29 mode=poll timer=10ms stats=60s server_utc_offset=+3.0h
   ```

2. **The script's own summary line:**

   ```
   [TickStreamer][CountTicks] done: symbols=29 total=41207 errors=0 csv=TickStreamer_counts.csv
   ```

3. **The chart symbol's row**, which always reads zero by construction: it reaches the wire
   through `OnTick`, so nothing ever calls `CopyTicks` on it. Ground truth is a statement
   about the *extra* symbols, the ones on the polled path.

**Fix:** shift the window by the server offset and run the script a second time; the first
pass does the synchronising.

---

## Tests hang or ports collide

**Cause:** the suite binds to port 0 and reads back the real port, so collisions should not
happen. A wedged run is usually a leftover process from the subprocess smoke test.

**Fix:**

```bash
pytest -m "not slow"     # skip the subprocess smoke test
```

## Windows: `NotImplementedError` from asyncio

**Cause:** Windows event loops do not implement `add_signal_handler`.

**Fix:** none needed for the CLI, which falls back to `KeyboardInterrupt` handling. If you
embed `Bridge` yourself, do the same rather than assuming POSIX signal handling exists.
