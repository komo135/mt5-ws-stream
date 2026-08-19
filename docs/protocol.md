# Protocol reference

Two independent layers:

1. a fixed-size binary record from feeders to the bridge, over plain TCP;
2. a WebSocket message protocol from the bridge to consumers.

Implementing either half needs nothing from the other.

## 1. Feeder → bridge

A feeder — the MQL5 EA, the bundled mock feeder, or anything you write — opens a
TCP connection to the bridge's feeder port (default `127.0.0.1:9800`) and writes
a stream of identical 64-byte records. There is no handshake, no framing header,
no delimiter, and nothing to read back: the bridge never sends anything on this
socket.

Fixed-size records make framing arithmetic. Whatever is left after
`len(buffer) // 64 * 64` bytes is an incomplete record, and no byte sequence
inside a payload can be mistaken for a separator.

### Record layout

All fields little-endian, no padding. The equivalent Python `struct` format is
`<HHI12sqddddI`.

| Offset | Size | Type | Field | Notes |
| ---: | ---: | --- | --- | --- |
| 0 | 2 | uint16 | `magic` | `0x4B54`, ASCII `TK` |
| 2 | 2 | uint16 | `record_size` | `64` |
| 4 | 4 | uint32 | `seq` | Per-feeder counter; wraps at 2³² |
| 8 | 12 | char[12] | `symbol` | ASCII, NUL-padded, **not** NUL-terminated |
| 20 | 8 | int64 | `time_msc` | UTC milliseconds since the Unix epoch |
| 28 | 8 | float64 | `bid` | |
| 36 | 8 | float64 | `ask` | |
| 44 | 8 | float64 | `last` | |
| 52 | 8 | float64 | `volume_real` | |
| 60 | 4 | uint32 | `flags` | `MqlTick.flags`, with bit 31 (`0x80000000`) set on heartbeats |
| | **64** | | | |

### Field notes

**`magic` and `record_size` are the first four bytes.** A reader can reject a
peer speaking a different protocol on the first four bytes, instead of decoding
garbage into plausible-looking prices.

**`seq` detects loss; it does not repair it.** The counter is per feeder
connection. The bridge takes the first record of a connection as the baseline —
that record never counts as a gap — and counts every discontinuity after it into
`stats.seq_gaps`. A wrap past 2³² is continuity, not a gap. There is no
retransmission and no negative acknowledgement: a gap is a diagnostic. Because
continuity is per connection, a feeder may either continue or restart its
counter after a reconnect.

**`symbol` is 12 bytes so broker suffixes fit** — `EURUSD.pro`, `EURUSD.raw`,
`XAUUSD-5`. A longer name is truncated to 12 bytes rather than rejected. Read
exactly 12 bytes and strip trailing NULs; the field is padded, not terminated,
so a 12-character name has no NUL at all.

**`time_msc` is UTC milliseconds, not the broker's clock.** MQL5's
`MqlTick.time_msc` is broker *server* time, which is commonly UTC+2 or UTC+3.
The EA normalises it before sending — controlled by its `InpUtcTimestamps`
input, on by default — by subtracting `TimeTradeServer() − TimeGMT()` rounded to
the nearest 30 minutes. A feeder you write must do the same and send UTC; one
that sends server time makes the bridge's `broker_lag_ms_p50` / `p99` wrong by
the server's UTC offset, usually a suspiciously round number of hours. Even when
correct, comparing `time_msc` against a local clock measures broker latency
*plus* clock skew between the two hosts — read it as a trend. The bridge reports
that comparison as `broker_lag_ms_p50` / `p99`, not as a latency.

**Heartbeats** are records with bit 31 of `flags` set, an empty `symbol` field,
and all prices and volume zero. Their `time_msc` is stamped in UTC at the source
and is never shifted by the server-clock correction. They let a consumer
distinguish a quiet market from a dead link. Consumers do not receive them unless
they ask for them with `?heartbeats=1`, and a consumer that asks gets them
whatever its symbol filter is — a heartbeat is link liveness, not a quote about
an instrument.

### What the bridge does with what you send

* **Any chunk length is legal.** The bridge buffers a trailing partial record and
  completes it from the next packet; the remainder it holds is always fewer than
  64 bytes. Records may straddle TCP packets freely.
* **A chunk is decoded all-or-nothing.** Every record in a chunk is decoded
  before any of them is published, so one bad record aborts the whole chunk:
  nothing from it reaches consumers and no counter moves.
* **A bad header ends the connection.** A record whose `magic` or `record_size`
  does not match makes the bridge log the peer as speaking a different protocol
  and drop the connection. In a fixed-size stream there is no safe resync point,
  so reconnecting is the only recovery — the fresh connection starts on a clean
  record boundary.

### Compatibility rules

* Never change a field's offset or width.
* To extend the record, define a new `record_size` and branch on it. A reader
  that does not know the new size rejects it loudly rather than misinterpreting
  it.

## 2. Bridge → consumer

The bridge serves the WebSocket stream, a read-only REST API, the dashboard and
the OpenAPI docs from one HTTP port (default `127.0.0.1:8765`). This section
covers the stream; [REST endpoints](#rest-endpoints) covers the rest.

### Connecting

`/ws` is the only stream route. `/` is the HTTP index, so a bare-host URL does
not connect.

```
ws://127.0.0.1:8765/ws?symbols=EURUSD,USDJPY&format=json
```

| Parameter | Default | Meaning |
| --- | --- | --- |
| `symbols` | every symbol | Comma-separated allow-list. Entries are stripped, empty ones dropped, duplicates collapsed. Omit it, or send it empty, for every symbol |
| `format` | `json` | `json` or `binary` |
| `conflate` | `0` | Truthy selects the `conflate` backpressure policy; otherwise `lossless` |
| `heartbeats` | `0` | Truthy also delivers heartbeat records |

**Truthiness** for `conflate` and `heartbeats`: any value other than the empty
string, `0`, `false`, `no` or `off`, compared case-insensitively after stripping
surrounding whitespace. `1`, `true` and `yes` are the canonical spellings.

**`format` is matched on its first letter.** Any value starting with `b`
(case-insensitive) selects `binary` — `binary`, `bin`, `bytes` and `banana` all
do. Any other non-empty value, including a typo such as `jsonn`, selects `json`,
and so does omitting the parameter or sending it empty. A consumer reaching for
binary should spell it exactly, since only the `j`-side of this rule is
forgiving.

**Unknown parameters are ignored**, so a bridge adding one later does not break
an older client. **A parameter given more than once resolves to its last
occurrence.**

Clients that build the query string canonically write `format` always and omit
every other parameter that is at its default, which makes
`ws://host:8765/ws?format=json` the common case.

**Origin checks.** When the bridge runs with `--allow-origin`, it accepts the
WebSocket and then closes it with code `1008` and reason `origin not allowed` if
the request carried an `Origin` header that is not listed. A request with no
`Origin` header — any non-browser client — is always allowed, and without
`--allow-origin` every origin is allowed. It is a browser CSRF guard, not
authentication.

### Server frames

Every JSON frame carries its kind in a `t` tag. There are six kinds: `hello`,
`ticks`, `stats`, `ack`, `pong` and `error`. Under `format=binary`, tick data
arrives as WebSocket binary frames instead (see Binary frames below); every other
kind stays JSON text.

Two words recur with different meanings:

* **`symbols`** in `hello` and `ack` is *this connection's subscription filter*.
  `null` means every symbol, a list means exactly those, and `[]` means none — a
  reachable state, by unsubscribing from everything you asked for.
* **`available`** in `hello` is the bridge's *catalogue*: every symbol it has
  seen since it started, whether or not you subscribed to it. The `stats` frame's
  `symbols` is that same catalogue, because a `stats` frame reports the process
  rather than a session.

**`hello`** — sent once, before any other frame.

```json
{"t":"hello","id":1,"protocol":1,"format":"json","backpressure":"lossless","record_size":64,"symbols":["EURUSD"],"available":["EURUSD","USDJPY"],"snapshot":[{"s":"EURUSD","ms":1700000000123,"b":1.08501,"a":1.08504,"l":0.0,"v":0.0,"f":6,"q":42}],"rx":1700000000.61}
```

| Field | Meaning |
| --- | --- |
| `id` | Session id; the bridge's connect and disconnect log lines use the same number |
| `protocol` | Frame-grammar version, currently `1` |
| `format` | The payload format this session was granted |
| `backpressure` | `lossless` or `conflate`, fixed for the life of the connection |
| `record_size` | Width of one binary record, `64`. A `format=binary` consumer takes its stride from here rather than hard-coding it |
| `symbols` | This connection's filter |
| `available` | The bridge's catalogue |
| `snapshot` | Latest quote per *subscribed* symbol, in the same key spelling a `ticks` frame uses, so a chart can draw immediately instead of waiting for every symbol to trade. May be empty |
| `rx` | Send timestamp, epoch seconds |

**`ticks`** — one or more quotes.

```json
{"t":"ticks","rx":1700000000.612,"d":[{"s":"EURUSD","ms":1700000000123,"b":1.08501,"a":1.08504,"l":0.0,"v":0.0,"f":6,"q":42}]}
```

`rx` is the bridge's send timestamp in epoch seconds. Subtracting it from the
receive time gives the bridge → consumer hop, and it is meaningful only when both
ends share a clock. `d` holds any number of quotes, including zero, so a consumer
must iterate it rather than assume one tick per frame.

Tick object keys are one or two characters:

| Key | Field |
| --- | --- |
| `s` | symbol |
| `ms` | `time_msc`, UTC milliseconds |
| `b` | bid |
| `a` | ask |
| `l` | last |
| `v` | `volume_real` |
| `f` | `flags` — the full value including bit 31, identical to the binary record's field, so a JSON consumer can spot a heartbeat exactly like a binary one |
| `q` | `seq` |

**`stats`** — pushed on the configured interval, and on request.

```json
{"t":"stats","uptime_s":12.3,"ticks":9,"tick_rate":41.8,"subscribers":2,"symbols":["EURUSD"],"seq_gaps":1,"heartbeats":3,"dropped":4,"broker_lag_ms_p50":1.5,"broker_lag_ms_p99":9.5}
```

| Field | Window | Meaning |
| --- | --- | --- |
| `uptime_s` | point in time | Seconds since the bridge started, rounded to 1 dp |
| `ticks` | cumulative | Quote records ingested since start |
| `heartbeats` | cumulative | Heartbeat records ingested since start |
| `seq_gaps` | cumulative | Sequence discontinuities counted across all feeder connections |
| `dropped` | cumulative | Ticks shed from lossless consumer queues, including consumers that have since disconnected |
| `tick_rate` | interval | Ticks per second over the interval, rounded to 1 dp |
| `broker_lag_ms_p50` / `p99` | interval | Percentiles of `now − time_msc` sampled on quotes only, rounded to 2 dp |
| `subscribers` | point in time | Consumers connected now |
| `symbols` | point in time | The bridge's catalogue |

The interval is the span since the previous *periodic* stats report
(`--stats-interval`, default 10 s), and only that periodic report closes it.
Requesting a `stats` frame with the `stats` control op, and polling
`GET /api/v1/stats`, are pure reads: neither resets the window another observer is
measuring. A percentile is `null` when nothing was measured in the interval,
which is distinct from a measured `0.0`.

**`ack`** — the reply to any control op that changes the subscription.

```json
{"t":"ack","op":"subscribe","symbols":["EURUSD","USDJPY"],"format":"json"}
```

One shape for `subscribe`, `unsubscribe` and `format` alike: `op` echoes which op
asked, and `symbols` and `format` are the subscription as it now stands, so a
consumer keeps its own copy in step from a single handler. `backpressure` is
absent because no control op can change it.

**`pong`** and **`error`** — the other two replies to a control frame.

```json
{"t":"pong","rx":1700000000.7,"echo":123}
```

```json
{"t":"error","reason":"unknown op: 'nope'"}
```

`echo` is whatever the client sent, returned untouched: it is the client's own
correlation token. `reason` is a human-readable string, and an `error` frame is
never fatal to the connection.

**Forward compatibility for consumers.**

* An unknown `t` tag must not stop the stream. A newer bridge may add a kind;
  ignore the frame and keep reading.
* `rx` may be absent or non-numeric on any frame. Treat either as "no send
  timestamp" and skip the hop measurement rather than substituting zero.

### Binary frames

With `format=binary`, tick data arrives as WebSocket **binary** frames holding
concatenated 64-byte records — the feeder's own bytes, unmodified, with no
envelope and always a whole number of records. Every other frame stays a JSON
**text** frame, so a consumer separates data from control by WebSocket frame type
without inspecting content.

A binary frame has nowhere to carry `rx`, so the bridge → consumer hop cannot be
measured under `format=binary`.

### Client control frames

Send them as WebSocket text frames holding a JSON object with an `op` key.

| Frame | Reply | Effect |
| --- | --- | --- |
| `{"op":"subscribe","symbols":["EURUSD"]}` | `ack` | Adds to the filter. An empty or absent list resets the filter to every symbol: a consumer with no filter cannot narrow itself by asking for nothing. When a filter already exists, the request is unioned with it |
| `{"op":"unsubscribe","symbols":["GBPUSD"]}` | `ack` | Removes from the filter. A no-op unless a filter already exists *and* the list is non-empty |
| `{"op":"format","value":"binary"}` | `ack` | Switches encoding mid-stream, parsed by the same first-letter rule as the query parameter; an empty value keeps the current format |
| `{"op":"stats"}` | `stats` | Returns the counters without closing the interval |
| `{"op":"ping","echo":123}` | `pong` | `echo` may be any JSON value and comes back untouched |

`symbols` must be a JSON array. Any other type — a bare string included — is read
as an empty request. Entries are stringified, stripped and de-duplicated.

Malformed control input is answered with an `error` frame and the connection
stays open:

| Input | `reason` |
| --- | --- |
| Not valid JSON | `invalid json` |
| Valid JSON that is not an object | `expected an object` |
| Unrecognised `op` | `unknown op: <value>`, e.g. `unknown op: 'nope'` |

A **binary** frame sent by the client is ignored without a reply: binary uploads
are not part of the control protocol.

### REST endpoints

All `GET`, all read-only; none of them changes bridge state.

| Route | Returns |
| --- | --- |
| `/` | The paths this process serves: `ws`, `dashboard`, `docs`, `api` |
| `/api/v1/health` | `status` (always `ok` when the process answers), `uptime_s`, `version` |
| `/api/v1/symbols` | Latest quote for every symbol seen since start, sorted by name; `?symbols=A,B` filters |
| `/api/v1/symbols/{symbol}` | Latest quote for one symbol; `404` with detail `unknown symbol: {symbol}` if it has not been seen |
| `/api/v1/stats` | The same field list the `stats` frame carries. Reading it never resets the interval |
| `/api/v1/feeders` | Connected feeders, oldest first: `peer`, `connected_at`, `ticks`, `heartbeats`, `seq_gaps`, `last_record_at`, `symbols` |
| `/dashboard` | The bundled single-file dashboard, `text/html` |
| `/docs`, `/openapi.json` | Interactive API docs and the OpenAPI schema |

`GET /api/v1/symbols`:

```json
[
  {
    "symbol": "EURUSD",
    "bid": 1.08501,
    "ask": 1.08504,
    "last": 0.0,
    "volume": 0.0,
    "spread": 0.00003,
    "time_msc": 1700000000123,
    "flags": 6,
    "seq": 10231,
    "received_at": 1700000000.118,
    "age_ms": 4.2,
    "ticks": 48120
  }
]
```

`spread` is `ask - bid`. `time_msc` is the tick's UTC milliseconds, as it came off
the wire. `received_at` is the Unix second the bridge decoded it, and `age_ms` is
the milliseconds since, clamped at zero, so a consumer can tell a stale symbol
from a live one without tracking clocks itself; the clock is read once per
request, so one response reports one consistent set of ages. `ticks` counts the
quotes seen for that symbol since the bridge started.

In `/api/v1/feeders`, `last_record_at` is `0` when nothing has arrived on that
connection yet.

**CORS** on these routes is open to every origin and limited to the `GET` method.
`--allow-origin` applies to the WebSocket alone and has no effect here.

## 3. Framing and batching

A session's writer loop wakes, takes **everything** queued for its consumer, and
sends it as one frame. There is no batching timer and no minimum frame interval.
Quiet market: one tick per frame. Under load, ticks share a frame.

One WebSocket message is one frame, and a frame may hold any number of ticks, so
a consumer must iterate a frame's contents rather than assume one tick per
message.

## 4. Backpressure

Publishing to consumers never blocks the feeder side: one stalled consumer cannot
delay the feeder or any other consumer. What the stalled consumer itself receives
depends on the policy it chose at connect time.

| Policy | Query | Behaviour | Use for |
| --- | --- | --- | --- |
| `lossless` (default) | — | Queue up to `--queue-limit` ticks (default 20000), then shed the oldest half | Recorders, strategy engines, anything where a missing tick is a bug |
| `conflate` | `conflate=1` | Keep only the newest tick per symbol | Dashboards, tickers, anything that only ever renders the latest value |

Under `lossless`, an overflowing queue sheds `max(overflow, half the queue)`
items from the front — the oldest — and each shed tick counts toward the session's
drop total, which appears in `stats.dropped` and in the bridge's disconnect log
line. Shedding half rather than the exact overflow keeps the queue from sitting
permanently full and turning every later batch into a drop.

Under `conflate` there is no queue limit and no drop accounting: memory is bounded
by the symbol count regardless of tick rate, and a slow consumer sees current
prices rather than a queue of stale ones.
