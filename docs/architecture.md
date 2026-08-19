# Architecture

```
                ┌──────────────────────────────────────────────┐
                │  feeders: anything that writes 64-byte       │
  protocol.py   │  records to a TCP socket                     │
    (shared)    │    TickStreamer.mq5 · MockFeeder · yours     │
                └───────────────────┬──────────────────────────┘
                                    │ TCP
                ┌───────────────────▼──────────────────────────┐
  hub.py        │  Hub: decode a batch once, fan the batch out │
                │  to N subscribers, enforce per-subscriber    │
                │  backpressure policy. Knows no WebSocket,    │
                │  owns no tasks                               │
  session.py    │  Session: one consumer's hello, control ops  │
  frames.py     │  and writer loop (drain -> encode -> send)   │
                └───────────────────┬──────────────────────────┘
                                    │ Sink protocol
                ┌───────────────────▼──────────────────────────┐
  bridge.py     │  Bridge: TCP feeder listener + one embedded  │
                │  uvicorn server + the periodic stats report  │
                │  (owns the process lifecycle)                │
                └───────────────────┬──────────────────────────┘
                                    │ ASGI
                ┌───────────────────▼──────────────────────────┐
  api.py        │  FastAPI app: WS /ws (query-string           │
subscription.py │  subscriptions, control frames) + read-only  │
                │  REST under /api/v1 + /dashboard + /docs     │
                └──────────────────────────────────────────────┘
```

## Shared vocabularies

Three modules are imported by both sides of the seam they describe, and none of
them imports a transport:

| Module | Vocabulary | Both sides |
| --- | --- | --- |
| `protocol.py` | the 64-byte record: layout, `pack_tick` / `unpack_tick` / `decode_records`, `Tick`, `PayloadFormat`, `BackpressurePolicy` | feeder and bridge |
| `frames.py` | the frame grammar: `hello`, `ticks`, `stats`, `ack`, `pong`, `error` — encoders and `decode_frame` in one module | bridge and consumer |
| `subscription.py` | the query string: `SubscriptionRequest.to_query()` renders the canonical spelling, `from_query()` is the lenient reader | client/dashboard and bridge |

Encode and decode stay in one module, so a key only one side knows about fails
the round-trip in `tests/test_frames.py` instead of at a consumer.
`subscription.py` imports only `protocol.py`, so a
client-only process builds a URL without pulling in FastAPI or the hub.

## Modules and what each owns

| Module | Owns | Imports of note |
| --- | --- | --- |
| `protocol.py` | the wire record and its codec; `RECORD_SIZE` is guarded at import time | nothing in this package |
| `frames.py` | the frame grammar, both directions; `PROTOCOL_VERSION` | `protocol` |
| `subscription.py` | `SubscriptionRequest`, `normalize_symbols`, the truthiness rule for `conflate` / `heartbeats` | `protocol` |
| `hub.py` | ingest framing (`FeederLink`), decode, fan-out, per-subscriber backpressure (`Subscriber`), the latest-price map, the symbol catalogue, the stats counters, and the `Sink` protocol | `protocol` |
| `session.py` | one consumer's options, `hello`, control ops, writer loop, and close counters | `hub`, `frames`, `protocol`, `subscription.normalize_symbols` |
| `bridge.py` | process lifecycle: the feeder TCP listener, the embedded uvicorn server, the periodic stats report, `BridgeConfig` | `hub`, `session`, `api` |
| `api.py` | the FastAPI app: `/ws`, the REST routes under `/api/v1`, `/dashboard`, `_WebSocketSink`, the `Origin` guard | `hub`, `session`, `subscription`, and `frames.stats_payload` |
| `feeders.py` | `FeederConnection` (one TCP link, `sendall` per batch) and `MockFeeder` | `protocol` |
| `client.py` | `TickStreamClient`: URL building, handshake, `ticks()` / `stream()`, control methods | `frames`, `protocol`, `subscription` |
| `decoder.py` | nothing — it re-exports `frames.decode_frame` and the frame classes under the name consumers know | `frames` |
| `cli.py` | the `bridge` / `mock` / `client` / `dashboard` subcommands and their argument-to-config mapping | all of the above |

`api.py` builds no frame of its own: it imports `frames.stats_payload` for the
`/api/v1/stats` body, and every frame a consumer receives is built by
`session.py` calling `frames.py`.

## Hot path

`Hub.feed(chunk, link)` runs once per chunk of bytes off the feeder socket.
The chunk is whatever the kernel handed the reader — any length, including one
that cuts a record in half or completes none:

1. `FeederLink.take_records(chunk)` frames it: whole records out, a partial
   tail held on the link for the next call. A chunk that is already a whole
   number of records with nothing held over — the common case — is returned
   uncopied. `FeederLink.pending_bytes` is always below `RECORD_SIZE`.
2. `protocol.decode_records` decodes that **batch** in full before anything is
   published. A bad header raises `ProtocolError` here, so the batch is
   rejected whole: no tick delivered, no counter moved.
3. Per record, `FeederLink.account(tick, now)` does the per-connection
   accounting — sequence continuity, `ticks`, `heartbeats`, `seq_gaps`, symbols
   seen — and returns whether this record broke the sequence, which the hub
   mirrors into its own total. Wraparound past `2**32` counts as continuity,
   not a gap.
4. Each quote updates the latest-price map (`hub.latest`, the source of
   `hello`'s `snapshot` and of `/api/v1/symbols`), its per-symbol tick count and
   its `received_at`.
5. Each quote appends `now_ms - tick.time_msc` to the broker-lag ring buffer, a
   `deque` capped at 20 000 samples. The cap evicts the oldest, so the
   percentiles keep tracking the present even with the periodic report
   disabled. Heartbeats are not sampled.
6. `Hub.publish(records, heartbeats=...)` offers the **whole batch** — each tick
   with the exact bytes it was decoded from — to every subscriber. The
   subscriber's symbol set and heartbeat flag are read once per batch, not once
   per record; the default subscription (every symbol, no heartbeats) is handed
   the caller's own list uncopied. Heartbeats bypass the symbol filter: a
   consumer that asked for heartbeats gets them whatever it is subscribed to.

Invariants to keep:

* **`publish` never awaits.** It extends a list (or overwrites dict entries,
  under `conflate`) and sets an `asyncio.Event` once per batch. A slow consumer
  cannot back-pressure the feeder, and a second consumer costs the first one
  almost nothing — see [latency.md](latency.md#measured-numbers).
* **A batch is all-or-nothing.** Decoding finishes before the first publish.
* **`Subscriber.offer` bounds itself.** Under `lossless` it sheds
  `max(excess, len(pending) // 2)` items from the front once the queue passes
  `queue_limit` (default 20 000) and adds them to `dropped`; shedding exactly
  the overflow would leave the queue permanently full. Under `conflate` it
  keeps only the newest tick per symbol, so memory is bounded by symbol count
  and there is no drop accounting.

### The session writer loop

`Session.run()` is `while await self._subscriber.wait(): await self.flush()`.
`flush()` drains **everything** queued, encodes it as **one** frame, sends it,
and adds 1 to `sent_frames` and `len(items)` to `sent_ticks`. There is no
batching timer: at low tick rates that is one tick per frame with no added
delay, and at high rates frames coalesce on their own.

Encoding is per session, because format and filter are per session; the decode
in step 2 happened once for everyone. A binary consumer gets the feeder's own
bytes back unmodified, which makes the frame a whole number of records by
construction and leaves no room for an `rx` timestamp — hence `TickFrame.hop`
is `None` for binary frames.

## The seams

**`Sink`** (`hub.Sink`) is anything with an awaitable `send(payload: str |
bytes)`. It is the delivery path's one outward seam: `_WebSocketSink` in
`api.py` and `RecordingSink` in `tests/conftest.py` are its two adapters, and a
different transport is a new adapter rather than a change to the hub. When
adding one: `_WebSocketSink` serialises its sends behind an `asyncio.Lock`
(the writer loop, control acks and the periodic stats broadcast all target the
same socket) and lets a failed send raise, so the handler's task group notices a
dead consumer.

**`Session`** owns one conversation: its options, the `hello` it is told, the
control ops that change those options, the writer loop, and the counters its
disconnect line reports. It is built from a `Sink` and a `Hub`, so
`tests/test_session.py` drives the whole control protocol with no socket.
`Session.run()` is a coroutine the caller places in its own structured
concurrency — in `api.py`, an `asyncio.TaskGroup` next to the receive loop — so
a failing `sink.send` raises out of `run()` and unwinds the session. Keep it
that way: a session that swallowed the error would leave a writer spinning
against a socket nobody can write to.

**The Hub owns no tasks.** `Hub.subscribe()` hands back a queue and nothing
else; `Hub.unsubscribe()` and `Hub.aclose()` *close* queues rather than
cancelling tasks, and the sessions draining them return on their own.
`aclose()` yields once so an already-scheduled writer loop can notice. Anything
new on the hub must keep this property: no `create_task`, no awaiting a
background worker. The records of these decisions are in `docs/adr/` —
0001 for the feeder seam, 0002 for the hub/session split.

## Bridge lifecycle

`Bridge.start()` brings up three things and pushes a teardown callable onto
`self._teardown` as each one actually succeeds:

1. **The feeder listener** — `asyncio.start_server(self._handle_feeder, ...)`,
   with `reuse_address` on POSIX only (on Windows `SO_REUSEADDR` would let a
   second bridge bind the same port and split feeders between them). Every
   accepted socket gets `TCP_NODELAY`.
2. **The consumer server** — `_start_http()`.
3. **The stats task** — only when `stats_interval_s > 0`.

`_start_http()` runs uvicorn inside this event loop:

* The listening socket is bound by `_bind_listener(ws_host, http_port)` before
  uvicorn sees it, and the real port is read back from `sock.getsockname()`.
  `http_port=0` is therefore usable, and the chosen port readable afterwards.
* uvicorn is driven by hand — `server.startup(sockets=[sock])`, then
  `server.main_loop()` as a task, then `server.shutdown()` from `aclose()` —
  rather than `Server.serve()`, which would install signal handlers. A library
  inside another event loop must not steal `SIGINT` from its host; the CLI
  installs its own (`cli._wait_for_shutdown`).
* The WebSocket implementation is pinned to `websockets-sansio`, which reuses
  the `websockets` dependency the client already needs.
* Per-message deflate is off, the access log is off, `log_config` is `None`
  (the host application keeps its logging setup), `lifespan` is `"off"` and
  `timeout_graceful_shutdown` is 5 seconds. `extra_serve_kwargs` is passed
  straight through to `uvicorn.Config`.
* `ws_ping_interval_s` / `ws_ping_timeout_s` are forwarded, but uvicorn's
  sansio protocol sends no server-initiated keep-alive pings, so they have no
  effect. Dead peers surface through TCP and through the feeder's own
  heartbeat records.

`create_app(hub, *, allowed_origins=None, feeders=None, sessions=None,
version=__version__)` takes a `Hub`, not the `Bridge`: those are all the routes
read, and `api.py` importing `bridge.py` back would be a cycle. `feeders` is a
callable because the bridge's registry changes as connections come and go;
`sessions` is the set the WebSocket handler adds each live session to, which is
what the periodic `stats` broadcast walks. The app can therefore be built and
driven over `httpx.ASGITransport` with no server at all — see the ASGI block in
`tests/test_api.py`.

**Teardown is one list.** `aclose()` pops `self._teardown` in reverse, then
closes the hub and sets the closed event; a `start()` that fails half-way
unwinds through the same list. Every step runs even if an earlier one raised —
a crashing stats task must not leave the HTTP server, HTTP socket or feeder
listener open — and the first escaped exception is re-raised once the stack is
empty. `aclose()` is safe twice and safe on a bridge that never started.

**The periodic stats report is one function**, `Bridge.report_once()`:
`Hub.consume_interval()` closes the interval and returns one snapshot; that
snapshot is logged through `api.stats_line()` and sent to every live session as
a `stats` frame. `_stats_loop` is a `sleep`-and-call loop over it, so a test or
an embedder can trigger a report without waiting out `stats_interval_s`.

* `consume_interval()` has exactly one intended caller. It moves the rate
  marker and clears the latency samples, so a second consumer would halve both
  windows without either noticing.
* The on-demand paths — `GET /api/v1/stats` and the `stats` control op — go
  through `Hub.snapshot_stats()`, a pure read that never closes the interval.
* The per-session sends are fanned out concurrently with `asyncio.gather`, each
  bounded by `stats_send_timeout_s` (default 1.0 s). A session that times out
  is logged at DEBUG and skipped; it delays neither the other sessions nor
  `report_once()`.

`ticks`, `heartbeats`, `seq_gaps` and `dropped` in a snapshot are cumulative;
`tick_rate` and the two broker-lag percentiles cover the interval since the
previous `consume_interval()`. `dropped` folds in the counts of subscribers
that have since disconnected, added at unsubscribe when the count is final.

## Feeders

The feeder boundary is a TCP socket carrying fixed-size 64-byte records. Any
process that can open a socket and write them is a first-class feeder: the EA
(`mql5/Experts/TickStreamer/TickStreamer.mq5`) is the live one, `MockFeeder` is
the one the tests and benchmarks use, and neither needs a conditional anywhere
in the bridge. The live feeder is written in MQL5 and runs inside a Windows GUI
application this project cannot import, link against, or run in CI; the socket
is the only contract between the two sides.

`FeederConnection` writes a whole batch in a single `sendall` and sets
`TCP_NODELAY`; quotes and heartbeats share one sequence counter, so `seq` stays
dense across both. `MockFeeder.run(link=None)` returns the number of quotes sent
(heartbeats excluded) and accepts an already-connected `FeederConnection`, so its
pacing logic can be driven in a test with no socket at all.

## Failure modes and what they do

| Failure | Behaviour |
| --- | --- |
| Feeder disconnects | Logged with its counters (`feeder disconnected: %s (ticks=%d heartbeats=%d gaps=%d)`); consumers stay connected and simply stop receiving |
| Feeder speaks a different protocol | `ProtocolError` on the first bad header drops the connection, and nothing from the batch that contained it is published — a consumer's stream never ends on a record the bridge went on to disown. A fixed-size stream has no safe resync point, so continuing would mean inventing prices |
| A record is split across TCP packets | The partial tail is buffered on the `FeederLink` until the rest arrives; neither an error nor a loss |
| Consumer stalls | Its own queue degrades per its policy — oldest half shed under `lossless`, newest-per-symbol kept under `conflate`; nothing else is affected |
| Consumer's socket dies | The failed `sink.send` raises out of `Session.run()` into the handler's task group, which unwinds the session and unsubscribes it — no queue or writer outlives the peer |
| Consumer sends garbage | An `error` frame; the connection survives. A binary upload is ignored |
| Bridge stalls | The EA's `SocketSend` times out at 200 ms (`SocketTimeouts(socket, 200, 200)`), the EA drops whatever it could not send, tears the socket down and reconnects. The loss is reported by the EA's own log line as `dropped=` and `reconnects=`. The bridge's `seq_gaps` does **not** show it: sequence continuity restarts per connection, so the first record over a fresh link starts the count again |
| A spy event is dropped (EA, `EXTRA_EVENT`) | Nothing is lost: the event carries no price, and the backstop timer collects from the same cursor. Counted as `evt_late` |

## Concurrency model

Single event loop, no locks and no threads in the serving path. The hub's
mutable state is touched only from loop callbacks, so there is nothing to
synchronise. The one lock in the process is `_WebSocketSink`'s, which serialises
the three writers that share a consumer socket.

Task ownership: the bridge owns the uvicorn `main_loop` task and the stats
task; the WebSocket handler owns a `TaskGroup` holding `session.run()` and the
receive loop; the hub owns nothing. `MockFeeder` is synchronous and blocking,
because it runs in its own process or a test executor.

On Windows the CLI selects `WindowsSelectorEventLoopPolicy` before
`asyncio.run`.

## Scope

This repo is the tick path: EA → TCP records → hub → WebSocket / REST.
Recording is a consumer (`examples/record_to_csv.py`). Bars and orders are
not on this wire.
