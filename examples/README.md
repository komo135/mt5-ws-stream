# Examples

Start a bridge with a live EA first (see the [README](../README.md#quick-start)):

```bash
mt5-ws-stream bridge            # HTTP + WebSocket on 127.0.0.1:8765
```

Without MetaTrader, `mt5-ws-stream mock` feeds synthetic ticks:
[CONTRIBUTING.md](../CONTRIBUTING.md#running-the-python-side-without-metatrader).

| File | What it shows |
| --- | --- |
| [`minimal_client.py`](minimal_client.py) | The smallest useful consumer: `async for tick in stream`, one line printed per tick. Symbols come from the command line, none means every symbol |
| [`record_to_csv.py`](record_to_csv.py) | Recording to CSV with `payload_format="binary"` and `backpressure="lossless"`, flushing every 500 rows |
| [`browser_minimal.html`](browser_minimal.html) | A bid/ask/spread/count table in about 30 lines of JavaScript, no build step; opens over `file://` |
| [`node_client.mjs`](node_client.mjs) | Decoding the 64-byte binary records outside Python, with a `DataView` |

Run them:

```bash
python examples/minimal_client.py EURUSD USDJPY
python examples/record_to_csv.py ticks.csv EURUSD USDJPY
npm install ws && node examples/node_client.mjs EURUSD USDJPY
```

Open `browser_minimal.html` in a browser; it connects to
`ws://127.0.0.1:8765/ws` and needs no server of its own.

## Connecting

* **The stream is `/ws`.** `ws://127.0.0.1:8765/ws?symbols=EURUSD&format=json` —
  `/` is the HTTP route index, not the stream. `TickStreamClient` appends the
  path for you.
* **Frames carry their kind in `t`.** `hello` arrives first and exactly once,
  then `ticks`. `ack`, `pong` and `error` are replies to a control frame, and
  `stats` arrives both on request and on the bridge's `--stats-interval`. Ignore
  a kind you do not recognise rather than treating it as an error; a `ticks`
  frame in binary format is the raw records with no JSON envelope.
* **`hello.available` is the bridge's catalogue** — every symbol it has seen
  since it started. `hello.symbols` is this connection's own filter, where
  `null` means every symbol and `[]` means none. `hello.snapshot` is the latest
  quote per subscribed symbol, so a chart can draw immediately.
  `node_client.mjs` prints `available`; `browser_minimal.html` seeds its table
  from `snapshot`.

## REST

The bridge serves a read-only REST API on the same port, so a plain `curl`
answers "which symbols is the bridge serving?":

```bash
curl http://127.0.0.1:8765/api/v1/symbols
```

For the full live monitor — sparklines, latency charts, dark mode — run
`mt5-ws-stream dashboard`.

See the [API section of the README](../README.md#api) or
[docs/protocol.md](../docs/protocol.md#rest-endpoints) for the full endpoint
list.
