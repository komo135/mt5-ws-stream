// Minimal Node.js consumer.
//
//   npm install ws
//   node examples/node_client.mjs EURUSD USDJPY
//
// Uses the binary format: outside a browser, decoding 64-byte records costs a
// fraction of what JSON.parse does, and Node gives you a DataView for free.

import WebSocket from "ws";

const RECORD_SIZE = 64;
const MAGIC = 0x4b54;
const FLAG_HEARTBEAT = 0x80000000;

const symbols = process.argv.slice(2);
const query = new URLSearchParams({ format: "binary" });
if (symbols.length) query.set("symbols", symbols.join(","));

const ws = new WebSocket(`ws://127.0.0.1:8765/ws?${query}`);
ws.binaryType = "arraybuffer";

function decode(view, offset) {
  const magic = view.getUint16(offset, true);
  const size = view.getUint16(offset + 2, true);
  if (magic !== MAGIC || size !== RECORD_SIZE) {
    throw new Error(`bad record header at ${offset}`);
  }
  const bytes = new Uint8Array(view.buffer, offset + 8, 12);
  const end = bytes.indexOf(0);
  return {
    seq: view.getUint32(offset + 4, true),
    symbol: new TextDecoder().decode(bytes.subarray(0, end === -1 ? 12 : end)),
    timeMsc: Number(view.getBigInt64(offset + 20, true)),
    bid: view.getFloat64(offset + 28, true),
    ask: view.getFloat64(offset + 36, true),
    last: view.getFloat64(offset + 44, true),
    volume: view.getFloat64(offset + 52, true),
    flags: view.getUint32(offset + 60, true),
  };
}

ws.on("open", () => console.log("connected"));

ws.on("message", (data, isBinary) => {
  if (!isBinary) {
    const frame = JSON.parse(data.toString());
    // `available` is every symbol the bridge has seen; `symbols` is this
    // connection's own filter (null = all). See docs/protocol.md §2.
    if (frame.t === "hello")
      console.log("symbols:", frame.available.join(", ") || "(none yet)");
    return;
  }
  const buffer = data instanceof ArrayBuffer ? data : new Uint8Array(data).buffer;
  const view = new DataView(buffer);
  for (let offset = 0; offset + RECORD_SIZE <= view.byteLength; offset += RECORD_SIZE) {
    const tick = decode(view, offset);
    if (tick.flags & FLAG_HEARTBEAT) continue;
    console.log(
      `${tick.symbol.padEnd(10)} bid=${tick.bid.toFixed(5)} ` +
        `ask=${tick.ask.toFixed(5)} spread=${(tick.ask - tick.bid).toFixed(5)}`,
    );
  }
});

ws.on("close", () => console.log("disconnected"));
ws.on("error", (err) => console.error("error:", err.message));
