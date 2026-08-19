"""Record every tick to a CSV file.

    python examples/record_to_csv.py ticks.csv EURUSD USDJPY

Two choices here are the whole point of the example:

* ``backpressure="lossless"`` -- a recorder that silently skips ticks is worse
  than one that falls behind, so this is the opposite of the dashboard's setting.
* ``payload_format="binary"`` -- nothing here renders anything, so spend 5-10x
  less CPU on decoding and leave the headroom for disk I/O.

Flushing every ``FLUSH_EVERY`` rows rather than every row keeps a Ctrl-C from
costing more than a second of data without paying an fsync per tick.
"""

from __future__ import annotations

import asyncio
import contextlib
import csv
import sys
from datetime import UTC, datetime
from pathlib import Path

from mt5_ws_stream import TickStreamClient

FLUSH_EVERY = 500


async def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(__doc__)

    path = Path(sys.argv[1])
    symbols = sys.argv[2:] or None

    # Blocking file I/O inside a coroutine is fine here: a CSV append is
    # microseconds, and the alternative (a thread pool) would add far more
    # complexity than the latency it saves. Buffer to a queue if that changes.
    with path.open("w", newline="", encoding="utf-8") as handle:  # noqa: ASYNC230
        writer = csv.writer(handle)
        writer.writerow(["utc", "time_msc", "symbol", "bid", "ask", "last", "volume", "seq"])

        async with TickStreamClient(
            symbols=symbols, payload_format="binary", backpressure="lossless"
        ) as stream:
            print(f"recording to {path} -- Ctrl-C to stop")
            count = 0
            async for tick in stream:
                writer.writerow(
                    [
                        datetime.fromtimestamp(tick.time_msc / 1000, UTC).isoformat(),
                        tick.time_msc,
                        tick.symbol,
                        tick.bid,
                        tick.ask,
                        tick.last,
                        tick.volume,
                        tick.seq,
                    ]
                )
                count += 1
                if count % FLUSH_EVERY == 0:
                    handle.flush()
                    print(f"\r{count} ticks", end="", flush=True)

    print(f"\nwrote {count} ticks to {path}")


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
