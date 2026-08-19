"""The smallest useful consumer: print every tick.

Usage:
    python examples/minimal_client.py
    python examples/minimal_client.py EURUSD USDJPY
"""

from __future__ import annotations

import asyncio
import sys

from mt5_ws_stream import TickStreamClient


async def main() -> None:
    symbols = sys.argv[1:] or None
    async with TickStreamClient(symbols=symbols) as stream:
        print(f"connected: {stream.url}")
        async for tick in stream:
            print(
                f"{tick.symbol:<10} bid={tick.bid:<12.5f} ask={tick.ask:<12.5f} "
                f"spread={tick.spread:.5f}"
            )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
