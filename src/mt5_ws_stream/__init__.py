"""Low-latency MetaTrader 5 tick streaming over WebSocket.

The package is a stack of modules that each own one concept:

* :mod:`~mt5_ws_stream.protocol` -- the 64-byte wire record every feeder emits.
* :mod:`~mt5_ws_stream.frames` -- the JSON frame grammar the bridge speaks to
  consumers, encode and decode in one place.
* :mod:`~mt5_ws_stream.subscription` -- the subscription request, rendered by a
  consumer and parsed by the bridge.
* :mod:`~mt5_ws_stream.hub` -- transport-agnostic fan-out with backpressure
  policy. Owns no tasks (ADR-0002).
* :mod:`~mt5_ws_stream.session` -- one consumer's connection: options, hello,
  control ops, writer loop.
* :mod:`~mt5_ws_stream.api` -- the FastAPI app the server runs: the WebSocket
  stream, a read-only REST API, the dashboard and OpenAPI docs, on one port.
* :mod:`~mt5_ws_stream.bridge` -- the process that joins a TCP feeder port to a
  consumer server.
* :mod:`~mt5_ws_stream.client` / :mod:`~mt5_ws_stream.decoder` -- the consumer
  side: the transport, and the client's view of the frame grammar.
* :mod:`~mt5_ws_stream.feeders` -- ``MockFeeder``, the Python feeder that stands
  in for MetaTrader in tests and demos.

Quick start:

.. code-block:: python

    import asyncio
    from mt5_ws_stream import TickStreamClient

    async def main() -> None:
        async with TickStreamClient(symbols=["EURUSD"]) as stream:
            async for tick in stream:
                print(tick.symbol, tick.bid, tick.ask)

    asyncio.run(main())
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any, Final

# Assigned before the submodule imports on purpose: `api.py` reads it at import
# time (`from . import __version__`) to default `create_app`'s version argument,
# and by then this module is only half-initialised. PEP 8 sanctions dunder
# assignments ahead of imports for exactly this kind of case.
__version__ = "0.1.0"

# `api` (FastAPI + pydantic) and `bridge` (uvicorn, and `api` itself) are the
# server-side half of the package. A client-only process that only needs
# `TickStreamClient` shouldn't pay to import them, so their public names are
# exported lazily via PEP 562's module `__getattr__` below instead of being
# imported here at module load time.
if TYPE_CHECKING:
    from .api import create_app as create_app
    from .bridge import Bridge as Bridge
    from .bridge import BridgeConfig as BridgeConfig

from .client import Connection, HandshakeError, TickStreamClient
from .decoder import ControlFrame, DecodedFrame, FrameDecodeError, FrameKind, TickFrame
from .feeders import FeederConnection, MockFeeder
from .hub import (
    FeederLink,
    Hub,
    HubStats,
    Subscriber,
    SubscriptionOptions,
)
from .protocol import (
    FLAG_HEARTBEAT,
    MAGIC,
    RECORD_SIZE,
    BackpressurePolicy,
    PayloadFormat,
    ProtocolError,
    Tick,
    decode_records,
    iter_ticks,
    pack_tick,
    unpack_tick,
)
from .session import Session

_LAZY_ATTRS: Final[dict[str, str]] = {
    "create_app": ".api",
    "Bridge": ".bridge",
    "BridgeConfig": ".bridge",
}


def __getattr__(name: str) -> Any:
    module_name = _LAZY_ATTRS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = importlib.import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))


__all__ = [
    "FLAG_HEARTBEAT",
    "MAGIC",
    "RECORD_SIZE",
    "BackpressurePolicy",
    "Bridge",
    "BridgeConfig",
    "Connection",
    "ControlFrame",
    "DecodedFrame",
    "FeederConnection",
    "FeederLink",
    "FrameDecodeError",
    "FrameKind",
    "HandshakeError",
    "Hub",
    "HubStats",
    "MockFeeder",
    "PayloadFormat",
    "ProtocolError",
    "Session",
    "Subscriber",
    "SubscriptionOptions",
    "Tick",
    "TickFrame",
    "TickStreamClient",
    "__version__",
    "create_app",
    "decode_records",
    "iter_ticks",
    "pack_tick",
    "unpack_tick",
]
