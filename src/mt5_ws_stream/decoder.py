"""The client-side half of the frame grammar, under the name consumers know.

The grammar itself -- every frame kind, encoded and decoded -- lives in
:mod:`mt5_ws_stream.frames`, next to the encoders the bridge uses, so that a key
renamed on one side fails against the other. This module is the consumer's view
of it: :func:`~mt5_ws_stream.frames.decode_frame` and the values it returns, and
nothing a consumer has no use for.

* :class:`TickFrame` -- the quotes a ``ticks`` frame carried, JSON or binary,
  together with the bridge's send timestamp (``rx``) and the local receive time.
* :class:`ControlFrame` -- ``hello``, ``stats``, ``ack``, ``pong``, ``error``,
  and anything else the bridge may tag later, as a typed value rather than a
  bare dict.

The unit is the *frame*, not the tick, because everything a consumer wants to
know about timing is per-frame: the hop is one measurement per frame, and
ticks-per-frame is the batching the bridge chose. Flattening to a tick stream
throws both away and forces the timing back out through a side channel.
"""

from __future__ import annotations

from .frames import (
    ControlFrame,
    DecodedFrame,
    FrameDecodeError,
    FrameKind,
    TickFrame,
    decode_frame,
)

__all__ = [
    "ControlFrame",
    "DecodedFrame",
    "FrameDecodeError",
    "FrameKind",
    "TickFrame",
    "decode_frame",
]
