"""One consumer's subscription session: options, hello, control ops, writer loop.

A :class:`Session` is everything a connected consumer *is*, minus the socket:
the options it asked for, the ``hello`` it is told on arrival, the control ops
that change those options mid-stream, the loop that drains its queue and writes
frames, and the counters its disconnect line reports. It is built from a
:class:`~mt5_ws_stream.hub.Sink` and a :class:`~mt5_ws_stream.hub.Hub`, so it
can be driven end to end without a WebSocket -- which is exactly what
``tests/test_session.py`` does.

The split against the hub is ADR-0002: the hub owns *delivery policy* (whose
queue a tick lands in, and what happens when that queue fills), a session owns
*one conversation*. Nothing here creates a task either: :meth:`Session.run` is a
coroutine the caller places wherever its structured concurrency wants it -- for
the WebSocket adapter in :mod:`mt5_ws_stream.api`, an ``asyncio.TaskGroup``
alongside the receive loop, so a failing sink ends the whole session instead of
being logged and forgotten.

This module imports :mod:`~mt5_ws_stream.hub`, :mod:`~mt5_ws_stream.frames`,
:mod:`~mt5_ws_stream.protocol` and the pure symbol normaliser from
:mod:`~mt5_ws_stream.subscription`, and nothing else: a session knows no more
about HTTP than the hub does. It builds no frame literals either -- every frame
it sends is one call into :mod:`~mt5_ws_stream.frames`, the one home of the
frame grammar.
"""

from __future__ import annotations

import dataclasses
import json
import time
from collections.abc import Sequence
from typing import Any

from . import frames
from .hub import Hub, HubStats, Sink, SubscriptionOptions
from .protocol import PayloadFormat, Tick
from .subscription import normalize_symbols

__all__ = ["Session"]


class Session:
    """One consumer's connection, from ``hello`` to the close summary."""

    __slots__ = (
        "_hub",
        "_sink",
        "_subscriber",
        "sent_frames",
        "sent_ticks",
    )

    def __init__(
        self,
        hub: Hub,
        sink: Sink,
        options: SubscriptionOptions | None = None,
    ) -> None:
        self._hub = hub
        self._sink = sink
        self._subscriber = hub.subscribe(options)
        self.sent_frames = 0
        self.sent_ticks = 0

    @property
    def id(self) -> int:
        """The subscriber id, which is what ``hello`` and the logs report."""
        return self._subscriber.id

    @property
    def options(self) -> SubscriptionOptions:
        """The live subscription options; control ops replace them."""
        return self._subscriber.options

    @property
    def dropped(self) -> int:
        """Ticks shed by the backpressure policy, for the close summary."""
        return self._subscriber.dropped

    # -- handshake -------------------------------------------------------

    async def send_hello(self) -> None:
        """Send the ``hello`` frame. Awaited before anything else writes.

        ``hello`` first is a contract of the frame grammar, not an accident of
        ordering (``TickStreamClient.connect()`` reads it before yielding), so
        the caller sends it *before* starting the writer loop and the receive
        loop rather than racing them.
        """
        options = self.options
        await self._sink.send(
            frames.hello_frame(
                session_id=self.id,
                payload_format=options.payload_format,
                backpressure=options.backpressure,
                symbols=options.symbols,
                available=self._hub.symbols,
                snapshot=self._hub.latest(options.symbols),
                rx=time.time(),
            )
        )

    # -- writer loop -----------------------------------------------------

    async def run(self) -> None:
        """Drain and send until the queue closes.

        Returns -- it does not need cancelling -- once :meth:`close` or
        :meth:`Hub.aclose` closes the queue. A failing sink raises out of here,
        which is the point: the caller's task group sees a dead consumer
        instead of a writer quietly spinning against a socket nobody can write
        to.
        """
        while await self._subscriber.wait():
            await self.flush()

    async def flush(self) -> None:
        """Send everything queued as one frame. No-op when nothing is queued.

        One iteration of :meth:`run`, exposed so tests can step the writer
        deterministically instead of waiting on a background task.
        """
        items = self._subscriber.drain()
        if not items:
            return
        await self._sink.send(self.encode(items, time.time()))
        self.sent_frames += 1
        self.sent_ticks += len(items)

    def encode(self, items: Sequence[tuple[Tick, bytes]], sent_at: float) -> str | bytes:
        """Encode a drained batch into a single frame payload.

        Binary consumers get the feeder's own bytes back, unmodified: no
        re-packing, and the frame is a whole number of records by construction.
        """
        if self.options.payload_format is PayloadFormat.BINARY:
            return frames.binary_ticks_frame(raw for _, raw in items)
        return frames.ticks_frame((tick for tick, _ in items), rx=sent_at)

    # -- control ---------------------------------------------------------

    async def handle(self, message: str) -> None:
        """Handle one client control frame. Malformed input is answered, not fatal.

        A consumer that sends garbage gets an ``error`` frame and keeps its
        connection: the alternative -- dropping the socket -- costs it the
        stream over a typo in a debugging console.
        """
        try:
            payload = json.loads(message)
        except ValueError, TypeError:
            await self._sink.send(frames.error_frame("invalid json"))
            return
        if not isinstance(payload, dict):
            await self._sink.send(frames.error_frame("expected an object"))
            return

        op = payload.get("op")
        options = self.options

        if op == "subscribe":
            requested = _string_set(payload.get("symbols"))
            # An empty request means "everything": a consumer with no filter
            # cannot narrow itself by asking for nothing.
            merged = (
                None
                if not requested
                else (requested if options.symbols is None else options.symbols | requested)
            )
            self._update(symbols=merged)
            await self._ack(op)
        elif op == "unsubscribe":
            requested = _string_set(payload.get("symbols"))
            if options.symbols is not None and requested:
                self._update(symbols=options.symbols - requested)
            await self._ack(op)
        elif op == "format":
            new = PayloadFormat.parse(str(payload.get("value", "")), options.payload_format)
            self._update(payload_format=new)
            await self._ack(op)
        elif op == "stats":
            await self.send_stats(self._hub.snapshot_stats())
        elif op == "ping":
            await self._sink.send(frames.pong_frame(echo=payload.get("echo"), rx=time.time()))
        else:
            await self._sink.send(frames.error_frame(f"unknown op: {op!r}"))

    async def send_stats(self, stats: HubStats) -> None:
        """Send *stats* as a ``stats`` frame.

        Both producers of one call this: the ``stats`` control op above, and the
        bridge's periodic broadcast. *stats* is a snapshot the caller took, so
        one broadcast reports one consistent interval to every session.
        """
        await self._sink.send(frames.stats_frame(stats))

    # -- lifecycle -------------------------------------------------------

    def close(self) -> None:
        """Unsubscribe and close the queue, ending :meth:`run`. Idempotent."""
        self._hub.unsubscribe(self._subscriber)

    # -- internals -------------------------------------------------------

    def _update(self, **changes: Any) -> None:
        self._subscriber.update_options(dataclasses.replace(self.options, **changes))

    async def _ack(self, op: str) -> None:
        """Answer a control op with the subscription as it now stands.

        One shape whichever op asked, so a consumer keeps its own copy of the
        subscription in step from a single handler --
        :func:`mt5_ws_stream.frames.ack_frame` says why.
        """
        options = self.options
        await self._sink.send(
            frames.ack_frame(op, symbols=options.symbols, payload_format=options.payload_format)
        )


def _string_set(value: object) -> frozenset[str]:
    """Symbols from a control op's JSON payload -- not a string, not garbage.

    A JSON string is iterable too (of characters), so the type check has to
    come first; once it has, the actual stripping/de-duplication is the same
    pure operation the subscription query string uses on its own symbol
    lists (:func:`~mt5_ws_stream.subscription.normalize_symbols`).
    """
    if not isinstance(value, (list, tuple, set, frozenset)):
        return frozenset()
    return normalize_symbols(value)
