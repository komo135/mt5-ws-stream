"""Verifying the bridge's REST and WebSocket surface against the live EA.

The unit tests already prove every one of these behaviours against a fake
feeder. What they cannot prove is that the behaviour survives contact with the
real one: a terminal that reconnects, a broker clock that is not the local
clock, symbols whose names carry a broker suffix, and tick rates nobody chose.
So this module re-asks the protocol's own questions -- the ones
``docs/protocol.md`` states as invariants -- of a bridge that is being fed by
MetaTrader 5 right now.

Each check returns a :class:`CheckResult` with its evidence rather than
asserting. A failed check is data: the run continues, the report shows which
invariant broke and what the bridge actually said, and one flaky check does not
cost the whole verification.

Two checks are deliberately weaker than they first look, and both for the same
reason -- a live stream moves:

* ``stats`` **is not compared for equality.** ``uptime_s`` and ``ticks``
  advance between two reads, and asserting byte-equality would fail at random.
  What ``docs/protocol.md`` actually promises is that reading stats is a *pure
  read*: "Asking for a ``stats`` frame, or polling ``GET /api/v1/stats``, is a
  pure read: it never closes the interval other observers are measuring." So
  the check asserts the key set is identical, cumulative counters never go
  backwards, and the REST view is not reset by the WebSocket read.
* **Binary and JSON are compared over their overlap**, keyed by ``seq``. Two
  sockets do not open or close on the same tick, so the windows differ at both
  ends; what must hold is that every record present in both decodes to the same
  :class:`~mt5_ws_stream.protocol.Tick`.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from websockets.asyncio.client import ClientConnection, connect

from mt5_ws_stream.decoder import ControlFrame, TickFrame, decode_frame
from mt5_ws_stream.protocol import FLAG_HEARTBEAT, Tick

from .bridge import http_get_json

__all__ = ["CheckResult", "Verification", "verify"]


@dataclass(frozen=True)
class CheckResult:
    """One invariant, asked of the live system."""

    name: str
    passed: bool
    detail: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": "PASS" if self.passed else "FAIL",
            "detail": self.detail,
            "evidence": self.evidence,
        }


@dataclass
class Verification:
    """Every check from one run, renderable as JSON or Markdown."""

    results: list[CheckResult] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)

    @property
    def passed(self) -> bool:
        return all(result.passed for result in self.results)

    def as_dict(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at,
            "duration_s": round(time.time() - self.started_at, 1),
            "passed": self.passed,
            "checks": [result.as_dict() for result in self.results],
        }

    def to_markdown(self) -> str:
        lines = ["| Check | Result | Detail |", "| --- | --- | --- |"]
        for result in self.results:
            status = "PASS" if result.passed else "**FAIL**"
            detail = result.detail.replace("|", "\\|")
            lines.append(f"| `{result.name}` | {status} | {detail} |")
        total = len(self.results)
        failed = sum(1 for result in self.results if not result.passed)
        lines.append("")
        lines.append(f"{total - failed}/{total} checks passed.")
        return "\n".join(lines)


# -- frame helpers ---------------------------------------------------------


async def _next_frame(
    connection: ClientConnection, *, timeout: float
) -> TickFrame | ControlFrame:
    message = await asyncio.wait_for(connection.recv(), timeout=timeout)
    return decode_frame(message)


async def _await_control(
    connection: ClientConnection, kind: str, *, timeout: float = 10.0
) -> ControlFrame:
    """The next control frame of *kind*, skipping the tick frames in between."""
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"no {kind!r} frame within {timeout:.0f}s")
        frame = await _next_frame(connection, timeout=remaining)
        if isinstance(frame, ControlFrame) and frame.kind == kind:
            return frame


async def _await_ticks(
    connection: ClientConnection,
    predicate: Callable[[TickFrame], bool],
    *,
    timeout: float = 15.0,
) -> TickFrame:
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"no matching ticks frame within {timeout:.0f}s")
        frame = await _next_frame(connection, timeout=remaining)
        if isinstance(frame, TickFrame) and predicate(frame):
            return frame


def _describe(tick: Tick) -> dict[str, Any]:
    """One decoded record as plain JSON, for the evidence block."""
    return {
        "symbol": tick.symbol,
        "time_msc": tick.time_msc,
        "bid": tick.bid,
        "ask": tick.ask,
        "last": tick.last,
        "volume": tick.volume,
        "flags": tick.flags,
        "seq": tick.seq,
    }


def _ticks_equal(left: Tick, right: Tick) -> bool:
    """Equal as records, with floats compared for closeness.

    JSON carries a decimal rendering of a double; the binary record carries the
    double. Python's ``repr``-shortest float rendering round-trips exactly, so
    these are normally identical -- ``isclose`` is here so that a bridge that
    ever rounded for the wire would show up as a *reported difference* rather
    than as an inequality with no number attached.
    """
    if (left.symbol, left.time_msc, left.flags, left.seq) != (
        right.symbol,
        right.time_msc,
        right.flags,
        right.seq,
    ):
        return False
    return all(
        math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12)
        for a, b in (
            (left.bid, right.bid),
            (left.ask, right.ask),
            (left.last, right.last),
            (left.volume, right.volume),
        )
    )


# -- the checks ------------------------------------------------------------


async def _guard(name: str, body: Callable[[], Awaitable[CheckResult]]) -> CheckResult:
    """Run one check, turning an unexpected failure into a FAIL row.

    Without this a timeout in check 7 would end the run before checks 8-15 said
    anything, and a verification that stops at the first problem is worth less
    than one that reports all of them.
    """
    try:
        return await body()
    except Exception as exc:  # a failed check is a result, not a crash
        return CheckResult(name, False, f"{type(exc).__name__}: {exc}")


async def verify(
    *,
    http_base: str,
    ws_url: str,
    compare_window_s: float = 10.0,
    heartbeat_timeout_s: float = 20.0,
) -> Verification:
    """Run every check against a live bridge and return the results."""
    report = Verification()
    add = report.results.append

    # -- REST ------------------------------------------------------------

    def api(path: str) -> str:
        return f"{http_base}/api/v1/{path}"

    symbols_seen: list[str] = []

    async def check_health() -> CheckResult:
        body = http_get_json(api("health"))
        ok = body.get("status") == "ok" and float(body.get("uptime_s", 0)) > 0
        return CheckResult("rest_health", ok, f"status={body.get('status')}", body)

    add(await _guard("rest_health", check_health))

    async def check_symbols() -> CheckResult:
        body = http_get_json(api("symbols"))
        symbols_seen.extend(str(row["symbol"]) for row in body)
        fresh = [row for row in body if float(row["age_ms"]) < 120_000]
        sane = all(float(row["ask"]) >= float(row["bid"]) for row in body)
        ok = bool(body) and sane and bool(fresh)
        return CheckResult(
            "rest_symbols",
            ok,
            f"{len(body)} symbol(s), {len(fresh)} seen in the last 2 min, ask>=bid: {sane}",
            {"symbols": symbols_seen[:10], "sample": body[:1]},
        )

    add(await _guard("rest_symbols", check_symbols))

    async def check_one_symbol() -> CheckResult:
        if not symbols_seen:
            return CheckResult("rest_symbol_one", False, "no symbols to ask about")
        name = symbols_seen[0]
        # This broker suffixes every symbol with "#", which starts a URL
        # fragment if it is not escaped -- the request would arrive as
        # /symbols/EURUSD and 404. Percent-encoding it is the consumer's job.
        body = http_get_json(api(f"symbols/{quote(name, safe='')}"))
        ok = body.get("symbol") == name and body.get("time_msc", 0) > 0
        return CheckResult("rest_symbol_one", ok, f"/symbols/{name}", body)

    add(await _guard("rest_symbol_one", check_one_symbol))

    stats_before: dict[str, Any] = {}

    async def check_stats() -> CheckResult:
        body = http_get_json(api("stats"))
        stats_before.update(body)
        required = {
            "uptime_s",
            "ticks",
            "tick_rate",
            "subscribers",
            "symbols",
            "seq_gaps",
            "heartbeats",
            "dropped",
        }
        missing = required - set(body)
        ok = not missing and int(body["ticks"]) > 0
        return CheckResult(
            "rest_stats",
            ok,
            f"ticks={body.get('ticks')} rate={body.get('tick_rate')}/s"
            + (f" missing={sorted(missing)}" if missing else ""),
            body,
        )

    add(await _guard("rest_stats", check_stats))

    async def check_feeders() -> CheckResult:
        body = http_get_json(api("feeders"))
        ok = len(body) >= 1
        return CheckResult(
            "rest_feeders", ok, f"{len(body)} feeder(s) connected", {"feeders": body}
        )

    add(await _guard("rest_feeders", check_feeders))

    # -- WebSocket handshake ---------------------------------------------

    async def check_hello() -> CheckResult:
        async with connect(ws_url, compression=None, max_queue=None) as connection:
            frame = await _next_frame(connection, timeout=10.0)
            if not isinstance(frame, ControlFrame):
                return CheckResult("ws_hello_first", False, "first frame was a ticks frame")
            payload = frame.payload
            hello_first = frame.kind == "hello"
            unfiltered = payload.get("symbols", "missing") is None
            available = payload.get("available") or []
            ok = hello_first and unfiltered and bool(available)
            return CheckResult(
                "ws_hello_first",
                ok,
                f"first frame t={frame.kind!r}, symbols={payload.get('symbols')!r}, "
                f"available={len(available)} symbol(s), "
                f"record_size={payload.get('record_size')}",
                {
                    "protocol": payload.get("protocol"),
                    "record_size": payload.get("record_size"),
                    "backpressure": payload.get("backpressure"),
                    "available": available[:10],
                    "snapshot_len": len(payload.get("snapshot") or []),
                },
            )

    add(await _guard("ws_hello_first", check_hello))

    async def check_hello_filtered() -> CheckResult:
        if not symbols_seen:
            return CheckResult("ws_hello_filtered", False, "no symbol to filter on")
        name = symbols_seen[0]
        # Same escaping question on the query side: an unescaped "#" would
        # truncate the query and the bridge would see symbols=EURUSD.
        async with connect(
            f"{ws_url}?symbols={quote(name, safe='')}", compression=None, max_queue=None
        ) as connection:
            frame = await _await_control(connection, "hello")
            got = frame.payload.get("symbols")
            ok = got == [name]
            return CheckResult(
                "ws_hello_filtered",
                ok,
                f"?symbols={name} -> hello.symbols={got!r} (contrast: unfiltered is null)",
                {"symbols": got},
            )

    add(await _guard("ws_hello_filtered", check_hello_filtered))

    async def check_heartbeat() -> CheckResult:
        async with connect(
            f"{ws_url}?format=json&heartbeats=1", compression=None, max_queue=None
        ) as connection:
            await _await_control(connection, "hello")
            frame = await _await_ticks(
                connection,
                lambda tick_frame: any(tick.is_heartbeat for tick in tick_frame.ticks),
                timeout=heartbeat_timeout_s,
            )
            beat = next(tick for tick in frame.ticks if tick.is_heartbeat)
            return CheckResult(
                "ws_heartbeats",
                True,
                f"heartbeat record seen: seq={beat.seq} flags=0x{beat.flags:08x}",
                {"flag_heartbeat": f"0x{FLAG_HEARTBEAT:08x}", "seq": beat.seq},
            )

    add(await _guard("ws_heartbeats", check_heartbeat))

    # -- binary vs JSON ---------------------------------------------------

    async def check_formats_agree() -> CheckResult:
        async def collect(url: str) -> dict[int, Tick]:
            out: dict[int, Tick] = {}
            async with connect(url, compression=None, max_queue=None) as connection:
                deadline = time.monotonic() + compare_window_s
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return out
                    try:
                        frame = await _next_frame(connection, timeout=remaining)
                    except TimeoutError:
                        return out
                    if isinstance(frame, TickFrame):
                        for tick in frame.ticks:
                            out[tick.seq] = tick
                    elif frame.kind == "hello":
                        for item in frame.payload.get("snapshot") or []:
                            # The snapshot predates the window on the other
                            # socket; it is state, not a record on the wire.
                            out.pop(int(item["q"]), None)

        json_ticks, binary_ticks = await asyncio.gather(
            collect(f"{ws_url}?format=json&heartbeats=1"),
            collect(f"{ws_url}?format=binary&heartbeats=1"),
        )
        shared = sorted(set(json_ticks) & set(binary_ticks))
        mismatched = [
            seq for seq in shared if not _ticks_equal(json_ticks[seq], binary_ticks[seq])
        ]
        ok = bool(shared) and not mismatched
        return CheckResult(
            "ws_binary_matches_json",
            ok,
            f"{len(shared)} record(s) in both windows, {len(mismatched)} differ "
            f"(json={len(json_ticks)}, binary={len(binary_ticks)}, "
            f"window={compare_window_s:.0f}s)",
            {
                "shared": len(shared),
                "mismatched_seqs": mismatched[:5],
                "sample": _describe(json_ticks[shared[0]]) if shared else None,
            },
        )

    add(await _guard("ws_binary_matches_json", check_formats_agree))

    # -- control ops -------------------------------------------------------

    async def check_control_ops() -> list[CheckResult]:
        """Every control op, on one connection, in the order that composes.

        ``heartbeats=1`` is not part of what is being tested -- it is what makes
        the test deterministic. A heartbeat bypasses the symbol filter by
        design, so this connection has ~1 record a second whatever it is
        subscribed to, and the format-switch check no longer waits on a quote
        that a thin session may not produce. Two checks failed exactly that way
        on a quiet EURUSD (7 records in 106 s) before this was added.
        """
        out: list[CheckResult] = []
        async with connect(
            f"{ws_url}?heartbeats=1", compression=None, max_queue=None
        ) as connection:
            hello = await _await_control(connection, "hello")

            await connection.send(json.dumps({"op": "ping", "echo": 4242}))
            pong = await _await_control(connection, "pong")
            out.append(
                CheckResult(
                    "ctl_ping_pong",
                    pong.payload.get("echo") == 4242,
                    f"pong echo={pong.payload.get('echo')!r} rx={pong.payload.get('rx')}",
                    pong.payload,
                )
            )

            names = [str(row) for row in (hello.payload.get("available") or [])][:2]
            if len(names) >= 1:
                await connection.send(json.dumps({"op": "subscribe", "symbols": names[:1]}))
                first = await _await_control(connection, "ack")
                merged: list[str] | None = first.payload.get("symbols")
                if len(names) >= 2:
                    await connection.send(
                        json.dumps({"op": "subscribe", "symbols": names[1:2]})
                    )
                    second = await _await_control(connection, "ack")
                    merged = second.payload.get("symbols")
                await connection.send(json.dumps({"op": "unsubscribe", "symbols": names}))
                emptied = await _await_control(connection, "ack")
                narrowed = sorted(merged or []) == sorted(names)
                ok = (
                    first.payload.get("symbols") == names[:1]
                    and narrowed
                    and emptied.payload.get("symbols") == []
                )
                out.append(
                    CheckResult(
                        "ctl_subscribe_merge",
                        ok,
                        f"null -> {names[:1]} -> {sorted(merged or [])} -> "
                        f"{emptied.payload.get('symbols')!r} "
                        "(a list narrows; [] is 'none', distinct from null)",
                        {"first": first.payload, "merged": merged, "emptied": emptied.payload},
                    )
                )
            else:
                out.append(CheckResult("ctl_subscribe_merge", False, "no symbols available"))

            # Back to everything, so the format switch has ticks to carry.
            await connection.send(json.dumps({"op": "subscribe", "symbols": []}))
            await _await_control(connection, "ack")

            await connection.send(json.dumps({"op": "format", "value": "binary"}))
            ack = await _await_control(connection, "ack")
            binary_frame = await _await_ticks(
                connection, lambda frame: frame.rx is None and bool(frame.ticks), timeout=20.0
            )
            out.append(
                CheckResult(
                    "ctl_format_switch",
                    ack.payload.get("format") == "binary" and bool(binary_frame.ticks),
                    f"ack.format={ack.payload.get('format')!r}; next ticks frame decoded "
                    f"{len(binary_frame.ticks)} binary record(s)",
                    {"ack": ack.payload},
                )
            )

            await connection.send(json.dumps({"op": "format", "value": "json"}))
            await _await_control(connection, "ack")

            await connection.send(json.dumps({"op": "stats"}))
            one = await _await_control(connection, "stats")
            await connection.send(json.dumps({"op": "stats"}))
            two = await _await_control(connection, "stats")
            rest_after = http_get_json(api("stats"))
            same_keys = set(one.payload) == set(two.payload)
            cumulative = ("ticks", "heartbeats", "dropped", "seq_gaps")
            monotonic = all(
                int(two.payload[key]) >= int(one.payload[key]) for key in cumulative
            )
            before_uptime = float(stats_before.get("uptime_s", 0))
            kept_uptime = float(rest_after["uptime_s"]) >= before_uptime
            kept_counters = all(
                int(rest_after[key]) >= int(stats_before.get(key, 0)) for key in cumulative
            )
            rest_kept = kept_uptime and kept_counters
            out.append(
                CheckResult(
                    "ctl_stats_pure_read",
                    same_keys and monotonic and rest_kept,
                    "two reads: same key set "
                    f"({same_keys}), counters non-decreasing ({monotonic}), REST view not "
                    f"reset by the WS read ({rest_kept}); ticks {one.payload.get('ticks')} -> "
                    f"{two.payload.get('ticks')}",
                    {"first": one.payload, "second": two.payload, "rest_after": rest_after},
                )
            )

            await connection.send("this is not json")
            error = await _await_control(connection, "error")
            await connection.send(json.dumps({"op": "ping", "echo": "alive"}))
            survived = await _await_control(connection, "pong")
            out.append(
                CheckResult(
                    "ctl_error_frame",
                    survived.payload.get("echo") == "alive",
                    f"garbage -> error({error.payload.get('reason')!r}); "
                    "connection still answers ping",
                    {"error": error.payload},
                )
            )

            await connection.send(json.dumps({"op": "nope"}))
            unknown = await _await_control(connection, "error")
            out.append(
                CheckResult(
                    "ctl_unknown_op",
                    "nope" in str(unknown.payload.get("reason", "")),
                    f"unknown op -> error({unknown.payload.get('reason')!r})",
                    unknown.payload,
                )
            )
        return out

    try:
        report.results.extend(await check_control_ops())
    except Exception as exc:  # report it rather than lose the checks after it
        add(CheckResult("ctl_ops", False, f"{type(exc).__name__}: {exc}"))

    # -- conflation --------------------------------------------------------

    async def check_conflate() -> CheckResult:
        # heartbeats=1 for the same reason as the control connection: it
        # guarantees traffic. They conflate on the empty symbol like any other,
        # so a duplicate inside one frame would still be the bug this looks for.
        async with connect(
            f"{ws_url}?conflate=1&heartbeats=1", compression=None, max_queue=None
        ) as connection:
            await _await_control(connection, "hello")
            frames_seen = 0
            duplicates = 0
            deadline = time.monotonic() + min(compare_window_s, 10.0)
            while time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                try:
                    frame = await _next_frame(connection, timeout=remaining)
                except TimeoutError:
                    break
                if isinstance(frame, TickFrame) and frame.ticks:
                    frames_seen += 1
                    names = [tick.symbol for tick in frame.ticks]
                    duplicates += len(names) - len(set(names))
            ok = frames_seen > 0 and duplicates == 0
            return CheckResult(
                "ws_conflate",
                ok,
                f"{frames_seen} frame(s), {duplicates} same-symbol duplicate(s) within a frame",
                {"frames": frames_seen, "duplicates": duplicates},
            )

    add(await _guard("ws_conflate", check_conflate))

    # -- session integrity -------------------------------------------------

    async def check_integrity() -> CheckResult:
        body = http_get_json(api("stats"))
        gaps = int(body["seq_gaps"])
        dropped = int(body["dropped"])
        return CheckResult(
            "session_seq_gaps_and_drops",
            gaps == 0 and dropped == 0,
            f"seq_gaps={gaps} dropped={dropped} over {body['uptime_s']:.0f}s "
            f"and {body['ticks']} records",
            body,
        )

    add(await _guard("session_seq_gaps_and_drops", check_integrity))

    return report
