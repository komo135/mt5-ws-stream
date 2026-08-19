"""The subscription request: render and parse, checked against each other.

:mod:`mt5_ws_stream.subscription` is the one place that knows both halves of
a subscription request -- ``SubscriptionRequest.to_query()`` (canonical,
what :class:`~mt5_ws_stream.client.TickStreamClient` sends) and
``SubscriptionRequest.from_query()`` (lenient, what the bridge actually
accepts). Three things are checked here:

* the round-trip property -- ``from_query(to_query(x)) == x`` for every
  combination of format, backpressure, heartbeats and symbol set -- which is
  the thing nothing checked before this module existed;
* the lenient-spelling tables (moved from ``test_bridge.py``), exercised
  through :func:`mt5_ws_stream.api.parse_subscription`, the public read-side
  entry point the bridge actually calls;
* the client's URL building (moved from ``test_client.py``), exercised
  through :attr:`~mt5_ws_stream.client.TickStreamClient.url`, the public
  write-side entry point.

No private imports: every assertion goes through a public name.
"""

from __future__ import annotations

import itertools

import pytest

# Starlette is what actually hands the bridge its query parameters (FastAPI's
# `websocket.query_params`), so the read side is pinned against the real thing
# rather than against a dict that happens to look like it.
from starlette.datastructures import QueryParams

from mt5_ws_stream.api import parse_subscription
from mt5_ws_stream.client import TickStreamClient
from mt5_ws_stream.protocol import BackpressurePolicy, PayloadFormat
from mt5_ws_stream.subscription import SubscriptionRequest

# -- round trip ------------------------------------------------------------

_SYMBOL_SETS: tuple[frozenset[str] | None, ...] = (
    None,  # no filter, spelled the canonical way
    frozenset(),  # no filter, spelled the empty way -- must collapse to None
    frozenset({"EURUSD"}),
    frozenset({"EURUSD", "USDJPY", "GBPUSD"}),
)


@pytest.mark.parametrize(
    ("symbols", "payload_format", "backpressure", "include_heartbeats"),
    list(
        itertools.product(
            _SYMBOL_SETS,
            (PayloadFormat.JSON, PayloadFormat.BINARY),
            (BackpressurePolicy.LOSSLESS, BackpressurePolicy.CONFLATE),
            (False, True),
        )
    ),
)
def test_round_trips_through_the_query_string(
    symbols: frozenset[str] | None,
    payload_format: PayloadFormat,
    backpressure: BackpressurePolicy,
    include_heartbeats: bool,
) -> None:
    request = SubscriptionRequest(
        symbols=symbols,
        payload_format=payload_format,
        backpressure=backpressure,
        include_heartbeats=include_heartbeats,
    )
    assert SubscriptionRequest.from_query(request.to_query()) == request
    # The rendered string is itself valid input, not just the dict: a real
    # WebSocket URL only ever carries the joined form.
    assert SubscriptionRequest.from_query(f"/ws?{request.to_query_string()}") == request


def test_empty_symbols_collapses_to_none_on_construction() -> None:
    """``symbols=frozenset()`` and ``symbols=None`` mean the same thing, so
    there is exactly one representation of it -- otherwise two "no filter"
    values could compare unequal."""
    assert SubscriptionRequest(symbols=frozenset()) == SubscriptionRequest(symbols=None)


# -- read half: lenient spellings ------------------------------------------


@pytest.mark.parametrize(
    ("path", "symbols", "fmt", "policy", "heartbeats"),
    [
        ("/", None, PayloadFormat.JSON, BackpressurePolicy.LOSSLESS, False),
        (
            "/?symbols=EURUSD,USDJPY",
            frozenset({"EURUSD", "USDJPY"}),
            PayloadFormat.JSON,
            BackpressurePolicy.LOSSLESS,
            False,
        ),
        ("/?format=binary", None, PayloadFormat.BINARY, BackpressurePolicy.LOSSLESS, False),
        ("/?conflate=1", None, PayloadFormat.JSON, BackpressurePolicy.CONFLATE, False),
        ("/?heartbeats=true", None, PayloadFormat.JSON, BackpressurePolicy.LOSSLESS, True),
        ("/?symbols=", None, PayloadFormat.JSON, BackpressurePolicy.LOSSLESS, False),
        ("/?unknown=x", None, PayloadFormat.JSON, BackpressurePolicy.LOSSLESS, False),
    ],
)
def test_parse_subscription(
    path: str,
    symbols: frozenset[str] | None,
    fmt: PayloadFormat,
    policy: BackpressurePolicy,
    heartbeats: bool,
) -> None:
    options = parse_subscription(path)
    assert options.symbols == symbols
    assert options.payload_format is fmt
    assert options.backpressure is policy
    assert options.include_heartbeats is heartbeats


def test_a_repeated_parameter_resolves_the_way_starlette_resolves_it() -> None:
    """A path string and Starlette's ``query_params`` are the two sources
    :meth:`SubscriptionRequest.from_query` accepts, and one URL must not mean
    two things depending on which of them read it. Starlette's multidict hands
    out the *last* value for a repeated key, so the path parser does too.
    """
    query = "symbols=EURUSD&symbols=USDJPY&format=json&format=binary"
    assert QueryParams(query).get("format") == "binary", "the behaviour being matched"

    from_path = SubscriptionRequest.from_query(f"/ws?{query}")
    from_mapping = SubscriptionRequest.from_query(QueryParams(query))

    assert from_path == from_mapping
    assert from_path.symbols == frozenset({"USDJPY"})
    assert from_path.payload_format is PayloadFormat.BINARY


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ({}, BackpressurePolicy.LOSSLESS),
        ({"conflate": ""}, BackpressurePolicy.LOSSLESS),
        ({"conflate": "0"}, BackpressurePolicy.LOSSLESS),
        ({"conflate": "false"}, BackpressurePolicy.LOSSLESS),
        ({"conflate": "off"}, BackpressurePolicy.LOSSLESS),
        ({"conflate": "1"}, BackpressurePolicy.CONFLATE),
        ({"conflate": "true"}, BackpressurePolicy.CONFLATE),
        ({"conflate": "yes"}, BackpressurePolicy.CONFLATE),
    ],
)
def test_conflate_is_read_leniently(
    query: dict[str, str], expected: BackpressurePolicy
) -> None:
    """``conflate=`` is typed by hand and by every dashboard's URL bar, so each
    spelling of a boolean has to land on a policy rather than on an error."""
    assert parse_subscription(query).backpressure is expected


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ({}, PayloadFormat.JSON),
        ({"format": ""}, PayloadFormat.JSON),
        ({"format": "json"}, PayloadFormat.JSON),
        ({"format": "binary"}, PayloadFormat.BINARY),
        ({"format": "bin"}, PayloadFormat.BINARY),
        ({"format": "nonsense"}, PayloadFormat.JSON),
    ],
)
def test_format_is_read_leniently(query: dict[str, str], expected: PayloadFormat) -> None:
    """A typo has to leave the consumer with the format it can definitely
    decode, not with bytes it cannot."""
    assert parse_subscription(query).payload_format is expected


def test_parse_subscription_accepts_a_query_mapping() -> None:
    """Starlette hands the handler a QueryParams mapping, not a path."""
    options = parse_subscription(
        {"symbols": "EURUSD,USDJPY", "format": "binary", "conflate": "1", "heartbeats": "yes"}
    )
    assert options.symbols == frozenset({"EURUSD", "USDJPY"})
    assert options.payload_format is PayloadFormat.BINARY
    assert options.backpressure is BackpressurePolicy.CONFLATE
    assert options.include_heartbeats is True


# -- write half: canonical spellings ----------------------------------------


def test_client_builds_the_expected_url() -> None:
    client = TickStreamClient(
        "ws://host:1/ws",
        symbols=["USDJPY", "EURUSD", " "],
        payload_format="binary",
        backpressure="conflate",
        include_heartbeats=True,
    )
    assert client.url == (
        "ws://host:1/ws?format=binary&symbols=EURUSD%2CUSDJPY&conflate=1&heartbeats=1"
    )


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        # No path means the stream path: "/" is the bridge's HTTP index, not a
        # WebSocket route, so a bare host must not be turned into "ws://h:1/?...".
        ("ws://host:1", "ws://host:1/ws?format=json"),
        ("ws://host:1/", "ws://host:1/ws?format=json"),
        # A URL that already names a path is left alone, and must not gain a
        # slash: the bridge answers "/ws/" with a redirect, which no WebSocket
        # client can follow.
        ("ws://host:1/ws", "ws://host:1/ws?format=json"),
        ("ws://host:1/ws/", "ws://host:1/ws?format=json"),
        ("ws://host:1/proxied/stream", "ws://host:1/proxied/stream?format=json"),
    ],
)
def test_client_url_defaults_the_path_to_the_stream_route(given: str, expected: str) -> None:
    assert TickStreamClient(given).url == expected
