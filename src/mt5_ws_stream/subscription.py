"""The subscription request: one concept, two directions.

A **subscription request** is the query-string vocabulary a consumer sends
when connecting -- ``symbols``, ``format``, ``conflate``, ``heartbeats``
(``CONTEXT.md`` -- Wire). Rendering it and parsing it used to live in two
modules that agreed only by convention: :mod:`mt5_ws_stream.client` wrote
``conflate=1``/``heartbeats=1``/``format=<v>`` and :mod:`mt5_ws_stream.api`
read them back with its own, separately-maintained vocabulary. Nothing
checked that the writer and the reader actually agreed.

:class:`SubscriptionRequest` is now the one place that knows both halves:

* :meth:`SubscriptionRequest.to_query` / :meth:`~SubscriptionRequest.to_query_string`
  render the **canonical** spellings -- what :mod:`mt5_ws_stream.client` puts
  on the wire and what the dashboard's JavaScript is written to match.
* :meth:`SubscriptionRequest.from_query` reads the **lenient** vocabulary a
  human typing a URL by hand (or an old dashboard) might use --
  ``conflate=yes``, ``format=bin``, an empty ``symbols=`` -- documented on the
  method itself, since that leniency *is* the read-side contract.

This module imports only :mod:`mt5_ws_stream.protocol`, not
:mod:`mt5_ws_stream.api` or :mod:`mt5_ws_stream.hub`: the client depends on
it, and the client must not pull in FastAPI or the hub's asyncio-flavoured
fan-out machinery just to build a URL.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from urllib.parse import parse_qs, urlencode, urlparse

from .protocol import BackpressurePolicy, PayloadFormat, split_symbols

__all__ = ["SubscriptionRequest", "normalize_symbols"]


def normalize_symbols(items: Iterable[object]) -> frozenset[str]:
    """Stripped, non-empty, de-duplicated symbol names from any iterable.

    Shared by both halves of a symbol allow-list: the query string's
    comma-joined ``symbols`` value (split first, see
    :func:`~mt5_ws_stream.protocol.split_symbols`) and the
    ``subscribe``/``unsubscribe`` control ops' JSON list. ``str(item)`` is
    applied before stripping so a control op's JSON list -- which may hold
    anything the peer chose to send -- does not raise on a non-string entry.
    """
    return frozenset(s for s in (str(item).strip() for item in items) if s)


@dataclass(frozen=True, slots=True)
class SubscriptionRequest:
    """The parsed vocabulary of a subscription request.

    ``symbols=None`` and ``symbols=frozenset()`` mean the same thing --
    everything -- so construction collapses the latter into the former:
    there is exactly one representation of "no filter", which is what makes
    :meth:`from_query` and :meth:`to_query` round-trip.
    """

    symbols: frozenset[str] | None = None
    payload_format: PayloadFormat = PayloadFormat.JSON
    backpressure: BackpressurePolicy = BackpressurePolicy.LOSSLESS
    include_heartbeats: bool = False

    def __post_init__(self) -> None:
        if self.symbols is not None and not self.symbols:
            object.__setattr__(self, "symbols", None)

    # -- read: lenient -----------------------------------------------------

    @classmethod
    def from_query(cls, source: str | Mapping[str, str]) -> SubscriptionRequest:
        """Build a request from a URL/path or a query mapping, leniently.

        Accepts either a raw path (``"/ws?symbols=EURUSD"``) or anything
        mapping-like, which is what Starlette's ``websocket.query_params`` is.

        Recognised query parameters:

        ``symbols``
            Comma-separated allow-list. Omit (or send empty) for every symbol.
        ``format``
            ``json`` (default) or ``binary``. Anything starting with ``b`` is
            binary; anything else named -- including a typo -- is ``json``,
            per :meth:`~mt5_ws_stream.protocol.PayloadFormat.parse`.
        ``conflate``
            Truthy for :attr:`~mt5_ws_stream.protocol.BackpressurePolicy.CONFLATE`.
            Accepted spellings: ``1``, ``true``, ``yes`` (case-insensitive) and
            any other non-empty value other than ``0``/``false``/``no``/``off``.
        ``heartbeats``
            Same truthy vocabulary as ``conflate``.

        Unknown parameters are ignored so that adding one later is not a
        breaking change for older clients. A parameter given more than once
        resolves to its *last* value, which is what Starlette's
        ``query_params`` already does -- the two sources have to agree, or the
        same URL would mean two things depending on which half read it.
        """
        if isinstance(source, str):
            query: Mapping[str, str] = {
                k: v[-1] for k, v in parse_qs(urlparse(source).query).items()
            }
        else:
            query = source
        raw_symbols = normalize_symbols(split_symbols(query.get("symbols") or ""))
        return cls(
            symbols=raw_symbols or None,
            payload_format=PayloadFormat.parse(query.get("format")),
            backpressure=_parse_backpressure(query.get("conflate")),
            include_heartbeats=_truthy(query.get("heartbeats")),
        )

    # -- write: canonical ----------------------------------------------------

    def to_query(self) -> dict[str, str]:
        """The canonical query parameters, omitting anything at its default.

        ``format`` is always present -- there is no "default" spelling a
        server can assume absent one -- everything else is only written when
        it differs from the quiet default, which is what keeps
        ``ws://host/ws?format=json`` (no filter, lossless, no heartbeats) the
        short, common case.
        """
        query: dict[str, str] = {"format": self.payload_format.value}
        if self.symbols:
            query["symbols"] = ",".join(sorted(self.symbols))
        if self.backpressure is BackpressurePolicy.CONFLATE:
            query["conflate"] = "1"
        if self.include_heartbeats:
            query["heartbeats"] = "1"
        return query

    def to_query_string(self) -> str:
        """:meth:`to_query`, URL-encoded and joined with ``&``."""
        return urlencode(self.to_query())


def _truthy(value: str | None) -> bool:
    return value is not None and value.strip().lower() not in ("", "0", "false", "no", "off")


def _parse_backpressure(value: str | None) -> BackpressurePolicy:
    """Lenient parse used for query strings (``conflate=1``, ``true``, ...)."""
    if not _truthy(value):
        return BackpressurePolicy.LOSSLESS
    return BackpressurePolicy.CONFLATE
