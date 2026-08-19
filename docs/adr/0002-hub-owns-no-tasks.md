# ADR-0002: The Hub owns no tasks; a Session owns its writer loop

Status: accepted 2026-08-17 (reopens the "Hub owns writer tasks" deferral in
`specs/refactoring-plan.md`)

## Context
`Hub.subscribe()` created and stored an `asyncio.Task` per subscriber and
`unsubscribe`/`aclose` cancelled them. Consequences: `subscribe` silently
required a running loop; writer exceptions were logged and swallowed, so a
dead sink was never noticed (backlog E11); the WebSocket handler could not
place the writer in its own TaskGroup and re-derived teardown order by hand.
The subscription session (options, hello, control ops, close summary) had no
owning module — it was split across `api.stream`, `api.handle_control` and
`hub.Subscriber`.

## Decision
- The Hub is delivery policy only: `publish` appends to a subscriber's queue
  and sets its event. The Hub creates, stores and cancels no tasks.
- A **Session** module owns one consumer's options, its `hello`, the control
  ops that mutate options, the writer loop (`run()`: wait → drain → encode →
  `sink.send`) and the close summary. It is built from a `Sink` and a `Hub`.
- The WebSocket handler is an adapter: accept, origin check, build the
  Session, run `session.run()` alongside the receive loop under structured
  concurrency, and let a sink failure end the session.
- `Sink` remains the Hub-side seam; `RecordingSink` remains the test adapter.
  Tests drive `run()` explicitly instead of waiting on background tasks.

## Consequences
- The Hub gets deeper (no task bookkeeping, no cancellation order).
- E11 and E13 are structural non-issues rather than backlog items.
- `tests/test_hub.py` changes shape once (explicit run/drain), becoming
  timing-independent.
- The periodic stats broadcast goes through sessions, not through a public
  `Subscriber.sink` write path.
