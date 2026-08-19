# ADR-0003: No compatibility ceremony before the first tagged release

Status: accepted 2026-08-17

## Context
Nothing has been pushed to a remote or tagged. There are no external users of
0.1. Yet the code carried a `/` WebSocket double registration "for 0.1
clients", `ws_port`/`http_port` alias pairs, and the plan discussed
deprecation periods for `ServerFrame`.

## Decision
Until a release is tagged, breaking changes to the Python public API, the
CLI, the EA inputs and (if a measured gain justifies it) the wire are made
directly. No shims, no aliases, no deprecation periods. The CHANGELOG records
facts under `[Unreleased]`; it does not maintain migration guides for
versions nobody ran.

## Consequences
- Remove the `/` WS route shim and the `ws_port` alias.
- `ServerFrame` may be replaced outright.
- The `hello`-frame-first guarantee stays: it is a contract of the frame
  grammar (`TickStreamClient.connect()` relies on it), not a compat shim.
- Compatibility discipline starts with the first tag. Setting up the remote
  and tagging is a separate, user-initiated step.
