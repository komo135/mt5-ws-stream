# ADR-0001: The feeder seam is the TCP wire, not a Python Protocol

Status: accepted (records the decision made in commit d79def4, 2026-08-17)

## Context
A `Feeder` Protocol and a `MetaTraderPollingFeeder` existed briefly. The
polling feeder required Windows + a co-located terminal, contradicting the
premise that the Python side is OS/terminal independent, and its expected
use case (cannot install an EA) does not exist in practice.

## Decision
The seam between "something that produces ticks" and the bridge is the TCP
wire carrying 64-byte records (`docs/protocol.md §1`). Adapters: the EA
(product) and `MockFeeder` (tests/benchmarks). No Python-level Feeder
interface. `MockFeeder.run(link=)` and `FeederConnection(start_seq=)` remain
as test seams for the mock only.

## Consequences
- A future feeder is a process, in any language, that opens a socket.
- Do not reintroduce a Feeder Protocol unless a second Python adapter with a
  real use case appears (one adapter = hypothetical seam).
- Anything shared between the EA and Python is a wire fact and lives in
  `docs/protocol.md`, mirrored by `protocol.py` and the EA writer block.
