# ADR-0004: The EA is in scope; performance work is measurement-driven

Status: accepted 2026-08-17 (supersedes the "EA is out of scope" scoping in
`specs/refactoring-plan.md §5`, which was a scope choice, not a rule)

## Context
The EA is the product's most important adapter and was excluded from the
last refactor only because Python tests cannot cover it. Its internals are
~20 file-scope globals shared by three entry points, parallel arrays, a
`FlushBuffer` doing four jobs, and hard-won connect/watchdog rules living
only in prose. Extra symbols are polled by `OnTimer` and only the newest
quote per poll is sent, so at N symbols the EA both adds latency (timer
floor 10–16 ms, per-poll cost ∝ N) and loses ticks.

## Decision
- The EA is refactored fully: state grouped into structs, one rule in one
  place, ordering constraints enforced by structure. Input names may change
  when a better name exists; the change is documented.
- Verification: `MetaEditor64 /compile` = 0 errors for every EA change; two
  live-terminal checkpoints (after the structural refactor: behaviour
  unchanged; after the performance change: numbers). Steps that need the
  terminal GUI are handed to the user as a wizard.
- Performance work targets **all ticks, minimum latency** and is measured
  before and after. The deliverable is the symbol scaling table
  (N × {p50, p99, timer callback µs, ticks lost}) plus the chosen extra-symbol
  delivery mechanism. A change with no number does not land.
- Wire changes are allowed only with a measured gain, as their own late node.

## Consequences
- `docs/latency.md` gains the scaling table and drops guesses.
- The EA's watchdog vocabulary (timed-out / dropped / unreachable connect)
  is preserved and made structural, not just commented.
