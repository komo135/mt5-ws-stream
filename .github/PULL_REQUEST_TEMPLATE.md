## What and why

<!-- What does this change, and what problem does it solve? -->

## How to verify

<!-- The commands a reviewer should run, and what they should see. -->

## Checklist

- [ ] `pytest` passes
- [ ] `ruff check . && ruff format --check .` passes
- [ ] `mypy` passes
- [ ] Added a test that fails without this change
- [ ] Updated `CHANGELOG.md` under `## [Unreleased]`
- [ ] Updated `docs/` if behaviour changed
- [ ] **Wire format changes only:** updated `protocol.py`, `TickStreamer.mq5`,
      `docs/protocol.md` and the reference vectors together
