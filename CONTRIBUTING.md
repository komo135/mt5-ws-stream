# Contributing

Issues and pull requests are both welcome.

## Getting set up

```bash
git clone https://github.com/komo135/mt5-ws-stream
cd mt5-ws-stream
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pre-commit install        # optional: ruff and mypy on every commit, no pytest
```

The `[dev]` extra pulls in `httpx`, `mypy`, `pytest`, `pytest-asyncio`,
`pytest-cov` and `ruff`.

## Running the Python side without MetaTrader

`mt5-ws-stream mock` sends synthetic ticks to a running bridge. Use it for tests,
CI, and client work on machines without MetaTrader.

```bash
mt5-ws-stream bridge            # terminal 1
mt5-ws-stream mock --rate 200   # terminal 2
mt5-ws-stream client --print    # terminal 3
```

Defaults to `EURUSD,USDJPY,GBPUSD,XAUUSD`. Options: `--host`, `--port`,
`--symbols`, `--rate`, `--batch`, `--duration`, `--seed`.

This is how the suite runs on Linux and macOS, and how CI tests the bridge.

## Measuring tick loss

`mql5/Scripts/TickStreamer/CountTicks.mq5` prints the terminal's
`CopyTicksRange()` count so you can compare it to the wire. Copy it into
`MQL5/Scripts/TickStreamer/` when you need that comparison. Steps:
[docs/latency.md](docs/latency.md#how-ticks-lost-is-measured).

## The checks

```bash
pytest                    # full suite
pytest -m "not slow"      # skip the subprocess smoke test
ruff check .
ruff format --check .
mypy
```

CI runs three jobs:

| Job | Runs | Where |
| --- | --- | --- |
| lint & types | `ruff check`, `ruff format --check`, `mypy` | Ubuntu, Python 3.12 |
| test | `pytest -v --cov=mt5_ws_stream --cov-report=xml --cov-report=term-missing` | Ubuntu on 3.11, 3.12 and 3.13; Windows and macOS on 3.12 |
| build & verify package | `python -m build`, `twine check --strict`, then install the wheel into a fresh venv and run `mt5-ws-stream --version` and `mt5-ws-stream dashboard --print-path` | Ubuntu, Python 3.12 |

The test job passes no `-m` filter, so `slow` tests run in CI as well.

Python 3.11 is the floor (`requires-python = ">=3.11"`): the WebSocket handler
uses `asyncio.TaskGroup` and `except*`. `mypy` is configured with
`files = ["src", "tests"]` and `strict = true`, so both trees have to stay clean
under strict mode; `warn_unreachable` and the `ignore-without-code`,
`redundant-expr` and `truthy-bool` error codes are on as well.

`.pre-commit-config.yaml` runs the pre-commit-hooks set, ruff (`--fix`),
`ruff-format` and mypy. It does not run pytest.

## How the wire format is verified without Windows

The MQL5 side runs inside a Windows GUI application, so CI cannot execute it.
Three things stand in for it.

1. **Reference vectors for the binary record.** `tests/test_protocol.py` encodes
   known ticks and compares the result against hex literals captured from a
   working end-to-end run. A pack/unpack round-trip still passes when two fields
   are swapped; the hex literals do not.
2. **Reference vectors for the JSON frames.** `tests/test_frames.py` pins every
   frame kind as the exact string that goes on the socket, then decodes it back.
   A renamed key or a reordered field fails there rather than at a consumer.
3. **Byte-level review.** A change to `protocol.py` and the matching change to
   `mql5/Experts/TickStreamer/TickStreamer.mq5` belong in the same commit, in
   either direction. The layout table in [`docs/protocol.md`](docs/protocol.md)
   is the contract both sides implement.

To extend the record, define a new `record_size` and branch on it. Do not change
an existing field's offset or width: a reader that does not understand the new
size then fails loudly instead of misreading prices.

## Pull requests

* One logical change per PR.
* Add a test that fails without your change and passes with it. For a bug fix,
  that test is the report.
* Update `CHANGELOG.md` under `## [Unreleased]`.
* If you change behaviour, update the relevant file in `docs/`.
* If you change the wire format, update `docs/protocol.md`, the MQL5 EA and the
  reference vectors together.
* Architectural decisions are recorded in `docs/adr/`; add a file there when a
  change fixes a constraint the rest of the code has to live with.

## Test layout

| File | Covers |
| --- | --- |
| `tests/test_protocol.py` | The binary record, pinned as hex literals |
| `tests/test_frames.py` | The frame grammar: every kind pinned as exact text, and round-tripped |
| `tests/test_subscription.py` | The subscription query string, rendered and parsed, checked against each other |
| `tests/test_hub.py` | Delivery policy: framing, fan-out, filtering, backpressure, sequence accounting, no network |
| `tests/test_session.py` | One consumer's conversation: the handshake, the control protocol and the writer loop, no socket |
| `tests/test_api.py` | The REST surface over the same port the WebSocket lives on, plus hub-only routes over `httpx.ASGITransport` |
| `tests/test_bridge.py` | End-to-end over real sockets: feeder TCP in, WebSocket out; chunks across packet boundaries, lifecycle, teardown |
| `tests/test_client.py` | The client adapter against a fake connection: the handshake it insists on |
| `tests/test_feeders_and_cli.py` | Feeder output is protocol-correct; CLI wiring |
| `tests/test_cli_smoke.py` | The shipped entry point, run as real subprocesses (the only `slow` file) |
| `tests/test_package.py` | The server side stays lazily imported for client-only processes |
| `tests/test_bench.py` | `benchmarks/bench.py`'s formatting (`print_run`) |
| `tests/test_symbol_scaling.py` | `benchmarks/symbol_scaling.py`'s `Aggregator`, fed decoded frame dicts |
| `tests/test_compare_tick_counts.py` | `benchmarks/compare_tick_counts.py`'s `compare()` against hand-written CSVs |
| `tests/test_live_rig.py` | The pure half of `benchmarks/live`: file transforms and the parsers that read the terminal back |

Shared builders and fixtures live in `tests/conftest.py`: `tick()`,
`heartbeat()`, `blob()`, `RecordingSink`, `wait_until()`, and the `bridge`,
`ws_url`, `http_url` and `feeder_socket` fixtures. `tests/` has no
`__init__.py`, so the builders are imported directly (`from conftest import
tick`) rather than requested as fixtures.

## Testing habits

* **Bind to port 0**, then read the real port back. The `bridge` fixture does
  this for both listeners and exposes `bridge.tcp_port` / `bridge.http_port`.
  Hard-coded ports make a suite that fails on someone else's machine.
* **Do not sleep to wait for async work.** Either drive the writer explicitly
  (`Session.flush()` — the hub owns no tasks, so there is nothing to race), or
  poll under a timeout (`wait_until()` in `tests/conftest.py`, default 5 s).
  Fixed sleeps are how a green suite becomes a flaky one on a slower runner.
* `asyncio_mode = "auto"`, so an async test needs no marker.
* `filterwarnings = ["error", ...]` turns warnings into failures. A new
  dependency that warns fails the suite.
* `--strict-markers` is on and `slow` is the only registered marker. A new
  marker has to be added to `markers` in `pyproject.toml` first.

## Style

`ruff format` decides formatting. Beyond that:

* Line length is 96; quotes are double; the ruff target version is `py311`.
* Public functions and classes get docstrings that say why, not only what.
* Comments explain non-obvious decisions.
* Keep `src/` and `tests/` clean under `mypy` strict mode.

## Releasing

Merging to `main` publishes nothing. `main` is a branch that is always green,
not a release channel. A release is a separate, deliberate act: an annotated
`vX.Y.Z` tag, and a human pressing approve.

The steps, in order:

1. On a branch, bump `version` in `pyproject.toml` and turn the
   `## [Unreleased]` heading in `CHANGELOG.md` into `## [X.Y.Z] - YYYY-MM-DD`,
   with a fresh empty `## [Unreleased]` above it. Open it as a PR like any
   other change and let CI go green.
2. Merge it. Still nothing is published.
3. Rehearse if the release path itself changed: run the **Release** workflow
   from the Actions tab with `target: testpypi`. It walks the same guards, the
   same CI, and the same Trusted Publishing exchange, against TestPyPI.
4. Tag the merge commit and push the tag:

   ```bash
   git switch main && git pull
   git tag -a v0.1.1 -m "v0.1.1"
   git push origin v0.1.1
   ```

5. The **Release** workflow then, in order:
   * checks the tag matches `pyproject.toml`, that the tagged commit is
     reachable from `main`, and that `CHANGELOG.md` has a section for it;
   * re-runs the whole CI workflow on the tagged tree -- lint, types, the test
     matrix, and the wheel smoke test -- and publishes only the `dist/`
     artifact that run produced;
   * **waits for a reviewer on the `pypi` environment.** Nothing reaches PyPI
     until someone approves the deployment;
   * uploads to PyPI via Trusted Publishing, and only then cuts the GitHub
     release.

Any one of those failing stops the release with nothing published. A tag that
turns out to be wrong before approval is cancelled by rejecting the deployment;
after approval, PyPI is immutable -- yank and ship a new patch version.

### One-time setup

* **PyPI Trusted Publishing** must exist before the first upload. On
  <https://pypi.org/manage/account/publishing/>, add a publisher with owner
  `komo135`, repository `mt5-ws-stream`, workflow `release.yml`, environment
  `pypi`. Repeat on <https://test.pypi.org> with environment `testpypi` if you
  want the rehearsal in step 3.
* **The `pypi` environment** needs a required reviewer, otherwise step 5's
  approval gate is not a gate. Settings -> Environments -> `pypi` -> required
  reviewers; and limit its deployment branches to tags matching `v*`.

## Reporting bugs

Include:

* What you ran and what happened.
* Bridge log output (`mt5-ws-stream -v bridge`; `-v` is a global flag and comes
  before the subcommand).
* For MetaTrader issues, the Experts tab output and your terminal build number.
* OS and Python version.
