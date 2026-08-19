"""Package-level import behaviour: the server side is exported lazily.

``mt5_ws_stream/__init__.py`` used to import ``api`` and ``bridge`` eagerly,
so importing anything from the package -- even just ``TickStreamClient`` --
pulled in FastAPI, pydantic and uvicorn. A client-only process pays that cost
for nothing. These tests pin the PEP 562 ``__getattr__``/``__dir__`` lazy
export: the heavy modules must stay unimported until a server-side name is
actually touched, while still being reachable, in ``dir()``, and covered by
``from mt5_ws_stream import *``.
"""

from __future__ import annotations

import subprocess
import sys

import pytest


def test_client_only_import_skips_server_dependencies() -> None:
    """Importing the client surface must not import fastapi/uvicorn/pydantic.

    Runs in a subprocess so this process's already-imported modules (e.g. from
    other tests that touch the server side) can't mask a regression.
    """
    code = (
        "import sys\n"
        "import mt5_ws_stream\n"
        "from mt5_ws_stream import TickStreamClient\n"
        "assert 'fastapi' not in sys.modules, sys.modules.keys()\n"
        "assert 'uvicorn' not in sys.modules, sys.modules.keys()\n"
        "assert 'pydantic' not in sys.modules, sys.modules.keys()\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_server_names_are_reachable_and_load_their_dependencies() -> None:
    """The lazy names still resolve, and touching one pulls its module in."""
    code = (
        "import sys\n"
        "import mt5_ws_stream\n"
        "from mt5_ws_stream import Bridge, BridgeConfig, create_app\n"
        "assert Bridge is mt5_ws_stream.bridge.Bridge\n"
        "assert BridgeConfig is mt5_ws_stream.bridge.BridgeConfig\n"
        "assert create_app is mt5_ws_stream.api.create_app\n"
        "assert 'fastapi' in sys.modules\n"
        "assert 'uvicorn' in sys.modules\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_dir_includes_lazy_names() -> None:
    import mt5_ws_stream

    names = dir(mt5_ws_stream)
    assert "create_app" in names
    assert "Bridge" in names
    assert "BridgeConfig" in names
    # dir() output stays de-duplicated and sorted, like the builtin default.
    assert names == sorted(set(names))


def test_unknown_attribute_still_raises_attribute_error() -> None:
    import mt5_ws_stream

    with pytest.raises(AttributeError):
        _ = mt5_ws_stream.this_name_does_not_exist


def test_star_import_exposes_the_same_names_as___all__() -> None:
    import mt5_ws_stream

    namespace: dict[str, object] = {}
    exec("from mt5_ws_stream import *", namespace)
    exported = set(namespace) - {"__builtins__"}
    assert exported == set(mt5_ws_stream.__all__)
    # And the server-side names it pulled in actually resolve.
    assert namespace["create_app"] is mt5_ws_stream.api.create_app
    assert namespace["Bridge"] is mt5_ws_stream.bridge.Bridge
