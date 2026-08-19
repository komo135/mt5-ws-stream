"""Allow ``python -m mt5_ws_stream``."""

from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
