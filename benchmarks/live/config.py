"""Where everything lives on this machine.

One dataclass rather than constants scattered across the rig, because every
path in it is derived from two roots -- the MetaTrader installation and the
terminal's *data folder* -- and those two are the only things that change
between machines. :meth:`LiveConfig.detect` finds them; everything else is a
property, so a wrong root fails once, loudly, instead of in five places.

The data folder is the hashed directory under ``%APPDATA%\\MetaQuotes\\Terminal``
-- **not** the installation directory. MetaTrader writes profiles, logs and
``config\\common.ini`` there, and installs compiled programs under its ``MQL5``
subtree.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

__all__ = ["DEFAULT_DATA_FOLDER_ID", "LiveConfig"]

#: The terminal this engagement drives (XMTrading demo 75537514).
DEFAULT_DATA_FOLDER_ID = "D0E8209F77C8CF37AD8BF550E51FF075"

_DEFAULT_INSTALL = Path(r"C:\Program Files\MetaTrader 5")


@dataclass(frozen=True)
class LiveConfig:
    """Absolute paths for one terminal installation plus its data folder."""

    install_dir: Path
    """Where ``terminal64.exe`` and ``MetaEditor64.exe`` live."""

    data_dir: Path
    """The hashed ``%APPDATA%\\MetaQuotes\\Terminal\\<id>`` folder."""

    repo_root: Path
    """This checkout, for compiling sources and running the bridge from."""

    @classmethod
    def detect(
        cls,
        *,
        data_folder_id: str = DEFAULT_DATA_FOLDER_ID,
        repo_root: Path | None = None,
    ) -> LiveConfig:
        """Build a config from the environment, with the documented defaults.

        ``MT5_INSTALL_DIR`` and ``MT5_DATA_DIR`` override the defaults so the
        rig can be pointed at a second terminal without editing code.
        """
        install = Path(os.environ.get("MT5_INSTALL_DIR", str(_DEFAULT_INSTALL)))
        override = os.environ.get("MT5_DATA_DIR")
        if override:
            data = Path(override)
        else:
            appdata = os.environ.get("APPDATA")
            if not appdata:
                raise RuntimeError("APPDATA is unset; pass MT5_DATA_DIR instead")
            data = Path(appdata) / "MetaQuotes" / "Terminal" / data_folder_id
        root = repo_root if repo_root is not None else Path(__file__).resolve().parents[2]
        return cls(install_dir=install, data_dir=data, repo_root=root)

    def check(self) -> None:
        """Fail now if a root is wrong, naming the path that is missing."""
        for label, path in (
            ("terminal64.exe", self.terminal_exe),
            ("MetaEditor64.exe", self.metaeditor_exe),
            ("data folder", self.data_dir),
            ("common.ini", self.common_ini),
        ):
            if not path.exists():
                raise FileNotFoundError(f"{label} not found at {path}")

    # -- installation ----------------------------------------------------

    @property
    def terminal_exe(self) -> Path:
        return self.install_dir / "terminal64.exe"

    @property
    def metaeditor_exe(self) -> Path:
        return self.install_dir / "MetaEditor64.exe"

    # -- data folder -----------------------------------------------------

    @property
    def common_ini(self) -> Path:
        return self.data_dir / "config" / "common.ini"

    @property
    def profiles_dir(self) -> Path:
        return self.data_dir / "MQL5" / "Profiles" / "Charts"

    @property
    def deleted_profiles_dir(self) -> Path:
        """Charts the terminal has closed. The rig reads templates from here."""
        return self.data_dir / "MQL5" / "Profiles" / "deleted"

    @property
    def experts_dir(self) -> Path:
        return self.data_dir / "MQL5" / "Experts"

    @property
    def indicators_dir(self) -> Path:
        return self.data_dir / "MQL5" / "Indicators"

    @property
    def scripts_dir(self) -> Path:
        return self.data_dir / "MQL5" / "Scripts"

    @property
    def logs_dir(self) -> Path:
        """Experts logs: one ``YYYYMMDD.log`` per day."""
        return self.data_dir / "MQL5" / "Logs"

    @property
    def files_dir(self) -> Path:
        """``MQL5\\Files`` -- where ``CountTicks.mq5`` writes its CSV."""
        return self.data_dir / "MQL5" / "Files"

    # -- repo ------------------------------------------------------------

    @property
    def ea_source(self) -> Path:
        return self.repo_root / "mql5" / "Experts" / "TickStreamer" / "TickStreamer.mq5"

    @property
    def spy_source(self) -> Path:
        return self.repo_root / "mql5" / "Indicators" / "TickStreamer" / "TickSpy.mq5"

    @property
    def count_ticks_source(self) -> Path:
        return self.repo_root / "mql5" / "Scripts" / "TickStreamer" / "CountTicks.mq5"

    @property
    def results_dir(self) -> Path:
        return self.repo_root / "benchmarks" / "results"
