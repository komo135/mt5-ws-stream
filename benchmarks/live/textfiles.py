"""Reading and writing the files MetaTrader owns.

Every text file the terminal writes -- ``config\\common.ini``, the profile
``*.chr`` charts, ``order.wnd``, the Experts logs, MetaEditor's compile log --
is **UTF-16LE with a byte-order mark and CRLF line endings**. Round-tripping one
through a UTF-8 editor corrupts it silently, so the rig never edits them with
anything but these two functions.

The pair is deliberately asymmetric:

* :func:`decode_terminal_text` accepts what it finds -- either byte order, or a
  UTF-8 file for the odd tool that writes one -- and normalises the line endings
  to ``\\n`` so parsers upstream never see a stray ``\\r``.
* :func:`encode_terminal_text` writes exactly one shape: BOM + UTF-16LE + CRLF.

So a file read and written back unchanged comes out in the terminal's own
format regardless of what went in, and no parser has to know about encodings.

The odd-length guard in :func:`decode_terminal_text` is not paranoia: the
Experts log is read *while the terminal is appending to it*, and a read that
lands mid-code-unit would otherwise raise.
"""

from __future__ import annotations

import codecs
import shutil
from pathlib import Path

__all__ = [
    "backup_once",
    "decode_terminal_text",
    "encode_terminal_text",
    "read_terminal_text",
    "write_terminal_text",
]

_BOM_LE = codecs.BOM_UTF16_LE
_BOM_BE = codecs.BOM_UTF16_BE


def decode_terminal_text(data: bytes, *, errors: str = "strict") -> str:
    """Decode *data* as the terminal wrote it, with ``\\n`` line endings.

    Args:
        data: Raw file bytes. UTF-16 with either BOM, or UTF-8 (BOM optional).
        errors: Passed to :meth:`bytes.decode`. ``"replace"`` for a file being
            written concurrently, where a truncated character is expected.

    Returns:
        The text, with every ``\\r\\n`` collapsed to ``\\n``.
    """
    if data.startswith((_BOM_LE, _BOM_BE)):
        # A concurrent writer can leave us half a code unit; drop it rather than
        # fail a poll that will be repeated a moment later anyway.
        if len(data) % 2:
            data = data[:-1]
        text = data.decode("utf-16", errors=errors)
    else:
        text = data.decode("utf-8-sig", errors=errors)
    return text.replace("\r\n", "\n")


def encode_terminal_text(text: str) -> bytes:
    """Encode *text* the way the terminal writes files: BOM + UTF-16LE + CRLF."""
    body = text.replace("\r\n", "\n").replace("\n", "\r\n")
    return _BOM_LE + body.encode("utf-16-le")


def read_terminal_text(path: Path, *, errors: str = "strict") -> str:
    """Read one terminal-owned file. Missing file -> empty string.

    A missing file is not an error because the two callers that hit it -- the
    Experts log for a date the terminal has not opened yet, and a profile that
    does not exist -- both mean "nothing yet", and raising would make every
    poll loop wrap this in a try.
    """
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return ""
    return decode_terminal_text(data, errors=errors)


def write_terminal_text(path: Path, text: str) -> None:
    """Write *text* to *path* in the terminal's format, creating parents."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encode_terminal_text(text))


def backup_once(path: Path, *, suffix: str) -> Path | None:
    """Copy *path* to ``<path><suffix>`` unless that backup already exists.

    "Once" is the point: the rig edits ``common.ini`` and reinstalls the EA
    many times in a sweep, and a backup taken on every edit would, after the
    second one, only preserve the rig's own output. The first copy is the one
    that holds the user's original state.

    Returns:
        The backup path if one was made, ``None`` if it already existed or the
        source does not exist.
    """
    if not path.exists():
        return None
    backup = path.with_name(path.name + suffix)
    if backup.exists():
        return None
    shutil.copy2(path, backup)
    return backup
