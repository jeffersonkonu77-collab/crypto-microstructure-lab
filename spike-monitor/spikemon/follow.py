"""Alerts-only live view: python -m spikemon --follow

Run the monitor in one cmd window (dashboard or not) and this in a second one.
It prints the last few alerts from alerts.log, then prints each new alert the
moment it is written. It only reads the file, so it cannot disturb the monitor.
"""

from __future__ import annotations

import os
import re
import sys
import time

_BOUNDARY = re.compile(rb"\r?\n\r?\n")   # a blank line, with Unix or Windows line endings


def _complete_end(data: bytes) -> int:
    """Offset just past the last finished alert (blank line), or 0 if none."""
    end = 0
    for m in _BOUNDARY.finditer(data):
        end = m.end()
    return end


def _blocks(text: str) -> list[str]:
    text = text.replace("\r\n", "\n")
    return [b.strip("\n") for b in text.split("\n\n") if b.strip()]


def _follow(path: str, tail_blocks: int, out, stop, poll: float) -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    out(f"Following {path} (Ctrl+C to stop). New alerts appear below as they fire.\n")
    waited = False
    while not os.path.exists(path):
        if stop():
            return 0
        if not waited:
            out("Waiting for the first alert (the file is created when one fires)...")
            waited = True
        time.sleep(poll)
    with open(path, "rb") as fh:
        data = fh.read()
    pos = _complete_end(data)
    for b in _blocks(data[:pos].decode("utf-8", errors="replace"))[-tail_blocks:]:
        out(b + "\n")
    try:
        while not stop():
            time.sleep(poll)
            size = os.path.getsize(path)
            if size < pos:          # file was truncated or replaced
                pos = 0
            if size > pos:
                with open(path, "rb") as fh:
                    fh.seek(pos)
                    chunk = fh.read()
                end = _complete_end(chunk)      # wait until the whole alert is written
                if end == 0:
                    continue
                pos += end
                for b in _blocks(chunk[:end].decode("utf-8", errors="replace")):
                    out(b + "\n")
    except KeyboardInterrupt:
        pass
    return 0


def follow(path: str, tail_blocks: int = 3, out=print, stop=lambda: False, poll: float = 0.5) -> int:
    try:
        return _follow(path, tail_blocks, out, stop, poll)
    except KeyboardInterrupt:
        out("\nstopped.")
        return 0
