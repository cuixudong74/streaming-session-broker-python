"""Durable state: append-only WAL + snapshot, for restart recovery.

Layout under ``path``:
- ``wal.log``      one JSON record per line, each line is
                   ``{"crc": <hex>, "record": {...}}`` where ``crc`` is the
                   SHA256 of the canonical JSON of ``record``.
- ``snapshot.json`` a full state dump plus the WAL offset it covers.

Recovery rules:
- the snapshot is loaded first, then WAL records are replayed in order;
- a torn / half-written trailing record (bad checksum or bad JSON) is
  discarded instead of being treated as complete state;
- snapshot writes go through a temp file + rename so a crash mid-write
  never leaves a half snapshot that could be mistaken for a complete one.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any


def _canonical(record: dict[str, Any]) -> bytes:
    return json.dumps(record, sort_keys=True, separators=(",", ":")).encode()


def _crc(record: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(record)).hexdigest()


class WalStore:
    """Append-only write-ahead log with snapshots."""

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._path.mkdir(parents=True, exist_ok=True)
        self._wal = self._path / "wal.log"
        self._snapshot = self._path / "snapshot.json"
        self._fh = open(self._wal, "a", encoding="utf-8")

    def close(self) -> None:
        self._fh.close()

    def append(self, record: dict[str, Any]) -> None:
        line = json.dumps({"crc": _crc(record), "record": record},
                          separators=(",", ":"))
        self._fh.write(line + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def write_snapshot(self, state: dict[str, Any]) -> None:
        """Atomically replace the snapshot, then truncate the WAL."""
        tmp = self._snapshot.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._snapshot)
        # WAL records are now covered by the snapshot; start a fresh WAL.
        self._fh.close()
        self._fh = open(self._wal, "w", encoding="utf-8")

    def recover(self) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """Return (snapshot_or_None, valid_records_after_snapshot)."""
        snapshot: dict[str, Any] | None = None
        if self._snapshot.exists():
            try:
                with open(self._snapshot, encoding="utf-8") as f:
                    snapshot = json.load(f)
            except (json.JSONDecodeError, OSError):
                snapshot = None  # never trust a half-written snapshot
        records: list[dict[str, Any]] = []
        if self._wal.exists():
            with open(self._wal, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        record = entry["record"]
                        if entry["crc"] != _crc(record):
                            break  # torn tail: stop, discard the rest
                    except (json.JSONDecodeError, KeyError, TypeError):
                        break  # torn tail: stop, discard the rest
                    records.append(record)
        return snapshot, records
