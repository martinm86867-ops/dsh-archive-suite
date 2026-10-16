"""Local text store: gzipped text on disk with a SQLite index.

Storage only. This module used to also carry a Python winnowing/ fingerprint
implementation and a ``rank_by_overlap`` search, which duplicated what
``corpus`` does in SQL — two similarity engines doing the same job, neither
aware of the other. All analysis now lives in ``corpus`` (and, above it,
``analysis``); this layer just holds text so it is downloaded at most once.

The design premise stands: acquiring text and analysing it are different jobs.
Acquisition is networked, slow and rate-limited; analysis should be local and
free. This is the local half.
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .textutil import tokenize

logger = logging.getLogger("archivist.textstore")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS texts (
    identifier   TEXT PRIMARY KEY,
    file_name    TEXT,
    format       TEXT,
    chars        INTEGER,
    tokens       INTEGER,
    sha256       TEXT,
    fetched_at   TEXT,
    source_url   TEXT,
    cached_path  TEXT
);
CREATE INDEX IF NOT EXISTS idx_texts_chars ON texts(chars);
"""


class TextStore:
    """On-disk gzipped text with a SQLite index, for offline analysis."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.texts_dir = self.root / "texts"
        self.db_path = self.root / "index.sqlite"
        self._init()

    def _init(self) -> None:
        self.texts_dir.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(_SCHEMA)

    # -- paths -------------------------------------------------------------

    def _safe(self, identifier: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]", "_", identifier.split("|")[0])[:120]

    def text_path(self, identifier: str) -> Path:
        return self.texts_dir / f"{self._safe(identifier)}.txt.gz"

    # -- read/write --------------------------------------------------------

    def has(self, identifier: str) -> bool:
        base = identifier.split("|")[0]
        if not self.text_path(base).exists():
            return False
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT 1 FROM texts WHERE identifier = ?", (base,)).fetchone()
        return row is not None

    def get(self, identifier: str) -> Optional[str]:
        base = identifier.split("|")[0]
        path = self.text_path(base)
        if not path.exists():
            return None
        try:
            return gzip.decompress(path.read_bytes()).decode("utf-8", errors="ignore")
        except OSError as err:
            logger.warning(f"could not read cached text for {base}: {err}")
            return None

    def put(
        self,
        identifier: str,
        text: str,
        file_name: str = "",
        fmt: str = "",
        source_url: str = "",
    ) -> Dict[str, Any]:
        base = identifier.split("|")[0]
        path = self.text_path(base)
        path.write_bytes(gzip.compress(text.encode("utf-8")))
        record = {
            "identifier": base,
            "file_name": file_name,
            "format": fmt,
            "chars": len(text),
            "tokens": len(tokenize(text)),
            "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source_url": source_url,
            "cached_path": str(path),
        }
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO texts
                   (identifier, file_name, format, chars, tokens, sha256,
                    fetched_at, source_url, cached_path)
                   VALUES (:identifier, :file_name, :format, :chars, :tokens,
                           :sha256, :fetched_at, :source_url, :cached_path)""",
                record,
            )
        return record

    # -- inspection --------------------------------------------------------

    def entries(self) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM texts ORDER BY chars DESC").fetchall()
        return [dict(r) for r in rows]

    def stats(self) -> Dict[str, Any]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS docs, COALESCE(SUM(chars),0) AS chars FROM texts"
            ).fetchone()
        return {
            "documents": row[0],
            "total_chars": row[1],
            "root": str(self.root),
        }

    def identifiers(self) -> List[str]:
        return [e["identifier"] for e in self.entries()]

    def discard(self, identifier: str) -> bool:
        base = identifier.split("|")[0]
        path = self.text_path(base)
        existed = path.exists()
        if existed:
            path.unlink()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM texts WHERE identifier = ?", (base,))
        return existed
