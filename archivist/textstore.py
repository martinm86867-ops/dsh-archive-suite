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
import json
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

CREATE TABLE IF NOT EXISTS metadata_cache (
    identifier    TEXT PRIMARY KEY,
    title         TEXT,
    creator       TEXT,
    date          TEXT,
    mediatype     TEXT,
    collection    TEXT,
    restricted    INTEGER,
    files_count   INTEGER,
    item_size     INTEGER,
    fetched_at    TEXT,
    metadata_json TEXT
);

CREATE TABLE IF NOT EXISTS artifacts (
    id            TEXT PRIMARY KEY,
    identifier    TEXT NOT NULL,
    kind          TEXT NOT NULL,
    file_name     TEXT NOT NULL,
    format        TEXT,
    size_bytes    INTEGER,
    sha256        TEXT,
    source_url    TEXT,
    local_path    TEXT,
    loan_status   TEXT,
    fetched_at    TEXT,
    metadata_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_artifacts_ident ON artifacts(identifier);
CREATE INDEX IF NOT EXISTS idx_artifacts_kind ON artifacts(kind);
"""


class TextStore:
    """On-disk local cache for item metadata, gzipped texts, borrowed works, and downloaded artifacts."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.texts_dir = self.root / "texts"
        self.artifacts_dir = self.root / "artifacts"
        self.db_path = self.root / "index.sqlite"
        self._init()

    def _init(self) -> None:
        self.texts_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
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
            conn.execute("DELETE FROM metadata_cache WHERE identifier = ?", (base,))
            rows = conn.execute("SELECT local_path FROM artifacts WHERE identifier = ?", (base,)).fetchall()
            for (lp,) in rows:
                if lp:
                    p = Path(lp)
                    if p.is_file() and str(p.resolve()).startswith(str(self.root.resolve())):
                        try:
                            p.unlink()
                        except OSError:
                            pass
            conn.execute("DELETE FROM artifacts WHERE identifier = ?", (base,))
        return existed

    # -- persistent metadata cache -----------------------------------------

    def get_metadata(self, identifier: str, ttl_hours: float = 168.0) -> Optional[Dict[str, Any]]:
        """Retrieve cached /metadata/<id> JSON if present and within TTL (default 7 days)."""
        base = identifier.split("|")[0]
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT metadata_json, fetched_at FROM metadata_cache WHERE identifier = ?",
                (base,),
            ).fetchone()
        if not row or not row[0]:
            return None
        try:
            if ttl_hours > 0 and row[1]:
                age_sec = (datetime.now(timezone.utc) - datetime.fromisoformat(row[1])).total_seconds()
                if age_sec > ttl_hours * 3600:
                    return None
            return json.loads(row[0])
        except Exception:
            return None

    def put_metadata(self, identifier: str, meta: Dict[str, Any]) -> None:
        """Persist item metadata in SQLite so repeat runs never hit the network."""
        base = identifier.split("|")[0]
        m = meta.get("metadata") or {}
        files = meta.get("files") or []
        colls = m.get("collection") or []
        if isinstance(colls, list):
            colls_str = ",".join(str(c) for c in colls)
        else:
            colls_str = str(colls)
        creator = m.get("creator") or ""
        if isinstance(creator, list):
            creator = ", ".join(str(c) for c in creator)
        restricted = 1 if (
            str(m.get("access-restricted-item", "")).lower() == "true"
            or "inlibrary" in colls_str
            or "printdisabled" in colls_str
        ) else 0
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO metadata_cache
                   (identifier, title, creator, date, mediatype, collection,
                    restricted, files_count, item_size, fetched_at, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    base,
                    str(m.get("title") or base),
                    str(creator),
                    str(m.get("date") or m.get("publicdate") or ""),
                    str(m.get("mediatype") or "unknown"),
                    colls_str,
                    restricted,
                    len(files),
                    int(meta.get("item_size") or 0),
                    now_iso,
                    json.dumps(meta, default=str),
                ),
            )

    def metadata_entries(self, limit: int = 100) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT identifier, title, creator, date, mediatype, collection,
                          restricted, files_count, item_size, fetched_at
                   FROM metadata_cache ORDER BY fetched_at DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # -- artifact & borrowed-item registry ---------------------------------

    def record_artifact(
        self,
        identifier: str,
        kind: str,
        file_name: str,
        fmt: str = "",
        size_bytes: int = 0,
        sha256: str = "",
        source_url: str = "",
        local_path: str = "",
        loan_status: str = "open",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Register any downloaded file, IIIF image, FTS-mined restricted bundle, or borrowed text."""
        base = identifier.split("|")[0]
        art_id = f"{base}::{kind}::{file_name}"
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        rec = {
            "id": art_id,
            "identifier": base,
            "kind": kind,
            "file_name": file_name,
            "format": fmt,
            "size_bytes": int(size_bytes or 0),
            "sha256": sha256,
            "source_url": source_url,
            "local_path": local_path,
            "loan_status": loan_status,
            "fetched_at": now_iso,
            "metadata_json": json.dumps(metadata or {}, default=str),
        }
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO artifacts
                   (id, identifier, kind, file_name, format, size_bytes, sha256,
                    source_url, local_path, loan_status, fetched_at, metadata_json)
                   VALUES (:id, :identifier, :kind, :file_name, :format, :size_bytes,
                           :sha256, :source_url, :local_path, :loan_status, :fetched_at, :metadata_json)""",
                rec,
            )
        return rec

    def artifacts(self, kind: Optional[str] = None, identifier: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        clauses = []
        params: List[Any] = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if identifier:
            clauses.append("identifier = ?")
            params.append(identifier.split("|")[0])
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"SELECT * FROM artifacts {where_sql} ORDER BY fetched_at DESC LIMIT ?",
                (*params, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_artifact(self, artifact_id_or_identifier: str, delete_files: bool = True) -> int:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT id, local_path FROM artifacts WHERE id = ? OR identifier = ?",
                (artifact_id_or_identifier, artifact_id_or_identifier),
            ).fetchall()
            if delete_files:
                for _aid, lp in rows:
                    if lp:
                        p = Path(lp)
                        if p.is_file():
                            try:
                                p.unlink()
                            except OSError:
                                pass
            cur = conn.execute(
                "DELETE FROM artifacts WHERE id = ? OR identifier = ?",
                (artifact_id_or_identifier, artifact_id_or_identifier),
            )
            return cur.rowcount

    def cache_summary(self) -> Dict[str, Any]:
        with sqlite3.connect(self.db_path) as conn:
            t_row = conn.execute("SELECT COUNT(*), COALESCE(SUM(chars), 0) FROM texts").fetchone()
            m_row = conn.execute("SELECT COUNT(*), COALESCE(SUM( restricted ), 0) FROM metadata_cache").fetchone()
            a_row = conn.execute("SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM artifacts").fetchone()
            by_kind = conn.execute("SELECT kind, COUNT(*), COALESCE(SUM(size_bytes), 0) FROM artifacts GROUP BY kind").fetchall()
        return {
            "root": str(self.root),
            "texts_cached": t_row[0],
            "texts_total_chars": t_row[1],
            "metadata_cached": m_row[0],
            "restricted_items_tracked": m_row[1],
            "artifacts_cached": a_row[0],
            "artifacts_total_bytes": a_row[1],
            "artifacts_by_kind": [
                {"kind": r[0], "count": r[1], "bytes": r[2]} for r in by_kind
            ],
        }
