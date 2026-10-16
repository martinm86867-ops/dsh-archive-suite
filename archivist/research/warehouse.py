"""DuckDB analytical research warehouse for sub-millisecond LLM retrieval and temporal indexing."""

from __future__ import annotations
import hashlib
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import duckdb

logger = logging.getLogger("archivist.research.warehouse")


class DuckDBWarehouse:
    """High-performance analytical research warehouse powered by DuckDB vectorized columnar engine."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._con: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            self._con = duckdb.connect(str(self.db_path))
            self._init_schema()
        return self._con

    def close(self) -> None:
        if self._con:
            self._con.close()
            self._con = None

    def __enter__(self) -> "DuckDBWarehouse":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _init_schema(self) -> None:
        con = self._con
        assert con is not None
        con.execute("""
            CREATE TABLE IF NOT EXISTS research_records (
                id VARCHAR PRIMARY KEY,
                source_type VARCHAR NOT NULL,
                source_key VARCHAR NOT NULL,
                timestamp VARCHAR,
                title VARCHAR,
                content VARCHAR,
                token_count INTEGER,
                digest VARCHAR,
                archive_url VARCHAR,
                metadata_json VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Ensure FTS extension is loaded from bundled/local directory first
        ext_dir = Path(__file__).resolve().parent.parent.parent / ".duckdb" / "extensions"
        try:
            if ext_dir.is_dir():
                con.execute(f"SET extension_directory = '{ext_dir}';")
            con.execute("LOAD fts;")
        except Exception:
            try:
                con.execute("INSTALL fts; LOAD fts;")
            except Exception as e:
                logger.warning(f"Could not load FTS extension: {e}")

    def rebuild_fts_index(self) -> None:
        """Create or refresh BM25 full-text search index."""
        con = self.connect()
        try:
            con.execute("PRAGMA drop_fts_index('research_records');")
        except Exception:
            pass

        con.execute("""
            PRAGMA create_fts_index(
                'research_records',
                'id',
                'title',
                'content',
                overwrite=1
            );
        """)

    def insert_record(
        self,
        record_id: str,
        source_type: str,
        source_key: str,
        content: str,
        title: Optional[str] = None,
        timestamp: Optional[str] = None,
        archive_url: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        con = self.connect()
        # Estimate token count (~4 characters per token heuristic)
        token_count = len(content) // 4
        digest = hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()
        meta_str = json.dumps(metadata or {})

        con.execute("""
            INSERT OR REPLACE INTO research_records
            (id, source_type, source_key, timestamp, title, content, token_count, digest, archive_url, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record_id, source_type, source_key, timestamp, title or source_key,
            content, token_count, digest, archive_url, meta_str
        ))

    def ingest_sqlite_snapshots(self, sqlite_db_path: Path, table: Optional[str] = None) -> int:
        """Zero-copy ingestion from SQLite snapshot tables."""
        con = self.connect()
        import sqlite3
        src_conn = sqlite3.connect(str(sqlite_db_path))
        if not table:
            tbl_row = src_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence' LIMIT 1").fetchone()
            if not tbl_row:
                src_conn.close()
                return 0
            table = tbl_row[0]
        else:
            valid_row = src_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone()
            if not valid_row:
                src_conn.close()
                raise ValueError(f"Table '{table}' does not exist in {sqlite_db_path}")

        quoted_table = '"' + str(table).replace('"', '""') + '"'
        rows = src_conn.execute(f"SELECT key, data_json, extracted_text FROM {quoted_table}").fetchall()
        src_conn.close()

        count = 0
        for key, data_str, text in rows:
            if not text:
                continue
            data = json.loads(data_str)
            orig = data.get("original", key)
            ts = data.get("timestamp", "")
            archive_url = f"https://web.archive.org/web/{ts}/{orig}" if ts else None
            self.insert_record(
                record_id=f"snap_{key}",
                source_type="wayback_snapshot",
                source_key=orig,
                content=text,
                title=f"Snapshot of {orig} ({ts})",
                timestamp=ts,
                archive_url=archive_url,
                metadata=data
            )
            count += 1

        self.rebuild_fts_index()
        return count

    def ingest_sqlite_catalog(self, sqlite_db_path: Path, table: Optional[str] = None) -> int:
        """Ingest harvested catalog items."""
        con = self.connect()
        import sqlite3
        src_conn = sqlite3.connect(str(sqlite_db_path))
        if not table:
            tbl_row = src_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence' LIMIT 1").fetchone()
            if not tbl_row:
                src_conn.close()
                return 0
            table = tbl_row[0]
        else:
            valid_row = src_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone()
            if not valid_row:
                src_conn.close()
                raise ValueError(f"Table '{table}' does not exist in {sqlite_db_path}")

        quoted_table = '"' + str(table).replace('"', '""') + '"'
        rows = src_conn.execute(f"SELECT key, data_json FROM {quoted_table}").fetchall()
        src_conn.close()

        count = 0
        for key, data_str in rows:
            data = json.loads(data_str)
            title = data.get("title", key)
            desc = data.get("description", "")
            ident = data.get("identifier", key)
            text_body = f"Title: {title}\nIdentifier: {ident}\nMediatype: {data.get('mediatype')}\nCollection: {data.get('collection')}\nDate: {data.get('date')}\nDescription: {desc}"
            archive_url = f"https://archive.org/details/{ident}"

            self.insert_record(
                record_id=f"item_{ident}",
                source_type="catalog_item",
                source_key=ident,
                content=text_body,
                title=title,
                timestamp=str(data.get("date", "")).replace("-", ""),
                archive_url=archive_url,
                metadata=data
            )
            count += 1

        self.rebuild_fts_index()
        return count

    def search_bm25(
        self,
        query: str,
        limit: int = 10,
        source_type: Optional[str] = None,
        from_ts: Optional[str] = None,
        to_ts: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Sub-millisecond BM25 full-text search with metadata filters."""
        con = self.connect()

        # Sanitize query for FTS
        clean_query = query.replace("'", " ").replace('"', ' ').strip()
        if not clean_query:
            return []

        where_clauses = ["score IS NOT NULL"]
        params: List[Any] = []

        if source_type:
            where_clauses.append("source_type = ?")
            params.append(source_type)
        if from_ts:
            where_clauses.append("timestamp >= ?")
            params.append(from_ts)
        if to_ts:
            where_clauses.append("timestamp <= ?")
            params.append(to_ts)

        where_stmt = " AND ".join(where_clauses)

        sql = f"""
            SELECT id, source_type, source_key, timestamp, title, content, token_count, digest, archive_url, score
            FROM (
                SELECT *, fts_main_research_records.match_bm25(id, '{clean_query}') AS score
                FROM research_records
            )
            WHERE {where_stmt}
            ORDER BY score DESC
            LIMIT {limit}
        """

        try:
            cursor = con.execute(sql, params)
            cols = [desc[0] for desc in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"BM25 Search failed: {e}")
            return []

    def get_timeline(self, source_key: str) -> List[Dict[str, Any]]:
        """Extract temporal timeline of captures for a given URL or entity."""
        con = self.connect()
        sql = """
            SELECT id, timestamp, length(content) as content_length, token_count, digest, archive_url
            FROM research_records
            WHERE source_key = ? OR source_key LIKE ?
            ORDER BY timestamp ASC
        """
        cursor = con.execute(sql, (source_key, f"%{source_key}%"))
        cols = [desc[0] for desc in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def get_stats(self) -> Dict[str, Any]:
        """Aggregate warehouse volume, token counts, and source distributions."""
        con = self.connect()
        total_records = con.execute("SELECT COUNT(*) FROM research_records").fetchone()[0]
        total_tokens = con.execute("SELECT COALESCE(SUM(token_count), 0) FROM research_records").fetchone()[0]
        sources = con.execute("SELECT source_type, COUNT(*) FROM research_records GROUP BY source_type").fetchall()
        return {
            "total_records": total_records,
            "total_tokens": total_tokens,
            "sources": dict(sources)
        }
