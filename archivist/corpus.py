"""Columnar corpus database for fast local analysis.

The insight this module is built on: the expensive derivations are *pure
functions of the text* — tokenisation, shingling, fingerprinting — yet they
were being recomputed in Python on every run. Materialise them once and the
research questions become indexed joins:

* "which documents share phrasing with this one?"  -> join on fingerprint
* "what is the longest verbatim passage they share?" -> self-join on shingle
  hash grouped by the *diagonal* (pos_a - pos_b)
* "what does one copy have that the other lacks?"   -> anti-join plus a
  gap-and-islands window function
* "where does this phrase appear?"                   -> positional join on tokens

Layout (DuckDB, columnar, vectorised):

    documents       one row per work copy (metadata, counts, quality)
    document_text   the text itself (for snippets and the BM25 index)
    tokens          (identifier, pos, token)  -> phrase search, co-occurrence
    shingles        (identifier, pos, hash)   -> reuse and diff, the workhorse
    fingerprints    (identifier, fp)          -> instant candidate ranking

Shingles are ~1 row per token; a 200k-token document is ~200k rows, which
DuckDB scans in milliseconds. That is the trade: disk for CPU.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, TYPE_CHECKING

import duckdb

if TYPE_CHECKING:  # avoid an import cycle at runtime
    from .textstore import TextStore

from .textutil import ocr_quality, tokenize

logger = logging.getLogger("archivist.corpus")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    identifier   VARCHAR PRIMARY KEY,
    title        VARCHAR,
    collection   VARCHAR,
    date         VARCHAR,
    format       VARCHAR,
    chars        BIGINT,
    tokens       BIGINT,
    pages        BIGINT,
    ocr_quality  DOUBLE,
    sha256       VARCHAR,
    source_url   VARCHAR,
    ingested_at  TIMESTAMP
);
CREATE TABLE IF NOT EXISTS document_text (
    identifier VARCHAR PRIMARY KEY,
    text       VARCHAR
);
CREATE TABLE IF NOT EXISTS tokens (
    identifier VARCHAR,
    pos        BIGINT,
    token      VARCHAR
);
CREATE TABLE IF NOT EXISTS shingles (
    identifier VARCHAR,
    pos        BIGINT,
    hash       UBIGINT
);
CREATE TABLE IF NOT EXISTS fingerprints (
    identifier VARCHAR,
    fp         UBIGINT
);
"""

_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_tokens_token ON tokens(token);
CREATE INDEX IF NOT EXISTS idx_tokens_ident ON tokens(identifier);
CREATE INDEX IF NOT EXISTS idx_shingles_hash ON shingles(hash);
CREATE INDEX IF NOT EXISTS idx_shingles_ident ON shingles(identifier);
CREATE INDEX IF NOT EXISTS idx_fp_fp ON fingerprints(fp);
CREATE INDEX IF NOT EXISTS idx_fp_ident ON fingerprints(identifier);
"""

_TITLE_RE = re.compile(r"[a-z0-9]+")


def _as_signed(values: Iterable[int]) -> List[int]:
    """Map unsigned 64-bit hashes into signed 64-bit.

    Shingle and fingerprint hashes are unsigned; DuckDB infers INT128 from
    Python ints and refuses to cast values above 2^63-1 into BIGINT. The
    mapping is a bijection, so equality and joins behave identically.
    """
    return [v - (1 << 64) if v >= (1 << 63) else v for v in values]


class CorpusDB:
    """DuckDB-backed corpus of harvested documents, for local analysis."""

    def __init__(self, path: Path | str = "./data/corpus.duckdb", read_only: bool = False,
                 n: int = 8, window: int = 8):
        self.path = Path(path)
        self.n = n
        self.window = window
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.path), read_only=read_only)
        # Analysis is single-user and local; give DuckDB the machine.
        try:
            self.con.execute("SET threads TO 8")
            self.con.execute("SET memory_limit='4GB'")
        except duckdb.Error:  # pragma: no cover - depends on host
            pass
        if not read_only:
            self.con.execute(_SCHEMA)

    # -- ingest ------------------------------------------------------------

    def has(self, identifier: str) -> bool:
        row = self.con.execute(
            "SELECT 1 FROM documents WHERE identifier = ?", [identifier]
        ).fetchone()
        return row is not None

    def add_document(
        self,
        identifier: str,
        text: str,
        title: str = "",
        collection: str = "",
        date: str = "",
        fmt: str = "",
        source_url: str = "",
        sha256: str = "",
        pages: Optional[int] = None,
        n: int = 8,
        window: int = 8,
        replace: bool = True,
    ) -> Dict[str, Any]:
        """Derive and store tokens, shingles and fingerprints for one document.

        All derivation happens **inside DuckDB**, on the vectorised engine, from
        the stored text. Doing it in Python and shipping the results in as
        parameter lists cost 229s for five documents; the SQL path is a small
        number of set operations and finished in a few seconds. The rule: send
        the text once, let the database compute the rest.

        Fingerprints are the minimum shingle hash per band of ``window``
        shingles — the non-overlapping variant of winnowing. Any passage shared
        for at least ``n * window`` tokens is guaranteed to contribute a common
        fingerprint.
        """
        if replace:
            self.remove_document(identifier)

        quality = ocr_quality(text)
        self.con.execute("INSERT INTO document_text VALUES (?,?)", [identifier, text])

        # Tokenise in SQL. Two unnests in one SELECT zip together.
        self.con.execute(
            """
            INSERT INTO tokens
            SELECT ?, unnest(range(1, len(toks) + 1)) - 1, unnest(toks)
            FROM (SELECT regexp_extract_all(lower(?), '[a-z0-9\u00c0-\u024f]+') AS toks)
            """,
            [identifier, text],
        )
        token_count = self.con.execute(
            "SELECT COUNT(*) FROM tokens WHERE identifier = ?", [identifier]
        ).fetchone()[0]

        # Shingles: hash the sliding window of n tokens, read from the tokens
        # table rather than re-parsing the text. Building this with a lateral
        # generate_series over `list_slice(regexp_extract_all(...))` looked
        # natural and was pathological — the regex was re-evaluated per row and
        # a single document did not finish in minutes. A windowed list
        # aggregate costs ~0.08s for 200k tokens.
        self.con.execute(
            f"""
            INSERT INTO shingles
            SELECT identifier, pos, hash(win) FROM (
                SELECT identifier, pos,
                       list(token) OVER w AS win,
                       COUNT(*) OVER w AS wc
                FROM tokens WHERE identifier = ?
                WINDOW w AS (PARTITION BY identifier ORDER BY pos
                             ROWS BETWEEN CURRENT ROW AND {n - 1} FOLLOWING)
            ) WHERE wc = {n}
            """,
            [identifier],
        )
        shingle_count = self.con.execute(
            "SELECT COUNT(*) FROM shingles WHERE identifier = ?", [identifier]
        ).fetchone()[0]

        # Fingerprints: one minimum hash per band.
        self.con.execute(
            f"""
            INSERT INTO fingerprints
            SELECT identifier, MIN(hash) FROM shingles WHERE identifier = ?
            GROUP BY identifier, pos // {window}
            """,
            [identifier],
        )
        fp_count = self.con.execute(
            "SELECT COUNT(*) FROM fingerprints WHERE identifier = ?", [identifier]
        ).fetchone()[0]

        self.con.execute(
            """INSERT INTO documents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [identifier, title, collection, date, fmt, len(text), token_count, pages,
             quality["score"], sha256, source_url, datetime.now(timezone.utc)],
        )

        return {
            "identifier": identifier, "chars": len(text), "tokens": token_count,
            "shingles": shingle_count, "fingerprints": fp_count,
            "ocr_quality": quality["score"],
        }

    def remove_document(self, identifier: str) -> None:
        for table in ("documents", "document_text", "tokens", "shingles", "fingerprints"):
            self.con.execute(f"DELETE FROM {table} WHERE identifier = ?", [identifier])

    def build_from_store(self, store: TextStore, metadata: Optional[Dict[str, Dict[str, Any]]] = None,
                         n: int = 8, window: int = 8, replace: bool = True,
                         indexes: bool = False) -> Dict[str, Any]:
        """Materialise every document held in a :class:`TextStore`.

        ``indexes`` defaults to False: ART indexes cost ~22 MB per document
        (about 80% of the database) and the analytical queries here are scans
        and joins, which DuckDB's columnar layout and zone maps already handle.
        Measure before enabling them.
        """
        metadata = metadata or {}
        built = []
        for entry in store.entries():
            ident = entry["identifier"]
            if self.has(ident) and not replace:
                continue
            text = store.get(ident)
            if not text:
                continue
            meta = metadata.get(ident, {})
            built.append(self.add_document(
                ident, text,
                title=meta.get("title", ""),
                collection=meta.get("collection", ""),
                date=str(meta.get("date", "")),
                fmt=entry.get("format", ""),
                source_url=entry.get("source_url", ""),
                sha256=entry.get("sha256", ""),
                n=n, window=window, replace=replace,
            ))
        if indexes:
            self.create_indexes()
        return {"documents": len(built), "detail": built, "stats": self.stats()}

    def create_indexes(self) -> None:
        self.con.execute(_INDEXES)

    # -- stats -------------------------------------------------------------

    def stats(self) -> Dict[str, Any]:
        row = self.con.execute(
            """SELECT (SELECT COUNT(*) FROM documents),
                      (SELECT COALESCE(SUM(tokens),0) FROM documents),
                      (SELECT COUNT(*) FROM shingles),
                      (SELECT COUNT(*) FROM fingerprints)"""
        ).fetchone()
        return {
            "path": str(self.path),
            "documents": row[0], "tokens": row[1],
            "shingles": row[2], "fingerprints": row[3],
        }

    # -- queries -----------------------------------------------------------

    def text(self, identifier: str) -> Optional[str]:
        row = self.con.execute(
            "SELECT text FROM document_text WHERE identifier = ?", [identifier]
        ).fetchone()
        return row[0] if row else None

    def _register_text(self, text: str, name: str) -> None:
        """Materialise an ad-hoc text as a temporary document.

        Lets a pasted passage be searched against the corpus with exactly the
        same SQL path as a stored document — no separate Python similarity code.
        """
        self.remove_document(name)
        self.con.execute("INSERT INTO document_text VALUES (?,?)", [name, text])
        self.con.execute(
            """
            INSERT INTO tokens
            SELECT ?, unnest(range(1, len(toks) + 1)) - 1, unnest(toks)
            FROM (SELECT regexp_extract_all(lower(?), '[a-z0-9\u00c0-\u024f]+') AS toks)
            """,
            [name, text],
        )
        self.con.execute(
            f"""
            INSERT INTO shingles
            SELECT identifier, pos, hash(win) FROM (
                SELECT identifier, pos,
                       list(token) OVER w AS win,
                       COUNT(*) OVER w AS wc
                FROM tokens WHERE identifier = ?
                WINDOW w AS (PARTITION BY identifier ORDER BY pos
                             ROWS BETWEEN CURRENT ROW AND {self.n - 1} FOLLOWING)
            ) WHERE wc = {self.n}
            """,
            [name],
        )
        self.con.execute(
            f"""
            INSERT INTO fingerprints
            SELECT identifier, MIN(hash) FROM shingles WHERE identifier = ?
            GROUP BY identifier, pos // {self.window}
            """,
            [name],
        )

    def reuse_of_text(
        self,
        text: str,
        min_tokens: int = 20,
        candidates: int = 8,
        runs_per_candidate: int = 3,
    ) -> List[Dict[str, Any]]:
        """Stored documents sharing verbatim runs with an ad-hoc passage."""
        query_id = "__query__"
        self._register_text(text, query_id)
        try:
            rows = self.con.execute(
                """
                WITH src AS (SELECT DISTINCT fp FROM fingerprints WHERE identifier = ?)
                SELECT f.identifier
                FROM fingerprints f JOIN src USING (fp)
                WHERE f.identifier <> ?
                GROUP BY 1 ORDER BY COUNT(DISTINCT f.fp) DESC LIMIT ?
                """,
                [query_id, query_id, candidates],
            ).fetchall()

            results = []
            for (ident,) in rows:
                runs = self.shared_runs(query_id, ident, min_tokens=min_tokens,
                                        limit=runs_per_candidate)
                if runs:
                    results.append({
                        "identifier": ident,
                        "longest_run_tokens": max(r["length_tokens"] for r in runs),
                        "runs": runs,
                    })
            results.sort(key=lambda r: -r["longest_run_tokens"])
            return results
        finally:
            self.remove_document(query_id)

    def similar(self, identifier: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Rank documents by fingerprint containment against one document."""
        rows = self.con.execute(
            """
            WITH src AS (SELECT DISTINCT fp FROM fingerprints WHERE identifier = ?)
            SELECT f.identifier, COUNT(DISTINCT f.fp) AS shared_fingerprints
            FROM fingerprints f JOIN src USING (fp)
            WHERE f.identifier <> ?
            GROUP BY 1 ORDER BY shared_fingerprints DESC LIMIT ?
            """,
            [identifier, identifier, limit],
        ).fetchall()
        total = self.con.execute(
            "SELECT COUNT(DISTINCT fp) FROM fingerprints WHERE identifier = ?", [identifier]
        ).fetchone()[0] or 1
        return [
            {"identifier": r[0], "shared_fingerprints": r[1],
             "containment": round(r[1] / total, 4)}
            for r in rows
        ]

    def shared_runs(self, a: str, b: str, min_tokens: int = 20, limit: int = 10) -> List[Dict[str, Any]]:
        """Exact maximal verbatim runs shared by two documents.

        Matching shingles are paired and tagged with the diagonal
        ``pos_a - pos_b``. Along one diagonal, a run of consecutive ``pos_a``
        values is a contiguous shared passage. Grouping by the diagonal alone is
        NOT enough: two near-identical documents place almost every shingle on
        one diagonal, so the group spans the whole document and any single gap
        breaks the contiguity test — the first version of this query silently
        returned nothing for copies that share 99% of their text. A
        gap-and-islands pass *within* each diagonal is what yields the runs.
        """
        rows = self.con.execute(
            """
            WITH j AS (
                SELECT a.pos AS pa, b.pos AS pb, (a.pos - b.pos) AS diag
                FROM shingles a JOIN shingles b ON a.hash = b.hash
                WHERE a.identifier = ? AND b.identifier = ?
            ), g AS (
                SELECT diag, pa, pb,
                       pa - ROW_NUMBER() OVER (PARTITION BY diag ORDER BY pa) AS island
                FROM j
            )
            SELECT MIN(pa) AS start_a, MIN(pb) AS start_b, COUNT(*) AS len
            FROM g GROUP BY diag, island HAVING COUNT(*) + {n_minus_1} >= ?
            ORDER BY len DESC LIMIT ?
            """.format(n_minus_1=self.n - 1),
            [a, b, min_tokens, limit],
        ).fetchall()
        return [
            {"start_a": r[0], "start_b": r[1],
             # COUNT is a number of shingles; the passage is that many tokens
             # plus the n-1 tokens the first shingle spans.
             "length_tokens": r[2] + self.n - 1,
             "snippet": self.tokens_between(a, r[0], r[0] + r[2] + self.n - 1, max_tokens=40)}
            for r in rows
        ]

    def diff_runs(self, base: str, other: str, min_run: int = 12, limit: int = 10) -> List[Dict[str, Any]]:
        """Regions of ``base`` with no counterpart in ``other``.

        Anti-join to find unmatched shingles, then gap-and-islands
        (``pos - ROW_NUMBER()``) to group consecutive positions into runs.
        """
        rows = self.con.execute(
            """
            WITH miss AS (
                SELECT pos FROM shingles
                WHERE identifier = ?
                  AND hash NOT IN (SELECT hash FROM shingles WHERE identifier = ?)
            ), grp AS (
                SELECT pos, pos - ROW_NUMBER() OVER (ORDER BY pos) AS island FROM miss
            )
            SELECT MIN(pos) AS start_pos, COUNT(*) AS len
            FROM grp GROUP BY island HAVING COUNT(*) + {n_minus_1} >= ?
            ORDER BY len DESC LIMIT ?
            """.format(n_minus_1=self.n - 1),
            [base, other, min_run, limit],
        ).fetchall()
        return [
            {"start_token": r[0], "length_tokens": r[1] + self.n - 1,
             "snippet": self.tokens_between(base, r[0], r[0] + r[1] + self.n, max_tokens=40)}
            for r in rows
        ]

    def tokens_between(self, identifier: str, start: int, end: int, max_tokens: int = 40) -> str:
        row = self.con.execute(
            """SELECT string_agg(token, ' ' ORDER BY pos) FROM tokens
               WHERE identifier = ? AND pos >= ? AND pos < ?""",
            [identifier, start, min(end, start + max_tokens)],
        ).fetchone()
        return row[0] or ""

    def phrase_search(self, phrase: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Documents containing an exact consecutive token sequence.

        A positional chain join on the tokens table: exact, and unlike a
        substring match it cannot cross a token boundary.
        """
        parts = tokenize(phrase)
        if not parts:
            return []
        if len(parts) == 1:
            rows = self.con.execute(
                "SELECT identifier, COUNT(*) FROM tokens WHERE token = ? GROUP BY 1 LIMIT ?",
                [parts[0], limit],
            ).fetchall()
            return [{"identifier": r[0], "occurrences": r[1]} for r in rows]

        sql = "SELECT t0.identifier, COUNT(*) FROM tokens t0"
        params: List[Any] = []
        for i in range(1, len(parts)):
            sql += (f" JOIN tokens t{i} ON t{i}.identifier = t0.identifier "
                    f"AND t{i}.pos = t{i-1}.pos + 1 AND t{i}.token = ?")
            params.append(parts[i])
        sql += " WHERE t0.token = ? GROUP BY 1 ORDER BY 2 DESC LIMIT ?"
        params.extend([parts[0], limit])
        rows = self.con.execute(sql, params).fetchall()
        return [{"identifier": r[0], "occurrences": r[1]} for r in rows]

    def create_fts(self) -> bool:
        """Build a BM25 index over the stored text, if the extension is present."""
        ext_dir = Path(__file__).resolve().parent.parent / ".duckdb" / "extensions"
        try:
            if ext_dir.is_dir():
                self.con.execute(f"SET extension_directory = '{ext_dir}'")
            try:
                self.con.execute("LOAD fts")
            except duckdb.Error:
                self.con.execute("INSTALL fts")
                self.con.execute("LOAD fts")
            self.con.execute(
                "PRAGMA create_fts_index('document_text', 'identifier', 'text', overwrite=1)"
            )
            return True
        except duckdb.Error as err:
            logger.warning(f"FTS index unavailable: {err}")
            return False

    def bm25(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        rows = self.con.execute(
            """SELECT identifier, score FROM (
                   SELECT *, fts_main_document_text.match_bm25(identifier, ?) AS score
                   FROM document_text
               ) WHERE score IS NOT NULL ORDER BY score DESC LIMIT ?""",
            [query, limit],
        ).fetchall()
        return [{"identifier": r[0], "score": r[1]} for r in rows]

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> List[tuple]:
        """Escape hatch for ad-hoc analysis."""
        return self.con.execute(query, list(params or [])).fetchall()

    def close(self) -> None:
        self.con.close()


# --- difference classification ---------------------------------------------
#
# "Redaction, truncation and OCR failure cannot be distinguished" was wrong.
# Each leaves a different structural signature, and the corpus holds everything
# needed to separate them:
#
#   truncation   the shorter copy simply stops: the difference reaches its end
#   excision     the other copy has ~no tokens where the base has coherent ones
#   OCR failure  the other copy has *comparable* tokens in that span, but noisy
#   revision     both sides have coherent text and they are different
#
# The signal that separates them is what the OTHER copy contains in the aligned
# region. Alignment comes from the last matching shingle before the run, which
# gives the diagonal between the two documents.

def _region_stats(self, identifier: str, start: Optional[int], end: Optional[int]) -> Dict[str, Any]:
    if start is None or end is None or end <= start:
        return {"tokens": 0, "short_ratio": None}
    row = self.con.execute(
        """SELECT COUNT(*),
                  AVG(CASE WHEN length(token) <= 2 THEN 1.0 ELSE 0.0 END)
           FROM tokens WHERE identifier = ? AND pos >= ? AND pos < ?""",
        [identifier, start, end],
    ).fetchone()
    return {"tokens": row[0] or 0, "short_ratio": row[1]}


def _anchor_diagonal(self, base: str, other: str, before: int) -> Optional[int]:
    """base_pos - other_pos for the closest matching shingle before a position."""
    row = self.con.execute(
        """SELECT a.pos - b.pos
           FROM shingles a JOIN shingles b ON a.hash = b.hash
           WHERE a.identifier = ? AND b.identifier = ? AND a.pos < ?
           ORDER BY a.pos DESC LIMIT 1""",
        [base, other, before],
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _noise(self, identifier: str, start: int, end: int) -> float:
    row = self.con.execute(
        """SELECT AVG(CASE WHEN length(token) <= 2 THEN 1.0 ELSE 0.0 END)
           FROM tokens WHERE identifier = ? AND pos >= ? AND pos < ?""",
        [identifier, start, end],
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else 0.0


def _modal_diagonal(self, base: str, other: str, lo: int, hi: int) -> Optional[int]:
    """Most common base_pos - other_pos among matches in a window.

    The *nearest* match is not the alignment: in a repetitive document it is a
    spurious repeat. The modal diagonal over a window is the real one.
    """
    if hi <= lo:
        return None
    row = self.con.execute(
        """SELECT a.pos - b.pos AS diag, COUNT(*) AS c
           FROM shingles a JOIN shingles b ON a.hash = b.hash
           WHERE a.identifier = ? AND b.identifier = ? AND a.pos >= ? AND a.pos < ?
           GROUP BY 1 ORDER BY c DESC LIMIT 1""",
        [base, other, lo, hi],
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def classify_differences(
    self,
    base: str,
    other: str,
    min_run: int = 25,
    max_runs: int = 10,
    window: int = 600,
) -> List[Dict[str, Any]]:
    """Label each region of ``base`` missing from ``other``, with its evidence.

    The discriminator is structural, not textual. Alignment before the run and
    after it is measured from the modal diagonal in a window on each side:

        no alignment after the run          -> truncation (the copy stops)
        diagonal shifts by ~the run length  -> a block was removed or added
        diagonal unchanged, text differs    -> OCR divergence or a revision

    A deletion does not leave an empty hole: the other copy still has text
    there, just different text. Counting tokens in the gap therefore
    misclassifies it as a revision, which is what the first version did.
    """
    runs = self.diff_runs(base, other, min_run=min_run, limit=max_runs)
    base_len = self.con.execute(
        "SELECT COUNT(*) FROM tokens WHERE identifier = ?", [base]).fetchone()[0] or 0
    other_len = self.con.execute(
        "SELECT COUNT(*) FROM tokens WHERE identifier = ?", [other]).fetchone()[0] or 0

    out = []
    for r in runs:
        p, length = r["start_token"], r["length_tokens"]
        d_before = _modal_diagonal(self, base, other, max(0, p - window), p)
        d_after = _modal_diagonal(self, base, other, p + length, p + length + window)

        base_noise = _noise(self, base, p, p + length)
        tolerance = max(5, int(length * 0.35))
        # Where this region would sit in the other copy, by the alignment that
        # holds before it. Repetitive text yields spurious matches past the end,
        # so "no match found afterwards" is not sufficient evidence on its own —
        # running past the other document's end is.
        aligned_here = (p - d_before) if d_before is not None else None
        past_end_of_other = aligned_here is not None and aligned_here >= other_len - 5

        if past_end_of_other or d_after is None or (p + length) >= base_len - 3:
            label = "truncation"
            why = (f"the aligned position ({aligned_here}) is at or past the end of the "
                   f"other copy ({other_len} tokens): this is where that text stops")
        elif d_before is not None and abs((d_after - d_before) - length) <= tolerance:
            shift = d_after - d_before
            label = "excision_clean" if base_noise < 0.28 else "excision_noisy"
            why = (f"alignment shifts by {shift} tokens across a {length}-token gap, "
                   f"matching the gap length: a block present here is absent there")
        elif d_before is not None and abs(d_after - d_before) <= tolerance:
            if base_noise >= 0.28:
                label = "ocr_divergence"
                why = (f"alignment unchanged across the span; text differs and is noisy "
                       f"(short-token ratio {base_noise:.2f})")
            else:
                label = "revision"
                why = "alignment unchanged; both copies carry coherent but different text"
        else:
            label = "unclassified"
            why = f"alignment shift {None if d_before is None else d_after - d_before} unrelated to run length {length}"

        out.append({
            **r, "label": label, "evidence": why,
            "diagonal_before": d_before, "diagonal_after": d_after,
            "aligned_position": aligned_here, "other_length": other_len,
            "base_noise": round(base_noise, 3),
        })
    return out


CorpusDB.classify_differences = classify_differences
CorpusDB._anchor_diagonal = _anchor_diagonal
