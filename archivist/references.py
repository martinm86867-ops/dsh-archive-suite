"""Reference drift: what the archive recorded versus what the web says now.

A citation asserts what a source said. Two things can make that assertion
unverifiable today: the source is gone from the web, or it changed. The archive
answers both — it holds the copy that existed when the claim was made.

This module takes a page (typically a reference-bearing article), finds the
snapshot the archive holds of it, extracts the external references it cites, and
for each one reports:

* the live HTTP status now — ``alive`` (2xx/3xx), ``blocked`` (401/403/429),
  ``dead`` (DNS failure, timeout, 4xx/5xx)
* how many captures the archive holds, and the most recent

That yields the four regimes a citation can be in:

    live_and_archived   verifiable either way
    archive_only        the archive is the only surviving witness
    live_only           not archived; a future change would be unprovable
    neither             the citation cannot be checked at all

Content comparison is deliberately NOT done here: the archivist owns the
archived side, Jina owns the live side, and the caller arbitrates. Note that
Jina refuses to fetch web.archive.org, so the two sides cannot share a fetcher.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .client import (
    ArchiveClient,
    ArchiveError,
    classify_status,
    OUTCOME_OK,
    OUTCOME_DENIED,
    OUTCOME_RATE_LIMITED,
    OUTCOME_REFUSED,
)

logger = logging.getLogger("archivist.references")

_CITE_NOTE_RE = re.compile(r'<li id="cite_note-[^"]*">(.*?)</li>', re.S | re.I)
_HREF_RE = re.compile(r'href="(https?://[^"]+)"', re.I)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

# The references section, located by its heading id. Wikipedia has changed the
# markup around the citations more than once (the 2024 capture uses
# `<li id="cite_note-...">`, a later one does not), but the heading id has
# stayed. Anchoring on content rather than on citation markup is what keeps this
# working across revisions.
_REF_HEADING_RE = re.compile(r'id="(?:References|Notes_and_references|Citations)"', re.I)
_NEXT_SECTION_RE = re.compile(r'<h2\b', re.I)

# Project, licence and sister-project links are page furniture, not references.
_SELF_HOSTS = (
    "wikipedia.org", "wikimedia.org", "wikiquote.org", "wiktionary.org",
    "wikidata.org", "wikisource.org", "wikibooks.org", "wikinews.org",
    "wikiversity.org", "wikimediafoundation.org", "creativecommons.org",
    "archive.org", "web.archive.org",
)


def _host(url: str) -> str:
    return urlparse(url).netloc.lower()


def _is_furniture(url: str) -> bool:
    host = _host(url)
    return not host or any(h in host for h in _SELF_HOSTS)


# --- live-probe identity ----------------------------------------------------
#
# The live check is the one place a browser identity is required: hosts refuse
# scripted clients routinely (gwu.edu returned 403 to a plain request while
# serving the same page to a browser engine), and a refusal is then misread as
# the source being gone. Ordinary desktop strings, rotated per request so the
# probe does not present one fixed fingerprint.
#
# Deliberately NOT applied to archive.org: those endpoints answer anonymously,
# and identifying there would contradict the no-identification stance.
_UA_POOL = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/18.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0",
)
_UA_INDEX = itertools.count()


def _browser_headers() -> Dict[str, str]:
    """Plausible browser headers for a live reachability probe."""
    return {
        "User-Agent": _UA_POOL[next(_UA_INDEX) % len(_UA_POOL)],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Cache-Control": "no-cache",
    }


def _reference_region(html: str) -> str:
    """The slice of a page from its references heading to the next section."""
    m = _REF_HEADING_RE.search(html)
    if not m:
        return html
    nxt = _NEXT_SECTION_RE.search(html, m.end())
    return html[m.end(): nxt.start() if nxt else len(html)]


def _text(fragment: str) -> str:
    return _WS_RE.sub(" ", unescape(_TAG_RE.sub(" ", fragment))).strip()


def extract_references(html: str, limit: int = 12) -> List[Dict[str, str]]:
    """Pull the external sources a page cites.

    Scoped to the references section rather than the whole document: a full page
    yields hundreds of external links (navigation, sister projects, licence and
    donation links), and treating those as citations produces confident nonsense.
    Within that section, citation blocks are used when present because they carry
    the citation text, with a plain link sweep as the fallback.
    """
    region = _reference_region(html)
    refs: List[Dict[str, str]] = []
    seen: set = set()

    for block in _CITE_NOTE_RE.findall(region):
        m = _HREF_RE.search(block)
        if not m:
            continue
        url = unescape(m.group(1))
        if _is_furniture(url) or url in seen:
            continue
        seen.add(url)
        refs.append({"url": url, "host": _host(url), "citation": _text(block)[:220]})

    if not refs:                       # no citation markup: sweep the section
        for m in _HREF_RE.finditer(region):
            url = unescape(m.group(1))
            if _is_furniture(url) or url in seen:
                continue
            seen.add(url)
            refs.append({"url": url, "host": _host(url), "citation": ""})

    return refs[:limit]


class ReferenceCache:
    """Local record of reference checks, so repeat runs cost nothing.

    Every row carries ``checked_at``. A cached "alive" is a claim about *when it
    was checked*, not about now, so the timestamp is returned with the data —
    otherwise caching reintroduces the very error this module exists to avoid,
    dressing a stale observation as a current fact.
    """

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS reference_checks (
        url               TEXT PRIMARY KEY,
        host              TEXT,
        live_status       INTEGER,
        live_state        TEXT,
        error             TEXT,
        archived_captures INTEGER,
        archived_latest   TEXT,
        capture_source    TEXT,
        checked_at        TEXT
    );
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.executescript(self.SCHEMA)

    def get(self, url: str, ttl_hours: float = 24.0) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(self.path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM reference_checks WHERE url = ?", (url,)
            ).fetchone()
        if not row:
            return None
        record = dict(row)
        checked = record.get("checked_at")
        if not checked:
            return None
        try:
            age = datetime.now(timezone.utc) - datetime.fromisoformat(checked)
        except ValueError:
            return None
        if age > timedelta(hours=ttl_hours):
            return None
        record["from_cache"] = True
        return record

    def put(self, record: Dict[str, Any]) -> None:
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO reference_checks
                   (url, host, live_status, live_state, error, archived_captures,
                    archived_latest, capture_source, checked_at)
                   VALUES (:url, :host, :live_status, :live_state, :error,
                           :archived_captures, :archived_latest, :capture_source,
                           :checked_at)""",
                record,
            )

    def stats(self) -> Dict[str, Any]:
        with sqlite3.connect(self.path) as conn:
            n = conn.execute("SELECT COUNT(*) FROM reference_checks").fetchone()[0]
        return {"cached_references": n, "path": str(self.path)}


class ReferenceDrift:
    """Compare a page's references between the archive and the live web."""

    def __init__(self, client: ArchiveClient, cache: Optional["ReferenceCache"] = None):
        self.client = client
        self.cache = cache

    async def _latest_snapshot(self, url: str, timestamp: Optional[str] = None) -> Optional[Dict[str, str]]:
        """Most recent successful capture.

        Uses CDX rather than the ``/wayback/available`` endpoint: the latter is
        heavily throttled and returns 429 for even modest use, while the CDX
        server answers a long sequence of queries without complaint.
        ``limit=-1`` asks CDX for the last matching row.
        """
        try:
            rows = await self.client.cdx_search(
                url, match_type="exact", limit=-1, status_codes=[200],
                to_ts=timestamp, fl=["timestamp", "original"],
            )
        except ArchiveError as err:
            logger.debug(f"latest snapshot lookup failed for {url}: {err}")
            return None
        if not rows:
            return None
        last = rows[-1] if isinstance(rows[-1], dict) else {}
        stamp = last.get("timestamp")
        if not stamp:
            return None
        return {
            "timestamp": str(stamp),
            "url": f"https://web.archive.org/web/{stamp}/{url}",
            "status": "200",
        }

    async def _archive_coverage(self, url: str, probe: int = 60) -> Dict[str, Any]:
        """Exact capture count and latest capture for a URL.

        Uses the Memento timemap: one request returns every capture, so the
        count is exact. The CDX path it replaces was capped (and needed
        retries), which meant reporting "60+" as a floor rather than a number.
        CDX remains the fallback.
        """
        try:
            response = await self.client._request_with_retry(
                "GET", f"https://web.archive.org/web/timemap/link/{url}",
                limiter=self.client._wayback_limiter,
            )
            if response.status_code == 200:
                stamps = set(re.findall(r"/web/(\d{14})/", response.text))
                if stamps:
                    return {"captures": len(stamps), "latest": max(stamps),
                            "capped": False, "source": "timemap"}
        except ArchiveError as err:
            logger.debug(f"timemap failed for {url}: {err}")

        try:
            rows = await self.client.cdx_search(
                url, match_type="exact", limit=probe,
                collapse="digest", fl=["timestamp", "statuscode"],
            )
        except ArchiveError as err:
            logger.debug(f"cdx failed for {url}: {err}")
            return {"captures": None, "latest": None, "capped": False, "source": None}
        stamps = []
        for row in rows or []:
            stamp = row.get("timestamp") if isinstance(row, dict) else (
                row[0] if isinstance(row, (list, tuple)) and row else None)
            if stamp:
                stamps.append(str(stamp))
        return {
            "captures": len(stamps),
            "latest": max(stamps) if stamps else None,
            "capped": len(stamps) >= probe,
            "source": "cdx",
        }

    async def _live_status(self, url: str, timeout: float = 20.0) -> Dict[str, Any]:
        """Live HTTP status without downloading bodies.

        Presented as a browser: the point of the probe is the resource, not this
        client's permission to read it. HEAD first; servers that reject HEAD
        (or the first refusal) get a one-byte ranged GET.
        """
        session = await self.client.get_session()
        headers = _browser_headers()
        try:
            r = await session.head(url, timeout=timeout, headers=headers)
            if r.status_code >= 400:
                r = await session.get(url, timeout=timeout,
                                      headers={**headers, "Range": "bytes=0-0"})
            return {"status": r.status_code, "final_url": str(r.url)}
        except Exception as err:                      # DNS failure, refused, timeout
            return {"status": None, "error": type(err).__name__}

    @staticmethod
    def _live_state(status: Optional[int], error: Optional[str]) -> str:
        """Classify a live probe.

        A refusal is a statement about the *client*, not the resource. This
        check returns 403 for hosts that are perfectly reachable in a browser
        (verified: a 403 from gwu.edu while a browser-engine fetch returned the
        page in full), so a refusal must never be reported as absence. The
        distinction that matters:

            alive     2xx/3xx — reachable
            dead      DNS failure, refused connection, timeout, 404/410
            refused   401/403/429/503 — unknown without a browser probe
            unknown   anything else
        """
        if error:
            return "dead" if error in (
                "ConnectError", "ConnectTimeout", "ReadTimeout", "RemoteProtocolError"
            ) else "unknown"
        if status is None:
            return "unknown"
        if status in (404, 410):
            return "dead"
        outcome = classify_status(status)
        if outcome == OUTCOME_OK:
            return "alive"
        if outcome in (OUTCOME_DENIED, OUTCOME_RATE_LIMITED, OUTCOME_REFUSED) or status in (401, 403, 429, 451, 503):
            return "refused"
        return "unknown"

    @staticmethod
    def _regime(live_state: str, captures: Optional[int]) -> str:
        """Combine reachability with archive coverage.

        Only *dead* justifies ``archive_only``: claiming the archive is the sole
        witness requires evidence the source is gone, not merely that it refused
        this client.
        """
        archived = bool(captures)
        if live_state == "alive":
            return "live_and_archived" if archived else "live_only"
        if live_state == "dead":
            return "archive_only" if archived else "neither"
        return "unverified"          # refused or unknown: cannot be concluded

    async def drift(
        self,
        url: str,
        timestamp: Optional[str] = None,
        max_refs: int = 12,
        check_live: bool = True,
        cache_ttl_hours: float = 24.0,
        refresh: bool = False,
    ) -> Dict[str, Any]:
        """Report the live/archived status of every reference a page cites.

        With a cache configured, freshly checked references are served locally
        and every reference carries ``checked_at``, so a cached observation is
        never presented as a current one.
        """
        snapshot = await self._latest_snapshot(url, timestamp)
        if not snapshot:
            return {"url": url, "error": "no archived snapshot of this page"}

        try:
            raw = await self.client.get_wayback_snapshot(url, snapshot["timestamp"], raw=True)
        except ArchiveError as err:
            return {"url": url, "error": f"could not read snapshot: {err}"}

        html = raw.decode("utf-8", errors="ignore")
        refs = extract_references(html, limit=max_refs)
        if not refs:
            return {"url": url, "snapshot": snapshot, "references": [],
                    "note": "no external references found in the archived page"}

        results = []
        served_locally = 0

        for ref in refs:
            cached = None
            if self.cache is not None and not refresh:
                cached = self.cache.get(ref["url"], ttl_hours=cache_ttl_hours)
            if cached:
                served_locally += 1
                results.append({
                    **ref,
                    "live_status": cached["live_status"],
                    "live_state": cached["live_state"],
                    "error": cached["error"],
                    "archived_captures": cached["archived_captures"],
                    "archived_latest": cached["archived_latest"],
                    "captures_capped": False,
                    "capture_source": cached["capture_source"],
                    "checked_at": cached["checked_at"],
                    "from_cache": True,
                    "regime": self._regime(cached["live_state"], cached["archived_captures"]),
                })
                continue

            coverage = await self._archive_coverage(ref["url"])
            live = await self._live_status(ref["url"]) if check_live else {"status": None}
            await asyncio.sleep(0.3)          # two requests per reference; stay polite

            status = live.get("status")
            state = self._live_state(status, live.get("error"))
            record = {
                "url": ref["url"],
                "host": ref["host"],
                "live_status": status,
                "live_state": state,
                "error": live.get("error"),
                "archived_captures": coverage["captures"],
                "archived_latest": coverage["latest"],
                "capture_source": coverage.get("source"),
                "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            if self.cache is not None:
                self.cache.put(record)

            results.append({
                **ref, **record,
                "captures_capped": coverage["capped"],
                "from_cache": False,
                "regime": self._regime(state, coverage["captures"]),
            })

        summary: Dict[str, int] = {}
        for r in results:
            summary[r["regime"]] = summary.get(r["regime"], 0) + 1

        return {
            "url": url,
            "snapshot": snapshot,
            "snapshot_url": snapshot.get("url"),
            "reference_count": len(results),
            "summary": summary,
            "served_from_cache": served_locally,
            "cache": self.cache.stats() if self.cache else None,
            "references": results,
            "note": ("alive/dead come from a plain HTTP probe; refused means the host "
                     "rejected this client and cannot be concluded without a browser "
                     "probe — a 403 is not evidence of absence. archive_only is only "
                     "asserted for genuinely dead references. Content comparison is "
                     "left to the caller (Jina refuses web.archive.org)."),
        }
