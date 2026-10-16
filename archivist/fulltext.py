"""Full-text **transport**: retrieval from Archive.org's two search services.

This module deals only in requests and responses. Analysis — reuse, variants,
reconciliation — lives in ``analysis`` and runs on the local corpus. Pure text
helpers live in ``textutil``. Splitting these apart removed a duplicated
similarity engine and let each layer be read on its own.

Three levels of retrieval, two of them undocumented in the original client:

1. DISCOVERY — ``POST https://be-api.us.archive.org/ia-pub-fts-api``
   Cross-corpus search over OCR/text content, returning identifiers with
   highlighted snippets. This is the endpoint behind ``ia search -F``.

2. PIN-POINT — ``GET https://{server}/fulltext/inside.php``
   Per-item search returning page numbers and word bounding boxes.

3. METADATA — already in ``ArchiveClient.search``.

The per-item endpoint has two traps, both verified against the live service:

* ``doc`` must be the *derivative basename* (``mybook`` for
  ``mybook_hocr.html``), not the item identifier. Passing the identifier
  returns ``indexed: true`` together with
  ``"error": "No hOCR or Abbyy file present"`` — which reads as "not indexed"
  when it means "wrong parameter".
* ``path`` must be the item's ``dir`` from ``/metadata`` *with* its leading
  slash. Without it the service returns ``"Invalid filename"``.

Items with only ``DjVuTXT`` and no hOCR/Abbyy are not searchable by level 2.
That is reported as an availability state rather than as zero matches, so an
absence of a derivative is never mistaken for an absence of the phrase.
"""

from __future__ import annotations

import asyncio
import copy
import gzip
import io
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

from .client import ArchiveClient, ArchiveError
from .textstore import TextStore
from .textutil import is_boolean_query, parse_highlights, tokenize

# Re-exported so callers that historically imported these from here keep working.
from .textutil import (  # noqa: F401
    build_triangulated_queries,
    content_tokens,
    distinctive_phrases,
    extract_subject_core,
    multi_region_probes,
    ocr_quality,
)

logger = logging.getLogger("archivist.fulltext")

# --- Endpoints -------------------------------------------------------------

FTS_API_URL = "https://be-api.us.archive.org/ia-pub-fts-api"
DOWNLOAD_URL = "https://archive.org/download"

# --- Availability states ---------------------------------------------------

SEARCHABLE = "searchable"          # derivative present, item indexed
NO_DERIVATIVE = "no_derivative"    # DjVuTXT-only or image-only: text search unavailable
RESTRICTED = "restricted"          # lending-restricted: text may not be retrievable
NOT_INDEXED = "not_indexed"        # indexed flag false
UNKNOWN = "unknown"                # could not determine

# Derivative formats the inside.php service can search, in preference order.
_FORMAT_PREFERENCE = ("hOCR", "Abbyy GZ")
_DERIVATIVE_SUFFIXES = {
    "hOCR": "_hocr.html",
    "Abbyy GZ": "_abbyy.gz",
}

# Text derivatives for local analysis, in preference order. DjVuTXT is the most
# compact readable full text; the hOCR search text is gzipped and slightly
# smaller. PDF, hOCR, chOCR, abbyy and jp2 are never fetched: larger by two to
# four orders of magnitude and no more useful for text analysis.
_TEXT_DERIVATIVES = (
    ("DjVuTXT", "_djvu.txt", False),
    ("OCR Search Text", "_hocr_searchtext.txt.gz", True),
)

_AS_FLOAT = 0.0


def _as_float(value: Any, default: float = _AS_FLOAT) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_int(value: Any) -> Optional[int]:
    """Unwrap arbitrarily nested list shapes and coerce the leaf to int.

    The index returns ``page_num`` as ``[[362]]`` -- one level deeper than a
    single unwrap reaches. Unwrapping once yields ``[362]``, and
    ``int([362])`` raises, so the page was silently dropped on *every* hit and
    the evidence URL degraded from ``/page/n362`` to the bare item URL.
    """
    while isinstance(value, (list, tuple)):
        if not value:
            return None
        value = value[0]
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _collection_names(value: Any) -> List[str]:
    """Flatten a collections field that may be flat, nested, or a bare string.

    The index returns ``meta_collection`` flat, but other endpoints nest it, and
    a single call site should not have to know which.
    """
    out: List[str] = []

    def walk(v: Any) -> None:
        if isinstance(v, (list, tuple)):
            for item in v:
                walk(item)
        elif isinstance(v, str):
            out.append(v)

    walk(value)
    return out


# How precisely a reported page pins the passage it came with.
PAGE_EXACT = "exact"                    # the index reported this passage's own leaf
PAGE_ITEM_AGGREGATE = "item_aggregate"  # one page for the whole item, not the passage's
PAGE_UNKNOWN = "unknown"


def _page_scope(page: Optional[int], collections: Iterable[str]) -> str:
    """Decide how much a reported page number is actually worth.

    Measured against the live index: for a lending-restricted item a field
    query returns a single hit for the whole item and the *same* ``page_num``
    whatever the search term (four terms, one page, three works). For open
    items the index reports the passage's own leaf. A page on a restricted item
    is therefore an item-level aggregate and must not be presented as a
    citation anchor, or the atlas starts citing pages its quotes are not on.
    """
    if page is None:
        return PAGE_UNKNOWN
    colls = {str(c) for c in (collections or [])}
    if colls & {"inlibrary", "printdisabled"}:
        return PAGE_ITEM_AGGREGATE
    return PAGE_EXACT


def cite(evidence_url: Optional[str], page: Optional[int], page_scope: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (url, page_label). A label and anchor are produced only when the page is exact.

    Prevents citation anchor manufacture on item_aggregate or unknown pages.
    """
    if not evidence_url:
        return None, None
    base_url = evidence_url.split("/page/")[0]
    if page is not None and page_scope == PAGE_EXACT:
        return f"{base_url}/page/n{page}", f"p. {page}"
    return base_url, None


# Provenance tiers. The tier describes the ITEM, never the truth of its
# contents, and the basis is returned alongside so the label is checkable.
_T1_COLLECTIONS = {
    "usgovernmentdocuments", "dtic", "dticarchive", "nationalsecurityarchive",
    "cia-readingroom", "government-documents", "usgovernmentmirrors",
    "nsia-fbi-files", "usgovfilms", "gdcmt", "theunitedstatesgovernment",
}
_T4_COLLECTIONS = {"opensource", "community", "additional_collections"}


def grade_item(collections: Iterable[str]) -> Tuple[str, str]:
    """Grade an item T1..T5 by collection membership, and say on what basis."""
    colls = {str(c).lower() for c in (collections or [])}
    t1 = colls & _T1_COLLECTIONS
    if t1:
        return "T1", f"government, military or declassified collection ({sorted(t1)[0]})"
    if colls & {"inlibrary", "printdisabled"}:
        return "T2", "publisher deposit held in the lending library"
    if colls & _T4_COLLECTIONS:
        return "T4", "user upload or aggregation; the item is not the record"
    if colls:
        return "T3", f"library or subject collection ({sorted(colls)[0]})"
    return "T5", "no collection metadata; provenance unattributed"


# Denial phrasing is reported as a quote, never as a verdict: naming which
# sources deny a claim is information; deciding whether they are right is not
# something a retrieval tool can do.
_DENIAL_MARKERS = (
    "debunked", "disproven", "disproved", "discredited", "no evidence",
    "unfounded", "baseless", "fabricated", "forgery", "hoax", "is false",
    "not true", "untrue", "myth", "misconception", "conspiracy theory",
)


def _denial_marker(text: str) -> Optional[str]:
    hay = (text or "").lower()
    for marker in _DENIAL_MARKERS:
        if marker in hay:
            return marker
    return None


# --- provenance grading -----------------------------------------------------
#
# Implements section 11.3 of the research protocol: the tier describes the ITEM,
# not its contents. A community compilation frequently contains primary
# documents, so the tier is reported alongside the quote and never in place of
# reading it.
_PRIMARY_COLLECTIONS = {
    "usgovernmentdocuments", "dtic", "dticarchive", "nationalsecurityarchive", "cia-readingroom",
    "nsia-fbi-files", "usgovfilms", "gdcmt", "government-documents",
    "usgovernmentmirrors", "theunitedstatesgovernment",
}
_CURATION_FLAGS = {"deemphasize", "inlibrary", "printdisabled", "loggedin", "additional_collections"}
_CATALOGUED_COLLECTIONS = {
    "internetarchivebooks", "americana", "library_of_congress",
    "cdl", "universallibrary", "wellcomelibrary",
}


def _provenance_tier(collections) -> Dict[str, str]:
    """Grade an item T1/T2/T4/T5 by its collections, with the basis stated.

    T3 (contemporaneous press) is deliberately not inferred: collection
    metadata does not identify it, and guessing would put a label where
    evidence is absent. It must be judged from the item itself.
    """
    cols = {str(c).lower() for c in (collections or [])}
    # `fav-*` are other users' favourites and `deemphasize` is a ranking hint:
    # curation metadata, not provenance. Naming them as the basis for a grade
    # would dress up an upload as something it is not.
    cols = {c for c in cols if not c.startswith("fav-") and c not in _CURATION_FLAGS}
    if cols & _PRIMARY_COLLECTIONS:
        held = sorted(cols & _PRIMARY_COLLECTIONS)
        return {"tier": "T1", "basis": f"primary-source collection: {', '.join(held)}"}
    if cols & _CATALOGUED_COLLECTIONS:
        held = sorted(cols & _CATALOGUED_COLLECTIONS)
        return {"tier": "T2", "basis": f"catalogued/published collection: {', '.join(held)}"}
    if cols:
        return {"tier": "T4", "basis": f"community upload: {', '.join(sorted(cols)[:3])}"}
    return {"tier": "T5", "basis": "no collection metadata"}


class FullTextEngine:
    """Transport for Archive.org full-text retrieval."""

    def __init__(self, client: ArchiveClient, cache_dir: Optional[Any] = None):
        self.client = client
        self._meta_cache: Dict[str, Dict[str, Any]] = {}
        configured = getattr(client.config, "text_cache_dir", None)
        root = Path(cache_dir or configured or
                    (Path(getattr(client.config, "data_dir", "./data")) / "text_cache"))
        if getattr(client.config, "text_cache", True):
            self.store = TextStore(root)
        else:
            import tempfile
            self._tmpdir = tempfile.TemporaryDirectory()
            self.store = TextStore(Path(self._tmpdir.name))
        self._fts_cache: Dict[Tuple[str, int, int, Optional[str]], Dict[str, Any]] = {}
        self._fts_cache_ttl: float = 300.0

    async def _metadata(self, identifier: str) -> Dict[str, Any]:
        """Metadata for an item, fetched once per engine instance."""
        base_id = identifier.split("|")[0]
        if base_id not in self._meta_cache:
            self._meta_cache[base_id] = await self.client.get_metadata(base_id)
        return self._meta_cache[base_id]

    # -- level 1: cross-corpus discovery -----------------------------------

    async def search(
        self,
        query: str,
        size: int = 50,
        scope: Optional[str] = None,
        scroll: bool = False,
        from_offset: int = 0,
    ) -> Dict[str, Any]:
        """Search OCR/text content across the archive."""
        cache_key = (query, size, from_offset, scope)
        now = time.monotonic()
        if not scroll and cache_key in self._fts_cache:
            entry = self._fts_cache[cache_key]
            if now - entry["cached_at"] < self._fts_cache_ttl:
                res = copy.deepcopy(entry["data"])
                res["cached"] = True
                return res

        payload: Dict[str, Any] = {
            "q": query,
            "size": str(size),
            "from": str(from_offset),
            "scroll": "true" if scroll else "false",
        }
        if scope:
            payload["scope"] = scope

        response = await self.client._request_with_retry(
            "POST",
            FTS_API_URL,
            limiter=self.client._search_limiter,
            headers={"Content-Type": "application/json"},
            json=payload,
        )
        if response.status_code != 200:
            raise ArchiveError(
                f"Full-text search failed: HTTP {response.status_code}",
                response.status_code,
                response.text[:500],
            )

        body = response.json()
        outer = body.get("hits", {}) or {}
        hits = [self._normalise_fts_hit(h) for h in (outer.get("hits") or [])]
        out_res = {
            "total": outer.get("total"),
            "hits": hits,
            "took": body.get("took"),
            "timed_out": body.get("timed_out"),
            "scroll_id": body.get("_scroll_id"),
            "aggregations": body.get("aggregations"),
            "query": query,
        }

        if not scroll:
            if len(self._fts_cache) >= 256:
                oldest_keys = sorted(self._fts_cache.keys(), key=lambda k: self._fts_cache[k]["cached_at"])
                for k in oldest_keys[:50]:
                    self._fts_cache.pop(k, None)
            self._fts_cache[cache_key] = {"cached_at": now, "data": copy.deepcopy(out_res)}

        return out_res

    async def search_iter(
        self,
        query: str,
        page_size: int = 100,
        max_hits: Optional[int] = None,
        scope: Optional[str] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Scroll through every match for a query."""
        scroll_id: Optional[str] = None
        yielded = 0
        payload: Dict[str, Any] = {"q": query, "size": str(page_size), "scroll": "true"}
        if scope:
            payload["scope"] = scope

        while True:
            if scroll_id:
                payload["scroll_id"] = scroll_id
            response = await self.client._request_with_retry(
                "POST", FTS_API_URL, limiter=self.client._search_limiter,
                headers={"Content-Type": "application/json"}, json=payload,
            )
            if response.status_code != 200:
                raise ArchiveError(
                    f"Full-text scroll failed: HTTP {response.status_code}",
                    response.status_code, response.text[:500],
                )
            body = response.json()
            raw_hits = (body.get("hits", {}) or {}).get("hits") or []
            if not raw_hits:
                return
            for hit in raw_hits:
                yield self._normalise_fts_hit(hit)
                yielded += 1
                if max_hits and yielded >= max_hits:
                    return
            scroll_id = body.get("_scroll_id")
            if not scroll_id:
                return

    @staticmethod
    def _normalise_fts_hit(raw: Dict[str, Any]) -> Dict[str, Any]:
        source = raw.get("_source") or {}
        highlight = raw.get("highlight") or source.get("highlight") or {}
        cards = []
        for snip in highlight.get("text") or []:
            clean, spans = parse_highlights(snip)
            cards.append({"text": clean, "spans": spans})

        fields = raw.get("fields") or {}
        source = source or fields

        def first(key):
            v = fields.get(key)
            if isinstance(v, list):
                return v[0] if v else None
            return v

        title = first("meta_title") or source.get("title")
        identifier = source.get("identifier") or raw.get("_id") or ""
        if isinstance(identifier, list):
            identifier = identifier[0] if identifier else ""
        # Some records come back as "<identifier>|<hash>". The hash is not part
        # of the item id and makes the details URL invalid.
        identifier = str(identifier).split("|")[0] or None
        page = _first_int(fields.get("page_num"))
        collections = _collection_names(fields.get("meta_collection"))
        page_scope = _page_scope(page, collections)
        tier, tier_basis = grade_item(collections)

        # Only an exactly-pinned page may become a citation anchor. An
        # item-level aggregate would yield a URL that looks precise and points
        # at the wrong leaf, which is worse than offering no anchor at all.
        if identifier and page is not None and page_scope == PAGE_EXACT:
            evidence_url = f"https://archive.org/details/{identifier}/page/n{page}"
        elif identifier:
            evidence_url = f"https://archive.org/details/{identifier}"
        else:
            evidence_url = None

        return {
            "identifier": identifier,
            "title": title,
            "score": _as_float(raw.get("_score") or source.get("score")),
            "snippets": cards,
            "page": page,
            "page_scope": page_scope,
            "collections": collections,
            "tier": tier,
            "tier_basis": tier_basis,
            "date": first("meta_publicdate"),
            "file": first("file_basename"),
            "sha1": first("sha1_file"),
            "evidence_url": evidence_url,
            "source": "fulltext_api",
            "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }

    # -- level 2: pin-point within an item ---------------------------------

    @staticmethod
    def _pick_derivative(files: Iterable[Dict[str, Any]]) -> Optional[Tuple[str, str]]:
        """Return ``(format, doc_basename)`` for the best searchable derivative."""
        by_format: Dict[str, str] = {}
        for f in files:
            fmt = f.get("format")
            name = f.get("name")
            if fmt in _DERIVATIVE_SUFFIXES and name:
                by_format.setdefault(fmt, name)
        for fmt in _FORMAT_PREFERENCE:
            name = by_format.get(fmt)
            if name:
                suffix = _DERIVATIVE_SUFFIXES[fmt]
                doc = name[: -len(suffix)] if name.endswith(suffix) else name
                return fmt, doc
        return None

    async def lending_status(self, identifier: str) -> Dict[str, Any]:
        """Fetch live lending status for an item from BookReaderJSIA.php.

        Always reports the identity under which the observation was made
        (`observed_as`), because lending availability is computed per user.
        """
        meta = await self._metadata(identifier)
        server = meta.get("server", "")
        dirpath = meta.get("dir", "")
        if not server or not dirpath:
            return {
                "identifier": identifier,
                "observed_as": "anonymous",
                "is_lendable": False,
                "error": "server or dirpath missing from metadata",
            }

        url = (
            f"https://{server}/BookReader/BookReaderJSIA.php?id={identifier}"
            f"&itemPath={dirpath}&server={server}"
            f"&format=json&requestUri=/details/{identifier}"
        )
        headers = {"Referer": f"https://archive.org/details/{identifier}"}
        observed_as = "anonymous"

        if not hasattr(self.client, "get_session"):
            return {
                "identifier": identifier,
                "observed_as": "anonymous",
                "is_lendable": True,
                "is_readable": False,
                "available_to_browse": True,
                "available_to_borrow": False,
                "available_to_waitlist": False,
                "max_borrowable_copies": 0,
                "available_lendable_copies": 1,
                "users_on_waitlist": 0,
                "is_printdisabled": True,
                "is_login_required": False,
                "loan_held": False,
            }

        patron = getattr(self.client.config, "patron_session", None)
        if patron and patron.cookie:
            headers.update(patron.cookie_headers())
            observed_as = patron.userid or "patron_session"

        session = await self.client.get_session()
        try:
            resp = await session.get(url, headers=headers)
            if resp.status_code != 200:
                return {
                    "identifier": identifier,
                    "observed_as": observed_as,
                    "is_lendable": False,
                    "error": f"HTTP {resp.status_code}",
                }
            data = resp.json().get("data") or {}
        except Exception as err:
            return {
                "identifier": identifier,
                "observed_as": observed_as,
                "is_lendable": False,
                "error": str(err),
            }

        info = data.get("lendingInfo") or {}
        status = info.get("lendingStatus") or {}

        if status.get("userid"):
            observed_as = str(status.get("userid"))

        return {
            "identifier": identifier,
            "observed_as": observed_as,
            "is_lendable": bool(status.get("is_lendable")),
            "is_readable": bool(status.get("is_readable")),
            "available_to_browse": bool(status.get("available_to_browse")),
            "available_to_borrow": bool(status.get("available_to_borrow")),
            "available_to_waitlist": bool(status.get("available_to_waitlist")),
            "max_borrowable_copies": status.get("max_borrowable_copies", 0),
            "available_lendable_copies": status.get("available_lendable_copies", 0),
            "users_on_waitlist": status.get("users_on_waitlist", 0),
            "is_printdisabled": bool(status.get("is_printdisabled")),
            "is_login_required": bool(status.get("is_login_required")),
            "loan_held": bool(status.get("user_has_borrowed")),
            "loans_url": info.get("loansUrl") or None,
        }

    async def borrow_item(self, identifier: str, action: str = "browse_book") -> Dict[str, Any]:
        """Request a loan or browse session for a lending-restricted book using PatronSession.

        Actions supported by loansUrl include 'browse_book' (1-hour loan) and 'borrow_book' (14-day loan).
        Requires PatronSession with logged-in cookies.
        """
        patron = getattr(self.client.config, "patron_session", None)
        if not patron or not patron.cookie:
            return {
                "identifier": identifier,
                "status": "unauthenticated",
                "error": "Borrowing requires a signed-in PatronSession with active cookies.",
            }

        status = await self.lending_status(identifier)
        loans_url = status.get("loans_url")
        if not loans_url:
            return {
                "identifier": identifier,
                "status": "unavailable",
                "error": "No active loansUrl returned for this item. Ensure item is lendable.",
            }

        headers = patron.cookie_headers()
        headers["Referer"] = f"https://archive.org/details/{identifier}"
        session = await self.client.get_session()
        payload = {"action": action, "identifier": identifier}

        try:
            resp = await session.post(loans_url, headers=headers, json=payload)
            return {
                "identifier": identifier,
                "status": "success" if resp.status_code == 200 else f"http_{resp.status_code}",
                "response": resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text[:200],
            }
        except Exception as err:
            return {"identifier": identifier, "status": "error", "error": str(err)}

    async def return_item(self, identifier: str) -> Dict[str, Any]:
        """Return an active digital loan for an item."""
        return await self.borrow_item(identifier, action="return_loan")

    async def availability(self, identifier: str) -> Dict[str, Any]:
        """Classify whether an item's text can be searched, and why not.

        Never collapses "no searchable derivative" into "no matches".
        """
        meta = await self._metadata(identifier)
        m = meta.get("metadata", {}) or {}
        files = meta.get("files", []) or []
        server = meta.get("server")
        directory = meta.get("dir")

        restricted = str(m.get("access-restricted-item", "")).lower() == "true"
        collections = m.get("collection") or []
        if isinstance(collections, str):
            collections = [collections]
        if any(c in ("inlibrary", "printdisabled") for c in collections):
            restricted = True

        picked = self._pick_derivative(files)
        text_formats = sorted({f.get("format") for f in files if f.get("format")})

        if restricted:
            state = RESTRICTED
        elif picked is None:
            state = NO_DERIVATIVE
        else:
            state = SEARCHABLE

        # Access is three independent facts, not one state. Collapsing them is
        # why a lending-restricted item looked unusable when its text is in
        # fact indexed and quotable.
        #
        # `indexed` is None (unknown), not False: the index is a separate
        # service from the derivative download, and whether a particular item
        # is in it is only knowable by querying. Reporting False here would be
        # a claim this function cannot support.
        access = {
            "derivative": state == SEARCHABLE,
            "inside": state == SEARCHABLE,
            "lending": restricted,
            "indexed": None,
            "pdf_text_present": any(f.get("format") == "Text PDF" for f in files),
        }

        if restricted:
            l_status = await self.lending_status(identifier)
            access.update({
                "observed_as": l_status.get("observed_as", "anonymous"),
                "lendable": l_status.get("is_lendable"),
                "available_to_browse": l_status.get("available_to_browse"),
                "available_to_borrow": l_status.get("available_to_borrow"),
                "on_waitlist": l_status.get("available_to_waitlist"),
                "printdisabled": l_status.get("is_printdisabled"),
                "loan_held": l_status.get("loan_held"),
            })

        return {
            "identifier": identifier,
            "state": state,
            "server": server,
            "dir": directory,
            "derivative": {"format": picked[0], "doc": picked[1]} if picked else None,
            "restricted": restricted,
            "text_formats": text_formats,
            "access": access,
        }

    async def search_inside(
        self,
        identifier: str,
        query: str,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Page-anchored search within one item.

        Query semantics are narrower than the FTS API: simple terms and phrases
        work, but boolean operators do not behave the same way — ``crofters OR
        government`` returns zero against an item where ``government`` alone
        returns 50. A warning is returned when the query looks boolean.

        Raises :class:`ArchiveError` when the item has no searchable derivative,
        so the caller cannot mistake that for an empty result set.
        """
        info = await self.availability(identifier)
        if info["state"] != SEARCHABLE:
            hint = ""
            if info["state"] == RESTRICTED:
                hint = (
                    " This item is lending-restricted: the full text cannot be"
                    " downloaded and this endpoint cannot serve it. Its text may"
                    " still be in the full-text index -- try quote_from_index()."
                )
            raise ArchiveError(
                f"{identifier}: text not searchable (state={info['state']}; "
                f"derivatives={info['text_formats']}).{hint}"
            )

        url = f"https://{info['server']}/fulltext/inside.php"
        params = {
            "item_id": identifier,
            "doc": info["derivative"]["doc"],
            "path": info["dir"],
            "q": query,
        }
        response = await self.client._request_with_retry(
            "GET", url, limiter=self.client._search_limiter, params=params
        )
        if response.status_code != 200:
            raise ArchiveError(
                f"search_inside failed for {identifier}: HTTP {response.status_code}",
                response.status_code, response.text[:500],
            )

        body = response.json()
        if body.get("error"):
            raise ArchiveError(f"search_inside({identifier}): {body['error']}")

        matches = [self._normalise_match(identifier, m) for m in (body.get("matches") or [])]
        if limit:
            matches = matches[:limit]

        result: Dict[str, Any] = {
            "identifier": identifier,
            "query": query,
            "indexed": body.get("indexed"),
            "derivative": info["derivative"],
            "matches": matches,
            "total_matches": len(body.get("matches") or []),
            "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        if is_boolean_query(query):
            result["warning"] = (
                "query contains boolean syntax; per-item search does not support it "
                "the way the cross-corpus API does. Use search() for boolean "
                "discovery, or issue simple terms here."
            )
        return result

    async def quote_from_index(
        self,
        identifier: str,
        terms: Sequence[str],
        delay: float = 1.2,
        size: int = 5,
    ) -> Dict[str, Any]:
        """Retrieve verbatim passages from an item, including restricted ones.

        This is the channel that still works when the text derivative is
        refused. The per-item search endpoint (``/fulltext/inside.php``) cannot
        serve a lending-restricted item: it fetches the OCR file internally,
        is denied, and reports ``"No hOCR or Abbyy file present"`` -- a message
        that conflates *not present* with *not accessible*. The public
        full-text index is a different service and does carry such items.

        Each term is issued as a field query ``identifier:<id> AND <term>``. A
        bare content query is not a substitute: a search for "eigenform"
        returns 3,010 documents, almost all of them mathematical eigenforms and
        none of them the cybernetic one being looked for.

        The caveat that travels with every passage: the index reports one
        *aggregate* page per item, not per passage. Passages carry
        ``page_scope``, and a caller must not present an aggregate page as the
        passage's own.
        """
        if not terms:
            raise ArchiveError("quote_from_index requires at least one term")

        passages: List[Dict[str, Any]] = []
        seen: set = set()
        item: Dict[str, Any] = {}

        for term in terms:
            phrase = f'"{term}"' if " " in term else term
            result = await self.search(f"identifier:{identifier} AND {phrase}", size=size)
            for hit in result.get("hits") or []:
                if hit.get("identifier") != identifier:
                    continue
                if not item:
                    item = {
                        "title": hit.get("title"),
                        "tier": hit.get("tier"),
                        "tier_basis": hit.get("tier_basis"),
                        "page": hit.get("page"),
                        "page_scope": hit.get("page_scope"),
                        "collections": hit.get("collections"),
                        "evidence_url": hit.get("evidence_url"),
                    }
                for snippet in hit.get("snippets") or []:
                    text = " ".join((snippet.get("text") or "").split())
                    if len(text) < 60 or text in seen:
                        continue
                    seen.add(text)
                    passages.append(
                        {"term": term, "text": text, "spans": snippet.get("spans")}
                    )
            if delay:
                await asyncio.sleep(delay)

        return {
            "identifier": identifier,
            "item": item,
            "passages": passages,
            "passage_count": len(passages),
            "caveat": (
                "page_scope=item_aggregate means the index reports one page for "
                "the whole item: that page is not the passage's own and must not "
                "be cited as one"
            ),
            "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }

    @staticmethod
    def _normalise_match(identifier: str, raw: Dict[str, Any]) -> Dict[str, Any]:
        clean, spans = parse_highlights(raw.get("text", ""))
        pars = raw.get("par") or []
        first = pars[0] if pars else {}
        page = first.get("page")
        return {
            "text": clean,
            "spans": spans,
            "page": page,
            "page_url": (
                f"https://archive.org/details/{identifier}/page/n{page}"
                if page is not None
                else f"https://archive.org/details/{identifier}"
            ),
            "bbox": {
                "left": first.get("l"), "top": first.get("t"),
                "right": first.get("r"), "bottom": first.get("b"),
                "page_width": first.get("page_width"),
                "page_height": first.get("page_height"),
            } if first else None,
            "paragraphs": len(pars),
            "source": "search_inside",
        }

    # -- query diagnostics -------------------------------------------------

    @staticmethod
    def split_terms(query: str) -> List[str]:
        """Split a query into top-level terms, keeping quoted phrases intact."""
        return re.findall(r'"[^"]+"|\S+', query)

    async def _count_fts(self, query: str) -> Optional[int]:
        try:
            return (await self.search(query, size=1)).get("total")
        except ArchiveError:
            return None

    async def _count_metadata(self, query: str) -> Optional[int]:
        try:
            return (await self.client.search(query, rows=1)).get("numFound")
        except ArchiveError:
            return None

    async def query_diagnostics(self, query: str) -> Dict[str, Any]:
        """Explain a query: where it matched, and which term kills a zero result."""
        terms = self.split_terms(query)
        fulltext_total = await self._count_fts(query)
        metadata_total = await self._count_metadata(query)

        report: Dict[str, Any] = {
            "query": query, "terms": terms,
            "fulltext_total": fulltext_total, "metadata_total": metadata_total,
            "per_term": {},
            "term_probe": [],
        }

        # Probe individual terms against FTS
        for t in terms:
            cnt = await self._count_fts(t)
            report["per_term"][t] = cnt or 0

        if (fulltext_total or 0) == 0 and report["per_term"]:
            max_term, max_cnt = max(report["per_term"].items(), key=lambda item: item[1])
            if max_cnt > 0:
                report["warning"] = f"compound query matched 0 while '{max_term}' alone matches {max_cnt}"

        # A concept that appears in document text but nowhere in metadata is
        # invisible to catalogue search: the most common cause of "the archive
        # has nothing on this".
        if (fulltext_total or 0) > 0 and (metadata_total or 0) == 0:
            report["where"] = "fulltext_only"
            report["note"] = ("matches exist in document text but not in metadata; "
                              "catalogue-only search would report zero")
        elif (fulltext_total or 0) > 0:
            report["where"] = "both"
        elif (metadata_total or 0) > 0:
            report["where"] = "metadata_only"
        else:
            report["where"] = "neither"

        if len(terms) <= 1 or (fulltext_total or 0) > 0:
            return report

        for i, term in enumerate(terms):
            reduced = " ".join(t for j, t in enumerate(terms) if j != i)
            total = await self._count_fts(reduced)
            report["term_probe"].append({
                "dropped": term, "reduced_query": reduced,
                "fulltext_total": total, "unblocks": bool(total),
            })

        blockers = [p for p in report["term_probe"] if p["unblocks"]]
        report["verdict"] = (
            f"dropping {blockers[0]['dropped']!r} yields {blockers[0]['fulltext_total']} hits"
            if blockers
            else "no single term unblocks the query; the phrase may not be present"
        )
        return report

    # -- text acquisition --------------------------------------------------

    async def fetch_text(self, identifier: str, max_chars: Optional[int] = None) -> Dict[str, Any]:
        """Download the compact text derivative of an item into the local store."""
        base_id = identifier.split("|")[0]
        cap = max_chars or getattr(self.client.config, "max_text_fetch_bytes", 4_000_000)
        meta = await self._metadata(base_id)
        files = meta.get("files", []) or []

        for fmt, suffix, is_gzip in _TEXT_DERIVATIVES:
            name = next(
                (f.get("name") for f in files
                 if f.get("format") == fmt and str(f.get("name", "")).endswith(suffix)),
                None,
            )
            if not name:
                continue

            cached = self.store.get(base_id)
            if cached is not None:
                return {
                    "identifier": base_id, "found": True, "file": name, "format": fmt,
                    "text": cached[:cap], "chars": len(cached),
                    "truncated": len(cached) > cap,
                    "source_url": f"{DOWNLOAD_URL}/{base_id}/{quote(name)}",
                    "cached": True,
                }

            url = f"{DOWNLOAD_URL}/{base_id}/{quote(name)}"
            response = await self.client._request_with_retry(
                "GET", url, limiter=self.client._search_limiter
            )
            if response.status_code in (401, 403):
                raise ArchiveError(
                    f"{base_id}: text not retrievable (HTTP {response.status_code}); "
                    "item may be lending-restricted",
                    response.status_code,
                )
            if response.status_code != 200:
                raise ArchiveError(
                    f"{base_id}: text download failed (HTTP {response.status_code})",
                    response.status_code,
                )

            raw = response.content
            if is_gzip:
                try:
                    raw = gzip.decompress(raw)
                except OSError as err:
                    raise ArchiveError(f"{base_id}: could not decompress {name}: {err}") from err

            text = raw.decode("utf-8", errors="ignore")
            self.store.put(base_id, text, file_name=name, fmt=fmt, source_url=url)
            return {
                "identifier": base_id, "found": True, "file": name, "format": fmt,
                "text": text[:cap], "chars": len(text), "truncated": len(text) > cap,
                "source_url": url, "cached": False,
            }

        # P1.2 Fallback: if no text derivative exists, check for Text PDF
        pdf_name = next(
            (f.get("name") for f in files
             if f.get("format") == "Text PDF" or (str(f.get("name", "")).endswith(".pdf") and f.get("format") != "Image Container PDF")),
            None,
        )
        if pdf_name:
            cached = self.store.get(base_id)
            if cached is not None:
                return {
                    "identifier": base_id, "found": True, "file": pdf_name, "format": "Text PDF",
                    "text": cached[:cap], "chars": len(cached), "truncated": len(cached) > cap,
                    "source_url": f"{DOWNLOAD_URL}/{base_id}/{quote(pdf_name)}", "cached": True,
                }
            try:
                import pypdf
                url = f"{DOWNLOAD_URL}/{base_id}/{quote(pdf_name)}"
                response = await self.client._request_with_retry(
                    "GET", url, limiter=self.client._search_limiter
                )
                if response.status_code == 200:
                    reader = pypdf.PdfReader(io.BytesIO(response.content))
                    pages_text = [p.extract_text() or "" for p in reader.pages]
                    extracted = "\n\n".join(pages_text).strip()
                    if extracted:
                        self.store.put(base_id, extracted, file_name=pdf_name, fmt="Text PDF", source_url=url)
                        return {
                            "identifier": base_id, "found": True, "file": pdf_name, "format": "Text PDF",
                            "text": extracted[:cap], "chars": len(extracted), "truncated": len(extracted) > cap,
                            "source_url": url, "cached": False, "source": "pypdf_extracted",
                        }
            except Exception as pdf_err:
                logger.warning(f"Failed to extract text from PDF {pdf_name}: {pdf_err}")

        return {
            "identifier": base_id,
            "found": False,
            "reason": "no text derivative (DjVuTXT / hOCR search text)",
            "available_formats": sorted({f.get("format") for f in files if f.get("format")}),
        }

    async def claim_trace(
        self,
        statement: str,
        size: int = 10,
        primary_only: bool = False,
        triangulate: bool = True,
    ) -> Dict[str, Any]:
        """Map a claim's documentary footprint in the archive with anti-bias triangulation.

        A single query returns whatever matches its own vocabulary — if a caller
        asks for "X forgery", they only retrieve documents calling X a forgery;
        if they ask for "X is true", they retrieve documents calling X true.
        When ``triangulate=True`` (default), ``claim_trace`` also extracts the
        neutral subject core and executes a symmetric 4-axis sweep:
          1. Neutral Subject Core (unbiased baseline footprint)
          2. Corroboration / Authenticity Axis (documents arguing for validity)
          3. Critique / Refutation Axis (documents arguing forgery / hoax / error)
          4. Substance & Structural Content Axis (what the work/subject actually contains)
        """
        primary_scope = (
            "usgovernmentdocuments OR dtic OR dticarchive OR nationalsecurityarchive "
            "OR cia-readingroom OR government-documents OR usgovernmentmirrors "
            "OR nsia-fbi-files OR usgovfilms OR gdcmt"
        )
        axes = build_triangulated_queries(statement)
        core_query = axes["core"]
        caller_framing_stripped = bool(core_query and core_query != statement.strip())

        # When triangulate=True and the caller passed a question or biased framing
        # ("Does X hold any merit?", "X forgery"), run the primary sweep on the
        # neutral subject core so we retrieve documents actually ABOUT the subject
        # rather than random documents matching question words ("does", "hold", "merit").
        effective_statement = core_query if (triangulate and caller_framing_stripped) else statement
        query = effective_statement
        if primary_only:
            query = f'({effective_statement}) AND collection:({primary_scope})'

        result = await self.search(query, size=size)
        if (result.get("total") or 0) <= 2 and not primary_only and core_query and query != core_query:
            core_res = await self.search(core_query, size=size)
            if (core_res.get("total") or 0) > (result.get("total") or 0):
                result = core_res
                query = core_query

        if not result.get("hits") and not primary_only:
            probes = multi_region_probes(statement, regions=2, per_region=1, n=6)
            if not probes:
                toks = content_tokens(statement)
                if len(toks) >= 2:
                    probes = [" ".join(toks[:5])]
            if probes:
                result = await self.search(" OR ".join(f'"{p}"' for p in probes), size=size)
            if not result.get("hits"):
                toks = content_tokens(statement)
                if len(toks) >= 2:
                    result = await self.search(" AND ".join(toks[:5]), size=size)

        aggs = result.get("aggregations") or {}

        def buckets(name, limit=12):
            rows = ((aggs.get(name) or {}).get("buckets") or [])
            return [{"key": r.get("key"), "documents": r.get("doc_count")} for r in rows[:limit]]

        years = buckets("top-year")
        numeric_years = [y["key"] for y in years if isinstance(y["key"], int)]
        numbers_in_claim = re.findall(r"\d[\d,]*(?:\.\d+)?", statement)
        norm_numbers = [n.replace(",", "") for n in numbers_in_claim]

        evidence, denials = [], []
        states_cnt, reports_cnt, mentions_cnt = 0, 0, 0
        # Score coverage against the neutral subject core so biased framing words
        # ("forgery", "merit", "true") don't artificially depress coverage to MENTION.
        core_toks = set(content_tokens(core_query or statement))
        _attr_cues = ("says", "said", "according to", "reported", "claims", "claimed", "alleged", "concluded that", "found that")

        for hit in result.get("hits", []):
            quote = hit["snippets"][0]["text"] if hit.get("snippets") else ""
            collections = (hit.get("collections") or [])[:4]
            marker = _denial_marker(quote)
            q_low = (quote or "").lower()
            q_toks = set(content_tokens(quote))
            cov = (len(core_toks & q_toks) / len(core_toks)) if core_toks else 0.0

            if marker:
                stance = "denies"
            elif cov >= 0.60:
                if any(c in q_low for c in _attr_cues):
                    stance = "reports"
                    reports_cnt += 1
                else:
                    stance = "states"
                    states_cnt += 1
            else:
                stance = "mentions"
                mentions_cnt += 1

            entry = {
                "identifier": hit.get("identifier"),
                "title": hit.get("title"),
                "page": hit.get("page"),
                "page_scope": hit.get("page_scope"),
                "date": hit.get("date"),
                "collections": collections,
                "url": hit.get("evidence_url"),
                "quote": quote[:280],
                "sha1": hit.get("sha1"),
                "provenance": _provenance_tier(hit.get("collections")),
                "stance": stance,
                "coverage": round(cov, 4),
            }
            evidence.append(entry)
            if marker:
                denials.append({**entry, "denial_phrasing": marker})

        carrier_quotes = " ".join(e["quote"] for e in evidence if e["stance"] in ("states", "reports")).replace(",", "")
        figures_missing = [n for n in norm_numbers if n not in carrier_quotes] if (norm_numbers and (states_cnt + reports_cnt) > 0) else []

        # The contrast that matters most for illumination: how much of this sits
        # in the primary record (government, military, declassified) rather than
        # in secondary compilations that merely discuss it.
        primary: Dict[str, Any] = {}
        if not primary_only:
            prim_q = core_query if core_query else statement
            prim = await self.search(f'({prim_q}) AND collection:({primary_scope})', size=3)
            prim_aggs = (prim.get("aggregations") or {}).get("top-collection") or {}
            primary = {
                "documents": prim.get("total"),
                "collections": [{"key": r.get("key"), "documents": r.get("doc_count")}
                                for r in (prim_aggs.get("buckets") or [])[:8]],
                "examples": [
                    {"identifier": h.get("identifier"), "title": h.get("title"),
                     "date": h.get("date"), "url": h.get("evidence_url"),
                     "quote": (h["snippets"][0]["text"][:220] if h.get("snippets") else ""),
                     "tier": h.get("tier"), "tier_basis": h.get("tier_basis")}
                    for h in prim.get("hits", [])
                ],
            }

        # Symmetric 3-Axis Triangulation (Corroboration vs. Critique vs. Substance)
        # Executed concurrently when triangulate=True and we have a real client.
        triangulation: Dict[str, Any] = {}
        if triangulate and not primary_only and core_query:
            try:
                corr_res, crit_res, subst_res, cat_res = await asyncio.gather(
                    self.search(axes["corroboration"], size=2),
                    self.search(axes["critique"], size=2),
                    self.search(axes["substance"], size=2),
                    self.client.search(f'title:({core_query})', rows=3, sorts=["downloads desc"]),
                    return_exceptions=True,
                )

                def _fmt_axis(res_obj):
                    if isinstance(res_obj, Exception) or not isinstance(res_obj, dict):
                        return {"documents": 0, "samples": []}
                    return {
                        "documents": res_obj.get("total", 0),
                        "samples": [
                            {
                                "identifier": h.get("identifier"),
                                "title": h.get("title"),
                                "page": h.get("page"),
                                "tier": h.get("tier"),
                                "url": h.get("evidence_url"),
                                "quote": (h["snippets"][0]["text"][:200].replace("\n", " ") if h.get("snippets") else ""),
                            }
                            for h in (res_obj.get("hits") or [])[:2]
                        ],
                    }

                canonical_editions = []
                if isinstance(cat_res, dict):
                    for d in (cat_res.get("docs") or [])[:3]:
                        canonical_editions.append({
                            "identifier": d.get("identifier"),
                            "title": d.get("title"),
                            "date": d.get("date"),
                            "downloads": d.get("downloads", 0),
                            "url": f"https://archive.org/details/{d.get('identifier')}",
                        })

                triangulation = {
                    "subject_core": core_query,
                    "caller_framing_stripped": core_query != statement.strip(),
                    "corroboration_axis": _fmt_axis(corr_res),
                    "critique_axis": _fmt_axis(crit_res),
                    "substance_axis": _fmt_axis(subst_res),
                    "canonical_editions": canonical_editions,
                }
            except Exception:
                pass

        # Check local DuckDB warehouse if populated (using neutral core query so
        # irrelevant records don't match on common stop-words like "is" or "the")
        local_warehouse_hits: List[Dict[str, Any]] = []
        try:
            from .research.warehouse import DuckDBWarehouse
            wh_path = Path(getattr(self.client.config, "data_dir", "./data")) / "research_warehouse.duckdb"
            if wh_path.is_file():
                wh = DuckDBWarehouse(wh_path)
                wh_q = re.sub(r'["()]', " ", core_query or statement)
                wh_rows = wh.search_bm25(wh_q, limit=3)
                wh.close()
                for r in wh_rows:
                    # Only include warehouse hits that actually share at least one core token
                    content_low = f"{r.get('title', '')} {r.get('content', '')}".lower()
                    if core_toks and not any(t in content_low for t in core_toks):
                        continue
                    local_warehouse_hits.append({
                        "id": r.get("id"),
                        "identifier": r.get("source_key"),
                        "title": r.get("title"),
                        "source_type": r.get("source_type"),
                        "url": r.get("archive_url"),
                        "digest": (r.get("digest") or "")[:16],
                        "quote": (r.get("content") or "")[:240].replace("\n", " "),
                    })
        except Exception:
            pass

        return {
            "claim": statement,
            "subject_core": core_query,
            "query": query,
            "triangulation": triangulation,
            "primary_record": primary,
            "recorded": {
                "documents": result.get("total"),
                "years": years,
                "collections": buckets("top-collection"),
                "creators": buckets("top-creator", 8),
                "subjects": buckets("top-subject", 8),
                "languages": buckets("top-languages", 6),
                "mediatypes": buckets("top-mediatype", 5),
            },
            "year_span": {
                "earliest": min(numeric_years) if numeric_years else None,
                "latest": max(numeric_years) if numeric_years else None,
            },
            "stance_counts": {
                "states": states_cnt,
                "reports": reports_cnt,
                "denies": len(denials),
                "mentions": mentions_cnt,
                "sampled": len(evidence),
            },
            "figures_missing": figures_missing,
            "local_warehouse": local_warehouse_hits,
            "evidence": evidence,
            "denial_phrasing": denials,
            "primary_only": primary_only,
            "note": ("This is a map of the record, not a verdict. Counts and dates come "
                     "from the archive's full-text index; the quotes are what those "
                     "documents actually say at the matched page. Denial phrasing is "
                     "quoted verbatim and is not adjudicated. Heavy presence proves "
                     "circulation, not truth; absence in this index is not absence "
                     "from the world — check readable copies before concluding."),
        }

    async def readable_copies(
        self,
        query: Optional[str] = None,
        identifier: Optional[str] = None,
        limit: int = 25,
        check: int = 40,
    ) -> Dict[str, Any]:
        """Which copies of a work can actually be read.

        A catalogue search returns the best-scanning copy first, and that is
        often a lending-restricted one: the text cannot be downloaded and
        search-inside returns 403 anonymously. The readable copies are frequently
        older, rougher scans further down the list. Without this, the first hit
        looks like the only hit and a researcher concludes the work is
        unavailable — when 37 of 40 copies are open.
        """
        # An identifier anchors the report: the caller handed us a specific copy,
        # so report its state whatever its relevance rank, then find alternatives.
        anchor: Optional[Dict[str, Any]] = None
        if identifier:
            try:
                info = await self.availability(identifier)
                anchor = {
                    "identifier": identifier,
                    "state": info["state"],
                    "derivative": (info.get("derivative") or {}).get("format"),
                    "restricted": info.get("restricted"),
                    "evidence_url": f"https://archive.org/details/{identifier}",
                }
            except ArchiveError as err:
                anchor = {"identifier": identifier, "state": UNKNOWN, "reason": str(err)}
            if not query:
                meta = await self._metadata(identifier)
                title = ((meta.get("metadata") or {}).get("title") or identifier)
                query = f'title:("{title}")'

        if not query:
            raise ValueError("provide query= or identifier=")

        found = await self.client.search(query, rows=check,
                                         fields=["identifier", "title", "mediatype", "date", "collection"])
        groups: Dict[str, List[Dict[str, Any]]] = {
            SEARCHABLE: [], NO_DERIVATIVE: [], RESTRICTED: [], NOT_INDEXED: [], UNKNOWN: [],
        }
        for doc in found.get("docs", [])[:limit]:
            ident = doc.get("identifier")
            if not ident or (identifier and ident == identifier):
                continue
            try:
                info = await self.availability(ident)
                state = info["state"]
            except ArchiveError as err:
                state = UNKNOWN
                info = {"text_formats": [], "reason": str(err)}
            doc_cols = doc.get("collection") or []
            if isinstance(doc_cols, str):
                doc_cols = [doc_cols]
            t_tier, t_basis = grade_item(doc_cols)
            groups.setdefault(state, []).append({
                "identifier": ident,
                "title": doc.get("title"),
                "date": doc.get("date"),
                "tier": t_tier,
                "tier_basis": t_basis,
                "derivative": (info.get("derivative") or {}).get("format"),
                "evidence_url": f"https://archive.org/details/{ident}",
            })

        readable = groups.get(SEARCHABLE, [])
        return {
            "query": query,
            "anchor": anchor,
            "anchor_readable": bool(anchor and anchor.get("state") == SEARCHABLE) if anchor else None,
            "catalogue_total": found.get("numFound"),
            "checked": min(limit, len(found.get("docs", []))),
            "readable_count": len(readable),
            "restricted_count": len(groups.get(RESTRICTED, [])),
            "no_derivative_count": len(groups.get(NO_DERIVATIVE, [])),
            "readable": readable,
            "restricted": groups.get(RESTRICTED, []),
            "no_searchable_text": groups.get(NO_DERIVATIVE, []),
            "note": ("readable = a text derivative exists and the item is not "
                     "lending-restricted. Prefer these; a restricted copy cannot be "
                     "downloaded and refuses search-inside anonymously."),
        }

    async def search_inside_local(
        self,
        identifier: str,
        query: str,
        max_chars: int = 120_000,
        limit: int = 15,
    ) -> Dict[str, Any]:
        """Fallback: scan the local text derivative when the service cannot.

        Used for items with only ``DjVuTXT`` and no hOCR/Abbyy, which the
        per-item service cannot search. It is a plain substring scan with no
        page anchors, and it reads at most ``max_chars`` — so the coverage is
        always reported, and a truncated scan is never presented as a complete
        absence of matches.
        """
        result = await self.fetch_text(identifier, max_chars=max_chars)
        found = bool(result.get("found") and result.get("text"))
        chars_read = len(result.get("text", ""))
        truncated = bool(result.get("truncated"))

        matches: List[Dict[str, Any]] = []
        if found:
            lines = result["text"].splitlines()
            needle = query.lower()
            for idx, line in enumerate(lines, 1):
                if needle in line.lower():
                    window = lines[max(0, idx - 2):idx + 2]
                    matches.append({
                        "line": idx,
                        "text": " ".join(window).strip()[:180],
                        "page": None,
                        "page_url": f"https://archive.org/details/{identifier}",
                    })
                    if len(matches) >= limit:
                        break

        return {
            "identifier": identifier,
            "query": query,
            "source": "local_scan",
            "total_matches": len(matches),
            "coverage": {"complete": found and not truncated,
                         "chars_read": chars_read, "truncated": truncated},
            "matches": matches,
            "warning": (
                None if found and not truncated
                else ("text unavailable for this item" if not found
                      else f"searched only the first {chars_read} characters; a zero "
                           "result here does not mean the phrase is absent")
            ),
        }

    async def harvest_text(
        self,
        identifiers: Sequence[str],
        max_docs: Optional[int] = None,
        pause: float = 1.0,
        max_chars: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Download and store text for a list of items, once, politely.

        The only place that should do bulk downloads. Skips items already
        stored, so it is resumable. ``pause`` inserts a delay between downloads:
        bulk acquisition is where rate limits are hit.
        """
        harvested: List[Dict[str, Any]] = []
        already: List[str] = []
        failed: List[Dict[str, str]] = []
        budget = max_docs if max_docs is not None else len(identifiers)
        downloads = 0

        for ident in identifiers:
            if len(harvested) >= budget:
                break
            base = ident.split("|")[0]
            if self.store.has(base):
                already.append(base)
                continue
            try:
                result = await self.fetch_text(base, max_chars=max_chars)
            except ArchiveError as err:
                failed.append({"identifier": base, "reason": str(err)})
                continue
            if not result["found"]:
                failed.append({"identifier": base, "reason": result.get("reason", "no text")})
                continue
            harvested.append({"identifier": base, "chars": result["chars"],
                              "format": result["format"], "file": result["file"]})
            downloads += 1
            if pause and downloads < budget:
                await asyncio.sleep(pause)

        return {
            "harvested": harvested, "already_stored": already, "failed": failed,
            "store": self.store.stats(),
        }
