"""Tests for full-text transport (archivist.fulltext).

Offline: both live services are exercised through a stub client holding
recorded response shapes. The shapes come from live probes:

* ``POST be-api.us.archive.org/ia-pub-fts-api`` ->
  ``{"hits": {"total": int, "hits": [{"_source": {...}, "highlight": {...}}]}}``
* ``GET {server}/fulltext/inside.php`` ->
  ``{"indexed": true, "matches": [{"text": ..., "par": [{"page": n, ...}]}]}``
"""

import pytest

from archivist.client import ArchiveError, TokenBucketRateLimiter
from archivist.fulltext import (
    FullTextEngine,
    NO_DERIVATIVE,
    RESTRICTED,
    SEARCHABLE,
)

# --- recorded fixtures ------------------------------------------------------

METADATA_HOCR_ONLY = {
    "server": "ia601804.us.archive.org",
    "dir": "/22/items/example-item",
    "metadata": {"identifier": "example-item", "title": "Example Report"},
    "files": [
        {"name": "example-item.pdf", "format": "Text PDF"},
        {"name": "example-item_djvu.txt", "format": "DjVuTXT"},
        {"name": "example-item_hocr.html", "format": "hOCR"},
        {"name": "example-item_page_numbers.json", "format": "Page Numbers JSON"},
    ],
}

METADATA_NO_TEXT_DERIVATIVE = {
    "server": "ia900000.us.archive.org",
    "dir": "/1/items/image-only",
    "metadata": {"identifier": "image-only"},
    "files": [{"name": "image-only.pdf", "format": "Image Container PDF"}],
}

METADATA_RESTRICTED = {
    "server": "ia803202.us.archive.org",
    "dir": "/2/items/lending-item",
    "metadata": {
        "identifier": "lending-item",
        "access-restricted-item": "true",
        "collection": ["inlibrary", "printdisabled"],
    },
    "files": [{"name": "lending-item_hocr.html", "format": "hOCR"}],
}

FTS_RESPONSE = {
    "took": 42,
    "timed_out": False,
    "hits": {
        "total": 18317,
        "hits": [
            {
                "_id": "doc1",
                "_score": 7.5,
                "_source": {"identifier": "example-item", "title": "Example Report"},
                "highlight": {"text": ["continues to {{{throw dust in the eyes}}} of John Bull"]},
            }
        ],
    },
    "aggregations": {"collection": []},
}

INSIDE_RESPONSE = {
    "ia": "example-item",
    "q": "debt",
    "indexed": True,
    "matches": [
        {
            "text": "Centrelink\u2019s <IA_FTS_MATCH>Automated</IA_FTS_MATCH> "
                    "<IA_FTS_MATCH>Debt</IA_FTS_MATCH> Raising and Recovery System",
            "par": [
                {"l": 1459, "t": 1325, "r": 1640, "b": 1390,
                 "page": 0, "page_width": 2550, "page_height": 3301}
            ],
        }
    ],
}


class StubResponse:
    def __init__(self, payload, status_code=200, text="", content=b""):
        self._payload = payload
        self.status_code = status_code
        self.text = text
        self.content = content

    def json(self):
        return self._payload


class StubConfig:
    text_cache = False
    data_dir = "/tmp/archivist-test-cache"
    max_text_fetch_bytes = 4_000_000
    privacy_mode = True


class StubClient:
    """Minimal stand-in for ArchiveClient covering the surfaces fulltext uses."""

    def __init__(self, metadata=None, get_payload=None, post_payload=None,
                 num_found=0, text_files=None, get_responses=None):
        self._metadata = metadata or {}
        self._get_payload = get_payload or {}
        self._post_payload = post_payload or {}
        self._num_found = num_found
        self._text_files = text_files or {}
        self._get_responses = get_responses or {}
        self.config = StubConfig()
        self._search_limiter = TokenBucketRateLimiter(rate=1000.0)
        self.calls = []

    async def get_metadata(self, identifier):
        if identifier not in self._metadata:
            raise ArchiveError(f"no metadata for {identifier}")
        return self._metadata[identifier]

    async def search(self, query, **kwargs):
        return {"numFound": self._num_found, "docs": []}

    async def _request_with_retry(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        for fragment, response in self._get_responses.items():
            if fragment in url:
                return response
        if method == "GET":
            for identifier, blob in self._text_files.items():
                if f"/download/{identifier}/" in url:
                    return StubResponse({}, content=blob)
            return StubResponse(self._get_payload)
        return StubResponse(self._post_payload)


# --- discovery --------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_normalises_hits():
    client = StubClient(post_payload=FTS_RESPONSE)
    res = await FullTextEngine(client).search('"throw dust in the eyes"', size=5)
    assert res["total"] == 18317
    hit = res["hits"][0]
    assert hit["identifier"] == "example-item"
    assert hit["source"] == "fulltext_api"
    assert hit["evidence_url"] == "https://archive.org/details/example-item"
    assert "{{{" not in hit["snippets"][0]["text"]
    assert hit["snippets"][0]["text"].startswith("continues to throw dust")


@pytest.mark.asyncio
async def test_search_posts_expected_payload():
    client = StubClient(post_payload=FTS_RESPONSE)
    await FullTextEngine(client).search("crofters", size=3, scope="collection:americana")
    call = client.calls[-1]
    assert call["method"] == "POST"
    assert call["json"]["q"] == "crofters"
    assert call["json"]["size"] == "3"
    assert call["json"]["scroll"] == "false"
    assert call["json"]["scope"] == "collection:americana"


# --- availability -----------------------------------------------------------

@pytest.mark.asyncio
async def test_availability_searchable():
    client = StubClient(metadata={"example-item": METADATA_HOCR_ONLY})
    info = await FullTextEngine(client).availability("example-item")
    assert info["state"] == SEARCHABLE
    assert info["derivative"] == {"format": "hOCR", "doc": "example-item"}


@pytest.mark.asyncio
async def test_availability_no_derivative_is_distinct_from_no_matches():
    client = StubClient(metadata={"image-only": METADATA_NO_TEXT_DERIVATIVE})
    info = await FullTextEngine(client).availability("image-only")
    assert info["state"] == NO_DERIVATIVE
    assert info["derivative"] is None


@pytest.mark.asyncio
async def test_availability_restricted_wins_over_derivative():
    client = StubClient(metadata={"lending-item": METADATA_RESTRICTED})
    info = await FullTextEngine(client).availability("lending-item")
    assert info["state"] == RESTRICTED
    assert info["restricted"] is True


# --- pin-point --------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_inside_returns_page_and_bbox():
    client = StubClient(metadata={"example-item": METADATA_HOCR_ONLY},
                        get_payload=INSIDE_RESPONSE)
    res = await FullTextEngine(client).search_inside("example-item", "debt")
    m = res["matches"][0]
    assert m["page"] == 0
    assert m["bbox"]["left"] == 1459
    assert m["bbox"]["page_width"] == 2550
    assert m["page_url"].endswith("/page/n0")
    assert "<IA_FTS_MATCH>" not in m["text"]
    assert res["total_matches"] == 1


@pytest.mark.asyncio
async def test_search_inside_uses_derivative_basename_not_identifier():
    """The trap: `doc` must be the derivative basename, not the item id."""
    client = StubClient(metadata={"example-item": METADATA_HOCR_ONLY},
                        get_payload=INSIDE_RESPONSE)
    await FullTextEngine(client).search_inside("example-item", "debt")
    params = client.calls[-1]["params"]
    assert params["doc"] == "example-item"
    assert params["path"] == "/22/items/example-item"      # leading slash required
    assert params["item_id"] == "example-item"


@pytest.mark.asyncio
async def test_search_inside_raises_when_not_searchable():
    """A missing derivative must never be reported as 'no matches'."""
    client = StubClient(metadata={"image-only": METADATA_NO_TEXT_DERIVATIVE})
    with pytest.raises(ArchiveError) as exc:
        await FullTextEngine(client).search_inside("image-only", "anything")
    assert NO_DERIVATIVE in str(exc.value)


@pytest.mark.asyncio
async def test_search_inside_warns_on_boolean_syntax():
    client = StubClient(metadata={"example-item": METADATA_HOCR_ONLY},
                        get_payload=INSIDE_RESPONSE)
    res = await FullTextEngine(client).search_inside("example-item", "crofters OR government")
    assert "warning" in res and "boolean" in res["warning"]


# --- text acquisition -------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_text_prefers_djvu_and_reports_size():
    client = StubClient(metadata={"example-item": METADATA_HOCR_ONLY},
                        text_files={"example-item": b"hello world text"})
    res = await FullTextEngine(client).fetch_text("example-item")
    assert res["found"] is True
    assert res["format"] == "DjVuTXT"
    assert res["text"] == "hello world text"
    assert res["truncated"] is False
    assert "example-item_djvu.txt" in res["source_url"]


@pytest.mark.asyncio
async def test_fetch_text_raises_on_restricted():
    client = StubClient(metadata={"example-item": METADATA_HOCR_ONLY},
                        get_responses={"download": StubResponse({}, status_code=401)})
    with pytest.raises(ArchiveError) as exc:
        await FullTextEngine(client).fetch_text("example-item")
    assert "lending-restricted" in str(exc.value)


@pytest.mark.asyncio
async def test_fetch_text_reports_missing_derivative():
    client = StubClient(metadata={"image-only": METADATA_NO_TEXT_DERIVATIVE})
    res = await FullTextEngine(client).fetch_text("image-only")
    assert res["found"] is False
    assert "Image Container PDF" in res["available_formats"]


@pytest.mark.asyncio
async def test_fetch_text_second_call_is_served_from_store(tmp_path):
    client = StubClient(metadata={"example-item": METADATA_HOCR_ONLY},
                        text_files={"example-item": b"cached body text"})
    client.config.text_cache = True
    engine = FullTextEngine(client, cache_dir=tmp_path)

    first = await engine.fetch_text("example-item")
    assert first["cached"] is False
    downloads_before = len([c for c in client.calls if "/download/" in c["url"]])

    second = await engine.fetch_text("example-item")
    downloads_after = len([c for c in client.calls if "/download/" in c["url"]])
    assert second["cached"] is True
    assert second["text"] == "cached body text"
    assert downloads_after == downloads_before, "cache miss caused a second download"


@pytest.mark.asyncio
async def test_harvest_skips_already_stored(tmp_path):
    client = StubClient(metadata={"example-item": METADATA_HOCR_ONLY},
                        text_files={"example-item": b"body text"})
    client.config.text_cache = True
    engine = FullTextEngine(client, cache_dir=tmp_path)
    engine.store.put("example-item", "already here")

    report = await engine.harvest_text(["example-item"], pause=0)
    assert report["already_stored"] == ["example-item"]
    assert report["harvested"] == []
    assert len([c for c in client.calls if "/download/" in c["url"]]) == 0


@pytest.mark.asyncio
async def test_metadata_fetched_once_per_item():
    client = StubClient(metadata={"example-item": METADATA_HOCR_ONLY},
                        text_files={"example-item": b"body"})
    engine = FullTextEngine(client)
    calls = {"n": 0}
    real = client.get_metadata

    async def counting(identifier):
        calls["n"] += 1
        return await real(identifier)

    client.get_metadata = counting
    await engine.fetch_text("example-item")
    await engine.fetch_text("example-item")
    await engine.availability("example-item")
    assert calls["n"] == 1


# --- privacy ----------------------------------------------------------------

def test_user_agent_is_not_identifying():
    from archivist.config import ArchivistConfig
    cfg = ArchivistConfig()
    ua = cfg.user_agent
    assert "ArchivistEngine" not in ua
    assert "@" not in ua                      # no account handle or email
    assert "archive.org" not in ua            # no homepage/account URL
    assert cfg.privacy_mode is True


def test_user_agent_override_is_opt_in(monkeypatch):
    from archivist.config import ArchivistConfig
    monkeypatch.setenv("IA_USER_AGENT", "MyResearchBot/1.0")
    assert ArchivistConfig.load().user_agent == "MyResearchBot/1.0"


# --- API surface guard ------------------------------------------------------

def test_transport_surface_is_intact():
    import archivist.fulltext as ft
    for name in ("SEARCHABLE", "NO_DERIVATIVE", "RESTRICTED", "NOT_INDEXED"):
        assert hasattr(ft, name)
    for name in ("search", "search_iter", "availability", "search_inside",
                 "fetch_text", "harvest_text", "query_diagnostics", "split_terms"):
        assert hasattr(ft.FullTextEngine, name), f"FullTextEngine.{name} missing"
