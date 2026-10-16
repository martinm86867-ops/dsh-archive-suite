"""Offline tests for P0, P1, and P2 upgrade implementations.

All tests are strictly offline using mock payloads matching empirical Archive.org shapes.
"""

import io
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from archivist.client import (
    ArchiveError,
    ArchiveClient,
    classify_status,
    OUTCOME_OK,
    OUTCOME_DENIED,
    OUTCOME_RATE_LIMITED,
    OUTCOME_REFUSED,
)
from archivist.config import ArchivistConfig, PatronSession
from archivist.fulltext import (
    FullTextEngine,
    cite,
    PAGE_EXACT,
    PAGE_ITEM_AGGREGATE,
    PAGE_UNKNOWN,
)
from archivist.search import SearchEngine
from archivist.references import ReferenceDrift


# --- P0.2 Citation Anchor Scoping -----------------------------------------

def test_cite_exact():
    url, label = cite("https://archive.org/details/my_book", 42, PAGE_EXACT)
    assert url == "https://archive.org/details/my_book/page/n42"
    assert label == "p. 42"


def test_cite_item_aggregate():
    """An item-aggregate page must never be turned into a leaf anchor."""
    url, label = cite("https://archive.org/details/my_book", 362, PAGE_ITEM_AGGREGATE)
    assert url == "https://archive.org/details/my_book"
    assert label is None


def test_cite_unknown():
    url, label = cite("https://archive.org/details/my_book", 10, PAGE_UNKNOWN)
    assert url == "https://archive.org/details/my_book"
    assert label is None


def test_cite_none_url():
    url, label = cite(None, 42, PAGE_EXACT)
    assert url is None
    assert label is None


# --- P0.1 Paging Integrity ------------------------------------------------

@pytest.mark.asyncio
async def test_iterate_search_paging_integrity_duplicate():
    """If advancedsearch repeats an identifier across pages, iterate_search raises."""
    mock_client = MagicMock()
    # Page 1 returns item_1; Page 2 also repeats item_1 (the deep paging bug)
    mock_client.search = AsyncMock(side_effect=[
        {"numFound": 2, "docs": [{"identifier": "item_1"}]},
        {"numFound": 2, "docs": [{"identifier": "item_1"}]},
    ])

    searcher = SearchEngine(mock_client)
    with pytest.raises(ArchiveError, match="paging integrity failure"):
        async for _ in searcher.iterate_search("test query", page_size=1, max_results=2):
            pass


# --- P1.1 Lending Status & PatronSession ----------------------------------

def test_patron_session_cookie():
    ps = PatronSession(cookie="logged-in-user=testuser; logged-in-sig=sig123", userid="testuser")
    assert ps.cookie_headers() == {"Cookie": "logged-in-user=testuser; logged-in-sig=sig123"}


@pytest.mark.asyncio
async def test_lending_status_anonymous(tmp_path):
    mock_client = MagicMock()
    mock_client.config = ArchivistConfig(data_dir=tmp_path)
    mock_client.get_metadata = AsyncMock(return_value={
        "server": "ia601006.us.archive.org",
        "dir": "/0/items/observingsystems0000vonf"
    })

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "data": {
            "lendingInfo": {
                "lendingStatus": {
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
                    "user_has_borrowed": False,
                    "userid": None
                },
                "loansUrl": ""
            }
        }
    }

    mock_session = MagicMock()
    mock_session.get = AsyncMock(return_value=mock_resp)
    mock_client.get_session = AsyncMock(return_value=mock_session)

    engine = FullTextEngine(mock_client, cache_dir=tmp_path)
    status = await engine.lending_status("observingsystems0000vonf")

    assert status["is_lendable"] is True
    assert status["available_to_browse"] is True
    assert status["available_to_borrow"] is False
    assert status["observed_as"] == "anonymous"


# --- P1.3 FTS Response Cache ----------------------------------------------

@pytest.mark.asyncio
async def test_fts_response_cache(tmp_path):
    mock_client = MagicMock()
    mock_client.config = ArchivistConfig(data_dir=tmp_path)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "hits": {
            "total": 1,
            "hits": [
                {
                    "_id": "test_item",
                    "fields": {"identifier": ["test_item"], "meta_title": ["Test Item"]},
                    "highlight": {"text": ["<<<term>>> snippet"]}
                }
            ]
        }
    }

    mock_client._request_with_retry = AsyncMock(return_value=mock_resp)
    mock_client._search_limiter = None

    engine = FullTextEngine(mock_client, cache_dir=tmp_path)

    # First call: network request
    res1 = await engine.search("test_query", size=5)
    assert res1.get("cached") is None or res1.get("cached") is False
    assert mock_client._request_with_retry.call_count == 1

    # Second call with same arguments: served from cache
    res2 = await engine.search("test_query", size=5)
    assert res2.get("cached") is True
    # Request count must remain 1
    assert mock_client._request_with_retry.call_count == 1


# --- P2.1 Classifier Unification ------------------------------------------

def test_references_classifier_unification():
    assert ReferenceDrift._live_state(200, None) == "alive"
    assert ReferenceDrift._live_state(404, None) == "dead"
    assert ReferenceDrift._live_state(403, None) == "refused"
    assert ReferenceDrift._live_state(429, None) == "refused"
    assert ReferenceDrift._live_state(503, None) == "refused"
    assert ReferenceDrift._live_state(None, "ConnectError") == "dead"


# --- P2.2 FTS Query Diagnostics -------------------------------------------

@pytest.mark.asyncio
async def test_query_diagnostics_narrowing_warning(tmp_path):
    mock_client = MagicMock()
    mock_client.config = ArchivistConfig(data_dir=tmp_path)

    engine = FullTextEngine(mock_client, cache_dir=tmp_path)
    # Mock _count_fts: compound query yields 0, individual terms yield hits
    async def mock_count(q: str):
        if "eigenform" in q and "self-reference" in q:
            return 0
        if "eigenform" in q:
            return 3010
        if "self-reference" in q:
            return 812
        return 0

    engine._count_fts = mock_count
    engine._count_metadata = AsyncMock(return_value=0)

    report = await engine.query_diagnostics('"eigenform" self-reference')
    assert report["fulltext_total"] == 0
    assert report["per_term"]["\"eigenform\""] == 3010
    assert report["per_term"]["self-reference"] == 812
    assert "warning" in report
    assert "matches 3010" in report["warning"]


# --- AI Agent Bridge & Ingestion Tests ------------------------------------

@pytest.mark.asyncio
async def test_agent_archive_bridge_offline(tmp_path):
    from archivist.research.agent import AgentArchiveBridge
    from archivist.research.warehouse import DuckDBWarehouse

    db_file = tmp_path / "agent_test.duckdb"
    wh = DuckDBWarehouse(db_file)
    mock_client = MagicMock()
    mock_client.config = ArchivistConfig(data_dir=tmp_path)

    bridge = AgentArchiveBridge(client=mock_client, warehouse=wh)

    # Ingest mock restricted work
    mock_meta = {
        "metadata": {
            "title": "Cybernetics",
            "date": "1948",
            "collection": ["inlibrary", "printdisabled"]
        }
    }
    bridge.engine._metadata = AsyncMock(return_value=mock_meta)
    bridge.engine.availability = AsyncMock(return_value={
        "state": "restricted",
        "access": {"lending": True, "derivative": False}
    })
    bridge.engine.quote_from_index = AsyncMock(return_value={
        "identifier": "cyberneticsorcon0000norb",
        "passage_count": 1,
        "item": {
            "tier": "T2",
            "tier_basis": "lending copy",
            "page_scope": "item_aggregate",
            "page": 246,
            "evidence_url": "https://archive.org/details/cyberneticsorcon0000norb"
        },
        "passages": [
            {"term": "feedback", "text": "excessive feedback is likely to be as serious a handicap...", "spans": [[0, 8]]}
        ],
        "caveat": "item_aggregate page"
    })

    res = await bridge.ingest_work("cyberneticsorcon0000norb", terms=["feedback"])
    assert res["status"] == "ingested"
    assert res["mode"] == "restricted_fts_mine"
    assert res["cards_ingested"] == 1

    # Query agent cards
    cards = bridge.query_agent_cards("feedback", limit=1)
    assert len(cards) == 1
    assert cards[0]["identifier"] == "cyberneticsorcon0000norb"
    assert "excessive feedback" in cards[0]["verbatim_text"]
    assert cards[0]["tier"] == "T2"

    prompt = bridge.format_agent_prompt("Explain feedback", cards)
    assert "cyberneticsorcon0000norb" in prompt
    assert "excessive feedback" in prompt

    wh.close()


# --- Persistent Metadata Cache, Artifact Registry & Auto-Borrow Tests -----

@pytest.mark.asyncio
async def test_metadata_cache_and_auto_borrow_caching(tmp_path):
    from archivist.textstore import TextStore

    store = TextStore(tmp_path / "cache")
    sample_meta = {
        "item_size": 102400,
        "metadata": {
            "title": "Observing Systems",
            "creator": "Heinz von Foerster",
            "date": "1984",
            "mediatype": "texts",
            "collection": ["inlibrary", "printdisabled"],
        },
        "files": [{"name": "obs_hocr.html", "format": "hOCR"}],
    }
    store.put_metadata("observingsystems0000vonf", sample_meta)
    cached_meta = store.get_metadata("observingsystems0000vonf")
    assert cached_meta is not None
    assert cached_meta["metadata"]["title"] == "Observing Systems"

    entries = store.metadata_entries()
    assert len(entries) == 1
    assert entries[0]["restricted"] == 1

    # Test auto_borrow_and_cache on a lending-restricted item
    mock_client = MagicMock()
    mock_client.config = ArchivistConfig(data_dir=tmp_path, text_cache=True, text_cache_dir=tmp_path / "cache")
    mock_client.get_metadata = AsyncMock(return_value=sample_meta)

    engine = FullTextEngine(mock_client, cache_dir=tmp_path / "cache")
    engine.lending_status = AsyncMock(return_value={
        "identifier": "observingsystems0000vonf",
        "observed_as": "anonymous",
        "is_lendable": True,
        "available_to_browse": True,
        "available_to_borrow": False,
        "loan_held": False,
    })
    engine.quote_from_index = AsyncMock(return_value={
        "identifier": "observingsystems0000vonf",
        "passage_count": 2,
        "item": {"tier": "T2", "page_scope": "item_aggregate", "page": 362},
        "passages": [
            {"term": "cybernetics", "text": "Second order cybernetics is the cybernetics of observing systems and self-reference."},
            {"term": "eigenform", "text": "Objects are tokens for eigenbehaviors in recursive observing systems."},
        ],
    })

    borrowed = await engine.auto_borrow_and_cache("observingsystems0000vonf", terms=["cybernetics", "eigenform"])
    assert borrowed["status"] == "fts_borrowed_and_cached"
    assert borrowed["passage_count"] == 2
    assert store.has("observingsystems0000vonf") is True

    # Verify artifact was recorded
    arts = store.artifacts(identifier="observingsystems0000vonf")
    assert len(arts) == 1
    assert arts[0]["kind"] == "borrowed_fts_digest"

    # Second call serves directly from local cache without network FTS query
    borrowed_2 = await engine.auto_borrow_and_cache("observingsystems0000vonf")
    assert borrowed_2["status"] == "cached"
    assert borrowed_2["cached"] is True

    # Discard removes text, metadata, and artifact records
    store.discard("observingsystems0000vonf")
    assert store.has("observingsystems0000vonf") is False
    assert store.get_metadata("observingsystems0000vonf") is None
    assert len(store.artifacts(identifier="observingsystems0000vonf")) == 0


