"""Tests for the access model, the FTS hit normaliser, and provenance grading.

Offline. The ``page_num`` payload shape is the one the live index actually
returns -- ``[[362]]``, nested one level deeper than a single unwrap reaches --
recorded from a field query against a lending-restricted item. The 401 body is
the literal nginx refusal those items serve.
"""

import pytest

from archivist.client import (
    ArchiveError,
    classify_status,
    OUTCOME_DENIED,
    OUTCOME_OK,
    OUTCOME_RATE_LIMITED,
    OUTCOME_REFUSED,
    OUTCOME_SERVER_ERROR,
)
from archivist.fulltext import (
    FullTextEngine,
    PAGE_EXACT,
    PAGE_ITEM_AGGREGATE,
    PAGE_UNKNOWN,
    RESTRICTED,
    _first_int,
    _page_scope,
    grade_item,
)


# --- HTTP outcome classification -------------------------------------------

@pytest.mark.parametrize("code,expected", [
    (200, OUTCOME_OK),
    (302, OUTCOME_OK),
    (429, OUTCOME_RATE_LIMITED),
    (503, OUTCOME_RATE_LIMITED),
    (401, OUTCOME_DENIED),
    (451, OUTCOME_DENIED),
    (500, OUTCOME_SERVER_ERROR),
    (502, OUTCOME_SERVER_ERROR),
])
def test_classify_status_direct(code, expected):
    assert classify_status(code) == expected


def test_lending_401_is_a_denial_not_a_rate_limit():
    """The live refusal is 401 with an nginx 'Authorization Required' body."""
    body = "<html><head><title>401 Authorization Required</title></head></html>"
    assert classify_status(401, {}, body) == OUTCOME_DENIED


def test_403_with_retry_after_is_throttling():
    assert classify_status(403, {"retry-after": "30"}, "") == OUTCOME_RATE_LIMITED


def test_403_with_a_denial_body_is_an_access_decision():
    assert classify_status(403, {}, "... lending restricted ...") == OUTCOME_DENIED


def test_403_without_evidence_is_refused_rather_than_asserted():
    """A bare 403 must not be reported as either cause: the status does not say."""
    assert classify_status(403, {}, "<html>403 Forbidden</html>") == OUTCOME_REFUSED


def test_classify_status_tolerates_a_missing_header_mapping():
    assert classify_status(403, None, "") == OUTCOME_REFUSED


# --- page anchors -----------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ([[362]], 362),
    ([362], 362),
    (362, 362),
    ([[[7]]], 7),
    ([], None),
    ("n[103]", None),
    (None, None),
])
def test_first_int_unwraps_nested_shapes(value, expected):
    assert _first_int(value) == expected


def test_restricted_page_is_an_item_aggregate():
    assert _page_scope(362, ["internetarchivebooks", "inlibrary"]) == PAGE_ITEM_AGGREGATE


def test_open_item_page_is_exact():
    assert _page_scope(20, ["arxiv"]) == PAGE_EXACT


def test_missing_page_is_unknown():
    assert _page_scope(None, ["arxiv"]) == PAGE_UNKNOWN


def test_normalise_keeps_the_page_but_withholds_an_anchor_it_cannot_support():
    """Regression: the page used to be dropped on every hit.

    ``page_num`` arrives as ``[[362]]``; ``int([362])`` used to raise, so the
    page was silently set to None and the deep link degraded for all hits.
    """
    raw = {
        "fields": {
            "identifier": ["observingsystems0000vonf"],
            "page_num": [[362]],
            "meta_collection": ["internetarchivebooks", "inlibrary", "printdisabled"],
            "meta_title": ["Observing systems"],
        },
        "highlight": {"text": ["a {{{second-order}}} concept"]},
    }
    hit = FullTextEngine._normalise_fts_hit(raw)
    assert hit["page"] == 362
    assert hit["page_scope"] == PAGE_ITEM_AGGREGATE
    # The page is reported, but not promoted to a citation anchor: for a
    # restricted item it is one aggregate page for the whole book.
    assert hit["evidence_url"] == "https://archive.org/details/observingsystems0000vonf"
    assert hit["tier"] == "T2"


def test_normalise_anchors_the_page_for_an_open_item():
    raw = {
        "fields": {
            "identifier": ["arxiv-1101.3377"],
            "page_num": [[20]],
            "meta_collection": ["arxiv"],
        },
        "highlight": {"text": ["is a Hecke {{{eigenform}}}"]},
    }
    hit = FullTextEngine._normalise_fts_hit(raw)
    assert hit["page"] == 20
    assert hit["page_scope"] == PAGE_EXACT
    assert hit["evidence_url"] == "https://archive.org/details/arxiv-1101.3377/page/n20"


def test_normalise_flattens_a_nested_collections_field():
    raw = {
        "fields": {
            "identifier": ["x"],
            "page_num": [[1]],
            "meta_collection": [["inlibrary", "printdisabled"]],
        },
        "highlight": {"text": ["text"]},
    }
    hit = FullTextEngine._normalise_fts_hit(raw)
    assert hit["tier"] == "T2"
    assert hit["page_scope"] == PAGE_ITEM_AGGREGATE


# --- tiering -----------------------------------------------------------------

@pytest.mark.parametrize("colls,tier", [
    (["usgovernmentdocuments"], "T1"),
    (["dtic"], "T1"),
    (["inlibrary", "printdisabled"], "T2"),
    (["americana"], "T3"),
    (["opensource"], "T4"),
    ([], "T5"),
])
def test_grade_item(colls, tier):
    got, basis = grade_item(colls)
    assert got == tier
    assert basis


def test_grade_item_basis_names_the_evidence_not_just_the_label():
    tier, basis = grade_item(["dtic"])
    assert tier == "T1"
    assert "dtic" in basis


# --- access model -----------------------------------------------------------

class StubConfig:
    text_cache = False
    data_dir = "/tmp/archivist-test-cache"
    max_text_fetch_bytes = 4_000_000
    privacy_mode = True


class StubResponse:
    def __init__(self, payload=None, status_code=200, text="", content=b""):
        self._payload = payload or {}
        self.status_code = status_code
        self.text = text
        self.content = content

    def json(self):
        return self._payload


class StubClient:
    def __init__(self, metadata=None, post_payload=None):
        self._metadata = metadata or {}
        self._post_payload = post_payload or {}
        self.config = StubConfig()
        self._search_limiter = None
        self.calls = []

    async def get_metadata(self, identifier):
        if identifier not in self._metadata:
            raise ArchiveError(f"no metadata for {identifier}")
        return self._metadata[identifier]

    async def _request_with_retry(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        if method == "POST":
            return StubResponse(self._post_payload)
        return StubResponse({})


RESTRICTED_META = {
    "server": "ia801006.us.archive.org",
    "dir": "/0/items/observingsystems0000vonf",
    "metadata": {
        "identifier": "observingsystems0000vonf",
        "collection": ["inlibrary", "printdisabled"],
    },
    "files": [
        {"name": "x_hocr.html", "format": "hOCR"},
        {"name": "x_djvu.txt", "format": "DjVuTXT"},
    ],
}

FTS_RESTRICTED = {
    "hits": {
        "total": 1,
        "hits": [{
            "fields": {
                "identifier": ["observingsystems0000vonf"],
                "page_num": [[362]],
                "meta_collection": ["inlibrary", "printdisabled"],
                "meta_title": ["Observing systems"],
            },
            "highlight": {"text": [
                "cybernetics of observed systems; Second order cybernetics: "
                "the cybernetics of observing systems"
            ]},
        }],
    },
}


@pytest.mark.asyncio
async def test_availability_separates_the_access_axes():
    """`restricted` must not read as 'unsearchable'."""
    client = StubClient(metadata={"observingsystems0000vonf": RESTRICTED_META})
    info = await FullTextEngine(client).availability("observingsystems0000vonf")
    assert info["state"] == RESTRICTED
    access = info["access"]
    assert access["lending"] is True
    assert access["derivative"] is False
    assert access["inside"] is False
    # Unknown, not False: this call cannot know whether the item is indexed,
    # and claiming False would be exactly the collapse being fixed.
    assert access["indexed"] is None


@pytest.mark.asyncio
async def test_quote_from_index_scopes_the_query_to_the_item():
    client = StubClient(post_payload=FTS_RESTRICTED)
    res = await FullTextEngine(client).quote_from_index(
        "observingsystems0000vonf", ["second order"], delay=0.0
    )
    assert res["passage_count"] == 1
    assert "observing systems" in res["passages"][0]["text"]
    assert res["item"]["page_scope"] == PAGE_ITEM_AGGREGATE
    # Scoped to the item rather than thrown at the whole corpus.
    assert client.calls[-1]["json"]["q"] == (
        'identifier:observingsystems0000vonf AND "second order"'
    )


@pytest.mark.asyncio
async def test_quote_from_index_requires_a_term():
    client = StubClient(post_payload={})
    with pytest.raises(ArchiveError):
        await FullTextEngine(client).quote_from_index("x", [], delay=0.0)


@pytest.mark.asyncio
async def test_search_inside_points_at_the_index_for_a_restricted_item():
    client = StubClient(metadata={"observingsystems0000vonf": RESTRICTED_META})
    with pytest.raises(ArchiveError) as err:
        await FullTextEngine(client).search_inside("observingsystems0000vonf", "observer")
    assert "quote_from_index" in str(err.value)
