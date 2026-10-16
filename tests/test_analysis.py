"""Tests for the analysis layer (archivist.analysis) over a real corpus."""

import pytest

from archivist.analysis import CorpusAnalysis, NULL_SEED
from archivist.corpus import CorpusDB

SHARED = (
    "the committee concluded that the operational recommendations should be "
    "implemented without further delay because the evidentiary record was "
    "complete and the statutory authority unambiguous"
)
EXTRA = (
    "this paragraph appears only in the fuller release and describes the "
    "ministerial direction that authorised the deviation from standing rules "
)
FILLER = "background material about administration and procedure in the department "
FILLER_B = "appendices concerning staffing levels and regional office accommodation "
UNRELATED = "gardeners plant tomatoes after the last frost and water deeply "


@pytest.fixture()
def corpus(tmp_path):
    db = CorpusDB(tmp_path / "corpus.duckdb")
    yield db
    db.close()


@pytest.fixture()
def analysis(corpus):
    return CorpusAnalysis(corpus)          # no engine: strictly offline


def test_reuse_of_a_document_reports_shared_shingles_against_null(corpus, analysis):
    corpus.add_document("source", FILLER * 20 + SHARED + FILLER * 20)
    corpus.add_document("copy", FILLER_B * 20 + SHARED + FILLER_B * 20)
    corpus.add_document("unrelated", UNRELATED * 40)

    res = analysis.reuse(identifier="source", min_tokens=20)
    assert res["mode"] == "document"
    assert res["results"], "the copy should be surfaced"
    top = res["results"][0]
    assert top["identifier"] == "copy"
    assert top["longest_run_tokens"] >= len(SHARED.split()) - 3
    assert top["shared_shingles"] > top["null_shingles"]
    assert "committee" in top["shared_passage"] or "operational" in top["shared_passage"]
    assert NULL_SEED == 20261005


def test_reuse_of_a_pasted_passage(corpus, analysis):
    corpus.add_document("copy", FILLER * 20 + SHARED + FILLER * 20)
    corpus.add_document("unrelated", UNRELATED * 40)

    res = analysis.reuse(text=SHARED, min_tokens=15)
    assert res["mode"] == "passage"
    assert [r["identifier"] for r in res["results"]] == ["copy"]
    assert res["results"][0]["longest_run_tokens"] >= len(SHARED.split()) - 3


def test_reuse_rejects_a_document_not_in_the_corpus(corpus, analysis):
    with pytest.raises(ValueError) as exc:
        analysis.reuse(identifier="absent")
    assert "not in the corpus" in str(exc.value)


@pytest.mark.asyncio
async def test_variants_confirmed_by_shared_text(corpus, analysis):
    corpus.add_document("source", FILLER * 20 + SHARED + FILLER * 20)
    corpus.add_document("sibling", FILLER_B * 20 + SHARED + FILLER_B * 20)
    corpus.add_document("unrelated", UNRELATED * 40)

    res = await analysis.variants("source", confirm_tokens=20, offline=True)
    ids = [v["identifier"] for v in res["variants"]]
    assert ids == ["sibling"]
    assert res["discovery"] == "local corpus"


@pytest.mark.asyncio
async def test_reconcile_ranks_base_and_reports_excisions(corpus, analysis):
    corpus.add_document("fuller", FILLER * 20 + SHARED + EXTRA + FILLER * 20)
    corpus.add_document("shorter", FILLER_B * 20 + SHARED + FILLER_B * 20)

    res = await analysis.reconcile("fuller", confirm_tokens=20, min_run=10, offline=True)
    assert res["base"] == "fuller"
    assert "largest complete text" in res["base_reason"]
    assert len(res["copies"]) == 2
    against = next(d for d in res["differences"] if d["against"] == "shorter")
    assert against["candidate_excisions"], "the extra paragraph should be reported"
    assert any("ministerial" in r["snippet"] or "authorised" in r["snippet"]
               for r in against["candidate_excisions"])
    assert any("page_numbers.json" in n for n in res["notes"])


@pytest.mark.asyncio
async def test_reconcile_reports_missing_document(corpus, analysis):
    res = await analysis.reconcile("absent", offline=True)
    assert "error" in res


@pytest.mark.asyncio
async def test_offline_analysis_makes_no_network_calls(corpus):
    class ExplodingEngine:
        store = None
        async def search(self, *a, **kw):
            raise AssertionError("offline analysis attempted a network call")

    analysis = CorpusAnalysis(corpus, ExplodingEngine())
    corpus.add_document("a", FILLER * 20 + SHARED + FILLER * 20)
    corpus.add_document("b", FILLER_B * 20 + SHARED + FILLER_B * 20)
    res = await analysis.variants("a", confirm_tokens=20, offline=True)
    assert [v["identifier"] for v in res["variants"]] == ["b"]


def test_public_surface_is_intact():
    for name in ("reuse", "variants", "reconcile", "ensure_local"):
        assert hasattr(CorpusAnalysis, name), f"CorpusAnalysis.{name} missing"
