"""Tests for the columnar corpus database (archivist.corpus).

Offline: a temporary DuckDB file, synthetic documents, no network.
"""

import pytest

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
# A different filler for the second copy, so the only long shared run is the
# passage under test rather than the whole document.
FILLER_B = "appendices concerning staffing levels and regional office accommodation "


@pytest.fixture()
def db(tmp_path):
    corpus = CorpusDB(tmp_path / "corpus.duckdb")
    yield corpus
    corpus.close()


def test_build_derives_tokens_shingles_fingerprints(db):
    db.add_document("doc-a", FILLER * 30 + SHARED)
    stats = db.stats()
    assert stats["documents"] == 1
    assert stats["tokens"] > 100
    # one shingle per token, minus the final n-1 windows
    assert stats["shingles"] < stats["tokens"]
    assert stats["fingerprints"] > 0
    # fingerprints are banded minima, so far fewer than shingles
    assert stats["fingerprints"] < stats["shingles"]


def test_similar_ranks_sibling_first_and_never_exceeds_one(db):
    db.add_document("source", FILLER * 30 + SHARED)
    db.add_document("copy", FILLER * 30 + SHARED)
    db.add_document("unrelated", "gardeners plant tomatoes after the last frost " * 20)

    ranked = db.similar("source")
    assert ranked, "expected at least one candidate"
    assert ranked[0]["identifier"] == "copy"
    assert ranked[0]["containment"] <= 1.0
    assert all(r["containment"] <= 1.0 for r in ranked)


def test_shared_runs_finds_contiguous_shared_passage(db):
    db.add_document("a", FILLER * 20 + SHARED + FILLER * 20)
    db.add_document("b", FILLER_B * 20 + SHARED + FILLER_B * 20)

    runs = db.shared_runs("a", "b", min_tokens=20)
    assert runs, "identical copies must share a run"
    assert runs[0]["length_tokens"] >= len(SHARED.split()) - 3
    assert "committee" in runs[0]["snippet"] or "operational" in runs[0]["snippet"]


def test_shared_runs_survives_a_gap_elsewhere(db):
    """The bug this guards: one diagonal spans the document, so a single gap
    must not destroy the whole run."""
    db.add_document("a", FILLER * 20 + SHARED + EXTRA + FILLER * 20)
    db.add_document("b", FILLER_B * 20 + SHARED + FILLER_B * 20)

    runs = db.shared_runs("a", "b", min_tokens=20)
    assert runs, "a gap elsewhere must not erase the shared run"
    assert runs[0]["length_tokens"] >= len(SHARED.split()) - 3


def test_diff_runs_finds_the_excision(db):
    db.add_document("fuller", FILLER * 20 + SHARED + EXTRA + FILLER * 20)
    db.add_document("shorter", FILLER * 20 + SHARED + FILLER * 20)

    diffs = db.diff_runs("fuller", "shorter", min_run=10)
    assert diffs, "the extra paragraph should be reported"
    assert any("ministerial" in d["snippet"] or "authorised" in d["snippet"] for d in diffs)


def test_phrase_search_is_positional_not_bag_of_words(db):
    db.add_document("doc", FILLER * 5 + "the quick brown fox jumps over the lazy dog")
    assert db.phrase_search("quick brown fox"), "exact phrase should match"
    assert db.phrase_search("brown quick fox") == [], "reordering must not match"


def test_reingest_replaces_rather_than_duplicates(db):
    db.add_document("doc", FILLER * 10)
    before = db.stats()["tokens"]
    db.add_document("doc", FILLER * 10)
    assert db.stats()["tokens"] == before
    assert db.stats()["documents"] == 1


def test_build_from_store(tmp_path):
    from archivist.textstore import TextStore
    store = TextStore(tmp_path / "store")
    store.put("one", FILLER * 20 + SHARED)
    store.put("two", FILLER * 20 + SHARED + EXTRA)

    db = CorpusDB(tmp_path / "corpus.duckdb")
    res = db.build_from_store(store)
    assert res["documents"] == 2
    assert db.stats()["documents"] == 2
    runs = db.shared_runs("one", "two", min_tokens=20)
    assert runs
    db.close()
