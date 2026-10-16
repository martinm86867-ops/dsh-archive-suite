"""Tests for the pure text helpers (archivist.textutil)."""

from archivist.textutil import (
    build_triangulated_queries,
    content_tokens,
    distinctive_phrases,
    extract_subject_core,
    is_boolean_query,
    multi_region_probes,
    ngrams,
    ocr_quality,
    parse_highlights,
    tokenize,
)

SHARED = (
    "the committee concluded that the operational recommendations should be "
    "implemented without further delay because the evidentiary record was "
    "complete and the statutory authority unambiguous"
)


def test_tokenize_is_lowercase_alphanumeric():
    assert tokenize("Hello, World! 42 times") == ["hello", "world", "42", "times"]


def test_content_tokens_drops_stopwords_and_short_words():
    toks = content_tokens("the committee and its operational record")
    assert "the" not in toks and "and" not in toks and "its" not in toks
    assert "committee" in toks and "operational" in toks


def test_ngrams_windows():
    assert ngrams(["a", "b", "c", "d"], 3) == [("a", "b", "c"), ("b", "c", "d")]
    assert ngrams(["a", "b"], 3) == []


def test_is_boolean_query():
    assert is_boolean_query("crofters OR government")
    assert is_boolean_query("(a AND b)")
    assert not is_boolean_query("crofters government")


def test_parse_highlights_curly_markers():
    clean, spans = parse_highlights("continues to {{{throw dust in the eyes}}} of John Bull")
    assert clean == "continues to throw dust in the eyes of John Bull"
    assert spans == [(13, 35)]
    assert clean[spans[0][0]:spans[0][1]] == "throw dust in the eyes"


def test_parse_highlights_inside_markers():
    clean, spans = parse_highlights(
        "Centrelink\u2019s <IA_FTS_MATCH>Automated</IA_FTS_MATCH> Debt "
        "<IA_FTS_MATCH>Raising</IA_FTS_MATCH>"
    )
    assert "<IA_FTS_MATCH>" not in clean
    assert clean == "Centrelink\u2019s Automated Debt Raising"
    assert [clean[s:e] for s, e in spans] == ["Automated", "Raising"]


def test_parse_highlights_no_markers_and_empty():
    assert parse_highlights("no markers here") == ("no markers here", [])
    assert parse_highlights("") == ("", [])


def test_ocr_quality_ranks_clean_above_noisy():
    clean = ocr_quality("the commission examined the procurement records carefully")
    noisy = ocr_quality("th c m ss n x m n d th pr c r m nt r c rds " + "1 2 3 4 5 6 7 8 9")
    assert clean["score"] > noisy["score"]
    assert noisy["single_letter_ratio"] > clean["single_letter_ratio"]


def test_distinctive_phrases_prefers_moderate_length_words():
    phrases = distinctive_phrases(SHARED, count=2, n=6)
    assert 1 <= len(phrases) <= 2
    for p in phrases:
        assert len(p.split()) == 6
        assert not all(w in {"the", "that", "and", "was", "for"} for w in p.split())


def test_multi_region_probes_samples_the_whole_document():
    doc = (SHARED + " ") * 3 + ("appendix material concerning staffing and accommodation " * 40)
    probes = multi_region_probes(doc, regions=3, per_region=1, n=8)
    assert len(probes) >= 2
    assert len(set(probes)) == len(probes), "probes must be unique"


def test_extract_subject_core_strips_confirmation_bias_and_keeps_anchor():
    # Leading / biased query with "forgery OR plagiarized"
    q1 = '"Protocols of the Elders of Zion" forgery OR plagiarized'
    assert extract_subject_core(q1) == '"Protocols of the Elders of Zion"'

    # Open-ended evaluative question
    q2 = 'Does "The Protocols of the Elders of Zion" hold any merit?'
    assert extract_subject_core(q2) == '"The Protocols of the Elders of Zion"'

    # Unquoted biased claim
    q3 = "The Protocols of the Elders of Zion is an authentic document"
    core3 = extract_subject_core(q3)
    assert "protocols" in core3 and "elders" in core3 and "zion" in core3
    assert "authentic" not in core3


def test_build_triangulated_queries_generates_symmetric_axes():
    axes = build_triangulated_queries('Does "The Protocols of the Elders of Zion" hold any merit?')
    assert axes["core"] == '"The Protocols of the Elders of Zion"'
    assert "authentic" in axes["corroboration"]
    assert "forgery" in axes["critique"]
    assert "analysis" in axes["substance"]

