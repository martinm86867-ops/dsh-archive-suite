"""Pure text helpers shared by the storage, transport and analysis layers.

These lived inside ``fulltext`` and were imported *from* it by other modules,
which is why ``fulltext`` could not be split and why a second, independent
fingerprint implementation grew inside ``corpus``. Keeping the pure functions
here lets each layer depend downward only:

    textutil   (pure functions; no I/O)
       ^  ^  ^
       |  |  |
  textstore  fulltext  corpus
                  ^        ^
                  |        |
               analysis ----+

Nothing in this module performs I/O or knows about Archive.org.
"""

from __future__ import annotations

import re
import zlib
from typing import Any, Dict, List, Sequence, Tuple

_TOKEN_RE = re.compile(r"[a-z0-9]+")

# Short list is enough: n-gram matching with n >= 6 is already selective, and
# the shuffled null baseline absorbs whatever common phrasing remains.
_STOPWORDS = frozenset(
    """a an the and or but if then than that this these those of to in on at by for with from
    as is are was were be been being it its his her their our your my not no nor so such we they
    he she you i will shall may might must can could would should do does did done have has had
    there here which who whom whose what when where why how all any both each few more most other
    some only own same too very just now also into over under again further once during out off
    about against between while because until upon within without""".split()
)

_HIGHLIGHT_RE = re.compile(
    r"\{\{\{(?P<curly>.*?)\}\}\}"
    r"|<IA_FTS_MATCH>(?P<ia>.*?)</IA_FTS_MATCH>",
    re.DOTALL,
)

# Boolean syntax that per-item search does not honour the way the cross-corpus
# API does. Verified: "crofters OR government" -> 0 matches on an item where
# "government" alone -> 50.
_BOOLEAN_RE = re.compile(r"\b(AND|OR|NOT)\b|[()]", re.IGNORECASE)


def tokenize(text: str) -> List[str]:
    """Lowercase alphanumeric tokens."""
    return _TOKEN_RE.findall(text.lower())


def content_tokens(text: str) -> List[str]:
    """Tokens with stopwords removed, for selecting distinctive phrases."""
    return [t for t in tokenize(text) if t not in _STOPWORDS and len(t) > 2]


def ngrams(tokens: Sequence[str], n: int) -> List[Tuple[str, ...]]:
    return [tuple(tokens[i:i + n]) for i in range(len(tokens) - n + 1)]


_MASK = (1 << 64) - 1
_BASE = 0x100000001B3


def shingle_hashes(tokens: Sequence[str], n: int = 8) -> List[int]:
    """Rolling polynomial hashes of every n-token window.

    Used where a shuffle has to be applied *before* shingling — the null model.
    Hashing the shingles again after shuffling the tokens is the only way the
    null can differ from the observation; shuffling a list of already-computed
    shingle hashes cannot change its multiset, which makes the comparison
    vacuous.
    """
    if len(tokens) < n:
        return []
    th = [zlib.crc32(t.encode("utf-8")) & 0xFFFFFFFF for t in tokens]
    base_pow = pow(_BASE, n - 1, 1 << 64)
    h = 0
    for i in range(n):
        h = ((h * _BASE) + th[i]) & _MASK
    out = [h]
    for i in range(n, len(th)):
        h = (((h - th[i - n] * base_pow) & _MASK) * _BASE + th[i]) & _MASK
        out.append(h)
    return out


# Evaluative and confirmation-bias words that a caller might inject into a
# claim or question ("is true", "holds merit", "forgery", "plagiarized",
# "authentic", "hoax", etc.). Stripping these yields the neutral subject core
# so the retrieval engine can probe BOTH corroborating and refuting axes
# without echoing the caller's framing bias.
_EVALUATIVE_NOISE = frozenset(
    """true false truth untruth authentic authenticity genuine real fake hoax
    forgery forgeries forged fabricate fabricated fabrication plagiarize
    plagiarized plagiarism copy copied merit merits valid validity credible
    credibility debunk debunked disprove disproven disproved discredit
    discredited myth myths misconception conspiracy theory theories proof
    prove proven verify verified fact check factcheck holds hold any does
    did whether indeed actually really claim claims claimed allege alleged
    allegation purports purported""".split()
)


def extract_subject_core(statement: str) -> str:
    """Extract the neutral entity/topic anchor from a potentially biased or open-ended query.

    1. If the caller quoted a multi-word work/entity title (e.g.
       ``"The Protocols of the Elders of Zion"``), preserve that quoted phrase
       as the primary anchor.
    2. Otherwise, strip evaluative/confirmation-bias words (``forgery``,
       ``authentic``, ``merit``, ``plagiarized``, ``hoax``, boolean operators)
       and keep the high-signal proper nouns and content tokens.
    """
    if not statement:
        return ""
    quoted = [q.strip() for q in re.findall(r'"([^"]+)"', statement) if len(q.strip()) >= 4]
    # Filter out single evaluative words that happened to be quoted
    neutral_quoted = [
        f'"{q}"' for q in quoted
        if not all(t in _EVALUATIVE_NOISE or t in _STOPWORDS for t in tokenize(q))
    ]
    # Also collect non-quoted content tokens that aren't evaluative noise
    unquoted_text = re.sub(r'"[^"]+"', " ", statement)
    unquoted_text = re.sub(r"\b(AND|OR|NOT)\b", " ", unquoted_text, flags=re.IGNORECASE)
    extra_toks = [
        t for t in content_tokens(unquoted_text)
        if t not in _EVALUATIVE_NOISE
    ]
    if neutral_quoted:
        # Keep the primary quoted anchor(s) plus up to 3 non-evaluative context tokens
        parts = list(neutral_quoted[:2])
        anchor_toks = set(tokenize(" ".join(neutral_quoted)))
        for t in extra_toks:
            if t not in anchor_toks and len(parts) < 4:
                parts.append(t)
        return " ".join(parts)

    if extra_toks:
        return " ".join(extra_toks[:6])
    toks = content_tokens(statement)
    return " ".join(toks[:6]) if toks else statement.strip()


def build_triangulated_queries(statement: str) -> Dict[str, str]:
    """Generate a symmetric 4-axis query plan from any user statement or question.

    Eliminates confirmation bias ('getting what you search for') by separating
    the query into:
      * ``core``: Neutral entity/topic anchor (what the record says overall)
      * ``corroboration``: Core + authenticity / primary-substance markers
      * ``critique``: Core + refutation / forgery / plagiarism / critique markers
      * ``substance``: Core + analytical / structural / historical-impact markers

    IMPORTANT: Archive.org's ``ia-pub-fts-api`` treats bare spaces as implicit
    AND and rejects parenthesized ``OR`` groups when combined with ``AND``.
    Therefore each axis pairs ``core`` with a single high-recall anchor term.
    """
    core = extract_subject_core(statement)
    return {
        "core": core,
        "corroboration": f"{core} authentic",
        "critique": f"{core} forgery",
        "substance": f"{core} analysis",
    }


def is_boolean_query(query: str) -> bool:
    return bool(_BOOLEAN_RE.search(query))


def parse_highlights(snippet: str) -> Tuple[str, List[Tuple[int, int]]]:
    """Strip match markers, returning clean text and match spans.

    The two services use different marker syntaxes for the same thing:

    * FTS API (``ia-pub-fts-api``) wraps matches in ``{{{...}}}``
    * inside.php wraps them in ``<IA_FTS_MATCH>...</IA_FTS_MATCH>``
    """
    if not snippet:
        return "", []

    out: List[str] = []
    spans: List[Tuple[int, int]] = []
    cursor = 0
    length = 0

    for m in _HIGHLIGHT_RE.finditer(snippet):
        plain = snippet[cursor:m.start()]
        out.append(plain)
        length += len(plain)
        start = length
        marked = m.group("curly") if m.group("curly") is not None else m.group("ia")
        marked = marked or ""
        out.append(marked)
        length += len(marked)
        spans.append((start, length))
        cursor = m.end()

    out.append(snippet[cursor:])
    return "".join(out), spans


def ocr_quality(text: str) -> Dict[str, Any]:
    """Heuristic OCR quality signals for comparing copies of a document.

    Proxies, not measurements. Useful for ranking two renderings of the same
    pages (a clean scan against a poor one), and misleading if used to compare
    different documents.
    """
    toks = tokenize(text)
    if not toks:
        return {"score": 0.0, "tokens": 0}
    single = sum(1 for t in toks if len(t) == 1)
    digits = sum(1 for t in toks if any(c.isdigit() for c in t))
    mean_len = sum(len(t) for t in toks) / len(toks)
    # Runs of very short tokens are typical of failed recognition.
    noisy = 0
    for i in range(len(toks) - 4):
        if all(len(t) <= 2 for t in toks[i:i + 5]):
            noisy += 1
    noisy_ratio = noisy / max(1, len(toks) - 4)
    score = max(
        0.0,
        min(1.0, 1.0 - (0.5 * (single / len(toks)) + 0.3 * noisy_ratio
                        + 0.2 * (1 if mean_len < 3 else 0))),
    )
    return {
        "score": round(score, 4),
        "tokens": len(toks),
        "single_letter_ratio": round(single / len(toks), 4),
        "digit_token_ratio": round(digits / len(toks), 4),
        "mean_token_length": round(mean_len, 2),
        "noisy_run_ratio": round(noisy_ratio, 4),
    }


def distinctive_phrases(text: str, count: int = 4, n: int = 6) -> List[str]:
    """Pick quotable, high-information phrases to probe a corpus with.

    Scoring prefers tokens of moderate length. Ranking by raw token length
    looked reasonable but performed badly in practice: the longest words in an
    OCR text are usually proper nouns and recognition artifacts, which differ
    between copies of the same document, so probes built from them match only
    the copy they came from.
    """
    tokens = tokenize(text)
    candidates: List[Tuple[float, Tuple[str, ...]]] = []
    seen: set = set()

    for gram in ngrams(tokens, n):
        if gram in seen:
            continue
        seen.add(gram)
        if all(t in _STOPWORDS or len(t) < 3 for t in gram):
            continue
        if any(len(t) > 14 for t in gram):      # likely an artifact or a name
            continue
        score = sum(6.0 - abs(len(t) - 6) for t in gram)
        candidates.append((score, gram))

    candidates.sort(key=lambda x: -x[0])

    chosen: List[str] = []
    used_positions: List[int] = []
    spans = {gram: i for i, gram in enumerate(ngrams(tokens, n))}
    for _score, gram in candidates:
        pos = spans.get(gram, 0)
        if any(abs(pos - s) < n for s in used_positions):
            continue
        chosen.append(" ".join(gram))
        used_positions.append(pos)
        if len(chosen) >= count:
            break
    return chosen


def multi_region_probes(text: str, regions: int = 3, per_region: int = 2, n: int = 8) -> List[str]:
    """Collect probe phrases from several parts of a document.

    A single probe set is a single point of failure: if the chosen passage sits
    in a region where two copies were recognised differently, that copy is
    never found. Sampling beginning, middle and end makes sibling discovery
    robust to localised OCR differences.
    """
    if not text:
        return []
    span = max(1, len(text) // regions)
    probes: List[str] = []
    seen: set = set()
    for r in range(regions):
        chunk = text[r * span:(r + 1) * span] if r < regions - 1 else text[r * span:]
        for phrase in distinctive_phrases(chunk, count=per_region, n=n):
            if phrase not in seen:
                seen.add(phrase)
                probes.append(phrase)
    return probes
