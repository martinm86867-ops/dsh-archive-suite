"""Analysis over the local corpus.

One analysis engine, not two. Previously ``fulltext`` carried a Python
implementation of reuse detection, variant discovery and reconciliation, while
``corpus`` carried a SQL one over the same data — the same job done twice, with
independent and mutually incompatible similarity sketches. Everything analytical
now lives here and runs on the corpus.

What stays in Python is only what SQL is genuinely bad at: the shuffled null
baseline that separates substantive shared phrasing from shared idiom. It is
computed over the corpus's own shingle hashes, so the statistic and its null
come from the same representation.

Network access (discovery by full-text probes, harvesting) is delegated to
``FullTextEngine`` and is entirely optional: with ``offline=True`` nothing here
touches the network.
"""

from __future__ import annotations

import logging
import random
from typing import Any, Dict, List, Optional, Sequence

from .corpus import CorpusDB
from .fulltext import FullTextEngine
from .textutil import multi_region_probes, shingle_hashes, tokenize

logger = logging.getLogger("archivist.analysis")

NULL_SEED = 20261005


def _label_counts(runs) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for r in runs:
        label = r.get("label", "unlabelled")
        counts[label] = counts.get(label, 0) + 1
    return counts


class CorpusAnalysis:
    """Reuse, variants and reconciliation, computed over the local corpus."""

    def __init__(self, corpus: CorpusDB, engine: Optional[FullTextEngine] = None):
        self.corpus = corpus
        self.engine = engine

    # -- preparation -------------------------------------------------------

    async def ensure_local(self, identifiers: Sequence[str], pause: float = 1.0) -> Dict[str, Any]:
        """Harvest any missing text (network) and ingest everything into the corpus.

        This is the only place analysis reaches the network, and it is explicit.
        """
        if self.engine is None:
            return {"harvested": [], "built": 0, "note": "no engine; offline only"}

        missing = [i for i in identifiers if not self.engine.store.has(i)]
        harvested = []
        if missing:
            report = await self.engine.harvest_text(missing, pause=pause)
            harvested = report["harvested"]
        built = self.corpus.build_from_store(self.engine.store)
        return {"harvested": harvested, "built": built["documents"]}

    def _shingle_list(self, identifier: str) -> List[int]:
        return [r[0] for r in self.corpus.sql(
            "SELECT hash FROM shingles WHERE identifier = ?", [identifier]
        )]

    def _overlap_and_null(self, text_a: str, text_b: str) -> tuple:
        """(observed, null) shared shingles between two texts.

        The null shuffles the *tokens* of b and re-shingles. It must be done
        before shingling: shuffling an existing list of shingle hashes cannot
        change its multiset, so that comparison always returns observed == null
        and measures nothing.
        """
        n = self.corpus.n
        a_sh = shingle_hashes(tokenize(text_a), n)
        b_sh = shingle_hashes(tokenize(text_b), n)
        if not a_sh or not b_sh:
            return 0, 0

        pool = set(a_sh)
        observed = sum(1 for h in b_sh if h in pool)

        shuffled = tokenize(text_b)
        random.Random(NULL_SEED).shuffle(shuffled)
        null_pool = set(shingle_hashes(shuffled, n))
        null = sum(1 for h in a_sh if h in null_pool)
        return observed, null

    # -- reuse -------------------------------------------------------------

    def reuse(
        self,
        text: Optional[str] = None,
        identifier: Optional[str] = None,
        min_tokens: int = 20,
        candidates: int = 8,
    ) -> Dict[str, Any]:
        """Documents sharing phrasing with a document or a pasted passage."""
        if text is not None:
            found = self.corpus.reuse_of_text(text, min_tokens=min_tokens, candidates=candidates)
            return {
                "mode": "passage",
                "results": [
                    {
                        "identifier": r["identifier"],
                        "evidence_url": f"https://archive.org/details/{r['identifier']}",
                        "longest_run_tokens": r["longest_run_tokens"],
                        "shared_passage": r["runs"][0]["snippet"],
                        "runs": r["runs"],
                    }
                    for r in found
                ],
                "min_tokens": min_tokens,
            }

        if not identifier:
            raise ValueError("provide text= or identifier=")
        source = self.corpus.text(identifier)
        if source is None:
            raise ValueError(f"{identifier} is not in the corpus; ingest or harvest it first")

        target = self.corpus.text(identifier)
        results: List[Dict[str, Any]] = []
        for row in self.corpus.similar(identifier, limit=candidates):
            other = row["identifier"]
            runs = self.corpus.shared_runs(identifier, other, min_tokens=min_tokens, limit=3)
            if not runs:
                continue
            other_text = self.corpus.text(other) or ""
            observed, null = self._overlap_and_null(target, other_text)
            results.append({
                "identifier": other,
                "evidence_url": f"https://archive.org/details/{other}",
                "shared_shingles": observed,
                "null_shingles": null,
                "excess_over_null": observed - null,
                "longest_run_tokens": max(r["length_tokens"] for r in runs),
                "shared_passage": runs[0]["snippet"],
                "containment": row["containment"],
                "runs": runs,
            })

        results.sort(key=lambda r: (-r["longest_run_tokens"], -r["excess_over_null"]))
        return {
            "mode": "document",
            "source": identifier,
            "min_tokens": min_tokens,
            "null_model": f"shuffled candidate shingles, seed {NULL_SEED}",
            "results": results,
            "note": "observed > null indicates substantive shared phrasing; "
                    "observed ~= null indicates shared idiom only",
        }

    # -- forensic comparison & adversarial audit ---------------------------

    def forensic_compare(
        self,
        identifier_a: str,
        identifier_b: str,
        window_tokens: int = 250,
        step_tokens: int = 125,
        top_passages: int = 8,
    ) -> Dict[str, Any]:
        """Direct primary-text forensic comparison between two documents in the corpus.

        Why 8-gram shingles alone fail on translated or adapted works:
        Two independent translations of the same source (e.g. a Russian
        adaptation of a French work, then translated into English by two
        different translators) rarely share 8-word verbatim strings, yet share
        dense clusters of distinctive content vocabulary in aligned windows.
        Conversely, an asserted 'plagiarism' between two same-language works
        (e.g. Joly 1864 vs. Sue 1849 in French) can be tested directly by
        measuring whether any shared 8-grams or content-token windows exceed
        the shuffled null baseline (excluding digitization boilerplate like
        Google Books headers).
        """
        text_a = self.corpus.text(identifier_a)
        text_b = self.corpus.text(identifier_b)
        if text_a is None:
            raise ValueError(f"{identifier_a} is not in the corpus; harvest it first")
        if text_b is None:
            raise ValueError(f"{identifier_b} is not in the corpus; harvest it first")

        # Strip common Google Books digitization header boilerplate (~first 3500 chars if present)
        def _strip_digitization_header(txt: str) -> str:
            for marker in ("Digitized by Google", "books.google.com"):
                idx = txt.find(marker)
                if 0 <= idx < 5000:
                    txt = txt[idx + len(marker):]
            return txt

        clean_a = _strip_digitization_header(text_a)
        clean_b = _strip_digitization_header(text_b)

        observed_sh, null_sh = self._overlap_and_null(clean_a, clean_b)
        exact_runs = self.corpus.shared_runs(identifier_a, identifier_b, min_tokens=10, limit=5)
        # Filter out runs that are purely Google Books boilerplate
        exact_runs = [
            r for r in exact_runs
            if "google is proud to partner" not in r["snippet"]
            and "programme google recherche" not in r["snippet"]
        ]

        # Sliding-window content-token overlap (catches translated / paraphrased parallels)
        from .textutil import content_tokens
        toks_a = content_tokens(clean_a)
        toks_b = content_tokens(clean_b)

        # Build inverse document frequency / rarity filter across the two texts.
        # Filter out 3-letter OCR fragments and ensure words appear in a reasonable
        # frequency band in BOTH texts so neither stopword leaks nor 1-off typos dominate.
        from collections import Counter
        freq_a = Counter(toks_a)
        freq_b = Counter(toks_b)
        windows_b = []
        for i in range(0, max(1, len(toks_b) - window_tokens + 1), step_tokens):
            win = toks_b[i:i + window_tokens]
            if len(win) < 30:
                continue
            windows_b.append((
                i,
                set(w for w in win if len(w) >= 4 and freq_b[w] <= 80 and freq_a.get(w, 0) <= 80),
                " ".join(win[:45]),
            ))

        aligned_windows = []
        if windows_b and len(toks_a) >= 30:
            for ia in range(0, max(1, len(toks_a) - window_tokens + 1), step_tokens):
                win_a = toks_a[ia:ia + window_tokens]
                if len(win_a) < 30:
                    continue
                set_a = set(w for w in win_a if len(w) >= 4 and freq_b.get(w, 0) <= 80 and freq_a[w] <= 80)
                if len(set_a) < 8:
                    continue
                best_j = -1
                best_shared = set()
                for jb, set_b, snip_b in windows_b:
                    common = set_a & set_b
                    if len(common) > len(best_shared):
                        best_shared = common
                        best_j = jb
                        best_snip_b = snip_b
                if len(best_shared) >= 7:
                    jaccard = len(best_shared) / max(1, len(set_a | set_b))
                    aligned_windows.append({
                        "pos_a": ia,
                        "pos_b": best_j,
                        "shared_distinctive_terms": len(best_shared),
                        "terms": sorted(best_shared)[:12],
                        "jaccard": round(jaccard, 4),
                        "excerpt_a": " ".join(win_a[:40]),
                        "excerpt_b": best_snip_b,
                    })

        aligned_windows.sort(key=lambda w: (-w["shared_distinctive_terms"], -w["jaccard"]))
        # Deduplicate overlapping windows
        dedup_windows = []
        used_a = []
        for w in aligned_windows:
            if any(abs(w["pos_a"] - ua) < step_tokens for ua in used_a):
                continue
            used_a.append(w["pos_a"])
            dedup_windows.append(w)
            if len(dedup_windows) >= top_passages:
                break

        return {
            "identifier_a": identifier_a,
            "identifier_b": identifier_b,
            "chars_a": len(clean_a),
            "chars_b": len(clean_b),
            "tokens_a": len(toks_a),
            "tokens_b": len(toks_b),
            "exact_8gram_shingles": {
                "observed": observed_sh,
                "shuffled_null": null_sh,
                "excess_over_null": observed_sh - null_sh,
            },
            "verbatim_runs": exact_runs,
            "conceptual_window_alignments": dedup_windows,
            "interpretation": (
                "exact_8gram_shingles measures same-language verbatim copying (after stripping "
                "Google Books headers); conceptual_window_alignments measures dense co-occurrence "
                "of distinctive content vocabulary across 250-token windows, which survives "
                "translation and paraphrase."
            ),
        }

    # -- variants ----------------------------------------------------------

    async def variants(
        self,
        identifier: str,
        confirm_tokens: int = 40,
        offline: bool = True,
        max_candidates: int = 8,
        pause: float = 1.0,
    ) -> Dict[str, Any]:
        """Sibling uploads of the same work.

        Copies are not duplicates to be collapsed: they differ in completeness,
        redaction and OCR quality, and the differences are the information.
        """
        provenance = "local corpus"
        if not offline:
            if self.engine is None:
                raise ValueError("offline=False requires a FullTextEngine")
            source_text = self.corpus.text(identifier)
            if source_text is None:
                raise ValueError(f"{identifier} is not in the corpus; ingest it first")
            probes = multi_region_probes(source_text, regions=3, per_region=2, n=8)
            if probes:
                found = await self.engine.search(" OR ".join(f'"{p}"' for p in probes), size=30)
                candidates = [h["identifier"].split("|")[0] for h in found["hits"]]
                await self.ensure_local(candidates, pause=pause)
                provenance = "full-text probes + local corpus"

        copies: List[Dict[str, Any]] = []
        for row in self.corpus.similar(identifier, limit=max_candidates):
            other = row["identifier"]
            runs = self.corpus.shared_runs(identifier, other, min_tokens=confirm_tokens, limit=1)
            if not runs:
                continue
            copies.append({
                "identifier": other,
                "containment": row["containment"],
                "longest_run_tokens": runs[0]["length_tokens"],
                "shared_passage": runs[0]["snippet"],
            })

        return {
            "identifier": identifier,
            "discovery": provenance,
            "confirm_tokens": confirm_tokens,
            "variants": copies,
        }

    # -- reconciliation ----------------------------------------------------

    async def reconcile(
        self,
        identifier: str,
        confirm_tokens: int = 40,
        min_run: int = 25,
        offline: bool = True,
        max_candidates: int = 8,
        pause: float = 1.0,
    ) -> Dict[str, Any]:
        """Compare the copies of one work and report what each adds or omits.

        Regions present in one copy and absent in another are *candidate
        excisions*: redaction, truncation and OCR failure all produce them and
        cannot be distinguished from the text alone.
        """
        found = await self.variants(
            identifier, confirm_tokens=confirm_tokens, offline=offline,
            max_candidates=max_candidates, pause=pause,
        )
        ids = [identifier] + [v["identifier"] for v in found["variants"]]

        rows = self.corpus.sql(
            """SELECT identifier, chars, tokens, ocr_quality, pages, source_url
               FROM documents WHERE identifier IN ({})""".format(",".join(["?"] * len(ids))),
            ids,
        )
        copies = [
            {"identifier": r[0], "chars": r[1], "tokens": r[2],
             "ocr_quality": r[3], "pages": r[4], "source_url": r[5]}
            for r in rows
        ]
        copies.sort(key=lambda c: (-c["chars"], -(c["ocr_quality"] or 0)))
        if not copies:
            return {"error": f"{identifier} is not in the corpus"}

        base = copies[0]["identifier"]
        differences = []
        for other in copies[1:]:
            # classify_differences labels each region from its structural
            # signature instead of assuming the cause is undecidable.
            only_in_base = self.corpus.classify_differences(base, other["identifier"], min_run=min_run)
            only_in_other = self.corpus.classify_differences(other["identifier"], base, min_run=min_run)
            differences.append({
                "against": other["identifier"],
                "base_only_tokens": sum(r["length_tokens"] for r in only_in_base),
                "variant_only_tokens": sum(r["length_tokens"] for r in only_in_other),
                "candidate_excisions": only_in_base,
                "candidate_additions": only_in_other,
                "base_labels": _label_counts(only_in_base),
                "variant_labels": _label_counts(only_in_other),
            })

        return {
            "identifier": identifier,
            "discovery": found["discovery"],
            "copies": copies,
            "base": base,
            "base_reason": (f"largest complete text ({copies[0]['chars']} chars, "
                            f"OCR quality {copies[0]['ocr_quality']})"),
            "differences": differences,
            "notes": [
                "Copies of one work differ in completeness, redaction, recognition and edition; "
                "the differences are information, not duplication to be collapsed.",
                "Each difference is labelled from its structural signature: truncation when the "
                "aligned position runs past the end of the other copy; excision when the alignment "
                "shifts by the length of the gap; ocr_divergence when the alignment is unchanged and "
                "the text is noisy; revision when the alignment is unchanged and the text is coherent. "
                "Verify a label against the page images before relying on it.",
                "differences are reported at token offsets, not page numbers: the "
                "archive's page_numbers.json carries no character offsets.",
            ],
        }

    # -- autonomous one-shot epistemic dossier ------------------------------

    async def epistemic_dossier(
        self,
        statement: str,
        compare_with: Optional[str] = None,
        focus_terms: Optional[Sequence[str]] = None,
        max_harvest: int = 2,
    ) -> Dict[str, Any]:
        """Execute the complete 5-stage epistemic & primary-text forensic pipeline in one call.

        Why this exists:
        Giving an LLM 42 granular primitives ('104 keys on a typewriter') forces
        the agent to run 8 to 15 sequential tool calls, burning millions of
        context tokens on raw 6,000-character OCR slices, file lists, and
        intermediate search outputs. Worse, if the agent stops after a search
        snippet, it falls prey to secondary-source confirmation bias.

        ``epistemic_dossier`` codifies the entire methodology server-side in a
        single deterministic pass and returns a compact (~1,200-token) verified
        briefing:
          Stage 1 (Bias-Stripped 4-Axis Sweep): Extracts the neutral subject
            core, queries Corroboration, Critique, Substance, and T1 Primary
            Records concurrently, and discovers the top canonical editions.
          Stage 2 (Autonomous Primary-Text Acquisition): Harvests the top open
            canonical edition(s) (plus ``compare_with`` if specified) directly
            into ``corpus.duckdb``; if the top work is lending-restricted,
            automatically mines verbatim passages via field-scoped FTS.
          Stage 3 (Edition Diff & Forensic Shingle/Window Audit): Automatically
            runs ``classify_differences`` (to surface added postscripts,
            confessions, or excisions across editions) and ``forensic_compare``
            when two editions or a ``compare_with`` source are present.
          Stage 4 (High-Signal Primary-Text Extraction): Extracts concentrated,
            boilerplate-stripped primary-text windows around the work's highest-
            density conceptual regions and ``focus_terms`` so the agent reads
            the actual substance of the work without dumping 100k chars into
            context.
          Stage 5 (Local Warehouse & Provenance Synthesis): Queries the local
            DuckDB warehouse and packages SHA-1 / SHA-256 host-independent
            citations.
        """
        if self.engine is None:
            raise ValueError("epistemic_dossier requires a FullTextEngine instance")

        import re
        from .textutil import content_tokens, distinctive_phrases

        # Stage 1: 4-Axis Triangulated Claim Trace + Canonical Edition Discovery
        trace = await self.engine.claim_trace(statement, size=6, triangulate=True)
        tri = trace.get("triangulation") or {}
        core_query = trace.get("subject_core") or statement
        canonical = tri.get("canonical_editions") or []
        core_toks = set(content_tokens(core_query))

        # Prioritize: (1) local warehouse / local corpus works whose identifier or title
        # directly matches the subject core, (2) catalog canonical editions (`title:(...)`),
        # and (3) top FTS evidence hits.
        candidate_ids: List[str] = []
        for wh_hit in trace.get("local_warehouse") or []:
            wh_id = (wh_hit.get("identifier") or "").strip()
            if wh_id.endswith(".txt"):
                wh_id = wh_id[:-4]
            if wh_id and wh_id not in candidate_ids and (self.corpus.has(wh_id) or self.engine.store.has(wh_id)):
                candidate_ids.append(wh_id)

        try:
            for (doc_id,) in self.corpus.sql("SELECT identifier FROM documents"):
                id_toks = set(content_tokens(doc_id.replace("-", " ").replace("_", " ")))
                if core_toks and len(core_toks & id_toks) >= max(2, len(core_toks) // 2):
                    if doc_id not in candidate_ids:
                        candidate_ids.append(doc_id)
        except Exception:
            pass

        for c in canonical:
            ident = c.get("identifier")
            if ident and ident not in candidate_ids:
                candidate_ids.append(ident)
        for ev in trace.get("evidence") or []:
            ident = ev.get("identifier")
            if ident and ident not in candidate_ids:
                candidate_ids.append(ident)

        # Stage 2: Autonomous Primary-Text Acquisition (Open Harvest or Restricted FTS Quote)
        harvested_ids: List[str] = []
        restricted_quotes: List[Dict[str, Any]] = []
        primary_excerpts: List[Dict[str, Any]] = []

        probe_terms = list(focus_terms) if focus_terms else []
        if not probe_terms:
            toks = [t for t in content_tokens(statement) if t not in core_toks]
            probe_terms = toks[:4] or list(core_toks)[:3] or ["introduction", "conclusion", "summary"]

        for ident in candidate_ids[:max_harvest + 4]:
            if len(harvested_ids) >= max_harvest:
                break
            if self.corpus.has(ident):
                txt_check = (self.corpus.text(ident) or "").lower()
                if not core_toks or any(t in txt_check for t in core_toks):
                    harvested_ids.append(ident)
                    continue
            if self.engine.store.has(ident) and not self.corpus.has(ident):
                stored_txt = self.engine.store.get(ident) or ""
                if stored_txt and (not core_toks or any(t in stored_txt.lower() for t in core_toks)):
                    self.corpus.add_document(ident, stored_txt, fmt="DjVuTXT")
                    harvested_ids.append(ident)
                    continue
            try:
                avail = await self.engine.availability(ident)
            except Exception:
                continue

            if avail.get("state") == "restricted" or avail.get("restricted"):
                if len(restricted_quotes) < 1:
                    try:
                        q_res = await self.engine.quote_from_index(ident, probe_terms[:3], delay=0.2, size=3)
                        if q_res.get("passages"):
                            restricted_quotes.append({
                                "identifier": ident,
                                "tier": (q_res.get("item") or {}).get("tier", "T2"),
                                "page_scope": (q_res.get("item") or {}).get("page_scope", "item_aggregate"),
                                "url": f"https://archive.org/details/{ident}",
                                "passages": q_res["passages"][:4],
                            })
                    except Exception:
                        pass
                continue

            # Try harvesting open text derivative into store & corpus, preferring files matching core_toks
            try:
                fetched = await self.engine.fetch_text(ident, max_chars=600_000, prefer_terms=list(core_toks))
                f_txt = fetched.get("text") or ""
                if fetched.get("found") and len(f_txt) >= 1500:
                    # Guard against multi-file reading-list bundles where the downloaded file
                    # is an unrelated document that does not mention any subject core terms
                    f_low = f_txt.lower()
                    if core_toks and not any(t in f_low for t in core_toks):
                        continue
                    full_stored = self.engine.store.get(ident) or f_txt
                    self.corpus.add_document(
                        ident,
                        full_stored,
                        fmt=fetched.get("format", "DjVuTXT"),
                        source_url=fetched.get("source_url", ""),
                    )
                    harvested_ids.append(ident)
            except Exception:
                continue

        if compare_with and compare_with not in harvested_ids:
            try:
                await self.ensure_local([compare_with], pause=0.2)
                if self.corpus.has(compare_with):
                    harvested_ids.append(compare_with)
            except Exception:
                pass

        # Stage 3: Autonomous Forensic Comparison & Edition Diffing
        forensic_report: Optional[Dict[str, Any]] = None
        edition_diffs: List[Dict[str, Any]] = []

        if compare_with and self.corpus.has(compare_with) and harvested_ids:
            primary_a = next((i for i in harvested_ids if i != compare_with), None)
            if primary_a:
                try:
                    forensic_report = self.forensic_compare(primary_a, compare_with, top_passages=3)
                    sh = forensic_report.get("exact_8gram_shingles") or {}
                    if (sh.get("excess_over_null") or 0) >= 5:
                        diffs_a_b = self.corpus.classify_differences(primary_a, compare_with, min_run=40, max_runs=2)
                        diffs_b_a = self.corpus.classify_differences(compare_with, primary_a, min_run=40, max_runs=2)
                        for d in diffs_a_b[:2]:
                            edition_diffs.append({"in": primary_a, "missing_from": compare_with, **d})
                        for d in diffs_b_a[:2]:
                            edition_diffs.append({"in": compare_with, "missing_from": primary_a, **d})
                except Exception:
                    pass
        elif len(harvested_ids) >= 2:
            id_a, id_b = harvested_ids[0], harvested_ids[1]
            try:
                cand = self.forensic_compare(id_a, id_b, top_passages=2)
                sh = cand.get("exact_8gram_shingles") or {}
                wins = cand.get("conceptual_window_alignments") or []
                # Only include forensic report & edition diffs between auto-harvested items
                # when they actually share substantive text (sibling editions or adaptations),
                # never when they are two unrelated books that both mention the subject.
                if (sh.get("excess_over_null") or 0) >= 5 or (wins and wins[0].get("jaccard", 0) >= 0.08):
                    forensic_report = cand
                if (sh.get("excess_over_null") or 0) >= 10:
                    diffs_a_b = self.corpus.classify_differences(id_a, id_b, min_run=40, max_runs=3)
                    diffs_b_a = self.corpus.classify_differences(id_b, id_a, min_run=40, max_runs=3)
                    for d in diffs_a_b[:2]:
                        edition_diffs.append({"in": id_a, "missing_from": id_b, **d})
                    for d in diffs_b_a[:2]:
                        edition_diffs.append({"in": id_b, "missing_from": id_a, **d})
            except Exception:
                pass

        # Stage 4: High-Signal Substantive Excerpt Extraction from Primary Texts
        def _strip_header(txt: str) -> str:
            for marker in ("Digitized by Google", "books.google.com"):
                idx = txt.find(marker)
                if 0 <= idx < 5000:
                    txt = txt[idx + len(marker):]
            return txt.strip()

        for ident in harvested_ids[:2]:
            raw_txt = self.corpus.text(ident)
            if not raw_txt:
                continue
            clean_txt = _strip_header(raw_txt)
            if not clean_txt:
                continue

            # Extract opening thesis window + highest-density focus windows + closing/notes window
            windows = []
            # 1. Opening substantive window (skipping table of contents noise)
            start_off = min(1200, len(clean_txt) // 10)
            windows.append({
                "region": "opening_thesis",
                "offset": start_off,
                "excerpt": re.sub(r"\s+", " ", clean_txt[start_off:start_off + 420]).strip(),
            })

            # 2. Focus term or distinctive phrase windows
            if focus_terms:
                pattern = "|".join(re.escape(t) for t in focus_terms if t)
                if pattern:
                    for m in re.finditer(pattern, clean_txt, re.IGNORECASE):
                        s = max(0, m.start() - 160)
                        e = min(len(clean_txt), m.end() + 220)
                        windows.append({
                            "region": f"match:{m.group(0)}",
                            "offset": m.start(),
                            "excerpt": re.sub(r"\s+", " ", clean_txt[s:e]).strip(),
                        })
                        if len(windows) >= 3:
                            break

            # 3. Closing / Notes / Epilogue window (where confessions, appendices, and footnotes sit)
            if len(clean_txt) > 6000:
                tail_off = max(0, len(clean_txt) - 2800)
                windows.append({
                    "region": "closing_and_notes",
                    "offset": tail_off,
                    "excerpt": re.sub(r"\s+", " ", clean_txt[tail_off:tail_off + 450]).strip(),
                })

            primary_excerpts.append({
                "identifier": ident,
                "chars": len(clean_txt),
                "url": f"https://archive.org/details/{ident}",
                "windows": windows[:4],
            })

        return {
            "statement": statement,
            "subject_core": core_query,
            "trace": trace,
            "harvested_primary_ids": harvested_ids,
            "primary_excerpts": primary_excerpts,
            "restricted_quotes": restricted_quotes,
            "forensic_report": forensic_report,
            "edition_diffs": edition_diffs,
        }
