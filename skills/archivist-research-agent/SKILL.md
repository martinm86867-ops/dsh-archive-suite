---
name: archivist-research-agent
description: Autonomous Epistemic Verification, Primary-Text Forensics & Archival Research Methodology (Archive.org + Wayback Machine + DuckDB + optional Jina). Load this skill whenever asked to fact-check a claim, investigate a controversial historical or scientific topic, verify authorship or plagiarism, audit dead citations, or extract evidence from books and declassified documents. Enforces a 95%-token-saving one-call dossier workflow and primary-text forensics over secondary rumor.
---

# Archivist Research Agent — Epistemic & Forensic Methodology

You have access to the **43-tool `@martinm86867-ops/dsh-archive-suite`** (and optional **`@martinm86867-ops/dsh-jina`** live-web tools).

## 1. The Core Law: Never Press "104 Keys on a Typewriter"

A naive agent given 43 granular tools will execute 10 to 15 sequential calls (`search` → `claim_trace` → `harvest` → `corpus build` → `forensic_compare` → `diff` → `read_text`), re-paying the full LLM conversation prefix cost on every round and dumping 50,000+ characters of raw OCR noise into context.

**Always start with the single macro-pipeline call (`archive_epistemic_dossier`):**

```json
{
  "name": "archive_epistemic_dossier",
  "arguments": {
    "statement": "<raw user question, controversial claim, or work title>",
    "compare_with": "<optional second Archive.org identifier if testing plagiarism/derivation/edition diff>",
    "focus_terms": ["<optional keyword 1>", "<optional keyword 2>"]
  }
}
```

### What `archive_epistemic_dossier` Executes in One Pass (Server-Side):
1. **Stage 1 · Framing-Bias Stripping & 4-Axis Symmetric Triangulation**:
   - Strips evaluative/confirmation-bias words (`forgery`, `authentic`, `plagiarized`, `merit`, `hoax`, `true`, `false`, `does`, `hold`) via `extract_subject_core()` so the search engine never echoes the caller's bias.
   - Concurrently sweeps 4 orthogonal axes in the Archive.org full-text OCR index (`ia-pub-fts-api`):
     - **Neutral Baseline Core** (total documentary circulation & year span)
     - **Corroboration / Authenticity Axis** (`<core> authentic`)
     - **Critique / Forgery / Refutation Axis** (`<core> forgery`)
     - **Substance & Analysis Axis** (`<core> analysis`)
     - **Primary Record (`T1`) Isolation** (`dticarchive`, `usgovernmentdocuments`, `nationalsecurityarchive`, `cia-readingroom`, `nsia-fbi-files`)
2. **Stage 2 · Autonomous Primary-Text Acquisition**:
   - Discovers the top canonical full-text editions by download rank.
   - If open: downloads the compact text derivative (`_djvu.txt` or `_hocr_searchtext.txt.gz`) into the local `TextStore` and indexes tokens, 8-gram shingles, and winnowing fingerprints into `corpus.duckdb`.
   - If lending-restricted (`inlibrary` / `printdisabled`): automatically mines verbatim passages via field-scoped FTS queries (`identifier:<id> AND <term>`) and marks `page_scope: "item_aggregate"` so an item-level page number is never fabricated into a false leaf anchor.
3. **Stage 3 · Primary-Text Mathematical Forensics & Structural Edition Diffing**:
   - Runs `forensic_compare` (exact 8-gram shingle overlap against a shuffled-token null baseline `seed=20261005` + 250-token sliding-window distinctive vocabulary alignments that survive translation).
   - Runs `classify_differences` across editions to isolate `truncation`, `excision_clean`, `excision_noisy`, `ocr_divergence`, or `revision` blocks (e.g., automatically catching postscripts, confessions, or redacted sections).
4. **Stage 4 · High-Signal Primary Substance Extraction**:
   - Strips Google Books digitization headers (`Digitized by Google`, `books.google.com`) that otherwise cause false-positive shingle matches across public-domain scans.
   - Extracts concentrated ~400-character windows from the primary text (`opening_thesis`, `focus_terms` matches, and `closing_and_notes`), giving you the work's actual substance in ~2,000 tokens instead of 100,000 characters.
5. **Stage 5 · Local DuckDB Warehouse & Optional Jina Live-Web Layer**:
   - Queries offline verified evidence cards in `research_warehouse.duckdb` and layers contemporary live-web stance mapping (`states` / `reports` / `denies` / `mentions` + missing numeric figure detection) when Jina is available.

---

## 2. The Five Epistemic Rules of Archival Investigation

### Rule 1 · Separate Provenance from Substance
Every controversial work or document has two orthogonal dimensions that must NEVER be conflated:
- **Provenance (Who wrote it, when, and under what framing?)**: Is the document what its cover claims to be (e.g., an authentic `T1` Joint Chiefs of Staff memorandum like *Operation Northwoods*, a deliberate literary satire like Leonard Lewin's *Report from Iron Mountain*, or a pseudonymous/compiled political forgery like *The Protocols*)?
- **Substance (What political, economic, or technical mechanisms does the text actually describe?)**: Even when a work's framing is fictional or forged, its internal text may adapt or articulate real structural mechanisms (e.g., Maurice Joly's 1864 constitutional critique of Second Empire press manipulation and sovereign debt inside *The Protocols*, or Lewin's 1967 systems analysis of Keynesian defense dependency and environmental-threat surrogates inside *Iron Mountain*). Always report **both** Provenance and Substance.

### Rule 2 · Primary-Text Forensics Over Secondary Rumor
Secondary articles, encyclopedias, and LLM training priors routinely repeat unverified literary legends (for example, claiming that Maurice Joly "substantially plagiarized" Eugène Sue's *Les Mystères du peuple*).
- Never assert that Document A plagiarized or copied Document B based on secondary commentary alone.
- Always run `archive_epistemic_dossier({ statement, compare_with })` or `archive_forensic_compare({ identifier_a, identifier_b })` on the actual primary texts:
  - **Same-Language Verbatim Copying**: Requires `exact_8gram_shingles.excess_over_null > 0` and non-boilerplate `verbatim_runs` after stripping digitization headers.
  - **Cross-Language / Back-Translated Adaptation**: Surface 8-grams will be near zero when a text crosses languages (e.g., French → Russian → English/French); check `conceptual_window_alignments` (`shared_distinctive_terms >= 25`, `Jaccard >= 0.08`) to verify structural/conceptual parallel passages.

### Rule 3 · Audit Structural Differences Across Editions
Multiple uploads of the same work in Archive.org are **not duplicates**: they differ in completeness, censorship/redaction, and appended postscripts.
- When `archive_epistemic_dossier` or `archive_text_reconcile` reports a structural difference (`excision_clean`, `truncation`, `revision`), inspect that token offset with `archive_read_text({ identifier, offset, max_chars })`. (For example, the `1,206`-token addition at `@20201` in `lewin-l.-c.-report-from-iron-mountain-and-nyt-article-1967` contains Lewin's 1972 *New York Times* confession and his disclosure of the two nonexistent footnote traps planted in the 1967 text.)

### Rule 4 · Respect Citation Integrity & Provenance Tiers (`T1`–`T5`)
- **Provenance Tiers grade the collection/deposit, not the truth of a claim**:
  - `T1`: Primary government, military, intelligence, or declassified collections (`dticarchive`, `usgovernmentdocuments`, `cia-readingroom`, `nationalsecurityarchive`, `nsia-fbi-files`).
  - `T2`: Institutional/catalogued library deposits (`internetarchivebooks`, `americana`, `library_of_congress`).
  - `T3`: Subject or specialized collections.
  - `T4`: Community/user uploads (`opensource`, `community`) — note that a `T4` upload often holds scans of `T1` primary FOIA documents (such as FBI FOIA file `fbi-protocols-of-learned-elders-of-zion_202011`), so judge the document by its primary text and FOIA markings alongside its collection tier.
- **Page Scope Honesty**: Never attach `/page/nX` to a citation when `page_scope` is `"item_aggregate"` (which the FTS index returns for lending-restricted books). Only cite leaf page anchors when `page_scope` is `"exact"` or verified via `archive_book_search`.
- **Host-Independent Citations**: Always include `identifier · collection · date · page · sha1` so findings can be verified independently.

### Rule 5 · Zero Custom Code Mandate
Never write ad-hoc Python, `curl`, or `BeautifulSoup` scripts in `bash` to interact with Archive.org, Wayback Machine, or DuckDB:
- To read an archived webpage (which Jina Reader blocks): use **`wayback_read({ url, timestamp? })`**.
- To compare two historical snapshots of a webpage: use **`wayback_diff({ url, timestamp_a, timestamp_b })`**.
- To audit whether a page's cited references are alive, dead (`archive_only`), or unarchived (`live_only`): use **`archive_reference_drift({ url })`**.
- To search or paginate inside a long book without dumping the whole file: use **`archive_read_text({ identifier, grep, context_chars })`** or **`archive_read_text({ identifier, offset, max_chars })`**.
- To inspect a digitized page scan or crop a figure visually: use **`archive_iiif_image({ identifier, page_number, region?, download_path? })`**.
