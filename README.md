# @martinm86867-ops/dsh-archive-suite 🏛️⚡

> DeepSeek Harness (DSH) native plugin harnessing the full power of Archive.org, the Wayback Machine, embedded DuckDB research warehouse, primary-text forensics, and multi-modal media (**43 native model tools** + **bundled `archivist-research-agent` skill** across Text, Audio, Video, IIIF, Software & Emulation).

## Bundled Agent Skill: `archivist-research-agent`

This plugin embeds and auto-registers the **`archivist-research-agent`** skill into the DSH `skills` registry (`provider: "dsh-archive-suite"`, `rank: 600`) and `~/.dsh/skills/archivist-research-agent`. When loaded via `skill({ name: "archivist-research-agent" })`, it equips the agent with the **5-Stage Epistemic & Forensic Methodology**:
1. **Never Press "104 Keys on a Typewriter"**: Start every investigation with the single-call macro-pipeline `archive_epistemic_dossier({ statement, compare_with?, focus_terms? })` to save ~95% of context tokens.
2. **Separate Provenance from Substance**: Always evaluate both *who wrote/compiled a document* (provenance) and *what structural/political/technical mechanisms the text describes* (substance).
3. **Primary-Text Mathematical Forensics Over Secondary Rumor**: Never repeat secondary literary claims without running `archive_forensic_compare` (8-gram shingle overlap vs. shuffled-token null baseline + 250-token sliding-window vocabulary alignment across translations).
4. **Structural Edition Diffing**: Treat multiple Archive.org uploads as distinct witnesses and inspect structural additions/excisions (`classify_differences`).
5. **Zero Custom Code Mandate**: Use the 43 native tools (`wayback_read`, `archive_read_text` with `grep`/`offset`, `archive_iiif_image`, `archive_quote_from_index`) instead of writing ad-hoc Python/curl scripts.

## Features & Model Tools (43 Native Tools)

### 0. One-Call Autonomous Epistemic & Forensic Dossier
- **`archive_epistemic_dossier`**: One-call 5-stage autonomous methodology (~95% context-token savings): (1) strips caller framing bias and runs 4-axis triangulation (Corroboration, Critique, Substance, and T1 Primary Records), (2) autonomously harvests the top canonical primary editions into `corpus.duckdb` (or mines FTS quotes if lending-restricted), (3) runs 8-gram shingle + sliding-window forensic comparisons and structural edition diffs (`classify_differences`), (4) extracts concentrated opening/focus/closing primary-text windows, and (5) layers Jina live-web stance mapping when available.
- **`archive_forensic_compare`**: Direct primary-text comparator testing plagiarism, translation, and textual derivation claims against primary works in `corpus.duckdb`. Strips digitization boilerplate, computes exact 8-gram shingle overlap against a shuffled-token null baseline, and finds sliding-window distinctive vocabulary alignments across translations.

### 1. Catalog, Item Management & IAS3 Storage
- **`archive_search`**: Search Archive.org catalog items with Lucene syntax, mediatypes, date ranges, and collections.
- **`archive_item_info`**: Inspect item metadata, server hosting nodes, and files list.
- **`archive_item_upload`**: Upload local files directly into an Archive.org item via IAS3.
- **`archive_item_manage`**: Create new Archive.org items (`create`), patch metadata (`patch_metadata`), delete remote files (`delete_file`), inspect background server derivation tasks (`tasks`), or inspect storage cluster nodes (`server_nodes`).

### 2. Multi-Modal Media, IIIF & Lending Access
- **`archive_media_manifest`**: Deep-inspect multi-modal assets: extract direct MP4/WebM video stream URLs, MP3/FLAC audio tracklists, in-browser DOSBox/MAME emulation launcher URLs, and software packages.
- **`archive_read_text`**: Read or regex-grep the full OCR text or transcription of a book/document with character `offset` pagination.
- **`archive_book_search`**: Search inside one document. Returns page numbers and word bounding boxes, falling back to a local text scan with explicit coverage when the item has no searchable OCR derivative.
- **`archive_iiif_image`**: Extract high-resolution digitized book pages, manuscripts, or bounding-box cropped figures via the Archive.org IIIF Image API 3.0 (URL or direct local download for `read_image`).
- **`archive_media_download`**: Download a specific audio track, video stream, software ROM/disk, or document to the local workspace.
- **`archive_readable_copies`**: Given an item or title query, report which copies of a work can actually be read versus lending-restricted or unindexed copies.
- **`archive_lending_borrow`**: Request a 1-hour browse loan (`browse_book`), 14-day loan (`borrow_book`), or loan return (`return_loan`) when an `IA_PATRON_COOKIE` session is configured.

### 3. Wayback Machine & Web Preservation
- **`wayback_cdx`**: Search historical snapshot captures across time.
- **`wayback_read`**: Fetch a historical Wayback Machine webpage snapshot and extract clean, readable text (or raw HTML) with pagination — essential because Jina Reader blocks `web.archive.org` URLs.
- **`wayback_save`**: Save URLs to Wayback Machine (Save Page Now 2 / SPN2) with capture polling.
- **`wayback_diff`**: Compare two historical snapshots across decades with textual diffing.

### 4. Pipelines & Analytical Research Warehouse
- **`archive_pipeline_run`**: Execute asynchronous data processing pipelines with checkpointed resumption.
- **`archive_research_search`**: BM25 search across the local DuckDB research warehouse. Note: this warehouse is a small demo set unless a pipeline has populated it — a zero result means the warehouse is empty, not that the archive lacks material.
- **`archive_research_prompt`**: Build verifiable, citation-grounded research prompts with evidence cards.
- **`archive_research_timeline`**: Retrieve the chronological timeline of all ingested captures and content digests for a URL or item identifier in the local DuckDB warehouse.

### 5. Full-Text Retrieval (document content, not metadata)

Catalogue search finds items; these find what the documents actually say.

- **`archive_fulltext_search`**: Search the text of documents archive-wide (OCR content). Supports `"quoted phrases"` and boolean operators. This is the tool for content questions that metadata search cannot answer.
- **`archive_book_search`**: Page-anchored search inside one document — page numbers and word bounding boxes for citation.
- **`archive_text_availability`**: Whether an item's text is searchable at all, and why not (`searchable` / `no_derivative` / `restricted` / `not_indexed`). Check before concluding a phrase is absent.
- **`archive_text_harvest`**: Download compact text derivatives into the local store, once, paced to avoid rate limits, resumable.

### 6. Provenance, Variants & the Local Corpus

Once text is harvested, analysis runs offline against a local DuckDB corpus at memory speed (milliseconds), with no further requests.

- **`archive_text_reuse`**: Documents sharing phrasing with a passage or document. Scores shared shingles against a *shuffled-null* baseline, so shared idiom is separated from real incorporation (quotation, derivation, copying).
- **`archive_text_variants`**: Sibling uploads of the same work. Copies differ in completeness, redaction and OCR quality — they are not duplicates, and the differences carry information.
- **`archive_text_reconcile`**: Compare the copies of one work: sizes, OCR quality, a ranked base copy, and regions present in one copy but absent in another (labelled *candidate excisions* — redaction, truncation and OCR failure cannot be told apart from text alone).
- **`archive_corpus`**: Query the columnar corpus directly — `stats`, `build`, `similar`, `shared` (longest verbatim passages two documents share), `diff` (regions missing from one copy), `phrase` (exact positional search).

**Measured** on a 5-document / 5.8M-character corpus (~1M shingle rows): build 1.9s · similar 0.013s · shared runs 0.032s · diff 0.023s · phrase 0.022s.

### 7. Reference Drift — the archive versus the live web

- **`archive_reference_drift`**: For a page, check the references it cites on both sides at once — live HTTP status now, archive capture count and latest capture, and the resulting regime: `live_and_archived`, `archive_only`, `live_only` or `neither`. `archive_only` means the archive is the only surviving witness; `live_only` means a future change to that source would be unprovable.

  Extraction is scoped to the page's references section, not the whole document: a full page yields hundreds of external links (navigation, sister projects, licence and donation links), and treating those as citations produces confident nonsense.

  Content comparison is deliberately left to the caller: **Jina refuses to fetch `web.archive.org`** (blocked for abuse), so the live side and the archived side cannot share a fetcher. The archivist owns the archive; Jina owns the live web.

### 8. Bulk Harvest & Local Store

- **`archive_scrape`**: Cursor-based bulk metadata harvest — the endpoint for large candidate sets (thousands of identifiers in one call), where `archive_search` is capped at a page. Pass the cursor back to continue the sweep.
- **`archive_text_store`**: What text is held locally for offline analysis. Corpus analysis can only see what has been harvested; check this when a document is reported as "not in the corpus".
- **`archive_corpus_sql`**: Read-only SQL against the local corpus (`documents`, `document_text`, `tokens`, `shingles`, `fingerprints`). The escape hatch — a new research question becomes a query rather than new code.

### 9. Quota Tracking & Key Auto-Rotation
- **`archive_quota`**: Inspect live authenticated Save Page Now 2 (SPN2) capture quotas, available concurrency slots, and daily usage.
- **`archive_item_views`**: Retrieve real-time viewership analytics and historical traffic metrics for any Archive.org item.
- **`archive_keys_status`**: List all S3 credential pairs in the pool with live quota exhaustion and active rotation status.
- **`archive_keys_rotate`**: Rotate active S3 credentials to the next available healthy key in the pool.

### 10. Epistemic Fact-Checking (Standalone Archive-First & Hybrid Jina Modes)
- **`archive_fact_check`**: Conventional web fact-checkers fail when primary evidence sits in offline books, declassified archives, lending-restricted monographs, or dead links no longer indexed on the live web. `archive_fact_check` solves this by operating in two seamless modes:
  1. **Standalone Archive-First Mode (No Jina Required)**: Sweeps the Archive.org full-text OCR index (`ia-pub-fts-api`), isolates the Primary Record (`T1` government, military, DTIC, and declassified collections), queries the local DuckDB warehouse (`research_warehouse.duckdb`), classifies archival passage stances (`states` / `reports` / `denies` / `mentions`), and flags numeric discrepancies.
  2. **Hybrid Dual-Engine Mode (With Jina)**: Automatically layers live-web stance mapping (`evaluateGroundedResults`) alongside the archival documentary footprint when a Jina API key is available.
- **`archive_claim_trace`**: Deep documentary footprint analysis across years, collections, creators, and languages, separating circulation from primary documentation.
- **`archive_quote_from_index`**: Extracts verbatim passages from lending-restricted (`inlibrary` / `printdisabled`) books via field-scoped FTS queries while enforcing honest `page_scope` (`item_aggregate` vs `exact`).
- **`archive_lending_status`**, **`archive_query_diagnostics`**, **`archive_agent_ingest`**, **`archive_agent_query`**: Complete agent research loop for diagnosing zero-hit compound queries and ingesting both open and lending-restricted works into the local DuckDB warehouse.

## Installation into DSH

Installed via DSH Plugin Manager or linked directly in profile dependencies:
```bash
# Link as a local bundle in DSH profile
"dependencies": {
  "@martinm86867-ops/dsh-archive-suite": "link:/home/martin/dsh-plugins/dsh-archive-suite"
}
```

## Configuration

Overridable by environment variable, so a rebuilt venv or a different checkout
does not break every tool silently:

| variable | default | purpose |
|---|---|---|
| `DSH_ARCHIVE_HOME` | `/home/martin/archive` | the archivist checkout |
| `DSH_ARCHIVE_PYTHON` | the checkout's `.venv/bin/python`, else `python3` | interpreter for the bridge |
| `DSH_ARCHIVE_TIMEOUT_MS` | `180000` | bridge timeout; a hung request is killed rather than left unsettled |
