# @martinm86867-ops/dsh-archive-suite 🏛️⚡

> DeepSeek Harness (DSH) native plugin harnessing the full power of Archive.org, the Wayback Machine, embedded DuckDB research warehouse, and multi-modal media (Text, Audio, Video, Software & Emulation).

## Features & Model Tools

### 1. Catalog & Storage
- **`archive_search`**: Search Archive.org catalog items with Lucene syntax, mediatypes, and collections.
- **`archive_item_info`**: Inspect item metadata, server hosting nodes, and files list.
- **`archive_item_upload`**: Upload local files directly into an Archive.org item via IAS3.

### 2. Multi-Modal Media Access (Text, Audio, Video, Software)
- **`archive_media_manifest`**: Deep-inspect multi-modal assets: extract direct MP4/WebM video stream URLs, MP3/FLAC audio tracklists, in-browser DOSBox/MAME emulation launcher URLs, and software packages.
- **`archive_read_text`**: Read the full OCR text or transcription of a book/document directly into LLM context.
- **`archive_book_search`**: Search inside one document. Returns page numbers and word bounding boxes, falling back to a local text scan with explicit coverage when the item has no searchable OCR derivative.
- **`archive_media_download`**: Download a specific audio track, video stream, software ROM/disk, or document to the local workspace.

### 3. Wayback Machine & Web Preservation
- **`wayback_cdx`**: Search historical snapshot captures across time.
- **`wayback_save`**: Save URLs to Wayback Machine (Save Page Now 2 / SPN2) with capture polling.
- **`wayback_diff`**: Compare two historical snapshots across decades with textual diffing.

### 4. Pipelines & Analytical Research
- **`archive_pipeline_run`**: Execute asynchronous data processing pipelines with checkpointed resumption.
- **`archive_research_search`**: BM25 search across the local DuckDB research warehouse. Note: this warehouse is a small demo set unless a pipeline has populated it — a zero result means the warehouse is empty, not that the archive lacks material.
- **`archive_research_prompt`**: Build verifiable, citation-grounded research prompts with evidence cards.

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
