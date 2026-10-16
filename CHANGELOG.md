# Changelog — `@martinm86867-ops/dsh-archive-suite`

## 1.9.0 (2026-10-16)

### Highlights
- **feat (`archive_epistemic_dossier` — Tool #43)**: One-call autonomous 5-stage epistemic and primary-text forensic pipeline that reduces agent context-token consumption by **~95%** (compressing 10–12 sequential tool calls and ~65,000 chars of raw OCR into a single ~2,300-token verified dossier).
  - **Stage 1**: Strips caller framing bias (`extract_subject_core`) and runs a parallel 4-axis sweep across Neutral Core, Corroboration (`authentic`), Critique (`forgery`), Substance (`analysis`), and `T1` Primary Government/Military Records.
  - **Stage 2**: Autonomously harvests top canonical primary-text editions into `corpus.duckdb` (or mines verbatim passages via field-scoped FTS if lending-restricted).
  - **Stage 3**: Executes primary-text shingle/window forensics (`forensic_compare`) and structural edition diffing (`classify_differences`) to isolate added postscripts, confessions, or excisions.
  - **Stage 4**: Strips Google Books digitization headers and extracts concentrated ~400-char primary-text windows (`opening_thesis`, `focus_terms`, `closing_and_notes`).
  - **Stage 5**: Attaches local DuckDB warehouse evidence cards and optional Jina live-web stance mapping.
- **feat (`archivist-research-agent` Skill)**: Embedded the `archivist-research-agent` skill (`skills/archivist-research-agent/SKILL.md`) and wired automatic registration into the DSH `skills` service (`ctx.inject(['skills'])`).
- **feat (`archive_forensic_compare` — Tool #42)**: Direct primary-text forensic comparator combining boilerplate-stripped 8-gram shingle overlap against a shuffled-token null baseline (`seed=20261005`) with 250-token sliding-window distinctive vocabulary alignments across translations.
- **feat (`archive_fact_check` — Tool #36)**: Standalone and Jina-hybrid epistemic fact-checker with anti-confirmation-bias triangulation.
- **feat (Zero-Code Agent Extensions — Tools #37–#41)**:
  - `wayback_read`: Clean readable text (or raw HTML) extraction from Wayback Machine snapshots (bypasses Jina Reader's `web.archive.org` block).
  - `archive_iiif_image`: Canonical IIIF Image API 3.0 page/crop URL builder and local image downloader for `read_image`.
  - `archive_lending_borrow`: Digital lending `browse_book` (1-hr), `borrow_book` (14-day), and `return_loan` via `BookReaderJSIA.php`.
  - `archive_item_manage`: IAS3 bucket creation, JSON-Patch metadata modification, file deletion, task queue inspection, and cluster server routing.
  - `archive_research_timeline`: Chronological capture timeline from `research_warehouse.duckdb`.
- **fix**: Resolved `resolvePython()` executable check (`existsSync('python3')` relative path bug), bundled pre-built DuckDB FTS extension loading for read-only home sandboxes, unified `dticarchive`/`usgovernmentmirrors` in `T1` primary collection scopes, and added live SPN2 quota synchronization to `archive_keys_status`.
- **fix (Output Signal & Primary-Text Forensic Precision)**:
  - **Unicode-Aware Tokenization & Multilingual Stopwords**: Expanded `textutil.tokenize` and DuckDB SQL `regexp_extract_all` from `'[a-z0-9]+'` to `'[a-z0-9\u00c0-\u024f]+'` and added French/German/Spanish stopwords so accented European OCR words (`différentes`, `sociétés`, `chrétiennes`, `démocratique`, `siècle`, `œuvre`) remain intact during shingling and sliding-window alignment.
  - **Multi-File Bundle Poisoning Guard**: Added `prefer_terms` filename ranking to `FullTextEngine.fetch_text` and core-token verification in `epistemic_dossier` Stage 2 so multi-PDF reading-list items never ingest unrelated files.
  - **Title-Anchor Triangulation & Local Corpus Priority**: Stripped authorship/publication framing verbs (`publish`, `wrote`, `book`, `paper`, 4-digit years) from `extract_subject_core`, anchored `build_triangulated_queries` and catalog `title:(...)` searches on `title_anchor`, prioritized local warehouse/corpus matches in Stage 2, and gated automatic Stage 3 edition diffing on verified shingle overlap (`excess_over_null >= 10`).
