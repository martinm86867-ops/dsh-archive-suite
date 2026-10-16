"""Agent Tool Definitions (OpenAI / Anthropic / MCP compatible) for LLM Research Assistants."""

from typing import Dict, Any, List

LLM_RESEARCH_TOOLS: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "archivist_fulltext_search",
            "description": (
                "Search the actual text of documents across archive.org (OCR content, not "
                "metadata). Returns identifiers with highlighted snippets. Supports quoted "
                "phrases and boolean operators.\n"
                "IMPORTANT: this index also covers items in the lending library "
                "(collections inlibrary / printdisabled). Their full text cannot be "
                "downloaded, but their passages are still returned verbatim here -- so this "
                "is the way to quote a restricted work.\n"
                "To confine a search to one item, use a field query: "
                "identifier:<id> AND <term>. A bare phrase search is not equivalent: "
                "'eigenform' alone returns 3,010 documents, nearly all of them mathematical.\n"
                "CAUTION: a compound query narrows silently and can return zero while each "
                "term alone matches. If a multi-term query returns nothing, call "
                "archivist_query_diagnostics before concluding the phrase is absent."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Terms, \"quoted phrases\", and/or boolean operators.",
                    },
                    "size": {"type": "integer", "description": "Maximum hits (default: 10).", "default": 10},
                    "scope": {"type": "string", "description": "Optional scope/filter for the FTS service."},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "archivist_search_inside",
            "description": (
                "Locate a passage inside one document, returning page numbers and word "
                "bounding boxes for citation. Simple terms and phrases only: boolean "
                "operators are not honoured by this endpoint. Raises a typed error when the "
                "item has no searchable text derivative, so an absence of a derivative is "
                "never confused with an absence of the phrase. Lending-restricted items "
                "cannot be served here -- this endpoint fetches the OCR file internally and "
                "is refused -- so use archivist_quote_from_index for those."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "identifier": {"type": "string", "description": "archive.org item identifier."},
                    "query": {"type": "string", "description": "Term or \"quoted phrase\"."},
                    "limit": {"type": "integer", "description": "Maximum matches (default: 5).", "default": 5},
                },
                "required": ["identifier", "query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "archivist_text_availability",
            "description": (
                "Report whether an item's text is searchable and why not. States: "
                "searchable, no_derivative (image-only or DjVuTXT-only), restricted "
                "(lending), not_indexed. Check this before concluding a document does not "
                "contain a phrase.\n"
                "Also returns an `access` object separating three independent facts: "
                "derivative (can the text file be downloaded), inside (can this item be "
                "searched in place), lending, and indexed. `restricted` in `state` means the "
                "text cannot be DOWNLOADED; it does NOT mean the text is unsearchable -- "
                "restricted items are still in the full-text index and quotable via "
                "archivist_quote_from_index. `indexed: null` means unknown, not absent."
            ),
            "parameters": {
                "type": "object",
                "properties": {"identifier": {"type": "string"}},
                "required": ["identifier"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "archivist_quote_from_index",
            "description": (
                "Retrieve verbatim passages from one item, INCLUDING lending-restricted "
                "items whose full text cannot be downloaded. This is the fallback to use "
                "when archivist_search_inside reports state=restricted: the per-item search "
                "endpoint cannot serve such an item, but the full-text index can, because "
                "it is a different service.\n"
                "Pass several terms; each is issued as a field query "
                "identifier:<id> AND <term>. Returns passages with the term that matched.\n"
                "CAVEAT: for a restricted item the index reports ONE aggregate page for the "
                "whole item, not per passage. Each result carries `page_scope`; when it is "
                "'item_aggregate' the page is not the passage's own and must not be cited "
                "as one."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "identifier": {"type": "string", "description": "archive.org item identifier."},
                    "terms": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Terms or phrases to look for inside that one item.",
                    },
                },
                "required": ["identifier", "terms"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "archivist_query_diagnostics",
            "description": (
                "Explain a query that returned nothing. Reports how many matches come from "
                "document text versus metadata, and drops terms one at a time to identify "
                "which term is responsible for a zero result."
            ),
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "archivist_find_variants",
            "description": (
                "List other uploads of the same document. Multiple copies are normal in an "
                "archive and are NOT duplicates to be collapsed: copies differ in "
                "completeness, redaction and OCR quality, and the differences carry "
                "information. Each sibling is confirmed by shared text, not by title alone."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "identifier": {"type": "string", "description": "archive.org item identifier."},
                    "max_variants": {"type": "integer", "description": "Copies to confirm (default: 6).", "default": 6},
                },
                "required": ["identifier"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "archivist_reconcile_variants",
            "description": (
                "Compare the several copies of one document and report what each adds: sizes, "
                "OCR quality, page counts, a ranked base copy, and regions present in one copy "
                "and absent in another. Regions in the base but missing from an otherwise "
                "near-identical copy are labelled candidate excisions (redaction, truncation "
                "or OCR failure are indistinguishable from text alone)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "identifier": {"type": "string", "description": "archive.org item identifier."},
                    "max_variants": {"type": "integer", "description": "Copies to compare (default: 6).", "default": 6},
                    "min_run": {"type": "integer", "description": "Minimum differing run in tokens (default: 12).", "default": 12},
                },
                "required": ["identifier"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "archivist_find_text_reuse",
            "description": (
                "Find documents that share phrasing with a passage or document, to establish "
                "derivation, quotation or provenance. Scores each candidate on shared n-grams "
                "against a shuffled-null baseline, so shared idiom is separated from "
                "substantive reuse: a high observed count with a near-zero null is real "
                "incorporation. Optionally reports several uploads of the same document, "
                "which often differ in completeness or redaction."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "identifier": {"type": "string", "description": "Source item identifier."},
                    "text": {"type": "string", "description": "Or supply the passage directly."},
                    "min_n": {"type": "integer", "description": "Minimum shared token run (default: 8).", "default": 8},
                    "max_candidates": {"type": "integer", "description": "Candidates to score (default: 8).", "default": 8},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "archivist_search",
            "description": "Execute sub-millisecond BM25 full-text search across the local analytical archive warehouse. Returns evidence cards with timestamps and provenance URLs.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Keywords or search phrases (e.g., 'Apollo 11 mission log' or 'domain registration terms')."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum evidence cards to retrieve (default: 5).",
                        "default": 5
                    },
                    "source_type": {
                        "type": "string",
                        "enum": ["wayback_snapshot", "catalog_item"],
                        "description": "Filter by source record type."
                    },
                    "from_ts": {
                        "type": "string",
                        "description": "Start timestamp filter (YYYYMMDD)."
                    },
                    "to_ts": {
                        "type": "string",
                        "description": "End timestamp filter (YYYYMMDD)."
                    }
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "archivist_timeline",
            "description": "Retrieve the temporal timeline of captures for a given URL or entity to analyze how content changed across decades.",
            "parameters": {
                "type": "object",
                "properties": {
                    "source_key": {
                        "type": "string",
                        "description": "Target URL (e.g. 'http://example.com') or catalog identifier."
                    }
                },
                "required": ["source_key"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "archivist_live_cdx",
            "description": "Execute live query against Archive.org Wayback CDX index when warehouse records are insufficient.",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "Target domain or URL pattern to query."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of snapshots to return (default: 10).",
                        "default": 10
                    }
                },
                "required": ["url"]
            }
        }
    }
]
