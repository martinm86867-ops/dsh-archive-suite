/**
 * Local Epistemic & Forensic Telemetry Observatory for @martinm86867-ops/dsh-archive-suite
 *
 * Human-friendly, plain-English research workbench and library manager.
 * Translates raw archival counts, provenance tiers, and shingle math into
 * clear Executive Summaries, Plain-English Verdicts, Side-by-Side Excerpts,
 * and an In-Browser Book & Document Reader Modal.
 */

import { appendFileSync, existsSync, mkdirSync, readFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { createServer } from 'node:http'

const __dirname = dirname(fileURLToPath(import.meta.url))
export const TELEMETRY_LOG_PATH = resolve(__dirname, 'data', 'telemetry_runs.jsonl')

/** Extract archival and live source citations from a bridge response payload. */
export function extractSourcesFromResult(command, args, res) {
  if (!res || typeof res !== 'object') return []
  const sources = []
  const seen = new Set()
  const add = (item) => {
    const key = item.identifier || item.url
    if (!key || seen.has(key)) return
    seen.add(key)
    sources.push(item)
  }

  // 1. Dossier or claim_trace evidence
  const trace = res.trace || (res.recorded ? res : null)
  if (trace) {
    for (const e of (trace.evidence || [])) {
      add({
        identifier: e.identifier,
        title: e.title || e.identifier,
        tier: (e.provenance && e.provenance.tier) || e.tier || 'T4',
        stance: e.stance || 'mentions',
        page: e.page,
        page_scope: e.page_scope || 'unknown',
        date: e.date ? String(e.date).slice(0, 10) : null,
        sha1: e.sha1 ? String(e.sha1).slice(0, 12) : null,
        url: e.url || (e.identifier ? `https://archive.org/details/${e.identifier}` : null),
        quote: e.quote ? String(e.quote).slice(0, 260) : '',
      })
    }
    const prim = trace.primary_record || {}
    for (const p of (prim.examples || [])) {
      add({
        identifier: p.identifier,
        title: p.title || p.identifier,
        tier: p.tier || 'T1',
        stance: 'primary_record',
        date: p.date ? String(p.date).slice(0, 10) : null,
        url: p.url || (p.identifier ? `https://archive.org/details/${p.identifier}` : null),
        quote: p.quote ? String(p.quote).slice(0, 260) : '',
      })
    }
    for (const w of (trace.local_warehouse || [])) {
      add({
        identifier: w.identifier || w.id,
        title: w.title || w.identifier,
        tier: 'LOCAL_DUCKDB',
        stance: 'warehouse_card',
        sha1: w.digest,
        url: w.url,
        quote: w.quote ? String(w.quote).slice(0, 260) : '',
      })
    }
  }

  // 2. Primary excerpts / restricted quotes in epistemic_dossier
  for (const pe of (res.primary_excerpts || [])) {
    add({
      identifier: pe.identifier,
      title: `Saved Primary Book (${pe.chars.toLocaleString()} chars)`,
      tier: 'PRIMARY_TEXT',
      stance: 'harvested',
      url: pe.url,
      quote: (pe.windows && pe.windows[0] && pe.windows[0].excerpt) || '',
      windows: pe.windows || [],
    })
  }
  for (const rq of (res.restricted_quotes || [])) {
    add({
      identifier: rq.identifier,
      title: `Borrowed Library Book (${rq.identifier})`,
      tier: rq.tier || 'T2',
      stance: 'fts_quote',
      page_scope: rq.page_scope,
      url: rq.url,
      quote: (rq.passages && rq.passages[0] && rq.passages[0].text) || '',
    })
  }

  // 3. Direct search / fulltext_search / quote_from_index
  for (const d of (res.docs || [])) {
    add({
      identifier: d.identifier,
      title: d.title || d.identifier,
      tier: 'CATALOG',
      stance: d.mediatype || 'item',
      date: d.date ? String(d.date).slice(0, 10) : null,
      url: `https://archive.org/details/${d.identifier}`,
      quote: `Downloads: ${(d.downloads || 0).toLocaleString()}`,
    })
  }
  for (const h of (res.hits || [])) {
    add({
      identifier: h.identifier,
      title: h.title || h.identifier,
      tier: h.tier || (h.provenance && h.provenance.tier) || 'T3',
      stance: 'fts_hit',
      page: h.page,
      page_scope: h.page_scope,
      url: h.evidence_url || h.url,
      quote: (h.snippets && h.snippets[0] && h.snippets[0].text) || h.quote || '',
    })
  }

  return sources.slice(0, 24)
}

/** Append one tool/bridge execution record to the local JSONL telemetry store. */
export function recordTelemetryRun({ command, args, res, error, durationMs }) {
  try {
    mkdirSync(dirname(TELEMETRY_LOG_PATH), { recursive: true })
    const rawJson = JSON.stringify(res || { error: String(error || '') })
    const outputChars = rawJson.length
    const outputTokensEst = Math.round(outputChars / 4)

    const baselineTokensEst = command === 'epistemic_dossier'
      ? 45000
      : command === 'forensic_compare'
        ? 25000
        : command === 'claim_trace'
          ? 12000
          : Math.max(outputTokensEst, 2500)

    const trace = (res && (res.trace || (res.recorded ? res : null))) || null
    const tri = (trace && trace.triangulation) || null
    const fr = (res && (res.forensic_report || (res.exact_8gram_shingles ? res : null))) || null

    const entry = {
      id: `run_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
      timestamp: new Date().toISOString(),
      command,
      query: (args && (args.statement || args.query || args.identifier || args.identifier_a || args.url || args.operation)) || command,
      args,
      ok: !error && !(res && res.error),
      duration_ms: Math.round(durationMs),
      output_chars: outputChars,
      output_tokens_est: outputTokensEst,
      baseline_tokens_est: baselineTokensEst,
      tokens_saved_est: Math.max(0, baselineTokensEst - outputTokensEst),
      triangulation: tri ? {
        subject_core: tri.subject_core,
        caller_framing_stripped: tri.caller_framing_stripped,
        total_circulation: (trace.recorded && trace.recorded.documents) || 0,
        primary_t1_docs: (trace.primary_record && trace.primary_record.documents) || 0,
        corroboration_docs: (tri.corroboration_axis && tri.corroboration_axis.documents) || 0,
        critique_docs: (tri.critique_axis && tri.critique_axis.documents) || 0,
        substance_docs: (tri.substance_axis && tri.substance_axis.documents) || 0,
        canonical_editions: tri.canonical_editions || [],
      } : null,
      forensics: fr ? {
        identifier_a: fr.identifier_a,
        identifier_b: fr.identifier_b,
        observed_8grams: (fr.exact_8gram_shingles && fr.exact_8gram_shingles.observed) ?? 0,
        null_8grams: (fr.exact_8gram_shingles && fr.exact_8gram_shingles.shuffled_null) ?? 0,
        excess_8grams: (fr.exact_8gram_shingles && fr.exact_8gram_shingles.excess_over_null) ?? 0,
        top_window_terms: (fr.conceptual_window_alignments && fr.conceptual_window_alignments[0] && fr.conceptual_window_alignments[0].shared_distinctive_terms) ?? 0,
        top_window_jaccard: (fr.conceptual_window_alignments && fr.conceptual_window_alignments[0] && fr.conceptual_window_alignments[0].jaccard) ?? 0,
        top_vocab: (fr.conceptual_window_alignments && fr.conceptual_window_alignments[0] && fr.conceptual_window_alignments[0].terms) || [],
        alignments: (fr.conceptual_window_alignments || []).slice(0, 3),
      } : null,
      primary_excerpts: (res && res.primary_excerpts) ? res.primary_excerpts.slice(0, 3) : [],
      edition_diffs: (res && res.edition_diffs) ? res.edition_diffs.slice(0, 4) : [],
      sources: extractSourcesFromResult(command, args, res),
    }
    appendFileSync(TELEMETRY_LOG_PATH, JSON.stringify(entry) + '\n', 'utf8')
    return entry
  } catch {
    return null
  }
}

/** Read telemetry runs from JSONL log (newest first). */
export function readTelemetryRuns(limit = 50) {
  if (!existsSync(TELEMETRY_LOG_PATH)) return []
  try {
    const lines = readFileSync(TELEMETRY_LOG_PATH, 'utf8')
      .split('\n')
      .map((l) => l.trim())
      .filter(Boolean)
    const parsed = []
    for (let i = lines.length - 1; i >= 0 && parsed.length < limit; i -= 1) {
      try { parsed.push(JSON.parse(lines[i])) } catch { /* skip malformed line */ }
    }
    return parsed
  } catch {
    return []
  }
}

/** Render self-contained, human-friendly HTML for the Epistemic & Forensic Observatory. */
export function renderObservatoryHtml() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Archivist Epistemic & Forensic Observatory</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    :root {
      --bg: #0f172a;
      --surface: #1e293b;
      --surface-hover: #26354d;
      --card: #0b1120;
      --border: #334155;
      --text: #f8fafc;
      --text-secondary: #cbd5e1;
      --muted: #94a3b8;
      --primary: #38bdf8;
      --primary-bg: rgba(56, 189, 248, 0.12);
      --green: #34d399;
      --green-bg: rgba(52, 211, 153, 0.14);
      --amber: #fbbf24;
      --amber-bg: rgba(251, 191, 36, 0.14);
      --red: #f87171;
      --red-bg: rgba(248, 113, 113, 0.14);
      --purple: #c084fc;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-size: 15px;
      line-height: 1.6;
      padding: 24px 32px;
      max-width: 1480px;
      margin: 0 auto;
    }

    /* Top Header & Navigation Tabs */
    .top-bar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 20px;
      border-bottom: 1px solid var(--border);
      margin-bottom: 24px;
      flex-wrap: wrap;
      gap: 16px;
    }
    .brand h1 {
      font-size: 22px;
      font-weight: 700;
      letter-spacing: -0.02em;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .brand p { font-size: 14px; color: var(--muted); margin-top: 2px; }
    .nav-tabs {
      display: flex;
      gap: 8px;
      background: var(--card);
      padding: 5px;
      border-radius: 12px;
      border: 1px solid var(--border);
    }
    .tab-btn {
      background: transparent;
      color: var(--text-secondary);
      border: none;
      padding: 9px 18px;
      border-radius: 8px;
      font-size: 14px;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.15s;
    }
    .tab-btn:hover { color: var(--text); background: rgba(255,255,255,0.05); }
    .tab-btn.active { background: var(--primary); color: #081226; }

    /* Hero Search / Investigation Box */
    .search-hero {
      background: linear-gradient(180deg, var(--surface) 0%, #162033 100%);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 22px 24px;
      margin-bottom: 24px;
      box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.3);
    }
    .search-hero-title {
      font-size: 16px;
      font-weight: 600;
      margin-bottom: 12px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 8px;
    }
    .search-row {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .search-input {
      flex: 1;
      min-width: 280px;
      height: 46px;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: var(--card);
      color: var(--text);
      padding: 0 16px;
      font-size: 15px;
    }
    .search-input:focus, .search-select:focus {
      outline: none;
      border-color: var(--primary);
      box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.2);
    }
    .search-select {
      height: 46px;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: var(--card);
      color: var(--text);
      padding: 0 14px;
      font-size: 14px;
      font-weight: 500;
    }
    .primary-btn {
      height: 46px;
      border-radius: 10px;
      border: none;
      background: var(--primary);
      color: #081226;
      font-weight: 700;
      font-size: 15px;
      padding: 0 24px;
      cursor: pointer;
      transition: transform 0.1s, opacity 0.15s;
    }
    .primary-btn:hover { opacity: 0.92; }
    .primary-btn:disabled { opacity: 0.5; cursor: wait; }
    .preset-chips {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 12px;
      align-items: center;
    }
    .preset-label { font-size: 12px; color: var(--muted); font-weight: 600; text-transform: uppercase; }
    .chip {
      font-size: 12px;
      background: rgba(255, 255, 255, 0.06);
      color: var(--text-secondary);
      border: 1px solid var(--border);
      padding: 4px 12px;
      border-radius: 999px;
      cursor: pointer;
      transition: all 0.15s;
    }
    .chip:hover { border-color: var(--primary); color: var(--primary); background: var(--primary-bg); }

    /* Main Workspace Layout */
    .workspace {
      display: grid;
      grid-template-columns: 330px 1fr;
      gap: 24px;
      align-items: start;
    }
    @media (max-width: 1024px) { .workspace { grid-template-columns: 1fr; } }

    .sidebar {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 18px;
    }
    .sidebar h2 {
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--muted);
      margin-bottom: 12px;
    }
    .history-card {
      padding: 12px 14px;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: var(--card);
      margin-bottom: 10px;
      cursor: pointer;
      transition: all 0.15s;
    }
    .history-card:hover { border-color: var(--muted); }
    .history-card.active {
      border-color: var(--primary);
      background: rgba(56, 189, 248, 0.08);
    }
    .history-title {
      font-size: 14px;
      font-weight: 600;
      color: var(--text);
      margin-bottom: 6px;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }
    .history-meta {
      display: flex;
      justify-content: space-between;
      font-size: 12px;
      color: var(--muted);
    }

    /* Report Canvas */
    .report-canvas {
      display: flex;
      flex-direction: column;
      gap: 20px;
    }
    .section-card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 22px 24px;
    }
    .section-title {
      font-size: 17px;
      font-weight: 700;
      margin-bottom: 6px;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .section-desc {
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 16px;
    }

    /* Executive Takeaway Banner */
    .verdict-banner {
      background: var(--card);
      border-left: 4px solid var(--primary);
      border-radius: 10px;
      padding: 16px 20px;
      margin-top: 12px;
    }
    .verdict-headline {
      font-size: 16px;
      font-weight: 700;
      color: var(--text);
      margin-bottom: 6px;
    }
    .verdict-prose {
      font-size: 14px;
      color: var(--text-secondary);
    }

    /* Human-Friendly Perspective Bars */
    .perspective-row {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 12px 16px;
      margin-bottom: 10px;
    }
    .perspective-head {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      margin-bottom: 6px;
      flex-wrap: wrap;
      gap: 8px;
    }
    .perspective-name { font-size: 14px; font-weight: 600; }
    .perspective-explain { font-size: 12px; color: var(--muted); }
    .perspective-count { font-size: 14px; font-weight: 700; font-family: monospace; }
    .bar-track {
      height: 10px;
      background: #0f172a;
      border-radius: 999px;
      overflow: hidden;
    }
    .bar-fill { height: 100%; border-radius: 999px; transition: width 0.35s ease; }

    /* Plain-English badges */
    .badge {
      display: inline-flex;
      align-items: center;
      padding: 3px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 600;
      margin-right: 6px;
      margin-bottom: 4px;
    }
    .badge-gov { background: var(--green-bg); color: var(--green); border: 1px solid rgba(52, 211, 153, 0.35); }
    .badge-book { background: var(--primary-bg); color: var(--primary); border: 1px solid rgba(56, 189, 248, 0.35); }
    .badge-comm { background: var(--amber-bg); color: var(--amber); border: 1px solid rgba(251, 191, 36, 0.35); }
    .badge-deny { background: var(--red-bg); color: var(--red); border: 1px solid rgba(248, 113, 113, 0.35); }

    /* Readable Evidence Cards */
    .evidence-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }
    .evidence-card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 16px 18px;
      transition: border-color 0.15s;
    }
    .evidence-card:hover { border-color: #475569; }
    .evidence-title {
      font-size: 15px;
      font-weight: 700;
      color: var(--text);
      margin: 6px 0 4px;
    }
    .evidence-quote {
      margin-top: 10px;
      padding: 12px 16px;
      background: rgba(255, 255, 255, 0.03);
      border-left: 3px solid var(--primary);
      border-radius: 0 8px 8px 0;
      font-size: 14px;
      color: var(--text-secondary);
      font-style: italic;
    }
    .evidence-actions {
      margin-top: 10px;
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
      font-size: 12px;
      color: var(--muted);
    }
    .small-btn {
      background: var(--surface);
      color: var(--text);
      border: 1px solid var(--border);
      padding: 5px 12px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      gap: 5px;
    }
    .small-btn:hover { border-color: var(--primary); color: var(--primary); }
    .small-btn.danger:hover { border-color: var(--red); color: var(--red); }

    /* Library Grid (Tab 2) */
    .library-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
      gap: 16px;
    }
    .book-card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 18px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      gap: 12px;
    }

    /* Reader Modal */
    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.75);
      display: none;
      justify-content: center;
      align-items: center;
      padding: 24px;
      z-index: 1000;
    }
    .modal-box {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 16px;
      width: 100%;
      max-width: 920px;
      max-height: 88vh;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      box-shadow: 0 25px 50px -12px rgba(0,0,0,0.6);
    }
    .modal-head {
      padding: 16px 22px;
      border-bottom: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .modal-body {
      padding: 22px;
      overflow-y: auto;
      white-space: pre-wrap;
      font-family: Georgia, 'Times New Roman', serif;
      font-size: 16px;
      line-height: 1.75;
      color: var(--text);
      background: var(--card);
    }
    .hidden { display: none !important; }
  </style>
</head>
<body>

  <!-- Top Header & Clear Navigation Tabs -->
  <div class="top-bar">
    <div class="brand">
      <h1>🏛️ Archivist Research & Fact-Check Studio</h1>
      <p>Verify claims against millions of historical books, declassified government files, and primary texts</p>
    </div>
    <div class="nav-tabs">
      <button class="tab-btn active" id="tabBtnInvestigate" onclick="switchTab('investigate')">🔍 Investigate & Fact-Check</button>
      <button class="tab-btn" id="tabBtnLibrary" onclick="switchTab('library')">📚 Saved Books & Offline Library (<span id="libCountBadge">0</span>)</button>
      <button class="tab-btn" id="tabBtnSpeed" onclick="switchTab('speed')">⚡ Speed & Token Savings</button>
    </div>
  </div>

  <!-- TAB 1: INVESTIGATE & FACT-CHECK -->
  <div id="tabInvestigate">
    <div class="search-hero">
      <div class="search-hero-title">
        <span>Ask a research question, test a controversial claim, or compare two books for copying:</span>
        <span style="font-size:12px; color:var(--green); font-weight:500;">✓ Automatically strips framing bias & checks primary sources</span>
      </div>
      <div class="search-row">
        <select class="search-select" id="toolSelect" onchange="toggleCompareInput()">
          <option value="epistemic_dossier">Full Investigation (Checks Books + Gov Files + Primary Text)</option>
          <option value="claim_trace">Quick Footprint Scan (Count how many books mention this)</option>
          <option value="forensic_compare">Compare Two Books for Plagiarism / Copying</option>
        </select>
        <input class="search-input" type="text" id="queryInput" placeholder='e.g. Does "Report from Iron Mountain" hold any merit?' />
        <input class="search-input" type="text" id="compareInput" placeholder="Second Book ID to compare against (optional)" style="max-width: 280px;" />
        <button class="primary-btn" id="runBtn" onclick="runLiveQuery()">Investigate</button>
      </div>
      <div class="preset-chips">
        <span class="preset-label">Try an example:</span>
        <span class="chip" onclick="runPreset('epistemic_dossier', 'Does &quot;Report from Iron Mountain&quot; hold any merit?', 'lewin-l.-c.-report-from-iron-mountain-and-nyt-article-1967')">Report from Iron Mountain (1967 Hoax vs. Confession)</span>
        <span class="chip" onclick="runPreset('epistemic_dossier', 'Does &quot;Operation Northwoods&quot; hold any merit?', '')">Operation Northwoods (1962 Declassified Pentagon Plan)</span>
        <span class="chip" onclick="runPreset('epistemic_dossier', 'There are only two functional sexes defined by anisogamy', '')">Two Functional Sexes (Anisogamy Biology)</span>
        <span class="chip" onclick="runPreset('forensic_compare', 'dialogueauxenfe00jolygoog', 'lesmystresdupe01suee')">Test Plagiarism: Maurice Joly (1864) vs. Eugène Sue (1849)</span>
      </div>
    </div>

    <div class="workspace">
      <div class="sidebar">
        <h2>Recent Investigations</h2>
        <div id="runList">Loading...</div>
      </div>

      <div class="report-canvas" id="detailBody">
        <div class="section-card" style="text-align:center; padding: 48px 24px; color: var(--muted);">
          Select an investigation on the left or click one of the example buttons above to view its plain-English report.
        </div>
      </div>
    </div>
  </div>

  <!-- TAB 2: SAVED BOOKS & OFFLINE LIBRARY -->
  <div id="tabLibrary" class="hidden">
    <div class="section-card" style="margin-bottom: 20px;">
      <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px;">
        <div>
          <div class="section-title">📚 Offline Book Cache & Borrowed Library</div>
          <div class="section-desc" style="margin-bottom:0;" id="libSummaryText">Every book downloaded or borrowed from Archive.org is saved here so you can read it anytime offline.</div>
        </div>
        <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
          <input class="search-input" type="text" id="borrowInput" placeholder="Enter Archive.org Book ID to borrow/save (e.g. observingsystems0000vonf)" style="height:38px; min-width:300px; font-size:13px;" />
          <button class="primary-btn" style="height:38px; font-size:13px; padding:0 16px;" onclick="borrowAndCacheBook()" id="borrowBtn">+ Borrow & Save Book</button>
          <button class="small-btn" style="height:38px;" onclick="syncCorpus()">🔄 Re-Index Search Database</button>
        </div>
      </div>
    </div>
    <div class="library-grid" id="libraryGrid"></div>
  </div>

  <!-- TAB 3: SPEED & TOKEN SAVINGS -->
  <div id="tabSpeed" class="hidden">
    <div class="section-card">
      <div class="section-title">⚡ Why This Pipeline Saves ~95% of AI Context Tokens</div>
      <div class="section-desc">Instead of the AI agent running 12 separate search and file-reading commands (which dumps 50,000+ raw OCR characters into the chat window), <code>archive_epistemic_dossier</code> runs all 5 stages on the server and returns a clean summary.</div>
      <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:16px; margin-top:16px;">
        <div class="perspective-row">
          <div class="perspective-explain">Total Investigations Logged</div>
          <div style="font-size:28px; font-weight:700; margin-top:4px;" id="kpiRuns">0</div>
        </div>
        <div class="perspective-row">
          <div class="perspective-explain">Average Investigation Speed</div>
          <div style="font-size:28px; font-weight:700; margin-top:4px;" id="kpiAvgMs">—</div>
        </div>
        <div class="perspective-row">
          <div class="perspective-explain">Estimated AI Context Tokens Saved</div>
          <div style="font-size:28px; font-weight:700; color:var(--green); margin-top:4px;" id="kpiSaved">0</div>
        </div>
        <div class="perspective-row">
          <div class="perspective-explain">Offline DuckDB Search Index</div>
          <div style="font-size:28px; font-weight:700; color:var(--primary); margin-top:4px;" id="kpiCorpusDocs">—</div>
          <div class="perspective-explain" id="kpiCorpusTokens"></div>
        </div>
      </div>
    </div>
  </div>

  <!-- BOOK READER MODAL -->
  <div class="modal-backdrop" id="readerModal" onclick="if(event.target===this)closeReader()">
    <div class="modal-box">
      <div class="modal-head">
        <div>
          <div style="font-size:16px; font-weight:700;" id="readerTitle">Book Reader</div>
          <div style="font-size:12px; color:var(--muted);" id="readerSub">Cached offline copy</div>
        </div>
        <button class="small-btn" onclick="closeReader()">✕ Close Reader</button>
      </div>
      <div class="modal-body" id="readerContent">Loading text...</div>
    </div>
  </div>

  <script>
    let allRuns = [];
    let cacheData = null;
    let selectedId = null;

    const apiBase = window.location.pathname.includes('/api/dsh-archive') ? '/api/dsh-archive' : '';

    function switchTab(tab) {
      document.getElementById('tabInvestigate').classList.toggle('hidden', tab !== 'investigate');
      document.getElementById('tabLibrary').classList.toggle('hidden', tab !== 'library');
      document.getElementById('tabSpeed').classList.toggle('hidden', tab !== 'speed');
      document.getElementById('tabBtnInvestigate').classList.toggle('active', tab === 'investigate');
      document.getElementById('tabBtnLibrary').classList.toggle('active', tab === 'library');
      document.getElementById('tabBtnSpeed').classList.toggle('active', tab === 'speed');
    }

    function toggleCompareInput() {
      const tool = document.getElementById('toolSelect').value;
      const cmp = document.getElementById('compareInput');
      cmp.style.display = (tool === 'claim_trace') ? 'none' : 'block';
    }

    function friendlyToolName(cmd) {
      const map = {
        epistemic_dossier: 'Full Investigation Dossier',
        claim_trace: 'Archive Footprint Scan',
        forensic_compare: 'Book-to-Book Plagiarism Test',
        search: 'Catalog Search',
        fulltext_search: 'Full-Text Book Search',
        quote_from_index: 'Borrowed Book Quote Lookup',
        media_read_text: 'Book Text Reader',
        wayback_read: 'Wayback Webpage Reader',
      };
      return map[cmd] || cmd;
    }

    function friendlyTierBadge(tier) {
      const t = String(tier || 'T4').toUpperCase();
      if (t === 'T1') return '<span class="badge badge-gov" title="Official Government, Military (DTIC), CIA, or FOIA Collection">🏛️ Official Gov / Military Record (T1)</span>';
      if (t === 'PRIMARY_TEXT') return '<span class="badge badge-gov" title="Full primary book text downloaded and verified locally">📖 Full Primary Book Text</span>';
      if (t === 'T2' || t === 'LOCAL_DUCKDB') return '<span class="badge badge-book" title="Published University or Library Book Deposit">📚 Published Library Book (T2)</span>';
      if (t === 'T3') return '<span class="badge badge-book">🗂️ Subject Archive Collection (T3)</span>';
      return '<span class="badge badge-comm" title="Community or User Upload on Archive.org">👤 Community Upload (T4)</span>';
    }

    function friendlyStanceBadge(stance) {
      const s = String(stance || '').toLowerCase();
      if (s === 'states' || s === 'primary_record') return '<span class="badge badge-gov">✓ Directly Addresses Topic</span>';
      if (s === 'denies') return '<span class="badge badge-deny">✗ Explicitly Refutes / Calls Forgery</span>';
      if (s === 'reports') return '<span class="badge badge-comm">🗣️ Reports as Claim</span>';
      if (s === 'harvested' || s === 'fts_quote') return '<span class="badge badge-book">🔎 Extracted Primary Excerpt</span>';
      return '';
    }

    function buildPlainEnglishSummary(r) {
      const t = r.triangulation;
      const f = r.forensics;
      const parts = [];

      if (t) {
        parts.push(\`We scanned Archive.org for <strong>\${escapeHtml(t.subject_core || r.query)}</strong> and found <strong>\${(t.total_circulation || 0).toLocaleString()} books and documents</strong> mentioning it.\`);
        if (t.primary_t1_docs > 0) {
          parts.push(\`Importantly, <strong>\${t.primary_t1_docs.toLocaleString()} of those are official government, military, or declassified files (Tier 1)</strong>.\`);
        } else {
          parts.push(\`None of the matches sit in official government or military collections—meaning this topic circulates primarily in books and public commentary.\`);
        }
        if (t.corroboration_docs > 0 || t.critique_docs > 0) {
          parts.push(\`To avoid confirmation bias, we checked both sides: <strong>\${t.corroboration_docs.toLocaleString()} documents</strong> discuss its authenticity/evidence, while <strong>\${t.critique_docs.toLocaleString()} documents</strong> discuss forgery, hoax, or refutation claims.\`);
        }
      }

      if (f) {
        if (f.excess_8grams >= 20) {
          parts.push(\`<br><br><strong>Direct Book Comparison (\${escapeHtml(f.identifier_a)} vs. \${escapeHtml(f.identifier_b)}):</strong> <span style="color:var(--green); font-weight:700;">Strong Verbatim Overlap Detected.</span> These two texts share <strong>\${f.observed_8grams.toLocaleString()} identical 8-word phrases</strong> (compared to \${f.null_8grams} by random chance), proving they are either editions of the same work or direct word-for-word copying.\`);
        } else if (f.top_window_jaccard >= 0.08 || f.top_window_terms >= 25) {
          parts.push(\`<br><br><strong>Direct Book Comparison (\${escapeHtml(f.identifier_a)} vs. \${escapeHtml(f.identifier_b)}):</strong> <span style="color:var(--amber); font-weight:700;">Structural / Translated Parallel Detected.</span> While exact 8-word strings differ (\${f.observed_8grams} matches, typical when translated across languages), matching 250-word sections share <strong>\${f.top_window_terms} distinctive topic words</strong> (\${Math.round(f.top_window_jaccard * 100)}% vocabulary overlap), indicating translated or paraphrased adaptation.\`);
        } else {
          parts.push(\`<br><br><strong>Direct Book Comparison (\${escapeHtml(f.identifier_a)} vs. \${escapeHtml(f.identifier_b)}):</strong> <span style="color:var(--red); font-weight:700;">No Verbatim Plagiarism Found.</span> After stripping library headers, these two books share only <strong>\${f.observed_8grams} exact 8-word phrases</strong> and low vocabulary overlap (\${Math.round(f.top_window_jaccard * 100)}%). Claims of direct copying between these specific files are not supported by the primary text.\`);
        }
      }

      if (parts.length === 0) {
        return \`Completed <strong>\${friendlyToolName(r.command)}</strong> in \${(r.duration_ms / 1000).toFixed(1)} seconds and extracted <strong>\${(r.sources || []).length} source records</strong>.\`;
      }
      return parts.join(' ');
    }

    async function loadTelemetry() {
      const res = await fetch(apiBase + '/telemetry');
      const data = await res.json();
      allRuns = data.runs || [];
      cacheData = data.cache || null;

      document.getElementById('kpiRuns').textContent = allRuns.length;
      const totalSaved = allRuns.reduce((s, r) => s + (r.tokens_saved_est || 0), 0);
      const avgMs = allRuns.length ? Math.round(allRuns.reduce((s, r) => s + (r.duration_ms || 0), 0) / allRuns.length) : 0;
      document.getElementById('kpiSaved').textContent = totalSaved.toLocaleString() + ' tokens';
      document.getElementById('kpiAvgMs').textContent = (avgMs / 1000).toFixed(1) + ' sec';

      if (data.corpus) {
        document.getElementById('kpiCorpusDocs').textContent = (data.corpus.documents || 0) + ' Books';
        document.getElementById('kpiCorpusTokens').textContent = ((data.corpus.tokens || 0).toLocaleString()) + ' words indexed for instant search';
      }

      renderRunList();
      renderLibraryTab();

      if (!selectedId && allRuns.length > 0) {
        selectRun(allRuns[0].id);
      } else if (selectedId) {
        selectRun(selectedId);
      }
    }

    function renderRunList() {
      const el = document.getElementById('runList');
      if (allRuns.length === 0) {
        el.innerHTML = '<div style="font-size:13px; color:var(--muted);">No investigations yet. Click an example above!</div>';
        return;
      }
      el.innerHTML = allRuns.map(r => \`
        <div class="history-card \${r.id === selectedId ? 'active' : ''}" onclick="selectRun('\${r.id}')">
          <div class="history-title">\${escapeHtml(String(r.query))}</div>
          <div class="history-meta">
            <span>\${friendlyToolName(r.command)}</span>
            <span>\${(r.duration_ms / 1000).toFixed(1)}s · \${(r.sources || []).length} sources</span>
          </div>
        </div>
      \`).join('');
    }

    function selectRun(id) {
      selectedId = id;
      renderRunList();
      const r = allRuns.find(x => x.id === id);
      if (!r) return;

      let html = \`
        <div class="section-card">
          <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px;">
            <span class="badge badge-book">\${friendlyToolName(r.command)}</span>
            <span style="font-size:13px; color:var(--muted);">Completed in \${(r.duration_ms / 1000).toFixed(1)}s · Saved ~\${(r.tokens_saved_est || 0).toLocaleString()} AI tokens</span>
          </div>
          <h2 style="font-size:20px; font-weight:700; margin-top:8px;">\${escapeHtml(String(r.query))}</h2>
          <div class="verdict-banner">
            <div class="verdict-headline">💡 Plain-English Summary of Findings</div>
            <div class="verdict-prose">\${buildPlainEnglishSummary(r)}</div>
          </div>
        </div>
      \`;

      if (r.triangulation) {
        const t = r.triangulation;
        const maxDoc = Math.max(1, t.total_circulation, t.corroboration_docs, t.critique_docs, t.substance_docs);
        html += \`
          <div class="section-card">
            <div class="section-title">🧭 How the Archive Treats This Topic (4-Way Balance Check)</div>
            <div class="section-desc">Instead of only searching for what confirms your question, we automatically stripped biased words and checked four different angles across Archive.org:</div>
            \${perspectiveBar('Total Books & Documents Mentioning Topic', 'Overall footprint across the Archive.org full-text library', t.total_circulation, maxDoc, '#38bdf8')}
            \${perspectiveBar('Official Government, Military & FOIA Files (Tier 1)', 'Declassified CIA, FBI, Pentagon (DTIC), and U.S. Congressional records', t.primary_t1_docs, maxDoc, '#34d399')}
            \${perspectiveBar('Documents Arguing for Authenticity / Evidence', 'Sources using corroborating or primary-evidence terminology', t.corroboration_docs, maxDoc, '#c084fc')}
            \${perspectiveBar('Documents Discussing Forgery, Hoax, or Refutation', 'Sources critiquing, debunking, or analyzing fabrication', t.critique_docs, maxDoc, '#f87171')}
            \${perspectiveBar('In-Depth Historical & Structural Analysis', 'Scholarly and analytical works examining the substance and impact', t.substance_docs, maxDoc, '#fbbf24')}
          </div>
        \`;
      }

      if (r.forensics) {
        const f = r.forensics;
        const pctOverlap = Math.round((f.top_window_jaccard || 0) * 100);
        html += \`
          <div class="section-card">
            <div class="section-title">🧬 Direct Book-to-Book Text Comparison</div>
            <div class="section-desc">Comparing <strong>\${escapeHtml(f.identifier_a)}</strong> directly against <strong>\${escapeHtml(f.identifier_b)}</strong> word-by-word:</div>
            <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap:12px;">
              <div class="perspective-row">
                <div class="perspective-explain">Identical 8-Word Phrases</div>
                <div style="font-size:22px; font-weight:700; color:var(--green); margin-top:4px;">\${f.observed_8grams.toLocaleString()} exact matches</div>
                <div class="perspective-explain">Random chance baseline: \${f.null_8grams}</div>
              </div>
              <div class="perspective-row">
                <div class="perspective-explain">Topic Vocabulary Overlap</div>
                <div style="font-size:22px; font-weight:700; color:var(--primary); margin-top:4px;">\${pctOverlap}% (\${f.top_window_terms} shared words)</div>
                <div class="perspective-explain">In best-matching 250-word paragraph</div>
              </div>
            </div>
            \${(f.top_vocab && f.top_vocab.length) ? \`
              <div style="margin-top:12px; font-size:13px; color:var(--text-secondary);">
                <strong>Shared Key Terms in Matching Section:</strong> \${f.top_vocab.map(w => \`<span class="chip" style="cursor:default;">\${escapeHtml(w)}</span>\`).join(' ')}
              </div>
            \` : ''}
          </div>
        \`;
      }

      if (r.edition_diffs && r.edition_diffs.length > 0) {
        html += \`
          <div class="section-card">
            <div class="section-title">🔍 Sections Added or Missing Between Editions</div>
            <div class="section-desc">Passages that appear in one version of the book but are missing from the other (such as added introductions, postscripts, or redactions):</div>
            <div class="evidence-grid">
        \`;
        for (const d of r.edition_diffs) {
          html += \`
            <div class="evidence-card">
              <span class="badge badge-gov">Added / Unique Section (\${d.length_tokens.toLocaleString()} words)</span>
              <div class="evidence-title">Present in "\${escapeHtml(d.in)}" (Missing from "\${escapeHtml(d.missing_from)}")</div>
              <div class="evidence-quote">"\${escapeHtml(String(d.snippet || '').slice(0, 320))}..."</div>
              <div class="evidence-actions">
                <button class="small-btn" onclick="openReader('\${escapeHtml(d.in)}')">📖 Read This Book in Browser</button>
              </div>
            </div>
          \`;
        }
        html += \`</div></div>\`;
      }

      const sources = r.sources || [];
      html += \`
        <div class="section-card">
          <div class="section-title">📖 Verbatim Passages & Primary Sources (\${sources.length})</div>
          <div class="section-desc">Direct quotes extracted from books, government files, and library records. Click any book to read it locally or open its original scan on Archive.org:</div>
          <div class="evidence-grid">
      \`;
      if (sources.length === 0) {
        html += \`<div style="color:var(--muted); font-size:14px;">No individual quotes recorded for this run.</div>\`;
      } else {
        for (const s of sources) {
          const pageLabel = (s.page !== null && s.page !== undefined)
            ? (s.page_scope === 'item_aggregate' ? \`Book-level match (lending preview)\` : \`Page \${s.page}\`)
            : '';
          html += \`
            <div class="evidence-card">
              <div>
                \${friendlyTierBadge(s.tier)}
                \${friendlyStanceBadge(s.stance)}
              </div>
              <div class="evidence-title">\${escapeHtml(s.title || s.identifier)}</div>
              \${s.quote ? \`<div class="evidence-quote">"\${escapeHtml(s.quote)}"</div>\` : ''}
              <div class="evidence-actions">
                \${s.identifier ? \`<button class="small-btn" onclick="openReader('\${escapeHtml(s.identifier)}')">📖 Read Local Copy</button>\` : ''}
                \${s.url ? \`<a class="small-btn" href="\${escapeHtml(s.url)}" target="_blank">🌐 Open on Archive.org ↗</a>\` : ''}
                \${pageLabel ? \`<span>📍 \${escapeHtml(pageLabel)}</span>\` : ''}
                \${s.date ? \`<span>📅 \${escapeHtml(s.date)}</span>\` : ''}
              </div>
            </div>
          \`;
        }
      }
      html += \`</div></div>\`;

      document.getElementById('detailBody').innerHTML = html;
    }

    function perspectiveBar(title, subtitle, count, max, color) {
      const pct = Math.min(100, Math.max(2, Math.round(((count || 0) / max) * 100)));
      return \`
        <div class="perspective-row">
          <div class="perspective-head">
            <div>
              <div class="perspective-name">\${title}</div>
              <div class="perspective-explain">\${subtitle}</div>
            </div>
            <div class="perspective-count" style="color:\${color}">\${(count || 0).toLocaleString()} documents</div>
          </div>
          <div class="bar-track">
            <div class="bar-fill" style="width:\${count ? pct : 0}%; background:\${color};"></div>
          </div>
        </div>
      \`;
    }

    function renderLibraryTab() {
      if (!cacheData) return;
      const s = cacheData.summary || {};
      const texts = cacheData.texts || [];
      const metaMap = {};
      for (const m of (cacheData.metadata || [])) {
        metaMap[m.identifier] = m;
      }
      document.getElementById('libCountBadge').textContent = texts.length;
      document.getElementById('libSummaryText').textContent =
        \`You have \${texts.length} books & documents saved offline (\${(s.texts_total_chars / (1024 * 1024)).toFixed(1)} MB of text) and \${s.metadata_cached || 0} item records cached.\`;

      const grid = document.getElementById('libraryGrid');
      if (texts.length === 0) {
        grid.innerHTML = '<div class="section-card">Your offline library is empty. Enter an Archive.org ID above to borrow or save a book!</div>';
        return;
      }

      grid.innerHTML = texts.map(t => {
        const m = metaMap[t.identifier] || {};
        const title = m.title || t.identifier.replace(/[-_]/g, ' ');
        const isBorrowed = String(t.format || '').toLowerCase().includes('borrowed');
        return \`
          <div class="book-card">
            <div>
              <div>
                <span class="badge \${isBorrowed ? 'badge-comm' : 'badge-gov'}">\${isBorrowed ? '🔑 Borrowed Library Digest' : '📖 Full Offline Book'}</span>
                <span style="font-size:12px; color:var(--muted);">\${Math.round((t.chars || 0) / 1024).toLocaleString()} KB (\${(t.tokens || 0).toLocaleString()} words)</span>
              </div>
              <div style="font-size:16px; font-weight:700; margin-top:8px; word-break:break-word;">\${escapeHtml(title)}</div>
              <div style="font-size:12px; color:var(--muted); font-family:monospace; margin-top:2px;">ID: \${escapeHtml(t.identifier)}</div>
              \${m.creator ? \`<div style="font-size:13px; color:var(--text-secondary); margin-top:4px;">Author: \${escapeHtml(m.creator)}</div>\` : ''}
            </div>
            <div style="display:flex; gap:8px; flex-wrap:wrap; padding-top:10px; border-top:1px solid var(--border);">
              <button class="small-btn" onclick="openReader('\${escapeHtml(t.identifier)}')">📖 Read Text</button>
              <a class="small-btn" href="https://archive.org/details/\${escapeHtml(t.identifier)}" target="_blank">🌐 Archive.org ↗</a>
              <button class="small-btn danger" style="margin-left:auto;" onclick="deleteCachedItem('\${escapeHtml(t.identifier)}')">🗑️ Remove</button>
            </div>
          </div>
        \`;
      }).join('');
    }

    async function openReader(identifier) {
      const modal = document.getElementById('readerModal');
      const titleEl = document.getElementById('readerTitle');
      const subEl = document.getElementById('readerSub');
      const bodyEl = document.getElementById('readerContent');
      modal.style.display = 'flex';
      titleEl.textContent = identifier;
      subEl.textContent = 'Loading from local cache...';
      bodyEl.textContent = 'Loading book text...';

      const res = await fetch(apiBase + '/cache', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ operation: 'inspect', identifier })
      });
      const data = await res.json();
      const info = data.result || {};
      if (info.has_cached_text && info.cached_text_preview) {
        titleEl.textContent = (info.metadata_summary && info.metadata_summary.title) || identifier;
        subEl.textContent = \`Saved offline (\${(info.cached_text_chars || 0).toLocaleString()} characters total — showing opening excerpt)\`;
        bodyEl.textContent = info.cached_text_preview;
      } else {
        subEl.textContent = 'Not yet saved offline — borrowing/fetching now...';
        const bRes = await fetch(apiBase + '/cache', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ operation: 'borrow_and_cache', identifier })
        });
        const bData = await bRes.json();
        const bInfo = bData.result || {};
        titleEl.textContent = bInfo.title || identifier;
        subEl.textContent = \`Status: \${bInfo.status || 'fetched'} (\${(bInfo.chars || 0).toLocaleString()} chars)\`;
        bodyEl.textContent = bInfo.text || 'No readable text available for this item.';
        await loadTelemetry();
      }
    }

    function closeReader() {
      document.getElementById('readerModal').style.display = 'none';
    }

    function runPreset(tool, q, cmp) {
      document.getElementById('toolSelect').value = tool;
      document.getElementById('queryInput').value = q;
      document.getElementById('compareInput').value = cmp || '';
      toggleCompareInput();
      runLiveQuery();
    }

    async function runLiveQuery() {
      const tool = document.getElementById('toolSelect').value;
      const q = document.getElementById('queryInput').value.trim();
      const cmp = document.getElementById('compareInput').value.trim();
      if (!q) return;
      const btn = document.getElementById('runBtn');
      btn.textContent = 'Investigating Archive...';
      btn.disabled = true;
      try {
        await fetch(apiBase + '/run', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ command: tool, query: q, compare_with: cmp || undefined })
        });
        selectedId = null;
        await loadTelemetry();
      } finally {
        btn.textContent = 'Investigate';
        btn.disabled = false;
      }
    }

    async function borrowAndCacheBook() {
      const ident = document.getElementById('borrowInput').value.trim();
      if (!ident) return;
      const btn = document.getElementById('borrowBtn');
      btn.textContent = 'Borrowing & Saving...';
      btn.disabled = true;
      try {
        await fetch(apiBase + '/cache', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ operation: 'borrow_and_cache', identifier: ident })
        });
        document.getElementById('borrowInput').value = '';
        await loadTelemetry();
      } finally {
        btn.textContent = '+ Borrow & Save Book';
        btn.disabled = false;
      }
    }

    async function deleteCachedItem(identifier) {
      await fetch(apiBase + '/cache', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ operation: 'delete', identifier })
      });
      await loadTelemetry();
    }

    async function syncCorpus() {
      await fetch(apiBase + '/cache', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ operation: 'sync_corpus' })
      });
      await loadTelemetry();
    }

    function escapeHtml(s) {
      return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    toggleCompareInput();
    loadTelemetry();
  </script>
</body>
</html>`
}

/** Build the JSON payload for `/api/dsh-archive/telemetry`. */
export async function buildTelemetryPayload(runBridgeFn) {
  const runs = readTelemetryRuns(50)
  let corpus = null
  let corpus_docs = []
  let warehouse = null
  let cache = null
  try {
    corpus = await runBridgeFn('corpus_stats', {}, 15000, { skipTelemetry: true })
    const sqlRes = await runBridgeFn('corpus_sql', {
      query: 'SELECT identifier, chars, tokens, ocr_quality FROM documents ORDER BY tokens DESC LIMIT 40',
    }, 15000, { skipTelemetry: true })
    corpus_docs = (sqlRes.rows || []).map((r) => ({
      identifier: r[0],
      chars: r[1],
      tokens: r[2],
      ocr_quality: r[3],
    }))
  } catch { /* optional */ }

  try {
    cache = await runBridgeFn('cache_manage', { operation: 'list', limit: 40 }, 15000, { skipTelemetry: true })
  } catch { /* optional */ }

  try {
    const whRes = await runBridgeFn('research_search', { query: 'system', limit: 1 }, 15000, { skipTelemetry: true })
    warehouse = whRes.warehouse_stats || null
  } catch { /* optional */ }

  return { runs, corpus, corpus_docs, warehouse, cache }
}

/** Execute a query initiated from the Observatory UI and record its telemetry. */
export async function executeObservatoryRun(runBridgeFn, body) {
  const cmd = body.command || 'epistemic_dossier'
  const q = String(body.query || '').trim()
  if (cmd === 'forensic_compare') {
    const parts = q.split(/\s+vs\s+|\s*,\s*/i).filter(Boolean)
    const idA = parts[0] || q
    const idB = body.compare_with || parts[1] || idA
    return await runBridgeFn('forensic_compare', { identifier_a: idA, identifier_b: idB, top_passages: 4 })
  }
  if (cmd === 'claim_trace') {
    return await runBridgeFn('claim_trace', { statement: q, size: 6, triangulate: true })
  }
  return await runBridgeFn('epistemic_dossier', {
    statement: q,
    compare_with: body.compare_with || undefined,
  })
}

/** Start standalone HTTP server when executed directly (`node observatory.js`). */
export function startStandaloneObservatory(runBridgeFn, port = Number(process.env.DSH_ARCHIVE_OBS_PORT || 8090)) {
  const server = createServer(async (req, res) => {
    const url = new URL(req.url || '/', `http://127.0.0.1:${port}`)
    if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/api/dsh-archive/observatory')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' })
      res.end(renderObservatoryHtml())
      return
    }
    if (req.method === 'GET' && (url.pathname === '/telemetry' || url.pathname === '/api/dsh-archive/telemetry')) {
      const payload = await buildTelemetryPayload(runBridgeFn)
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' })
      res.end(JSON.stringify(payload))
      return
    }
    if (req.method === 'POST' && (url.pathname === '/run' || url.pathname === '/api/dsh-archive/run')) {
      let raw = ''
      req.on('data', (c) => { raw += c.toString('utf8') })
      req.on('end', async () => {
        try {
          const body = JSON.parse(raw || '{}')
          const out = await executeObservatoryRun(runBridgeFn, body)
          res.writeHead(200, { 'Content-Type': 'application/json' })
          res.end(JSON.stringify({ ok: true, result: out }))
        } catch (err) {
          res.writeHead(500, { 'Content-Type': 'application/json' })
          res.end(JSON.stringify({ ok: false, error: String((err && err.message) || err) }))
        }
      })
      return
    }
    if (req.method === 'POST' && (url.pathname === '/cache' || url.pathname === '/api/dsh-archive/cache')) {
      let raw = ''
      req.on('data', (c) => { raw += c.toString('utf8') })
      req.on('end', async () => {
        try {
          const body = JSON.parse(raw || '{}')
          const out = await runBridgeFn('cache_manage', body, 60000, { skipTelemetry: true })
          res.writeHead(200, { 'Content-Type': 'application/json' })
          res.end(JSON.stringify({ ok: true, result: out }))
        } catch (err) {
          res.writeHead(500, { 'Content-Type': 'application/json' })
          res.end(JSON.stringify({ ok: false, error: String((err && err.message) || err) }))
        }
      })
      return
    }
    res.writeHead(404)
    res.end('Not found')
  })

  server.listen(port, '127.0.0.1', () => {
    console.log(`Archivist Epistemic & Forensic Observatory listening at http://127.0.0.1:${port}`)
  })
  return server
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  import('./index.js').then((mod) => {
    startStandaloneObservatory(mod.runBridge)
  })
}
