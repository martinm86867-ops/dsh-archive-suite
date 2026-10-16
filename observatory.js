/**
 * Local Epistemic & Forensic Telemetry Observatory for @martinm86867-ops/dsh-archive-suite
 *
 * Records every bridge command execution (latency, payload bytes, estimated
 * tokens saved, extracted source URLs/identifiers, and 4-axis triangulation /
 * shingle metrics) to an append-only JSONL log and serves an interactive
 * dark-mode Web Observatory at `/api/dsh-archive/observatory` (on the DSH web
 * server) or standalone via `node observatory.js` on http://127.0.0.1:8090.
 */

import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
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
        quote: e.quote ? String(e.quote).slice(0, 220) : '',
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
        quote: p.quote ? String(p.quote).slice(0, 220) : '',
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
        quote: w.quote ? String(w.quote).slice(0, 220) : '',
      })
    }
  }

  // 2. Primary excerpts / restricted quotes in epistemic_dossier
  for (const pe of (res.primary_excerpts || [])) {
    add({
      identifier: pe.identifier,
      title: `Primary Text (${pe.chars} chars harvested)`,
      tier: 'PRIMARY_TEXT',
      stance: 'harvested',
      url: pe.url,
      quote: (pe.windows && pe.windows[0] && pe.windows[0].excerpt) || '',
    })
  }
  for (const rq of (res.restricted_quotes || [])) {
    add({
      identifier: rq.identifier,
      title: `Lending-Restricted FTS Quote (${rq.identifier})`,
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
      quote: `Downloads: ${d.downloads || 0}`,
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

  return sources.slice(0, 20)
}

/** Append one tool/bridge execution record to the local JSONL telemetry store. */
export function recordTelemetryRun({ command, args, res, error, durationMs }) {
  try {
    mkdirSync(dirname(TELEMETRY_LOG_PATH), { recursive: true })
    const rawJson = JSON.stringify(res || { error: String(error || '') })
    const outputChars = rawJson.length
    const outputTokensEst = Math.round(outputChars / 4)

    // Estimate tokens that would have been burned in an unbundled multi-turn loop
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
      } : null,
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

/** Render self-contained HTML for the Epistemic & Forensic Observatory. */
export function renderObservatoryHtml() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Archivist Epistemic & Forensic Observatory</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    :root {
      --bg: #0b0f17;
      --panel: #131a26;
      --panel-alt: #192231;
      --border: #263347;
      --text: #e2e8f0;
      --muted: #8b9bb4;
      --accent: #38bdf8;
      --green: #22c55e;
      --amber: #f59e0b;
      --red: #ef4444;
      --purple: #a855f7;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      padding: 20px 28px;
      line-height: 1.5;
    }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid var(--border);
      padding-bottom: 16px;
      margin-bottom: 20px;
      flex-wrap: wrap;
      gap: 12px;
    }
    h1 { font-size: 20px; font-weight: 700; display: flex; align-items: center; gap: 10px; }
    .pill {
      font-size: 11px;
      font-weight: 600;
      padding: 3px 10px;
      border-radius: 999px;
      background: rgba(56, 189, 248, 0.15);
      color: var(--accent);
      border: 1px solid rgba(56, 189, 248, 0.35);
    }
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 14px;
      margin-bottom: 20px;
    }
    .kpi {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 14px 16px;
    }
    .kpi-label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); }
    .kpi-val { font-size: 24px; font-weight: 700; margin-top: 4px; color: var(--text); font-family: monospace; }
    .kpi-sub { font-size: 11px; color: var(--green); margin-top: 2px; }
    .runner-bar {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 14px 16px;
      margin-bottom: 20px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }
    input, select, button {
      height: 36px;
      border-radius: 7px;
      border: 1px solid var(--border);
      background: #080b11;
      color: var(--text);
      padding: 0 12px;
      font-size: 13px;
    }
    input { flex: 1; min-width: 240px; }
    button {
      background: var(--accent);
      color: #06101e;
      font-weight: 600;
      border: none;
      cursor: pointer;
      padding: 0 16px;
    }
    button:hover { opacity: 0.9; }
    .layout {
      display: grid;
      grid-template-columns: 360px 1fr;
      gap: 18px;
    }
    @media (max-width: 980px) { .layout { grid-template-columns: 1fr; } }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 16px;
    }
    .panel h2 { font-size: 14px; font-weight: 600; margin-bottom: 12px; color: var(--accent); text-transform: uppercase; letter-spacing: 0.04em; }
    .run-item {
      padding: 10px 12px;
      border-radius: 8px;
      border: 1px solid var(--border);
      background: var(--panel-alt);
      margin-bottom: 8px;
      cursor: pointer;
      transition: border-color 0.15s;
    }
    .run-item:hover, .run-item.active { border-color: var(--accent); }
    .run-top { display: flex; justify-content: space-between; font-size: 12px; font-weight: 600; }
    .run-cmd { color: var(--accent); font-family: monospace; }
    .run-q { font-size: 12px; color: var(--text); margin: 4px 0; word-break: break-word; }
    .run-meta { font-size: 11px; color: var(--muted); display: flex; gap: 10px; font-family: monospace; }
    .bar-row { margin-bottom: 10px; }
    .bar-head { display: flex; justify-content: space-between; font-size: 12px; margin-bottom: 4px; }
    .bar-track { height: 10px; background: #080b11; border-radius: 5px; overflow: hidden; border: 1px solid var(--border); }
    .bar-fill { height: 100%; border-radius: 4px; transition: width 0.3s; }
    .source-card {
      background: var(--panel-alt);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 10px 12px;
      margin-bottom: 8px;
      font-size: 12px;
    }
    .tier-badge {
      display: inline-block;
      padding: 1px 6px;
      border-radius: 4px;
      font-size: 10px;
      font-weight: 700;
      font-family: monospace;
      margin-right: 6px;
    }
    .tier-T1, .tier-PRIMARY_TEXT { background: rgba(34, 197, 94, 0.2); color: var(--green); border: 1px solid var(--green); }
    .tier-T2, .tier-LOCAL_DUCKDB { background: rgba(56, 189, 248, 0.2); color: var(--accent); border: 1px solid var(--accent); }
    .tier-T3, .tier-T4, .tier-CATALOG { background: rgba(245, 158, 11, 0.2); color: var(--amber); border: 1px solid var(--amber); }
    .quote-box {
      margin-top: 6px;
      padding: 6px 8px;
      background: #080b11;
      border-left: 3px solid var(--accent);
      font-family: monospace;
      font-size: 11px;
      color: var(--muted);
    }
    a { color: var(--accent); text-decoration: none; }
    a:hover { text-decoration: underline; }
    .corpus-tag {
      display: inline-block;
      padding: 4px 8px;
      margin: 3px;
      border-radius: 6px;
      background: #080b11;
      border: 1px solid var(--border);
      font-family: monospace;
      font-size: 11px;
    }
  </style>
</head>
<body>
  <header>
    <h1>🏛️⚡ Archivist Epistemic & Forensic Observatory <span class="pill">43 Native Tools + DuckDB</span></h1>
    <div style="font-size:12px; color:var(--muted);">Live Telemetry · 4-Axis Triangulation · Shingle & Window Forensics · Provenance Graph</div>
  </header>

  <div class="kpi-grid">
    <div class="kpi">
      <div class="kpi-label">Recorded Tool Runs</div>
      <div class="kpi-val" id="kpiRuns">0</div>
      <div class="kpi-sub" id="kpiAvgMs">Avg Latency: — ms</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Context Tokens Saved</div>
      <div class="kpi-val" id="kpiSaved">0</div>
      <div class="kpi-sub">vs. 12-Step Typewriter Loop (~95% reduction)</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Local DuckDB Corpus</div>
      <div class="kpi-val" id="kpiCorpusDocs">—</div>
      <div class="kpi-sub" id="kpiCorpusTokens">— tokens / shingles indexed</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Research Warehouse</div>
      <div class="kpi-val" id="kpiWhCards">—</div>
      <div class="kpi-sub" id="kpiWhTokens">BM25 offline evidence cards</div>
    </div>
  </div>

  <div class="runner-bar">
    <select id="toolSelect">
      <option value="epistemic_dossier">archive_epistemic_dossier (1-Call 5-Stage Dossier)</option>
      <option value="claim_trace">archive_claim_trace (4-Axis Triangulation)</option>
      <option value="forensic_compare">archive_forensic_compare (Shingle + Window Forensics)</option>
    </select>
    <input type="text" id="queryInput" placeholder='Enter claim, question, or "id_a vs id_b" (e.g. Does "Report from Iron Mountain" hold any merit?)' />
    <input type="text" id="compareInput" placeholder="Optional compare_with identifier" style="max-width: 240px;" />
    <button onclick="runLiveQuery()" id="runBtn">Execute & Visualize</button>
  </div>

  <div class="layout">
    <div class="panel">
      <h2>🕒 Query Execution Stream</h2>
      <div id="runList">Loading telemetry...</div>
      <h2 style="margin-top:18px;">📚 Indexed DuckDB Corpus Works</h2>
      <div id="corpusList"></div>
      <h2 style="margin-top:18px; display:flex; justify-content:space-between; align-items:center;">
        <span>🗄️ Local Cache & Borrowed Artifacts</span>
        <button style="height:24px; font-size:11px; padding:0 8px;" onclick="syncCorpus()">Sync Corpus</button>
      </h2>
      <div id="cacheSummary" style="font-size:11px; color:var(--muted); font-family:monospace; margin-bottom:8px;"></div>
      <div id="cacheList"></div>
    </div>

    <div class="panel" id="detailPanel">
      <h2>🔬 Selected Query Forensics, Triangulation & Sources</h2>
      <div id="detailBody" style="color:var(--muted); font-size:13px;">Select a query run on the left or execute a new query above to inspect its 4-Axis Triangulation, Shingle/Window Forensics, and Primary Sources.</div>
    </div>
  </div>

  <script>
    let allRuns = [];
    let selectedId = null;

    const apiBase = window.location.pathname.includes('/api/dsh-archive') ? '/api/dsh-archive' : '';

    async function loadTelemetry() {
      const res = await fetch(apiBase + '/telemetry');
      const data = await res.json();
      allRuns = data.runs || [];

      document.getElementById('kpiRuns').textContent = allRuns.length;
      const totalSaved = allRuns.reduce((s, r) => s + (r.tokens_saved_est || 0), 0);
      const avgMs = allRuns.length ? Math.round(allRuns.reduce((s, r) => s + (r.duration_ms || 0), 0) / allRuns.length) : 0;
      document.getElementById('kpiSaved').textContent = totalSaved.toLocaleString() + ' tok';
      document.getElementById('kpiAvgMs').textContent = 'Avg Latency: ' + avgMs.toLocaleString() + ' ms';

      if (data.corpus) {
        document.getElementById('kpiCorpusDocs').textContent = (data.corpus.documents || 0) + ' works';
        document.getElementById('kpiCorpusTokens').textContent = ((data.corpus.tokens || 0).toLocaleString()) + ' tokens · ' + ((data.corpus.shingles || 0).toLocaleString()) + ' shingles';
      }
      if (data.warehouse) {
        document.getElementById('kpiWhCards').textContent = ((data.warehouse.total_records || 0).toLocaleString()) + ' cards';
        document.getElementById('kpiWhTokens').textContent = ((data.warehouse.total_tokens || 0).toLocaleString()) + ' tokens indexed';
      }
      if (data.corpus_docs) {
        document.getElementById('corpusList').innerHTML = data.corpus_docs.map(d =>
          \`<a class="corpus-tag" href="https://archive.org/details/\${d.identifier}" target="_blank" title="OCR Quality: \${d.ocr_quality}">\${d.identifier} (\${Math.round(d.tokens/1000)}k tok)</a>\`
        ).join('');
      }
      if (data.cache) {
        const s = data.cache.summary || {};
        document.getElementById('cacheSummary').textContent =
          \`Texts: \${s.texts_cached || 0} (\${Math.round((s.texts_total_chars || 0)/1024)} KB) · Metadata: \${s.metadata_cached || 0} (Restricted: \${s.restricted_items_tracked || 0}) · Artifacts: \${s.artifacts_cached || 0}\`;
        const texts = data.cache.texts || [];
        const arts = data.cache.artifacts || [];
        const itemsHtml = texts.slice(0, 15).map(t => \`
          <div style="display:flex; justify-content:space-between; align-items:center; padding:6px 8px; margin-bottom:5px; background:var(--panel-alt); border:1px solid var(--border); border-radius:6px; font-size:11px;">
            <div style="min-width:0; flex:1;">
              <a href="https://archive.org/details/\${escapeHtml(t.identifier)}" target="_blank" style="font-family:monospace; font-weight:600;">\${escapeHtml(t.identifier)}</a>
              <span style="color:var(--muted); margin-left:6px;">(\${escapeHtml(t.format || 'txt')}, \${Math.round((t.chars || 0)/1024)} KB)</span>
            </div>
            <button style="height:22px; font-size:10px; padding:0 8px; background:var(--red); color:#fff; margin-left:8px;" onclick="deleteCachedItem('\${escapeHtml(t.identifier)}')">Delete</button>
          </div>
        \`).join('');
        const artsHtml = arts.slice(0, 10).map(a => \`
          <div style="display:flex; justify-content:space-between; align-items:center; padding:5px 8px; margin-bottom:4px; background:#080b11; border:1px solid var(--border); border-radius:6px; font-size:11px;">
            <div style="min-width:0; flex:1;">
              <span class="tier-badge tier-T2">\${escapeHtml(a.kind)}</span>
              <span style="font-family:monospace;">\${escapeHtml(a.identifier)}/\${escapeHtml(a.file_name)}</span>
              <span style="color:var(--muted);">[\${escapeHtml(a.loan_status || 'open')}]</span>
            </div>
            <button style="height:20px; font-size:10px; padding:0 6px; background:var(--red); color:#fff;" onclick="deleteCachedItem('\${escapeHtml(a.id)}')">Del</button>
          </div>
        \`).join('');
        document.getElementById('cacheList').innerHTML = (itemsHtml + artsHtml) || '<div style="font-size:11px; color:var(--muted);">Cache empty.</div>';
      }

      renderRunList();
      if (!selectedId && allRuns.length > 0) {
        selectRun(allRuns[0].id);
      } else if (selectedId) {
        selectRun(selectedId);
      }
    }

    function renderRunList() {
      const el = document.getElementById('runList');
      if (allRuns.length === 0) {
        el.innerHTML = '<div style="font-size:12px; color:var(--muted);">No recorded runs yet. Execute a query above!</div>';
        return;
      }
      el.innerHTML = allRuns.map(r => \`
        <div class="run-item \${r.id === selectedId ? 'active' : ''}" onclick="selectRun('\${r.id}')">
          <div class="run-top">
            <span class="run-cmd">\${r.command}</span>
            <span style="color:\${r.ok ? 'var(--green)' : 'var(--red)'}">\${r.duration_ms} ms</span>
          </div>
          <div class="run-q">\${escapeHtml(String(r.query).slice(0, 90))}</div>
          <div class="run-meta">
            <span>Out: \${r.output_tokens_est} tok</span>
            <span style="color:var(--green)">Saved: ~\${r.tokens_saved_est} tok</span>
            <span>Sources: \${(r.sources || []).length}</span>
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
        <div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:14px; border-bottom:1px solid var(--border); padding-bottom:10px;">
          <div>
            <div style="font-size:16px; font-weight:700;">\${escapeHtml(String(r.query))}</div>
            <div style="font-size:12px; color:var(--muted); font-family:monospace; margin-top:2px;">
              Command: \${r.command} · Time: \${r.timestamp.slice(0, 19).replace('T', ' ')} · Latency: \${r.duration_ms} ms · Output: \${r.output_chars.toLocaleString()} chars (~\${r.output_tokens_est} tokens)
            </div>
          </div>
          <span class="pill">Saved ~\${(r.tokens_saved_est || 0).toLocaleString()} tokens</span>
        </div>
      \`;

      if (r.triangulation) {
        const t = r.triangulation;
        const maxDoc = Math.max(1, t.total_circulation, t.corroboration_docs, t.critique_docs, t.substance_docs);
        html += \`
          <div style="margin-bottom:18px;">
            <div style="font-size:13px; font-weight:600; margin-bottom:8px;">
              🧭 4-Axis Anti-Confirmation-Bias Triangulation (Core: <code>\${escapeHtml(t.subject_core || '')}</code> \${t.caller_framing_stripped ? '<span class="pill" style="margin-left:6px;">Framing Stripped</span>' : ''})
            </div>
            \${barRow('Neutral Circulation Baseline', t.total_circulation, maxDoc, '#38bdf8')}
            \${barRow('Primary Record (T1 Gov/Military/FOIA)', t.primary_t1_docs, maxDoc, '#22c55e')}
            \${barRow('Corroboration / Authenticity Axis', t.corroboration_docs, maxDoc, '#a855f7')}
            \${barRow('Critique / Forgery / Refutation Axis', t.critique_docs, maxDoc, '#ef4444')}
            \${barRow('Substance & Structural Analysis Axis', t.substance_docs, maxDoc, '#f59e0b')}
          </div>
        \`;
      }

      if (r.forensics) {
        const f = r.forensics;
        html += \`
          <div style="margin-bottom:18px; background:var(--panel-alt); border:1px solid var(--border); border-radius:8px; padding:12px;">
            <div style="font-size:13px; font-weight:600; margin-bottom:8px; color:var(--accent);">
              🧬 Primary-Text Mathematical Forensics: <code>\${escapeHtml(f.identifier_a)}</code> vs <code>\${escapeHtml(f.identifier_b)}</code>
            </div>
            <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(160px, 1fr)); gap:10px; font-family:monospace; font-size:12px;">
              <div>Observed 8-Grams: <strong style="color:var(--green)">\${f.observed_8grams}</strong></div>
              <div>Shuffled Null Baseline: <strong>\${f.null_8grams}</strong></div>
              <div>Excess Over Null: <strong style="color:var(--accent)">+\${f.excess_8grams}</strong></div>
              <div>Peak 250-Tok Window: <strong>\${f.top_window_terms} terms (Jaccard \${f.top_window_jaccard})</strong></div>
            </div>
            \${(f.top_vocab && f.top_vocab.length) ? \`<div style="margin-top:8px; font-size:11px; color:var(--muted);">Aligned Window Vocabulary: <code>\${escapeHtml(f.top_vocab.join(', '))}</code></div>\` : ''}
          </div>
        \`;
      }

      if (r.edition_diffs && r.edition_diffs.length > 0) {
        html += \`<div style="margin-bottom:18px;"><div style="font-size:13px; font-weight:600; margin-bottom:8px;">🔍 Structural Edition Differences (Additions / Excisions / Postscripts)</div>\`;
        for (const d of r.edition_diffs) {
          html += \`
            <div class="source-card">
              <span class="tier-badge tier-T1">\${escapeHtml(d.label || 'diff')}</span>
              <strong>Present in <code>\${escapeHtml(d.in)}</code></strong> (absent in <code>\${escapeHtml(d.missing_from)}</code>) — <strong>\${d.length_tokens} tokens @ offset \${d.start_token}</strong>
              <div class="quote-box">"\${escapeHtml(String(d.snippet || '').slice(0, 260))}"</div>
            </div>
          \`;
        }
        html += \`</div>\`;
      }

      const sources = r.sources || [];
      html += \`<div><div style="font-size:13px; font-weight:600; margin-bottom:8px;">🏛️ Verified Sources, Provenance Tiers & Verbatim Quotes (\${sources.length})</div>\`;
      if (sources.length === 0) {
        html += \`<div style="font-size:12px; color:var(--muted);">No individual source cards recorded for this command.</div>\`;
      } else {
        for (const s of sources) {
          const tierCls = 'tier-' + String(s.tier || 'T4').replace(/[^A-Za-z0-9_]/g, '');
          html += \`
            <div class="source-card">
              <div>
                <span class="tier-badge \${tierCls}">\${escapeHtml(s.tier || 'T4')}</span>
                <span class="tier-badge" style="background:#080b11; color:var(--muted); border:1px solid var(--border);">\${escapeHtml(String(s.stance || 'source').toUpperCase())}</span>
                <a href="\${escapeHtml(s.url || '#')}" target="_blank"><strong>\${escapeHtml(s.title || s.identifier)}</strong></a>
                \${s.page !== null && s.page !== undefined ? \` <span style="color:var(--muted); font-family:monospace;">(p. \${s.page}, scope: \${s.page_scope || 'exact'})</span>\` : ''}
                \${s.date ? \` <span style="color:var(--muted); font-family:monospace;">· \${escapeHtml(s.date)}</span>\` : ''}
                \${s.sha1 ? \` <span style="color:var(--muted); font-family:monospace;">· sha:\${escapeHtml(s.sha1)}</span>\` : ''}
              </div>
              \${s.quote ? \`<div class="quote-box">"\${escapeHtml(s.quote)}"</div>\` : ''}
            </div>
          \`;
        }
      }
      html += \`</div>\`;

      document.getElementById('detailBody').innerHTML = html;
    }

    function barRow(label, val, max, color) {
      const pct = Math.min(100, Math.max(2, Math.round(((val || 0) / max) * 100)));
      return \`
        <div class="bar-row">
          <div class="bar-head"><span>\${label}</span><span style="font-family:monospace; font-weight:600;">\${(val || 0).toLocaleString()} docs</span></div>
          <div class="bar-track"><div class="bar-fill" style="width:\${val ? pct : 0}%; background:\${color};"></div></div>
        </div>
      \`;
    }

    async function runLiveQuery() {
      const tool = document.getElementById('toolSelect').value;
      const q = document.getElementById('queryInput').value.trim();
      const cmp = document.getElementById('compareInput').value.trim();
      if (!q) return;
      const btn = document.getElementById('runBtn');
      btn.textContent = 'Running Pipeline...';
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
        btn.textContent = 'Execute & Visualize';
        btn.disabled = false;
      }
    }

    function escapeHtml(s) {
      return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
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
      query: 'SELECT identifier, chars, tokens, ocr_quality FROM documents ORDER BY tokens DESC LIMIT 30',
    }, 15000, { skipTelemetry: true })
    corpus_docs = (sqlRes.rows || []).map((r) => ({
      identifier: r[0],
      chars: r[1],
      tokens: r[2],
      ocr_quality: r[3],
    }))
  } catch { /* optional */ }

  try {
    cache = await runBridgeFn('cache_manage', { operation: 'list', limit: 25 }, 15000, { skipTelemetry: true })
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

