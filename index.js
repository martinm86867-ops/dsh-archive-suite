/**
 * @martinm86867-ops/dsh-archive-suite — DeepSeek Harness Plugin for Archive.org, Wayback Machine & Epistemic Verification
 *
 * Provides 43 native tools for:
 *   - One-call autonomous epistemic & primary-text forensic dossiers (`archive_epistemic_dossier`)
 *   - Primary-text 8-gram shingle & sliding-window forensic comparison (`archive_forensic_compare`)
 *   - Archive.org catalog search, cursor-based bulk scraping & metadata inspection
 *   - Standalone & Jina-hybrid epistemic fact-checking (`archive_fact_check`)
 *   - Full-text OCR search across books/documents & T1 primary-record claim tracing
 *   - Lending-restricted FTS passage extraction, digital borrowing & multi-copy variant reconciliation
 *   - IIIF Image API 3.0 high-res page & bounding-box figure crop extraction (`archive_iiif_image`)
 *   - IAS3 high-throughput file uploads & item management with multi-key rotation
 *   - Wayback Machine CDX querying, snapshot reading (`wayback_read`), SPN2 archival, reference drift & temporal diffing
 *   - Asynchronous declarative pipeline execution with SQLite state checkpoints
 *   - Sub-millisecond DuckDB research warehouse retrieval, temporal timelines & LLM citation prompt synthesis
 */

import { spawn } from 'node:child_process'
import { homedir } from 'node:os'
import { resolve, dirname, delimiter, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { existsSync, accessSync, constants, readFileSync } from 'node:fs'
import { evaluateGroundedResults } from './eval-grounded.js'
import {
  recordTelemetryRun,
  renderObservatoryHtml,
  buildTelemetryPayload,
  executeObservatoryRun,
} from './observatory.js'

const __dirname = dirname(fileURLToPath(import.meta.url))

function isExecutable(p) {
  try {
    if (!existsSync(p)) return false
    accessSync(p, constants.X_OK)
    return true
  } catch {
    return false
  }
}

export function resolvePython() {
  const explicit = process.env.DSH_ARCHIVE_PYTHON
  if (typeof explicit === 'string' && explicit.trim() !== '') return explicit.trim()
  const candidates = [
    resolve(__dirname, '.venv/bin/python'),
    '/home/martin/archive/.venv/bin/python',
    '/usr/bin/python3',
    '/usr/local/bin/python3',
  ]
  const known = candidates.find(isExecutable)
  if (known !== undefined) return known
  const path = process.env.PATH || ''
  const names = process.platform === 'win32' ? ['python.exe', 'python3.exe'] : ['python3', 'python']
  for (const dir of path.split(delimiter)) {
    if (dir === '') continue
    for (const name of names) {
      const candidate = join(dir, name)
      if (isExecutable(candidate)) return candidate
    }
  }
  return 'python3'
}

export const PYTHON_PATH = resolvePython()
export const BRIDGE_PATH = resolve(__dirname, 'bridge.py')
export const BRIDGE_TIMEOUT_MS = Number(process.env.DSH_ARCHIVE_TIMEOUT_MS || 180000)
export const ARCHIVIST_SKILL_NAME = 'archivist-research-agent'
export const ARCHIVIST_SKILL_DIR = resolve(__dirname, 'skills', ARCHIVIST_SKILL_NAME)
export const ARCHIVIST_SKILL_PATH = resolve(ARCHIVIST_SKILL_DIR, 'SKILL.md')

/** Parse YAML frontmatter description and markdown body from a SKILL.md file. */
export function parseBundledSkill(raw) {
  const fm = /^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/u.exec(String(raw || ''))
  if (!fm) throw new Error('SKILL.md missing YAML frontmatter')
  const descMatch = /^description:\s*(.+)$/m.exec(fm[1])
  const description = descMatch ? descMatch[1].trim().replace(/^['"]|['"]$/g, '') : ''
  if (!description) throw new Error('SKILL.md missing description in frontmatter')
  return {
    description,
    content: String(raw).slice(fm[0].length).trim(),
  }
}

export const name = '@martinm86867-ops/dsh-archive-suite'
export const inject = ['tools']

const OUT = {
  schema: { type: 'string' },
  render(_args, value) { return [{ type: 'text', text: String(value) }] },
}

/** Whether the archivist Python bridge is available on this host. */
export function archiveAvailable() {
  if (!existsSync(BRIDGE_PATH)) return false
  const explicit = process.env.DSH_ARCHIVE_PYTHON
  if (typeof explicit === 'string' && explicit.trim() !== '') return true
  return isExecutable(PYTHON_PATH)
}

/** Run Python bridge command with JSON stdin/stdout and automatic telemetry recording. */
export function runBridge(command, args = {}, timeoutMs = BRIDGE_TIMEOUT_MS, opts = {}) {
  const startedAt = Date.now()
  return new Promise((resolvePromise, rejectPromise) => {
    const py = resolvePython()
    let child
    try {
      child = spawn(py, [BRIDGE_PATH, command], {
        stdio: ['pipe', 'pipe', 'pipe']
      })
    } catch (err) {
      rejectPromise(err)
      return
    }

    let stdout = ''
    let stderr = ''
    let settled = false

    const finish = (err, value) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      if (!opts.skipTelemetry && command !== 'keys_status' && command !== 'corpus_stats' && command !== 'corpus_sql') {
        recordTelemetryRun({
          command,
          args,
          res: value,
          error: err,
          durationMs: Date.now() - startedAt,
        })
      }
      if (err) rejectPromise(err)
      else resolvePromise(value)
    }

    child.stdout.on('data', (d) => { stdout += d.toString('utf8') })
    child.stderr.on('data', (d) => { stderr += d.toString('utf8') })
    child.on('error', (err) => finish(err))

    const timer = setTimeout(() => {
      try { child.kill('SIGKILL') } catch { /* already exited */ }
      finish(new Error(`Bridge timed out after ${timeoutMs} ms: ${command}`))
    }, timeoutMs)

    child.on('close', (code) => {
      if (code !== 0) {
        finish(new Error(`Bridge failed (exit ${code}): ${stderr || stdout}`))
        return
      }
      try {
        const parsed = JSON.parse(stdout)
        finish(null, parsed)
      } catch (err) {
        finish(null, { raw: stdout, stderr })
      }
    })

    try {
      child.stdin.write(JSON.stringify(args))
      child.stdin.end()
    } catch (err) {
      finish(err)
    }
  })
}

/**
 * Optional Jina key discovery so archive_fact_check works both WITH Jina
 * (dual-engine: Archive + Live Web) and WITHOUT Jina (standalone Archive +
 * local DuckDB warehouse).
 */
async function resolveOptionalJinaKey(ctx, explicitKey) {
  if (typeof explicitKey === 'string' && explicitKey.trim() !== '') return explicitKey.trim()
  if (explicitKey === false || explicitKey === 'none') return undefined
  try {
    const svc = ctx && typeof ctx.get === 'function' ? ctx.get('credentials') : undefined
    if (svc && typeof svc.resolve === 'function') {
      const resolved = await svc.resolve('JINA_API_KEY')
      if (resolved && resolved.value) return String(resolved.value).trim()
    }
  } catch { /* optional */ }
  const envKey = process.env && process.env.JINA_API_KEY ? String(process.env.JINA_API_KEY).trim() : ''
  if (envKey !== '') return envKey
  const dshHome = (process.env && process.env.DSH_HOME) || join(homedir(), '.dsh')
  for (const p of [join(process.cwd(), 'jina-api-key.txt'), join(dshHome, 'jina-api-key.txt')]) {
    try {
      if (existsSync(p)) {
        const raw = readFileSync(p, 'utf8').trim()
        if (raw) return raw
      }
    } catch { /* ignore */ }
  }
  return undefined
}

/**
 * Render the unified epistemic fact-check report combining the Archive.org
 * documentary record (FTS index + Primary T1 collections + local DuckDB
 * warehouse) and, when available, the Live Web stance map.
 */
export function formatUnifiedFactCheck(statement, archiveRes, liveRes) {
  const lines = [`### Epistemic Fact-Check & Documentary Sweep: ${statement}`, '']

  lines.push('1. ARCHIVE RECORD — Documentary Footprint (archive.org full-text index & primary collections)')
  if (archiveRes && archiveRes.ok && archiveRes.value && !archiveRes.value.error) {
    const v = archiveRes.value
    const rec = v.recorded || {}
    const prim = v.primary_record || {}
    const yr = v.year_span || {}
    const sc = v.stance_counts || {}
    const tri = v.triangulation || {}
    if (tri.subject_core && tri.caller_framing_stripped) {
      lines.push(`  · Neutral Subject Core (framing bias stripped): ${tri.subject_core} (queried as: ${v.query || statement})`)
    }
    lines.push(`  · Circulation: ${rec.documents ?? 0} documents carry this phrasing (year span: ${yr.earliest ?? '—'}–${yr.latest ?? '—'})`)
    lines.push(`  · Primary Record (T1 government/military/declassified): ${prim.documents ?? 0} documents` +
      ((prim.collections || []).length ? ` (${prim.collections.map((c) => c.key).slice(0, 5).join(', ')})` : ''))
    if (tri.corroboration_axis || tri.critique_axis || tri.substance_axis) {
      const cCnt = tri.corroboration_axis ? tri.corroboration_axis.documents : 0
      const rCnt = tri.critique_axis ? tri.critique_axis.documents : 0
      const sCnt = tri.substance_axis ? tri.substance_axis.documents : 0
      lines.push(`  · Multi-Axis Triangulation (Anti-Confirmation-Bias Sweep):`)
      lines.push(`      - Corroboration / Authenticity Axis: ${cCnt} documents`)
      for (const s of ((tri.corroboration_axis && tri.corroboration_axis.samples) || []).slice(0, 1)) {
        lines.push(`        [${s.tier || '?'}] ${s.title || s.identifier} (${s.url}) — "${s.quote}"`)
      }
      lines.push(`      - Critique / Forgery / Refutation Axis: ${rCnt} documents`)
      for (const s of ((tri.critique_axis && tri.critique_axis.samples) || []).slice(0, 1)) {
        lines.push(`        [${s.tier || '?'}] ${s.title || s.identifier} (${s.url}) — "${s.quote}"`)
      }
      lines.push(`      - Substance & Historical Impact Axis: ${sCnt} documents`)
      for (const s of ((tri.substance_axis && tri.substance_axis.samples) || []).slice(0, 1)) {
        lines.push(`        [${s.tier || '?'}] ${s.title || s.identifier} (${s.url}) — "${s.quote}"`)
      }
      if ((tri.canonical_editions || []).length > 0) {
        const eds = tri.canonical_editions.map((e) => `${e.identifier} (${e.downloads || 0} dl)`).join(', ')
        lines.push(`      - Canonical Archive Texts for Direct Substance Inspection (use archive_read_text / archive_book_search): ${eds}`)
      }
    }
    if (sc.sampled !== undefined) {
      lines.push(`  · Archival Stance Distribution (of ${sc.sampled} sampled): ${sc.states || 0} state · ${sc.reports || 0} report · ${sc.denies || 0} deny · ${sc.mentions || 0} mention`)
    }
    if ((v.figures_missing || []).length) {
      lines.push(`  · Caution (Numeric Discrepancy): figure(s) ${v.figures_missing.join(', ')} do not appear in the archival passages carrying the phrasing`)
    }
    lines.push(`  · Languages: ${(rec.languages || []).map((l) => l.key).slice(0, 5).join(', ') || '—'}`)
    lines.push(`  · Denial phrasing in ${(v.denial_phrasing || []).length} of the sampled instances`)
    if ((v.evidence || []).length > 0) {
      lines.push('  · Dated archival instances:')
      for (const e of (v.evidence || []).slice(0, 4)) {
        const prov = e.provenance || {}
        const st = e.stance ? ` [${String(e.stance).toUpperCase()}]` : ''
        lines.push(`    - ${e.date ? e.date.slice(0, 10) : 'undated'} — ${e.title || e.identifier}${e.page !== null && e.page !== undefined ? ` (page ${e.page})` : ''}${st} [${prov.tier || '?'} ${prov.basis || ''}]`)
        lines.push(`      ${e.url}`)
        if (e.quote) lines.push(`      "${e.quote.slice(0, 160).replace(/\n/g, ' ')}"`)
      }
      lines.push('  · Host-independent citations (identifier · collection · date · page · sha1):')
      for (const e of (v.evidence || []).slice(0, 2)) {
        lines.push(`    ${e.identifier}${e.page !== null && e.page !== undefined ? ` p.${e.page}` : ''}`
          + `${(e.collections || []).length ? ` · ${(e.collections || []).slice(0, 2).join(', ')}` : ''}`
          + `${e.date ? ` · ${String(e.date).slice(0, 10)}` : ''}`
          + `${e.sha1 ? ` · sha1 ${String(e.sha1).slice(0, 12)}` : ''}`)
      }
    }
    if ((v.local_warehouse || []).length > 0) {
      lines.push('  · Local DuckDB Research Warehouse Matches (offline verified cards):')
      for (const w of v.local_warehouse.slice(0, 3)) {
        lines.push(`    - [${w.id}] ${w.title || w.identifier} (${w.source_type}, sha256:${w.digest}) — ${w.url}`)
        lines.push(`      "${w.quote}"`)
      }
    }
  } else {
    const errMsg = (archiveRes && (archiveRes.error || (archiveRes.value && archiveRes.value.error))) || 'archivist backend unavailable'
    lines.push(`  unavailable — ${errMsg}`)
  }

  lines.push('', '2. LIVE WEB RECORD — Contemporary Source Stance')
  if (liveRes && liveRes.mode === 'jina' && liveRes.map && liveRes.map.evidence && liveRes.map.evidence.length > 0) {
    const live = liveRes.map
    lines.push('  · ' + live.summary)
    lines.push(`  · term coverage: ${live.termCoverage} (share of the claim's vocabulary present — not a probability)`)
    if (live.caution) lines.push('  · ' + live.caution)
    if ((live.figuresMissing || []).length) {
      lines.push(`  · figures absent from live sources: ${live.figuresMissing.join(', ')}`)
    }
    for (const e of live.evidence.slice(0, 4)) {
      lines.push(`  [${e.index}] ${String(e.stance).toUpperCase()} — ${e.title || 'Source'} (${e.url || ''})`)
      if (e.snippet) lines.push(`      "${e.snippet.slice(0, 150)}"`)
    }
  } else if (liveRes && liveRes.mode === 'archive_standalone') {
    lines.push('  · Operating in standalone Archive-First mode (no Jina API key or live web disabled).')
    lines.push('  · Epistemic Advantage: Live web search suffers from link rot, paywalls, and unindexed historical books. Verification above is grounded directly in the permanent Archive.org full-text OCR index, primary T1 government/military collections, and the local DuckDB warehouse.')
  } else {
    lines.push(`  · no usable live web results${liveRes && liveRes.error ? ` (${liveRes.error})` : ''}`)
  }

  lines.push('', 'Epistemic Synthesis: A single search query always returns what it asks for (confirmation bias). Compare the Corroboration Axis against the Critique Axis, the Primary Record (T1), and the substantive text of the canonical editions before concluding.')
  return lines.join('\n')
}

export function apply(ctx) {
  // 1. archive_search
  ctx.tools.register({
    name: 'archive_search',
    description: 'Search the Archive.org catalog with Lucene queries, collection/mediatype/creator/subject/year/date-range/format filters, sorting, and row limits.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Lucene query or keywords (e.g. "collection:nasa AND mediatype:image", or "*:*" when using structured filters)' },
        rows: { type: 'integer', description: 'Number of results to return (default: 10)' },
        page: { type: 'integer', description: 'Page number (default: 1)' },
        mediatype: { type: 'string', description: 'Optional media type filter (texts, audio, movies, image, software, data)' },
        collection: { type: 'string', description: 'Optional collection identifier (e.g. "nasa", "dticarchive", "americana")' },
        creator: { type: 'string', description: 'Optional author / creator filter' },
        title: { type: 'string', description: 'Optional title phrase filter' },
        subject: { type: 'string', description: 'Optional subject/topic filter' },
        year: { type: 'string', description: 'Optional publication year filter (e.g. "1948")' },
        date_from: { type: 'string', description: 'Optional start date for range filter (YYYY-MM-DD, pair with date_to)' },
        date_to: { type: 'string', description: 'Optional end date for range filter (YYYY-MM-DD, pair with date_from)' },
        format: { type: 'string', description: 'Optional file format filter (e.g. "Text PDF", "DjVuTXT", "hOCR")' },
        sort: { type: 'string', description: 'Optional sort expression (e.g. "downloads desc", "date asc", "publicdate desc")' }
      },
      required: ['query']
    },
    async execute(args) {
      const res = await runBridge('search', args)
      if (res.error) return `Error: ${res.error}`
      const docs = res.docs || []
      const lines = docs.map(d => `- **${d.identifier}**: ${d.title || 'Untitled'} (${d.mediatype || 'unknown'}) | Creator: ${d.creator || 'N/A'} | Date: ${d.date || 'N/A'} | Downloads: ${d.downloads || 0}`)
      return `### Archive.org Catalog Hits (${res.num_found} total found, showing ${docs.length} for \`${res.query}\`):\n${lines.join('\n')}`
    }
  })

  // 2. archive_item_info
  ctx.tools.register({
    name: 'archive_item_info',
    description: 'Inspect Archive.org item metadata, size, server hosting node, description, and file inventory (supports filtering files by format or substring and setting max_files).',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'The exact item identifier on Archive.org' },
        max_files: { type: 'integer', description: 'Maximum files to list from the item inventory (default: 25)' },
        format_filter: { type: 'string', description: 'Optional substring filter on file format or filename (e.g. "pdf", "txt", "mp3", "jp2")' }
      },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('item_info', args)
      if (res.error) return `Error: ${res.error}`
      const sizeMb = (res.item_size / (1024 * 1024)).toFixed(2)
      const filesPreview = (res.sample_files || []).map(f => `  - \`${f.name}\` (${(Number(f.size || 0) / 1024).toFixed(1)} KB, format: ${f.format || 'unknown'})`).join('\n')
      return `### Item: [${res.identifier}](${res.url})
- **Title**: ${res.title || 'N/A'}
- **Creator**: ${res.creator || 'N/A'} | **Date**: ${res.date || 'N/A'}
- **Mediatype**: ${res.mediatype || 'N/A'} | **Server**: \`${res.server || 'N/A'}\`
- **Collection**: ${Array.isArray(res.collection) ? res.collection.join(', ') : (res.collection || 'N/A')}
- **Total Size**: ${sizeMb} MB (${res.files_count} total files, ${res.filtered_files_count ?? res.files_count} matching filter)
${res.description ? `- **Description**: ${res.description}\n` : ''}- **Files (${(res.sample_files || []).length} shown)**:
${filesPreview}`
    }
  })

  // 3. archive_item_upload
  ctx.tools.register({
    name: 'archive_item_upload',
    description: 'Upload a local file directly into an Archive.org item via IAS3 S3 protocol.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Target Archive.org item identifier' },
        file_path: { type: 'string', description: 'Local path of the file to upload' }
      },
      required: ['identifier', 'file_path']
    },
    async execute(args) {
      const res = await runBridge('item_upload', args)
      if (res.error) return `Upload Failed: ${res.error}`
      return `Successfully uploaded \`${res.filename}\` to [${res.identifier}](${res.url}) (${res.size} bytes)`
    }
  })

  // 4. wayback_cdx
  ctx.tools.register({
    name: 'wayback_cdx',
    description: 'Query the Wayback Machine CDX server for historical web captures across time.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        url: { type: 'string', description: 'Target domain or URL pattern' },
        limit: { type: 'integer', description: 'Maximum snapshots to return (default: 15)' },
        match_type: { type: 'string', enum: ['exact', 'prefix', 'host', 'domain'], description: 'URL matching mode' },
        from_ts: { type: 'string', description: 'Start timestamp filter (YYYYMMDD)' },
        to_ts: { type: 'string', description: 'End timestamp filter (YYYYMMDD)' }
      },
      required: ['url']
    },
    async execute(args) {
      const res = await runBridge('wayback_cdx', args)
      if (res.error) return `CDX Query Error: ${res.error}`
      const snaps = res.snapshots || []
      const rows = snaps.map(s => `- **${s.timestamp}** [${s.statuscode}] (\`${s.mimetype}\`): [Playback](https://web.archive.org/web/${s.timestamp}/${s.original}) - \`${s.original}\``)
      return `### Wayback CDX Captures for "${args.url}" (${snaps.length} returned):\n${rows.join('\n')}`
    }
  })

  // 5. wayback_save
  ctx.tools.register({
    name: 'wayback_save',
    description: 'Save a URL to the Wayback Machine (Save Page Now 2 / SPN2) with authenticated capture verification.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        url: { type: 'string', description: 'The web URL to preserve' },
        wait: { type: 'boolean', description: 'Wait for snapshot capture completion (default: true)' }
      },
      required: ['url']
    },
    async execute(args) {
      const res = await runBridge('wayback_save', args)
      if (res.status === 'error') {
        return `Wayback Save Notice: ${res.message || res.status_ext || JSON.stringify(res)}`
      }
      if (res.status === 'success') {
        return `Successfully archived ${args.url}!\n- Timestamp: ${res.timestamp}\n- Playback URL: https://web.archive.org/web/${res.timestamp}/${args.url}`
      }
      return `Capture job queued: Job ID \`${res.job_id}\` (Status: ${res.status || 'submitted'})`
    }
  })

  // 6. wayback_diff
  ctx.tools.register({
    name: 'wayback_diff',
    description: 'Compare content of a webpage between two Wayback historical snapshot dates across decades.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        url: { type: 'string', description: 'Target webpage URL' },
        timestamp_a: { type: 'string', description: 'First capture timestamp (YYYYMMDDHHMMSS)' },
        timestamp_b: { type: 'string', description: 'Second capture timestamp (YYYYMMDDHHMMSS)' }
      },
      required: ['url', 'timestamp_a', 'timestamp_b']
    },
    async execute(args) {
      const res = await runBridge('wayback_diff', args)
      if (res.error) return `Diff Error: ${res.error}`
      const lines = res.diff_lines || []
      if (lines.length === 0) return `No textual differences between ${args.timestamp_a} and ${args.timestamp_b} for ${args.url}`
      return `### Web Evolution Diff (${args.timestamp_a} ➔ ${args.timestamp_b})\n\`\`\`diff\n${lines.slice(0, 60).join('\n')}\n${lines.length > 60 ? `\n... [${lines.length - 60} more lines omitted]` : ''}\n\`\`\``
    }
  })

  // 7. archive_pipeline_run
  ctx.tools.register({
    name: 'archive_pipeline_run',
    description: 'Execute an asynchronous Archive.org data processing pipeline defined in YAML or template.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        config: { type: 'string', description: 'Path to pipeline YAML configuration (e.g. "pipelines/collection_harvester.yaml")' },
        resume_id: { type: 'string', description: 'Optional previous run_id to resume without duplicating work' }
      },
      required: ['config']
    },
    async execute(args) {
      const res = await runBridge('pipeline_run', args)
      if (res.error) return `Pipeline Error: ${res.error}`
      const s = res.stats || {}
      return `### Pipeline Execution Finished: \`${res.pipeline_name}\`
- **Run ID**: \`${res.run_id}\`
- **Status**: ${res.status}
- **Metrics**: Produced ${s.produced || 0} | Transformed ${s.transformed || 0} | Consumed ${s.consumed || 0} | Errors: ${s.failed || 0}`
    }
  })

  // 8. archive_research_search
  ctx.tools.register({
    name: 'archive_research_search',
    description: 'Search the local DuckDB research warehouse (BM25). NOTE: this warehouse is a small demo set unless populated by a pipeline — a zero result here means the warehouse is empty, NOT that the archive lacks material. Use archive_fulltext_search to search actual document text.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Keywords or search phrase' },
        limit: { type: 'integer', description: 'Maximum evidence cards to retrieve (default: 5)' }
      },
      required: ['query']
    },
    async execute(args) {
      const res = await runBridge('research_search', args)
      if (res.error) return `Search Error: ${res.error}`
      const hits = res.results || []
      if (hits.length === 0) return `No research evidence found in warehouse for query "${args.query}"`
      const cards = hits.map(h => `#### [${h.id}] (${h.source_type} @ ${h.timestamp || 'N/A'}) - Score: ${h.score ? h.score.toFixed(3) : 'N/A'}\n- **Source**: \`${h.source_key}\`\n- **Permanent URL**: ${h.archive_url || 'N/A'}\n- **Content**:\n> ${h.content.slice(0, 300).replace(/\n/g, ' ')}...`).join('\n\n')
      return `### DuckDB Research Hits (${hits.length} retrieved | Warehouse: ${res.warehouse_stats.total_records} records, ${res.warehouse_stats.total_tokens} tokens):\n\n${cards}`
    }
  })

  // 9. archive_research_prompt
  ctx.tools.register({
    name: 'archive_research_prompt',
    description: 'Generate an LLM-ready research prompt with citation-grounded evidence cards from the analytical warehouse.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'The research question or topic' },
        limit: { type: 'integer', description: 'Maximum evidence cards to include (default: 3)' }
      },
      required: ['query']
    },
    async execute(args) {
      const res = await runBridge('research_prompt', args)
      if (res.error) return `Prompt Generation Error: ${res.error}`
      return res.prompt
    }
  })

  // 10. archive_media_manifest
  ctx.tools.register({
    name: 'archive_media_manifest',
    description: 'Deep-inspect multi-modal assets of any Archive.org item: extracts direct audio/video stream URLs, resolutions, tracklists, emulation launcher details, and playable embed links.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Archive.org item identifier' }
      },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('media_manifest', args)
      if (res.error) return `Media Manifest Error: ${res.error}`

      let output = `### Multimedia Manifest: [${res.title || res.identifier}](${res.details_url}) (${res.mediatype})\n`
      output += `- **Interactive Player / Embed**: ${res.embed_url}\n`

      if (res.emulation && res.emulation.supported) {
        output += `- **🎮 In-Browser Emulation**: Emulator \`${res.emulation.emulator}\` | Play: ${res.emulation.play_url}\n`
      }

      if (res.video && res.video.length > 0) {
        output += `\n#### 🎬 Video Streams (${res.video.length}):\n`
        for (const v of res.video) {
          const resStr = v.width ? `${v.width}x${v.height}` : 'N/A'
          output += `- **${v.name}** (\`${v.format}\`, ${resStr}, ${v.size_mb} MB): [Direct Stream](${v.download_url})\n`
        }
      }

      if (res.audio && res.audio.length > 0) {
        output += `\n#### 🎵 Audio Tracks (${res.audio.length}):\n`
        for (const a of res.audio) {
          output += `- **${a.title}** (\`${a.format}\`, ${a.size_mb} MB): [Direct Audio](${a.download_url})\n`
        }
      }

      if (res.texts && res.texts.length > 0) {
        output += `\n#### 📄 Documents & Books (${res.texts.length}):\n`
        for (const t of res.texts.slice(0, 10)) {
          output += `- **${t.name}** (\`${t.format}\`, ${t.size_mb} MB): [Download](${t.download_url})\n`
        }
      }

      if (res.software && res.software.length > 0) {
        output += `\n#### 💾 Software & Disk Images (${res.software.length}):\n`
        for (const s of res.software) {
          output += `- **${s.name}** (\`${s.format}\`, ${s.size_mb} MB): [Direct Package](${s.download_url})\n`
        }
      }

      return output
    }
  })

  // 11. archive_read_text
  ctx.tools.register({
    name: 'archive_read_text',
    description: 'Read or regex-search the full OCR text or transcription of a book, document, or publication on Archive.org. Supports character `offset` pagination to read middle/later chapters without writing code, and `grep` regex matching to extract surrounding excerpts across the entire text.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Archive.org document/book identifier' },
        offset: { type: 'integer', description: 'Starting character offset for pagination (default: 0)' },
        max_chars: { type: 'integer', description: 'Maximum characters to read from offset (default: 6000)' },
        grep: { type: 'string', description: 'Optional regex or phrase to search across the full text instead of returning a single slice' },
        context_chars: { type: 'integer', description: 'Characters of surrounding context per grep match (default: 240)' }
      },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('media_read_text', args)
      if (res.error) return `Text Extraction Error: ${res.error}`
      if (!res.found) return res.message || `No readable text found for ${args.identifier}`
      if (res.mode === 'grep') {
        const matches = res.matches || []
        if (matches.length === 0) return `No matches for pattern \`${res.pattern}\` in \`${res.identifier}\` (${res.total_chars} chars scanned).`
        const blocks = matches.map((m, i) => `**[Match #${i + 1} @ offset ${m.offset}]**: ...${m.excerpt}...`).join('\n\n')
        return `### Document Grep: \`${res.pattern}\` in \`${res.identifier}\` (${res.match_count} matches across ${res.total_chars} chars)\n- **Full Text Link**: ${res.full_file_url}\n\n${blocks}`
      }
      const nextHint = res.next_offset ? ` | Next Offset: \`${res.next_offset}\`` : ' | [End of Document]'
      return `### Document OCR Text: \`${res.identifier}\` (File: \`${res.filename}\` | Offset: ${res.offset ?? 0}..${(res.offset ?? 0) + res.length_read} of ${res.total_chars ?? res.length_read} chars${nextHint})\n- **Full Text Link**: ${res.full_file_url}\n\n\`\`\`text\n${res.text}\n\`\`\``
    }
  })

  // 12. archive_book_search
  ctx.tools.register({
    name: 'archive_book_search',
    description: 'Search inside one document. Returns page numbers and word bounding boxes when the item has a searchable OCR derivative; otherwise falls back to scanning the local text derivative and reports its coverage (a truncated scan is never reported as an absence of matches).',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Archive.org book identifier' },
        query: { type: 'string', description: 'Word or phrase to search inside the book' }
      },
      required: ['identifier', 'query']
    },
    async execute(args) {
      const res = await runBridge('media_book_search', args)
      if (res.error) return `Book Search Error: ${res.error}`
      const matches = res.matches || []
      if (matches.length === 0) return `No matches found for "${args.query}" inside book ${args.identifier}`
      const lines = matches.map(m => `- **Page ${m.page}**: "${m.text}" | [View Page](${m.page_url})`)
      return `### Book Search: "${args.query}" inside \`${args.identifier}\` (${res.total_matches} occurrences):\n${lines.join('\n')}`
    }
  })

  // 13. archive_media_download
  ctx.tools.register({
    name: 'archive_media_download',
    description: 'Download a specific audio track, video stream, software ROM/disk, or document from an Archive.org item to the local workspace.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Archive.org item identifier' },
        filename: { type: 'string', description: 'Exact filename to download' },
        destination_dir: { type: 'string', description: 'Optional local destination directory (default: "./downloads")' }
      },
      required: ['identifier', 'filename']
    },
    async execute(args) {
      const res = await runBridge('media_download', args)
      if (res.error) return `Download Error: ${res.error}`
      return `Successfully downloaded \`${res.filename}\` from \`${res.identifier}\`!\n- **Local Path**: \`${res.saved_path}\`\n- **Size**: ${(res.size / (1024 * 1024)).toFixed(2)} MB`
    }
  })

  // 14. archive_quota
  ctx.tools.register({
    name: 'archive_quota',
    description: 'Inspect live authenticated Save Page Now 2 (SPN2) capture quotas, available concurrency slots, and daily usage.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {}
    },
    async execute() {
      const res = await runBridge('quota', {})
      if (res.error) return `Quota Query Error: ${res.error}`
      const u = res.user || {}
      const s = res.system || {}
      return `### Archive.org Authenticated Quota & Ingress Status
- **Daily Capture Quota**: ${u.daily_captures_limit ? u.daily_captures_limit.toLocaleString() : 'N/A'} URLs/day (Used today: ${u.daily_captures || 0})
- **Available Concurrent Workers**: ${u.available || 0} slots (Active: ${u.processing || 0})
- **Daily Status Queries Limit**: ${u.daily_status_limit ? u.daily_status_limit.toLocaleString() : 'N/A'}
- **Global Cluster Load**: ${s.status || 'ok'} (${s.recent_captures ? s.recent_captures.toLocaleString() : '0'} recent captures)`
    }
  })

  // 15. archive_item_views
  ctx.tools.register({
    name: 'archive_item_views',
    description: 'Retrieve real-time viewership analytics and historical traffic metrics for any Archive.org item from the backend views API.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Archive.org item identifier' }
      },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('views', args)
      if (res.error) return `Views Query Error: ${res.error}`
      return `### Viewership Traffic Analytics: \`${args.identifier}\`
- **All Time Views**: ${res.all_time ? res.all_time.toLocaleString() : '0'}
- **Last 30 Days**: ${res.last_30day ? res.last_30day.toLocaleString() : '0'}
- **Last 7 Days**: ${res.last_7day ? res.last_7day.toLocaleString() : '0'}`
    }
  })

  // 16. archive_keys_status
  ctx.tools.register({
    name: 'archive_keys_status',
    description: 'List all S3 credential pairs in the pool with live quota exhaustion and active rotation status.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {}
    },
    async execute() {
      const res = await runBridge('keys_status', {})
      if (res.error) return `Key Status Error: ${res.error}`
      const keys = res.keys || []
      const rows = keys.map(k => `- **[${k.is_active ? 'ACTIVE' : 'STANDBY'}]** \`${k.label}\` (${k.access_key}) | Status: ${k.is_exhausted ? `EXHAUSTED (${k.exhaustion_reason})` : 'HEALTHY'} | Captures: ${k.daily_captures}/${k.daily_limit} | Slots: ${k.available_slots}`)
      return `### S3 Credential Pool (${res.total_keys} keys registered):\n${rows.join('\n')}`
    }
  })

  // 17. archive_keys_rotate
  ctx.tools.register({
    name: 'archive_keys_rotate',
    description: 'Rotate active S3 credentials to the next available healthy key in the pool.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        reason: { type: 'string', description: 'Optional justification reason for manual rotation' }
      }
    },
    async execute(args) {
      const res = await runBridge('keys_rotate', args)
      if (res.error) return `Rotation Error: ${res.error}`
      return `### S3 Key Rotated Successfully!\n- **Previous Key**: \`${res.previous_key}\`\n- **Active Key**: \`${res.active_key}\` (${res.access_key})\n- **Reason**: ${res.reason}`
    }
  })
  // 18. archive_fulltext_search
  ctx.tools.register({
    name: 'archive_fulltext_search',
    description: 'Search the TEXT of documents across archive.org (OCR content, not metadata). Finds which documents mention a phrase. Supports "quoted phrases" and boolean operators. This is the tool for content questions that catalogue search cannot answer.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Terms, "quoted phrases", and/or boolean operators.' },
        size: { type: 'integer', description: 'Maximum hits (default: 10)' },
        scope: { type: 'string', description: 'Optional scope/filter applied by the search service' }
      },
      required: ['query']
    },
    async execute(args) {
      const res = await runBridge('fulltext_search', args)
      if (res.error) return `Full-text search error: ${res.error}`
      const hits = res.hits || []
      if (hits.length === 0) return `No document text matched "${args.query}" (total: ${res.total}).`
      const cards = hits.map(h => {
        const snip = (h.snippets && h.snippets[0]) ? h.snippets[0].text.replace(/\n/g, ' ').slice(0, 300) : '(no snippet)'
        return `#### [${h.identifier}](${h.evidence_url})\n> ${snip}`
      }).join('\n\n')
      return `### Document text matches (${res.total} total, showing ${hits.length})\n\n${cards}`
    }
  })

  // 19. archive_text_availability
  ctx.tools.register({
    name: 'archive_text_availability',
    description: "Report whether an item's text can be searched, and why not. States: searchable, no_derivative (image-only or DjVuTXT-only), restricted (lending), not_indexed. Check this before concluding that a document does not contain a phrase.",
    output: OUT,
    parameters: {
      type: 'object',
      properties: { identifier: { type: 'string', description: 'The exact item identifier' } },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('text_availability', args)
      if (res.error) return `Error: ${res.error}`
      const deriv = res.derivative ? `${res.derivative.format} (doc: ${res.derivative.doc})` : 'none'
      return `### Text availability: ${res.identifier}\n- **State**: ${res.state}\n- **Searchable derivative**: ${deriv}\n- **Lending restricted**: ${res.restricted}\n- **Text formats present**: ${(res.text_formats || []).join(', ') || 'none'}`
    }
  })

  // 20. archive_text_harvest
  ctx.tools.register({
    name: 'archive_text_harvest',
    description: 'Download the compact text derivative of items into the local store, once, politely (paced to avoid rate limits; skips items already stored), and automatically index them into the local DuckDB columnar corpus (`auto_build: true` by default) so offline analysis tools (`archive_corpus`, `archive_text_reuse`, `archive_text_variants`, `archive_text_reconcile`) work immediately.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifiers: { type: 'array', items: { type: 'string' }, description: 'Item identifiers to harvest' },
        pause: { type: 'number', description: 'Seconds between downloads (default: 1.0)' },
        max_docs: { type: 'integer', description: 'Stop after N downloads' },
        auto_build: { type: 'boolean', description: 'Automatically build/update the DuckDB corpus after harvesting (default: true)' }
      },
      required: ['identifiers']
    },
    async execute(args) {
      const res = await runBridge('text_harvest', args)
      if (res.error) return `Harvest error: ${res.error}`
      const done = (res.harvested || []).map(h => `  - ${h.identifier} (${h.chars} chars, ${h.format})`).join('\n')
      const failed = (res.failed || []).map(f => `  - ${f.identifier}: ${f.reason}`).join('\n')
      const corpusNote = res.corpus_stats ? `\n- Corpus auto-indexed: ${res.corpus_stats.documents} total docs (${res.corpus_stats.tokens} tokens)` : ''
      return `### Harvested ${(res.harvested || []).length}\n${done}\n- Already stored: ${(res.already_stored || []).length}\n${failed ? `- Failed:\n${failed}\n` : ''}- Store: ${JSON.stringify(res.store)}${corpusNote}`
    }
  })

  // 21. archive_text_reuse
  ctx.tools.register({
    name: 'archive_text_reuse',
    description: 'Find documents sharing phrasing with a passage, or with a document already in the local corpus. Scores shared shingles against a shuffled-null baseline: a high observed count with a near-zero null is real incorporation (quotation, derivation, copying); observed close to null is merely shared idiom.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Source item already ingested into the corpus' },
        text: { type: 'string', description: 'Or supply the passage directly' },
        min_tokens: { type: 'integer', description: 'Minimum shared run in tokens (default: 20)' },
        candidates: { type: 'integer', description: 'Candidates to score (default: 8)' }
      }
    },
    async execute(args) {
      const res = await runBridge('text_reuse', args)
      if (res.error) return `Reuse error: ${res.error}`
      const rows = res.results || []
      if (rows.length === 0) return `No shared phrasing found (mode: ${res.mode}). Ingest documents with archive_corpus first.`
      const cards = rows.map(r => {
        const nullInfo = (r.shared_shingles !== undefined) ? `shared=${r.shared_shingles} null=${r.null_shingles}` : 'passage mode'
        return `#### [${r.identifier}](${r.evidence_url})\n- longest shared run: **${r.longest_run_tokens} tokens** (${nullInfo})\n> ${(r.shared_passage || '').slice(0, 300)}`
      }).join('\n\n')
      return `### Shared-text candidates (${rows.length})\n\n${cards}`
    }
  })

  // 22. archive_text_variants
  ctx.tools.register({
    name: 'archive_text_variants',
    description: 'List sibling uploads of the same work. Multiple copies are normal in an archive and are NOT duplicates: they differ in completeness, redaction and OCR quality, and the differences carry information. Siblings are confirmed by shared text, not by title alone.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string' },
        confirm_tokens: { type: 'integer', description: 'Shared-run threshold to accept a sibling (default: 40)' },
        offline: { type: 'boolean', description: 'Use only the local corpus (no requests). Default false.' }
      },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('text_variants', args)
      if (res.error) return `Variants error: ${res.error}`
      const rows = (res.variants || []).map(v => `  - ${v.identifier} (run ${v.longest_run_tokens} tokens, containment ${v.containment})`).join('\n')
      return `### Variants of ${res.identifier} (discovery: ${res.discovery})\n${rows || '  (none found)'}`
    }
  })

  // 23. archive_text_reconcile
  ctx.tools.register({
    name: 'archive_text_reconcile',
    description: 'Compare the several copies of one document and report what each adds or omits: sizes, OCR quality, a ranked base copy, and regions present in one copy but absent in another. Such regions are labelled candidate excisions — redaction, truncation and OCR failure are indistinguishable from the text alone.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string' },
        confirm_tokens: { type: 'integer', description: 'Shared-run threshold to accept a sibling (default: 40)' },
        min_run: { type: 'integer', description: 'Minimum differing run in tokens (default: 25)' },
        offline: { type: 'boolean', description: 'Use only the local corpus (no requests). Default false.' }
      },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('text_reconcile', args)
      if (res.error) return `Reconcile error: ${res.error}`
      const copies = (res.copies || []).map(c => `  - ${c.identifier} (${c.chars} chars, OCR ${c.ocr_quality})`).join('\n')
      const diffs = (res.differences || []).map(d => {
        const exc = (d.candidate_excisions || []).map(r => `    - ${r.length_tokens} tokens: ${r.snippet.slice(0, 160)}`).join('\n')
        return `#### vs ${d.against}\n  base-only=${d.base_only_tokens} tokens, variant-only=${d.variant_only_tokens} tokens\n${exc}`
      }).join('\n\n')
      return `### Copies of ${args.identifier}\n${copies}\n\n**Base**: ${res.base} (${res.base_reason})\n\n${diffs}`
    }
  })

  // 24. archive_corpus
  ctx.tools.register({
    name: 'archive_corpus',
    description: 'Query the local columnar corpus (DuckDB) at memory speed: build it from harvested text, or run similar / shared / diff / phrase / stats. `shared` returns the longest verbatim passages two documents share; `diff` returns regions in one copy absent from another; `phrase` is exact positional search across the whole corpus.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        operation: { type: 'string', enum: ['stats', 'build', 'similar', 'shared', 'diff', 'phrase'], description: 'Which corpus query to run' },
        identifier: { type: 'string', description: 'For similar' },
        a: { type: 'string', description: 'For shared (first document)' },
        b: { type: 'string', description: 'For shared (second document)' },
        base: { type: 'string', description: 'For diff (the fuller copy)' },
        other: { type: 'string', description: 'For diff (the copy to compare against)' },
        phrase: { type: 'string', description: 'For phrase' },
        min_tokens: { type: 'integer', description: 'For shared (default 50)' },
        min_run: { type: 'integer', description: 'For diff (default 25)' },
        limit: { type: 'integer', description: 'Maximum rows (default 10)' }
      },
      required: ['operation']
    },
    async execute(args) {
      const op = args.operation
      const command = { stats: 'corpus_stats', build: 'corpus_build', similar: 'corpus_similar',
                        shared: 'corpus_shared', diff: 'corpus_diff', phrase: 'corpus_phrase' }[op]
      if (!command) return `Unknown operation: ${op}`
      const res = await runBridge(command, args)
      if (res.error) return `Corpus error: ${res.error}`
      if (op === 'stats') return `### Corpus\n- documents: ${res.documents}\n- tokens: ${res.tokens}\n- shingles: ${res.shingles}\n- fingerprints: ${res.fingerprints}\n- path: ${res.path}`
      if (op === 'build') return `### Corpus built\n- documents indexed: ${res.documents}`
      if (op === 'similar') {
        const rows = (res.results || []).map(r => `  - ${r.identifier} (containment ${r.containment})`).join('\n')
        return `### Similar to ${res.identifier}\n${rows || '  (none)'}`
      }
      if (op === 'phrase') {
        const rows = (res.results || []).map(r => `  - ${r.identifier} x${r.occurrences}`).join('\n')
        return `### Phrase "${res.phrase}"\n${rows || '  (no matches)'}`
      }
      const runs = (res.runs || []).map(r => `  - **${r.length_tokens} tokens**${r.start_a !== undefined ? ` (a@${r.start_a}, b@${r.start_b})` : ` @${r.start_token}`}: ${r.snippet.slice(0, 200)}`).join('\n')
      return `### ${op}: ${res.a || res.base} vs ${res.b || res.other}\n${runs || '  (no runs >= threshold)'}`
    }
  })

  // 25. archive_scrape
  ctx.tools.register({
    name: 'archive_scrape',
    description: 'Bulk metadata harvest via the cursor-based Scrape API — the endpoint for large candidate sets (thousands of identifiers), where archive_search is capped at a page. Pass the returned cursor back in to continue the sweep.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Lucene query (e.g. "collection:nasa mediatype:image")' },
        fields: { type: 'array', items: { type: 'string' }, description: 'Fields to return (e.g. ["identifier","title"])' },
        cursor: { type: 'string', description: 'Cursor from a previous call, to continue the sweep' }
      },
      required: ['query']
    },
    async execute(args) {
      const res = await runBridge('scrape', args)
      if (res.error) return `Scrape error: ${res.error}`
      const items = res.items || []
      const lines = items.slice(0, 50).map(i => `  - ${i.identifier}${i.title ? ` — ${i.title}` : ''}`).join('\n')
      return `### Scrape: ${res.count} items\n${lines}\n\n${res.cursor ? `**Next cursor**: \`${res.cursor}\`` : '**Sweep complete** (no further cursor)'}`
    }
  })

  // 26. archive_text_store
  ctx.tools.register({
    name: 'archive_text_store',
    description: 'List the text held locally for offline analysis (documents, size, when fetched). Corpus analysis can only see what has been harvested — check this when a document is reported as "not in the corpus".',
    output: OUT,
    parameters: { type: 'object', properties: {} },
    async execute() {
      const res = await runBridge('text_store', {})
      if (res.error) return `Store error: ${res.error}`
      const entries = res.entries || []
      if (entries.length === 0) return 'Local text store is empty. Use archive_text_harvest, then archive_corpus {operation:"build"}.'
      const lines = entries.map(e => `  - ${e.identifier} (${e.chars} chars, ${e.format || 'unknown'}, fetched ${(e.fetched_at || '').slice(0, 19)})`).join('\n')
      return `### Local text store: ${res.stats.documents} documents, ${res.stats.total_chars} chars\n${lines}`
    }
  })

  // 27. archive_corpus_sql
  ctx.tools.register({
    name: 'archive_corpus_sql',
    description: 'Run a read-only SQL query against the local corpus. Tables: documents(identifier,title,chars,tokens,ocr_quality,source_url), document_text(identifier,text), tokens(identifier,pos,token), shingles(identifier,pos,hash), fingerprints(identifier,fp). The escape hatch — any new research question becomes a query rather than new code.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'A read-only query (SELECT / WITH / DESCRIBE / SUMMARIZE)' }
      },
      required: ['query']
    },
    async execute(args) {
      const res = await runBridge('corpus_sql', args)
      if (res.error) return `SQL error: ${res.error}`
      const rows = res.rows || []
      if (rows.length === 0) return `No rows (${res.row_count} returned).`
      const body = rows.slice(0, 50).map(r => `  ${JSON.stringify(r)}`).join('\n')
      return `### ${res.row_count} rows\n${body}`
    }
  })

  // 28. archive_reference_drift
  ctx.tools.register({
    name: 'archive_reference_drift',
    description: 'For a page, compare the references it cites between the archive and the live web: live HTTP status, exact archive capture count (from the Memento timemap, uncapped), and the regime — live_and_archived, archive_only, live_only or unverified. archive_only is asserted ONLY for genuinely dead references: a refusal (403/429) is reported as unverified, never as absence, because hosts refuse scripted clients routinely. Results are cached locally with a checked_at timestamp; set refresh=true to re-probe. Every reference reports when it was checked, so a cached observation is never presented as current. Pair with Jina for content comparison — Jina refuses web.archive.org, so the two sides cannot share a fetcher.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        url: { type: 'string', description: 'Page whose references should be checked (e.g. an archived article)' },
        max_refs: { type: 'integer', description: 'Maximum references to check (default: 12)' },
        check_live: { type: 'boolean', description: 'Check live HTTP status (default: true)' },
        timestamp: { type: 'string', description: 'Snapshot timestamp; defaults to the latest successful capture' },
        cache_ttl_hours: { type: 'number', description: 'Reuse a cached check younger than this (default: 24)' },
        refresh: { type: 'boolean', description: 'Ignore the cache and re-probe every reference' }
      },
      required: ['url']
    },
    async execute(args) {
      const res = await runBridge('reference_drift', args)
      if (res.error) return `Reference drift error: ${res.error}`
      const refs = res.references || []
      const rows = refs.map(r => {
        const caps = r.captures_capped ? `${r.archived_captures}+` : `${r.archived_captures}`
        return `  - **${r.host}** — live ${r.live_status ?? r.error}, ${caps} captures, latest ${r.archived_latest || 'none'} → \`${r.regime}\`${r.from_cache ? ` _(checked ${r.checked_at})_` : ''}`
      }).join('\n')
      const summary = Object.entries(res.summary || {}).map(([k, v]) => `${k}: ${v}`).join(' · ')
      return `### Reference drift for ${res.url}\n- snapshot: ${res.snapshot?.timestamp} (${res.snapshot_url})\n- ${res.reference_count} references — ${summary}\n\n${rows}\n\n${res.note || ''}`
    }
  })

  // 29. archive_readable_copies
  ctx.tools.register({
    name: 'archive_readable_copies',
    description: 'Given an item or a title query, report which copies of a work can actually be READ. A catalogue search ranks the best-scanned copy first, and that is frequently a lending-restricted one: its text cannot be downloaded and search-inside returns 403 anonymously, so the work looks unavailable when dozens of readable copies exist. Returns the anchor item\'s state plus readable alternatives, restricted copies, and copies with no searchable text.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'The item you were given; reported first whatever its rank' },
        query: { type: 'string', description: 'Or a Lucene query, e.g. title:("some work")' },
        limit: { type: 'integer', description: 'Copies to report per group (default: 15)' },
        check: { type: 'integer', description: 'Catalogue rows to examine (default: 40)' }
      }
    },
    async execute(args) {
      const res = await runBridge('readable_copies', args)
      if (res.error) return `Readable-copies error: ${res.error}`
      const a = res.anchor
      const head = a
        ? `### Anchor: ${a.identifier}\n- state: **${a.state}**${a.restricted ? ' (lending-restricted)' : ''}${a.derivative ? ` · ${a.derivative}` : ''}\n- readable: **${res.anchor_readable ? 'yes' : 'no'}**\n`
        : ''
      const list = (res.readable || []).map(r => `  - ${r.identifier}${r.derivative ? ` (${r.derivative})` : ''} — ${r.evidence_url}`).join('\n')
      const restricted = (res.restricted || []).map(r => `  - ${r.identifier}`).join('\n')
      const noText = (res.no_searchable_text || []).map(r => `  - ${r.identifier}`).join('\n')
      return `${head}### ${res.readable_count} readable of ${res.checked} checked (work has ${res.catalogue_total} copies)\n${list}\n` +
        (restricted ? `\n**Restricted, unusable anonymously:**\n${restricted}\n` : '') +
        (noText ? `\n**No searchable text (a zero result here is not absence of the phrase):**\n${noText}\n` : '')
    }
  })

  // 30. archive_claim_trace
  ctx.tools.register({
    name: 'archive_claim_trace',
    description: 'Map a claim\'s documentary footprint in the archive. NOT a fact checker and it renders no verdict: it reports how many documents carry the phrasing, across which years, collections, languages and creators; the dated, page-anchored instances with the matched text; the same figures restricted to the PRIMARY record (government, military, declassified) so circulation can be told apart from documentation; and which sampled sources use denial phrasing, quoted verbatim. Heavy presence proves circulation, not truth; absence in this index is not absence from the world.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        statement: { type: 'string', description: 'The claim or phrasing to trace' },
        size: { type: 'integer', description: 'Sample instances to return (default: 10)' },
        primary_only: { type: 'boolean', description: 'Restrict to primary-source collections' }
      },
      required: ['statement']
    },
    async execute(args) {
      const r = await runBridge('claim_trace', args)
      if (r.error) return `Claim trace error: ${r.error}`
      const rec = r.recorded || {}, prim = r.primary_record || {}
      const ys = (rec.years || []).map(y => `${y.key}(${y.documents})`).slice(0, 8).join(' ')
      const lines = [
        `### Documentary footprint: ${r.claim}`,
        `- **${rec.documents ?? '?'} documents** carry this phrasing`,
        `- **${prim.documents ?? 'n/a'}** of them sit in the primary record (${(prim.collections || []).map(c => c.key).slice(0, 5).join(', ') || 'none found'})`,
        `- year span: ${r.year_span?.earliest ?? '?'} – ${r.year_span?.latest ?? '?'}   top years: ${ys}`,
        `- languages: ${(rec.languages || []).map(l => l.key).slice(0, 5).join(', ')}`,
        `- denial phrasing in ${(r.denial_phrasing || []).length} of the sampled instances`,
        '',
        'Dated instances:',
        ...(r.evidence || []).slice(0, 5).map(e => {
          const prov = e.provenance || {}
          return `  - ${e.date ? e.date.slice(0,10) : 'undated'} · ${e.title || e.identifier} · page ${e.page ?? '-'} · [${prov.tier || '?'} ${prov.basis || ''}]\n      ${e.url}\n      "${(e.quote || '').slice(0, 160)}"`
        }),
        '',
        'Cite independently of this host (identifier · collection · date · page · sha1 · quote):',
        ...(r.evidence || []).slice(0, 2).map(e => `  ${e.identifier}${e.page !== null && e.page !== undefined ? ` p.${e.page}` : ''}${(e.collections||[]).length ? ` · ${e.collections.slice(0,2).join(', ')}` : ''}${e.date ? ` · ${String(e.date).slice(0,10)}` : ''}${e.sha1 ? ` · sha1 ${String(e.sha1).slice(0,12)}` : ''}`),
      ]
      if ((prim.examples || []).length) {
        lines.push('', 'Primary record:')
        for (const e of prim.examples.slice(0, 3)) lines.push(`  - ${e.title || e.identifier} · ${(e.date||'').slice(0,10)} · ${e.url}`)
      }
      lines.push('', r.note || '')
      return lines.join('\n')
    }
  })

  // 31. archive_quote_from_index
  ctx.tools.register({
    name: 'archive_quote_from_index',
    description: 'Retrieve verbatim passages from one item, INCLUDING lending-restricted items whose full text cannot be downloaded. Uses the public full-text index API (ia-pub-fts-api) with field query identifier:<id> AND <term>. Reports honest page_scope: when "item_aggregate", the page is for the whole item and must not be cited as the passage\'s own leaf.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Archive.org item identifier' },
        terms: {
          type: 'array',
          items: { type: 'string' },
          description: 'Terms or phrases to look for inside that one item'
        },
        delay: { type: 'number', description: 'Seconds between queries (default: 1.0)' }
      },
      required: ['identifier', 'terms']
    },
    async execute(args) {
      const res = await runBridge('quote_from_index', args)
      if (res.error) return `Index Quote Error: ${res.error}`
      const item = res.item || {}
      const passages = res.passages || []
      const lines = [
        `### Verbatim Passages from Index: ${res.identifier} (${res.passage_count} found)`,
        `- **Tier**: ${item.tier || 'T2'} (${item.tier_basis || 'catalogued'})`,
        `- **Reported Page**: ${item.page ?? 'N/A'} (scope: **${item.page_scope || 'item_aggregate'}**)`,
        `- **Evidence URL**: ${item.evidence_url || `https://archive.org/details/${res.identifier}`}`,
        '',
        ...passages.slice(0, 8).map(p => `> [${p.term}] "${p.text}"`),
        '',
        `*${res.caveat || ''}*`
      ]
      return lines.join('\n')
    }
  })

  // 32. archive_query_diagnostics
  ctx.tools.register({
    name: 'archive_query_diagnostics',
    description: 'Explain a search query that returned zero results. Reports whether matches exist in document full-text vs metadata, tests terms individually to find which term blocks the query, and warns when a compound query narrows prematurely.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'The query to diagnose' }
      },
      required: ['query']
    },
    async execute(args) {
      const res = await runBridge('query_diagnostics', args)
      if (res.error) return `Diagnostics Error: ${res.error}`
      const lines = [
        `### Query Diagnostics: "${res.query}"`,
        `- **Where matches exist**: ${res.where} (fulltext: ${res.fulltext_total ?? 'N/A'}, metadata: ${res.metadata_total ?? 'N/A'})`,
      ]
      if (res.warning) lines.push(`- **Warning**: ${res.warning}`)
      if (res.per_term) {
        lines.push('', '**Per-Term Match Counts**:')
        for (const [term, cnt] of Object.entries(res.per_term)) {
          lines.push(`  - \`${term}\`: ${cnt} matches`)
        }
      }
      if (res.verdict) lines.push('', `**Verdict**: ${res.verdict}`)
      if (res.note) lines.push('', `*Note*: ${res.note}`)
      return lines.join('\n')
    }
  })

  // 33. archive_lending_status
  ctx.tools.register({
    name: 'archive_lending_status',
    description: 'Query live digital lending/borrowing status from BookReaderJSIA.php. Reports is_lendable, is_readable, available_to_browse (1-hr loan), available_to_borrow (14-day loan), available_to_waitlist, and observed_as identity.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Archive.org book/item identifier' }
      },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('lending_status', args)
      if (res.error) return `Lending Status Error: ${res.error}`
      return `### Lending Status: \`${res.identifier}\`
- **Observed As**: \`${res.observed_as}\`
- **Is Lendable**: ${res.is_lendable ? 'Yes' : 'No'}
- **Is Readable**: ${res.is_readable ? 'Yes' : 'No'}
- **Available to Browse (1-hour)**: ${res.available_to_browse ? 'Yes' : 'No'}
- **Available to Borrow (14-day)**: ${res.available_to_borrow ? 'Yes' : 'No'}
- **Available on Waitlist**: ${res.available_to_waitlist ? 'Yes' : 'No'}
- **Copies**: ${res.available_lendable_copies} available / ${res.max_borrowable_copies} total
- **Users on Waitlist**: ${res.users_on_waitlist}
- **Print-Disabled Only**: ${res.is_printdisabled ? 'Yes' : 'No'}
- **Loan Currently Held**: ${res.loan_held ? 'Yes' : 'No'}`
    }
  })

  // 34. archive_agent_ingest
  ctx.tools.register({
    name: 'archive_agent_ingest',
    description: 'Ingest any Archive.org item directly into the local DuckDB research database with zero friction. Automatically handles BOTH open and lending-restricted items: downloads, chunks, and digests open works, or automatically mines verbatim passages via FTS for restricted works. Once ingested, the work is queryable offline in milliseconds.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Archive.org item identifier (open OR restricted)' },
        terms: {
          type: 'array',
          items: { type: 'string' },
          description: 'Optional focus terms to mine if item is restricted (defaults to subject keywords if omitted)'
        }
      },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('agent_ingest', args)
      if (res.error) return `Ingestion Error: ${res.error}`
      if (res.status === 'error') return `Ingestion Refusal: ${res.reason}`

      const modeStr = res.mode === 'restricted_fts_mine'
        ? `Lending-Restricted FTS Mine (${res.terms_mined?.join(', ') || 'keywords'})`
        : `Open Work Full-Text Ingestion (${res.total_chars?.toLocaleString() || 0} chars)`

      return `### Work Ingested into DuckDB Warehouse: \`${res.identifier}\`
- **Title**: *${res.title}*
- **Ingestion Mode**: ${modeStr}
- **Cards / Chunks Ingested**: ${res.cards_ingested} evidence records
- **Provenance Tier**: **${res.tier}** (${res.tier_basis})
- **Permanent URL**: ${res.evidence_url}
- **Page Scope**: \`${res.page_scope || 'exact'}\`${res.caveat ? `\n- *Note*: ${res.caveat}` : ''}
*The work is now permanently resident in the local warehouse for sub-millisecond retrieval.*`
    }
  })

  // 35. archive_agent_query
  ctx.tools.register({
    name: 'archive_agent_query',
    description: 'Query the local DuckDB research warehouse for verified evidence cards. Returns verbatim quotations from ingested open and restricted works with provenance tiers (T1-T5), page scopes, integrity hashes, and audit links.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'The research question or topic to search in local database' },
        limit: { type: 'integer', description: 'Maximum evidence cards to retrieve (default: 5)' },
        tier: { type: 'string', description: 'Optional tier filter (e.g. "T1", "T2")' }
      },
      required: ['query']
    },
    async execute(args) {
      const res = await runBridge('agent_query', args)
      if (res.error) return `Agent Query Error: ${res.error}`
      if (!res.cards || res.cards.length === 0) {
        return `No matching evidence cards found in local database for query "${res.query}". Ingest relevant works first using archive_agent_ingest.`
      }
      return res.formatted_prompt
    }
  })

  // 36. archive_fact_check
  ctx.tools.register({
    name: 'archive_fact_check',
    description: 'Professional epistemic fact-checker that works both WITH and WITHOUT Jina. Conventional fact-checkers fail when primary evidence sits in offline books, declassified archives, or dead links not indexed on the live web. This tool sweeps the Archive.org full-text OCR index (millions of books and documents), isolates the Primary Record (T1 government, military, declassified collections), queries the local DuckDB warehouse, classifies stance (state/report/deny/mention) and numeric discrepancies, and automatically combines Live Web stance via Jina when a Jina API key is available (or runs 100% standalone against the Archive when Jina is absent).',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        statement: { type: 'string', description: 'The factual claim, historical assertion, or technical statement to verify' },
        size: { type: 'integer', description: 'Archive instances to sample (default: 8)' },
        primary_only: { type: 'boolean', description: 'Restrict archive sweep strictly to primary-source (T1) collections' },
        include_live_web: { type: 'boolean', description: 'Also sweep the live web via Jina when available (default: true)' },
        apiKey: { type: 'string', description: 'Optional Jina API key override for the live-web half' }
      },
      required: ['statement']
    },
    async execute(args) {
      const statement = String(args.statement)
      const size = Number(args.size || 8)
      const primary_only = Boolean(args.primary_only)
      const includeLive = args.include_live_web !== false

      const archivePromise = runBridge('claim_trace', { statement, size, primary_only })
        .then((v) => ({ ok: !v.error, value: v, error: v.error }))
        .catch((err) => ({ ok: false, error: String((err && err.message) || err) }))

      let livePromise = Promise.resolve({ mode: 'archive_standalone' })
      if (includeLive) {
        const jinaKey = await resolveOptionalJinaKey(ctx, args.apiKey)
        if (jinaKey) {
          livePromise = archivePromise.then(async (arch) => {
            const evalTarget = (arch && arch.ok && arch.value && arch.value.subject_core)
              ? arch.value.subject_core.replace(/"/g, '')
              : statement
            try {
              const r = await fetch('https://svip.jina.ai/', {
                method: 'POST',
                headers: {
                  Accept: 'application/json',
                  'Content-Type': 'application/json',
                  Authorization: `Bearer ${jinaKey}`,
                },
                body: JSON.stringify({ q: evalTarget, num: 5 }),
                signal: AbortSignal.timeout(25000),
              })
              if (!r.ok) return { mode: 'archive_standalone', error: `Jina HTTP ${r.status}` }
              const data = await r.json()
              return { mode: 'jina', map: evaluateGroundedResults(evalTarget, data) }
            } catch (err) {
              return { mode: 'archive_standalone', error: String((err && err.message) || err) }
            }
          })
        }
      }

      const [archiveRes, liveRes] = await Promise.all([archivePromise, livePromise])
      return formatUnifiedFactCheck(statement, archiveRes, liveRes)
    }
  })

  // 37. wayback_read
  ctx.tools.register({
    name: 'wayback_read',
    description: 'Fetch a historical Wayback Machine webpage snapshot and extract clean, readable text (or raw HTML). Essential because Jina Reader blocks web.archive.org URLs — use this tool whenever you need to read the content of an archived webpage without writing custom Python/curl scripts. If `timestamp` is omitted, automatically fetches the latest HTTP 200 snapshot.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        url: { type: 'string', description: 'Target webpage URL to read from the Wayback Machine (e.g. "https://example.com/article")' },
        timestamp: { type: 'string', description: 'Optional 14-digit Wayback timestamp (YYYYMMDDHHMMSS). Defaults to latest capture.' },
        offset: { type: 'integer', description: 'Starting character offset for pagination (default: 0)' },
        max_chars: { type: 'integer', description: 'Maximum characters to return (default: 8000)' },
        raw_html: { type: 'boolean', description: 'Return raw HTML source instead of extracted clean text (default: false)' }
      },
      required: ['url']
    },
    async execute(args) {
      const res = await runBridge('wayback_read', args)
      if (res.error) return `Wayback Read Error: ${res.error}`
      if (!res.found) return res.error || `No snapshot found for ${args.url}`
      const nextHint = res.next_offset ? ` | Next Offset: \`${res.next_offset}\`` : ''
      return `### Wayback Snapshot: [${res.title || res.url}](${res.playback_url})
- **Timestamp**: \`${res.timestamp}\` | **Length**: ${res.total_chars} chars (showing offset ${res.offset}..${res.offset + res.text.length}${nextHint})

\`\`\`text
${res.text}
\`\`\``
    }
  })

  // 38. archive_iiif_image
  ctx.tools.register({
    name: 'archive_iiif_image',
    description: 'Extract high-resolution digitized book pages, manuscripts, or bounding-box cropped figures via the Archive.org IIIF Image API 3.0. Can return the direct IIIF URL or download the image/crop directly to a local file (`download_path`) so you can inspect figures, diagrams, or non-OCR pages visually with `read_image`.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Archive.org book/document identifier' },
        page_number: { type: 'integer', description: '1-based leaf/page number (default: 1)' },
        region: { type: 'string', description: 'IIIF region: "full", "square", or pixel crop "x,y,w,h" (e.g. "100,200,800,600")' },
        size: { type: 'string', description: 'IIIF size: "max", "pct:50", or "w,h" (default: "max")' },
        rotation: { type: 'integer', description: 'Rotation in degrees (0, 90, 180, 270; default: 0)' },
        format: { type: 'string', enum: ['jpg', 'png', 'webp'], description: 'Image format (default: "jpg")' },
        download_path: { type: 'string', description: 'Optional local file path to save the rendered page/crop (e.g. "./downloads/page_12.jpg")' }
      },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('iiif_image', args)
      if (res.error) return `IIIF Image Error: ${res.error}`
      const dlLine = res.downloaded
        ? `\n- **Saved Locally**: \`${res.saved_path}\` (${(res.bytes / 1024).toFixed(1)} KB) — ready for \`read_image\``
        : ''
      return `### IIIF Page Image: \`${res.identifier}\` (Page ${res.page_number})
- **IIIF 3.0 URL**: ${res.iiif_url}
- **Archive BookReader**: ${res.details_page}
- **Target File**: \`${res.target_file}\` (Region: \`${res.region}\`, Size: \`${res.size}\`)${dlLine}`
    }
  })

  // 39. archive_lending_borrow
  ctx.tools.register({
    name: 'archive_lending_borrow',
    description: 'Request a 1-hour browse loan (`browse_book`), 14-day loan (`borrow_book`), or loan return (`return_loan`) for a lending-restricted book on Archive.org when a patron session cookie (`IA_PATRON_COOKIE` or `patron_cookie`) is available.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Archive.org lending book identifier' },
        action: { type: 'string', enum: ['browse_book', 'borrow_book', 'return_loan'], description: 'Loan action to execute (default: "browse_book")' },
        patron_cookie: { type: 'string', description: 'Optional Archive.org logged-in session cookie override ("logged-in-user=...; logged-in-sig=...")' }
      },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('lending_borrow', args)
      if (res.error && res.status !== 'unauthenticated' && res.status !== 'unavailable') {
        return `Lending Action Error: ${res.error}`
      }
      return `### Digital Lending Action (\`${args.action || 'browse_book'}\`) for \`${res.identifier}\`
- **Status**: \`${res.status}\`
${res.error ? `- **Detail**: ${res.error}\n- **Tip**: Even without a patron cookie, you can extract verbatim passages from lending-restricted books using \`archive_quote_from_index\` or \`archive_agent_ingest\`.` : `- **Response**: \`${JSON.stringify(res.response)}\``}`
    }
  })

  // 40. archive_item_manage
  ctx.tools.register({
    name: 'archive_item_manage',
    description: 'Manage Archive.org items via IAS3 and Tasks API: create a new item bucket (`create`), patch item metadata (`patch_metadata`), delete a remote file (`delete_file`), inspect background server derivation tasks (`tasks`), or inspect physical storage cluster nodes (`server_nodes`).',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        operation: { type: 'string', enum: ['create', 'patch_metadata', 'delete_file', 'tasks', 'server_nodes'], description: 'Management operation to run' },
        identifier: { type: 'string', description: 'Target Archive.org item identifier' },
        title: { type: 'string', description: 'For create: item title' },
        mediatype: { type: 'string', description: 'For create: mediatype (default: "data")' },
        collection: { type: 'string', description: 'For create: collection (default: "opensource")' },
        description: { type: 'string', description: 'For create: item description' },
        creator: { type: 'string', description: 'For create: item creator' },
        date: { type: 'string', description: 'For create: item date (YYYY-MM-DD)' },
        filename: { type: 'string', description: 'For delete_file: remote filename to delete' },
        patches: {
          type: 'array',
          items: { type: 'object' },
          description: 'For patch_metadata: JSON Patch operations array, e.g. [{"op":"add","path":"/subject","value":"cybernetics"}]'
        }
      },
      required: ['operation', 'identifier']
    },
    async execute(args) {
      const res = await runBridge('item_manage', args)
      if (res.error) return `Item Management Error: ${res.error}`
      return `### Item Management (\`${args.operation}\`) on \`${args.identifier}\`\n\`\`\`json\n${JSON.stringify(res, null, 2).slice(0, 2000)}\n\`\`\``
    }
  })

  // 41. archive_research_timeline
  ctx.tools.register({
    name: 'archive_research_timeline',
    description: 'Retrieve the chronological timeline of all ingested captures and content digests for a URL or item identifier in the local DuckDB research warehouse.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        source_key: { type: 'string', description: 'Target URL or Archive.org identifier to trace across time' }
      },
      required: ['source_key']
    },
    async execute(args) {
      const res = await runBridge('research_timeline', args)
      if (res.error) return `Timeline Error: ${res.error}`
      const rows = (res.timeline || []).map(t => `- **${t.timestamp || 'N/A'}** (\`${t.id}\`): ${t.content_length} chars, ${t.token_count} tokens, sha256:\`${(t.digest || '').slice(0, 12)}\` — ${t.archive_url || 'N/A'}`).join('\n')
      return `### Warehouse Temporal Timeline for \`${res.source_key}\` (${res.count} captures):\n${rows || '  (no records found in local warehouse)'}`
    }
  })

  // 42. archive_forensic_compare
  ctx.tools.register({
    name: 'archive_forensic_compare',
    description: 'Primary-text forensic comparator that tests plagiarism, translation, and textual derivation claims directly against the primary texts themselves rather than secondary commentary. Automatically harvests both documents into the DuckDB corpus, strips Google Books digitization boilerplate, computes exact 8-gram shingle overlap against a shuffled-token null baseline (to test same-language verbatim copying), and computes sliding-window distinctive vocabulary alignments (to detect translated or paraphrased parallels across languages).',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier_a: { type: 'string', description: 'First Archive.org document identifier (e.g. primary source A)' },
        identifier_b: { type: 'string', description: 'Second Archive.org document identifier (e.g. primary source B)' },
        window_tokens: { type: 'integer', description: 'Sliding window size in content tokens (default: 250)' },
        top_passages: { type: 'integer', description: 'Maximum aligned passage pairs to return (default: 6)' }
      },
      required: ['identifier_a', 'identifier_b']
    },
    async execute(args) {
      const res = await runBridge('forensic_compare', args)
      if (res.error) return `Forensic Compare Error: ${res.error}`
      const sh = res.exact_8gram_shingles || {}
      const runs = (res.verbatim_runs || []).map(r => `  - **${r.length_tokens} tokens** (a@${r.start_a}, b@${r.start_b}): "${r.snippet}"`).join('\n')
      const wins = (res.conceptual_window_alignments || []).slice(0, args.top_passages || 6).map((w, i) =>
        `  **[Alignment #${i + 1} | ${w.shared_distinctive_terms} shared distinctive terms | Jaccard ${w.jaccard}]** (a@${w.pos_a}, b@${w.pos_b})\n`
        + `  - Shared Vocabulary: \`${(w.terms || []).join(', ')}\`\n`
        + `  - **${res.identifier_a}**: "${w.excerpt_a}..."\n`
        + `  - **${res.identifier_b}**: "${w.excerpt_b}..."`
      ).join('\n\n')
      return `### Primary-Text Forensic Comparison: \`${res.identifier_a}\` vs \`${res.identifier_b}\`
- **Document A**: \`${res.identifier_a}\` (${res.chars_a} chars, ${res.tokens_a} content tokens)
- **Document B**: \`${res.identifier_b}\` (${res.chars_b} chars, ${res.tokens_b} content tokens)
- **Exact 8-Gram Shingle Overlap (Boilerplate Stripped)**: Observed = **${sh.observed}** | Shuffled Null = **${sh.shuffled_null}** | Excess Over Null = **${sh.excess_over_null}**
- **Verbatim Runs (>=10 tokens)**:
${runs || '  (none — no verbatim same-language copying above threshold)'}
- **Sliding-Window Distinctive Vocabulary Alignments (survives translation/paraphrase)**:
${wins || '  (no high-density conceptual alignments found)'}

*${res.interpretation}*`
    }
  })

  // 43. archive_epistemic_dossier
  ctx.tools.register({
    name: 'archive_epistemic_dossier',
    description: 'ONE-CALL AUTONOMOUS EPISTEMIC & FORENSIC DOSSIER (95% context-token reduction). Instead of calling 10+ separate search, harvest, diff, and read tools ("104 keys on a typewriter"), call this single macro-tool first for ANY controversial claim, historical question, or book evaluation. In one server-side pass it: (1) strips framing bias and runs 4-axis triangulation (Core, Corroboration, Critique, Substance + T1 Primary Records), (2) autonomously harvests the top canonical primary editions into DuckDB (or mines FTS quotes if lending-restricted), (3) runs 8-gram shingle + sliding-window forensic comparisons and structural edition diffs (`classify_differences`), (4) extracts concentrated opening/focus/closing primary-text windows directly from the works, and (5) layers Live Web stance when Jina is available — returning one dense ~1,200-token verified dossier.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        statement: { type: 'string', description: 'The raw question, controversial claim, or work title to investigate' },
        compare_with: { type: 'string', description: 'Optional Archive.org identifier of a second primary text to forensically compare against the canonical work' },
        focus_terms: {
          type: 'array',
          items: { type: 'string' },
          description: 'Optional focus keywords to extract targeted windows from the primary text'
        },
        include_live_web: { type: 'boolean', description: 'Also include Jina live-web stance mapping when available (default: true)' },
        apiKey: { type: 'string', description: 'Optional Jina API key override' }
      },
      required: ['statement']
    },
    async execute(args) {
      const statement = String(args.statement)
      const includeLive = args.include_live_web !== false

      const dossierPromise = runBridge('epistemic_dossier', {
        statement,
        compare_with: args.compare_with,
        focus_terms: args.focus_terms,
        max_harvest: 2,
      })

      let livePromise = Promise.resolve({ mode: 'archive_standalone' })
      if (includeLive) {
        const jinaKey = await resolveOptionalJinaKey(ctx, args.apiKey)
        if (jinaKey) {
          livePromise = dossierPromise.then(async (dos) => {
            const evalTarget = (dos && dos.subject_core) ? String(dos.subject_core).replace(/"/g, '') : statement
            try {
              const r = await fetch('https://svip.jina.ai/', {
                method: 'POST',
                headers: {
                  Accept: 'application/json',
                  'Content-Type': 'application/json',
                  Authorization: `Bearer ${jinaKey}`,
                },
                body: JSON.stringify({ q: evalTarget, num: 5 }),
                signal: AbortSignal.timeout(25000),
              })
              if (!r.ok) return { mode: 'archive_standalone', error: `Jina HTTP ${r.status}` }
              const data = await r.json()
              return { mode: 'jina', map: evaluateGroundedResults(evalTarget, data) }
            } catch (err) {
              return { mode: 'archive_standalone', error: String((err && err.message) || err) }
            }
          })
        }
      }

      const [dos, liveRes] = await Promise.all([dossierPromise, livePromise])
      if (dos.error) return `Epistemic Dossier Error: ${dos.error}`

      const baseReport = formatUnifiedFactCheck(statement, { ok: true, value: dos.trace }, liveRes)
      const lines = [baseReport, '', '3. AUTONOMOUS PRIMARY-TEXT FORENSICS & SUBSTANCE INSPECTION (Server-Side Pipeline)']

      if ((dos.primary_excerpts || []).length > 0) {
        lines.push('  · Harvested & Indexed Primary Editions:')
        for (const pe of dos.primary_excerpts) {
          lines.push(`    - **${pe.identifier}** (${pe.chars.toLocaleString()} chars) — ${pe.url}`)
          for (const w of (pe.windows || [])) {
            lines.push(`      [${w.region} @ offset ${w.offset}]: "${w.excerpt.slice(0, 320)}"`)
          }
        }
      }

      if ((dos.restricted_quotes || []).length > 0) {
        lines.push('  · Lending-Restricted Primary Edition Quotes (Mined via FTS Index):')
        for (const rq of dos.restricted_quotes) {
          lines.push(`    - **${rq.identifier}** [${rq.tier}, scope: ${rq.page_scope}] — ${rq.url}`)
          for (const p of (rq.passages || []).slice(0, 3)) {
            lines.push(`      [${p.term}]: "${p.text.slice(0, 260)}"`)
          }
        }
      }

      if (dos.forensic_report) {
        const fr = dos.forensic_report
        const sh = fr.exact_8gram_shingles || {}
        lines.push(`  · Primary-Text Forensic Comparison (\`${fr.identifier_a}\` vs \`${fr.identifier_b}\`):`)
        lines.push(`    - Exact 8-Gram Shingle Overlap: Observed = **${sh.observed}** | Shuffled Null = **${sh.shuffled_null}** | Excess = **${sh.excess_over_null}**`)
        for (const w of (fr.conceptual_window_alignments || []).slice(0, 2)) {
          lines.push(`    - Window Alignment (a@${w.pos_a}, b@${w.pos_b} | ${w.shared_distinctive_terms} shared terms | Jaccard ${w.jaccard}): \`${(w.terms || []).slice(0, 10).join(', ')}\``)
        }
      }

      if ((dos.edition_diffs || []).length > 0) {
        lines.push('  · Structural Edition Differences (Additions / Excisions / Postscripts):')
        for (const d of dos.edition_diffs.slice(0, 3)) {
          lines.push(`    - Present in \`${d.in}\` (absent in \`${d.missing_from}\`) [${d.label} | ${d.length_tokens} tokens @${d.start_token}]: "${(d.snippet || '').slice(0, 240)}"`)
        }
      }

      return lines.join('\n')
    }
  })

  // 44. archive_cache_manage
  ctx.tools.register({
    name: 'archive_cache_manage',
    description: 'Manage the local Archive.org cache (metadata, gzipped texts, auto-borrowed lending items, and downloaded media/IIIF artifacts). Operations: `stats` (summary counts and sizes), `list` (list cached texts, metadata, and artifacts), `inspect` (inspect cached metadata, text preview, and artifacts for one identifier), `borrow_and_cache` (automatically borrow a lending-restricted item or mine its FTS passages and cache/index it locally), `delete` (delete cached text, metadata, artifacts, and corpus rows for an identifier), or `sync_corpus` (rebuild DuckDB corpus from all cached texts).',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        operation: {
          type: 'string',
          enum: ['stats', 'list', 'inspect', 'borrow_and_cache', 'delete', 'sync_corpus'],
          description: 'Cache management operation to run (default: "stats")'
        },
        identifier: { type: 'string', description: 'Target Archive.org identifier or artifact ID (for inspect, borrow_and_cache, delete, or filtered list)' },
        kind: { type: 'string', description: 'Optional artifact kind filter for list (e.g. "text_derivative", "borrowed_fts_digest", "borrowed_text", "media_download", "iiif_image")' },
        terms: {
          type: 'array',
          items: { type: 'string' },
          description: 'Optional focus terms to mine when borrowing/caching a lending-restricted item'
        },
        patron_cookie: { type: 'string', description: 'Optional Archive.org patron session cookie override for borrowing' },
        limit: { type: 'integer', description: 'Maximum rows to list (default: 25)' }
      },
      required: ['operation']
    },
    async execute(args) {
      const res = await runBridge('cache_manage', args)
      if (res.error) return `Cache Management Error: ${res.error}`
      const op = args.operation || 'stats'
      if (op === 'stats') {
        const byKind = (res.artifacts_by_kind || []).map(k => `  - \`${k.kind}\`: ${k.count} items (${(k.bytes / 1024).toFixed(1)} KB)`).join('\n')
        return `### Local Archive Cache & Artifact Registry (\`${res.root}\`)
- **Cached Texts**: ${res.texts_cached} works (${(res.texts_total_chars / 1024).toFixed(1)} KB uncompressed)
- **Cached Item Metadata**: ${res.metadata_cached} items (${res.restricted_items_tracked} lending-restricted tracked)
- **Registered Artifacts**: ${res.artifacts_cached} files (${(res.artifacts_total_bytes / 1024).toFixed(1)} KB)
${byKind ? `- **Artifacts by Kind**:\n${byKind}\n` : ''}${res.corpus ? `- **DuckDB Corpus**: ${res.corpus.documents} docs (${res.corpus.tokens} tokens, ${res.corpus.shingles} shingles)` : ''}`
      }
      if (op === 'list') {
        const s = res.summary || {}
        const tRows = (res.texts || []).slice(0, 15).map(t => `  - **${t.identifier}** (\`${t.format}\`, ${t.chars} chars, sha256:\`${(t.sha256 || '').slice(0, 10)}\`, fetched ${(t.fetched_at || '').slice(0, 19)})`).join('\n')
        const mRows = (res.metadata || []).slice(0, 10).map(m => `  - **${m.identifier}**: *${m.title}* (${m.mediatype}, ${m.files_count} files${m.restricted ? ', [LENDING-RESTRICTED]' : ''})`).join('\n')
        const aRows = (res.artifacts || []).slice(0, 15).map(a => `  - [${a.kind}] \`${a.identifier}\` / \`${a.file_name}\` (${(a.size_bytes / 1024).toFixed(1)} KB, status: \`${a.loan_status}\`, path: \`${a.local_path}\`)`).join('\n')
        return `### Local Cache Inventory (${s.texts_cached || 0} texts, ${s.metadata_cached || 0} metadata records, ${s.artifacts_cached || 0} artifacts)
#### Cached Texts:
${tRows || '  (none)'}

#### Cached Metadata:
${mRows || '  (none)'}

#### Cached Artifacts & Borrowed Works:
${aRows || '  (none)'}`
      }
      if (op === 'borrow_and_cache') {
        return `### Auto-Borrow & Local Cache Result: \`${res.identifier}\`
- **Title**: *${res.title || res.identifier}*
- **Status**: \`${res.status}\` (Loan Mode: \`${res.loan_status || 'n/a'}\`)
- **Cached Characters**: ${res.chars || 0} chars (${res.passage_count || 0} passages)
- **Local Path**: \`${res.cached_path || 'N/A'}\`
*Work is now cached in TextStore and indexed in the local DuckDB corpus for offline analysis.*`
      }
      if (op === 'delete') {
        return `### Deleted Cached Content for \`${res.identifier}\`
- **Removed Text**: ${res.deleted_text} | **Removed Artifacts**: ${res.deleted_artifacts} | **Removed from Corpus**: ${res.removed_from_corpus}
- **Remaining Cache**: ${res.summary?.texts_cached || 0} texts, ${res.summary?.artifacts_cached || 0} artifacts`
      }
      return `### Cache Operation (\`${op}\`)\n\`\`\`json\n${JSON.stringify(res, null, 2).slice(0, 2500)}\n\`\`\``
    }
  })

  // Inject system prompt guidance and bundled archivist-research-agent skill when services are present
  if (typeof ctx.inject === 'function') {
    ctx.inject(['systemPrompt'], (pctx) => {
      try {
        pctx.systemPrompt.section({
          name: 'tool:archive-suite',
          order: 2160,
          text: [
            '# Archive.org, Wayback Machine & Epistemic Verification Suite (43 native tools)',
            '- ONE-CALL AUTONOMOUS PIPELINE (95% TOKEN SAVINGS): For ANY fact-check, controversial topic, book evaluation, or historical investigation, call `archive_epistemic_dossier({ statement, compare_with?, focus_terms? })` FIRST. It executes the entire 5-stage methodology (4-axis bias-stripped triangulation + T1 primary record check + autonomous primary-text harvesting + 8-gram/window forensic comparison + structural edition diffing + substantive excerpt extraction) in ONE call, saving 90%+ of context tokens.',
            '- ZERO-CODE MANDATE: Never write ad-hoc Python, curl, or BeautifulSoup scripts to query Archive.org, read Wayback snapshots, paginate books, or query DuckDB — all 43 operations are exposed as native tools.',
            '- PRIMARY-TEXT FORENSICS OVER SECONDARY RUMOR: Whenever a claim asserts that Document A copied/plagiarized Document B, use `archive_epistemic_dossier({ statement, compare_with })` or `archive_forensic_compare({ identifier_a, identifier_b })` to measure the exact shingle overlap, null baseline, and sliding-window alignments on the primary texts themselves.',
            '- Granular Tools (When Needed After Dossier): `archive_read_text` (`offset` / `grep`), `archive_book_search`, `archive_quote_from_index`, `wayback_read`, `wayback_diff`, `archive_reference_drift`, `archive_iiif_image`.',
          ].join('\n'),
        })
      } catch { /* optional */ }
    })

    ctx.inject(['skills'], (sctx) => {
      try {
        if (!existsSync(ARCHIVIST_SKILL_PATH)) return
        const raw = readFileSync(ARCHIVIST_SKILL_PATH, 'utf8')
        const { description, content } = parseBundledSkill(raw)
        const candidate = {
          name: ARCHIVIST_SKILL_NAME,
          description,
          invocation: { modelInvocable: true, userInvocable: true },
          provider: 'dsh-archive-suite',
          source: 'bundled',
          rank: 600,
          resourceBase: { kind: 'directory', path: ARCHIVIST_SKILL_DIR },
          locator: ARCHIVIST_SKILL_PATH,
        }
        const provider = {
          name: 'dsh-archive-suite',
          list: () => Promise.resolve([candidate]),
          async get(entry) {
            const { rank: _rank, locator: _locator, ...summary } = entry
            return { ...summary, content }
          },
        }
        sctx.skills.registerProvider(() => provider)
      } catch { /* skill already registered or optional */ }
    })

    // Register local Web Observatory endpoints on the DSH Web Server (http://127.0.0.1:3080/api/dsh-archive/observatory)
    ctx.inject(['webServer'], (wctx) => {
      try {
        wctx.webServer.register({
          kind: 'exact',
          path: '/api/dsh-archive/observatory',
          handler: async (req, res) => {
            if (req.method !== 'GET') { res.writeHead(405); res.end(); return }
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' })
            res.end(renderObservatoryHtml())
          },
        })
        wctx.webServer.register({
          kind: 'exact',
          path: '/api/dsh-archive/telemetry',
          handler: async (req, res) => {
            if (req.method !== 'GET') { res.writeHead(405); res.end(); return }
            const payload = await buildTelemetryPayload(runBridge)
            res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' })
            res.end(JSON.stringify(payload))
          },
        })
        wctx.webServer.register({
          kind: 'exact',
          path: '/api/dsh-archive/run',
          handler: async (req, res) => {
            if (req.method !== 'POST') { res.writeHead(405); res.end(); return }
            let raw = ''
            req.on('data', (c) => { raw += c.toString('utf8') })
            req.on('end', async () => {
              try {
                const body = JSON.parse(raw || '{}')
                const out = await executeObservatoryRun(runBridge, body)
                res.writeHead(200, { 'Content-Type': 'application/json' })
                res.end(JSON.stringify({ ok: true, result: out }))
              } catch (err) {
                res.writeHead(500, { 'Content-Type': 'application/json' })
                res.end(JSON.stringify({ ok: false, error: String((err && err.message) || err) }))
              }
            })
          },
        })
        wctx.webServer.register({
          kind: 'exact',
          path: '/api/dsh-archive/cache',
          handler: async (req, res) => {
            if (req.method !== 'POST') { res.writeHead(405); res.end(); return }
            let raw = ''
            req.on('data', (c) => { raw += c.toString('utf8') })
            req.on('end', async () => {
              try {
                const body = JSON.parse(raw || '{}')
                const out = await runBridge('cache_manage', body, 60000, { skipTelemetry: true })
                res.writeHead(200, { 'Content-Type': 'application/json' })
                res.end(JSON.stringify({ ok: true, result: out }))
              } catch (err) {
                res.writeHead(500, { 'Content-Type': 'application/json' })
                res.end(JSON.stringify({ ok: false, error: String((err && err.message) || err) }))
              }
            })
          },
        })
      } catch { /* routes already registered or webServer unavailable */ }
    })
  }
}
