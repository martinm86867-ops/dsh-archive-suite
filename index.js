/**
 * @martinm86867-ops/dsh-archive-org — DeepSeek Harness Plugin for Archive.org & Wayback Machine
 *
 * Provides native tools for:
 *   - Archive.org catalog search & metadata inspection
 *   - IAS3 high-throughput file uploads & item creation
 *   - Wayback Machine CDX querying, SPN2 archival, and temporal snapshot diffing
 *   - Asynchronous declarative pipeline execution with SQLite state checkpoints
 *   - Sub-millisecond DuckDB research warehouse retrieval & LLM citation prompt synthesis
 */

import { spawn } from 'node:child_process'
import { resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { existsSync } from 'node:fs'

const __dirname = dirname(fileURLToPath(import.meta.url))
// Overridable so a rebuilt venv or a different checkout does not break every
// tool with no diagnostic.
const PYTHON_PATH = process.env.DSH_ARCHIVE_PYTHON
  || ['/home/martin/archive/.venv/bin/python', 'python3'].find(existsSync)
  || 'python3'
const BRIDGE_PATH = resolve(__dirname, 'bridge.py')
const BRIDGE_TIMEOUT_MS = Number(process.env.DSH_ARCHIVE_TIMEOUT_MS || 180000)

export const name = '@martinm86867-ops/dsh-archive-suite'
export const inject = ['tools']

const OUT = {
  schema: { type: 'string' },
  render(_args, value) { return [{ type: 'text', text: String(value) }] },
}

/** Run Python bridge command with JSON stdin/stdout. */
function runBridge(command, args = {}) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn(PYTHON_PATH, [BRIDGE_PATH, command], {
      stdio: ['pipe', 'pipe', 'pipe']
    })

    let stdout = ''
    let stderr = ''

    child.stdout.on('data', (d) => { stdout += d.toString('utf8') })
    child.stderr.on('data', (d) => { stderr += d.toString('utf8') })

    const timer = setTimeout(() => {
      child.kill('SIGKILL')
      rejectPromise(new Error(`Bridge timed out after ${BRIDGE_TIMEOUT_MS} ms: ${command}`))
    }, BRIDGE_TIMEOUT_MS)

    child.on('close', (code) => {
      clearTimeout(timer)
      if (code !== 0) {
        rejectPromise(new Error(`Bridge failed (exit ${code}): ${stderr || stdout}`))
        return
      }
      try {
        const parsed = JSON.parse(stdout)
        resolvePromise(parsed)
      } catch (err) {
        resolvePromise({ raw: stdout, stderr })
      }
    })

    child.stdin.write(JSON.stringify(args))
    child.stdin.end()
  })
}

export function apply(ctx) {
  // 1. archive_search
  ctx.tools.register({
    name: 'archive_search',
    description: 'Search the Archive.org catalog with Lucene queries, collection filters, mediatypes, and row limits.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Lucene query or keywords (e.g. "collection:nasa AND mediatype:image")' },
        rows: { type: 'integer', description: 'Number of results to return (default: 10)' },
        page: { type: 'integer', description: 'Page number (default: 1)' },
        mediatype: { type: 'string', description: 'Optional media type filter (texts, audio, movies, image, software, data)' },
        collection: { type: 'string', description: 'Optional collection identifier' }
      },
      required: ['query']
    },
    async execute(args) {
      const res = await runBridge('search', args)
      if (res.error) return `Error: ${res.error}`
      const docs = res.docs || []
      const lines = docs.map(d => `- **${d.identifier}**: ${d.title || 'Untitled'} (${d.mediatype || 'unknown'}) | Date: ${d.date || 'N/A'} | Downloads: ${d.downloads || 0}`)
      return `### Archive.org Catalog Hits (${res.num_found} total found, showing ${docs.length}):\n${lines.join('\n')}`
    }
  })

  // 2. archive_item_info
  ctx.tools.register({
    name: 'archive_item_info',
    description: 'Inspect Archive.org item metadata, size, server hosting node, and file inventory.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'The exact item identifier on Archive.org' }
      },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('item_info', args)
      if (res.error) return `Error: ${res.error}`
      const sizeMb = (res.item_size / (1024 * 1024)).toFixed(2)
      const filesPreview = (res.sample_files || []).map(f => `  - \`${f.name}\` (${(f.size / 1024).toFixed(1)} KB, format: ${f.format || 'unknown'})`).join('\n')
      return `### Item: [${res.identifier}](${res.url})
- **Title**: ${res.title || 'N/A'}
- **Mediatype**: ${res.mediatype || 'N/A'}
- **Collection**: ${res.collection || 'N/A'}
- **Total Size**: ${sizeMb} MB (${res.files_count} files)
- **Files Sample**:
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
    description: 'Read the full OCR text or transcription of a book, document, or publication on Archive.org.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifier: { type: 'string', description: 'Archive.org document/book identifier' },
        max_chars: { type: 'integer', description: 'Maximum characters to read (default: 4000)' }
      },
      required: ['identifier']
    },
    async execute(args) {
      const res = await runBridge('media_read_text', args)
      if (res.error) return `Text Extraction Error: ${res.error}`
      if (!res.found) return res.message
      return `### Document OCR Text: \`${res.identifier}\` (File: \`${res.filename}\` | Read: ${res.length_read} chars)\n- **Full Text Link**: ${res.full_file_url}\n\n\`\`\`text\n${res.text}\n\`\`\``
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
    description: 'Download the compact text derivative of items into the local store, once, politely (paced to avoid rate limits; skips items already stored). After harvesting, analysis runs offline with no further requests.',
    output: OUT,
    parameters: {
      type: 'object',
      properties: {
        identifiers: { type: 'array', items: { type: 'string' }, description: 'Item identifiers to harvest' },
        pause: { type: 'number', description: 'Seconds between downloads (default: 1.0)' },
        max_docs: { type: 'integer', description: 'Stop after N downloads' }
      },
      required: ['identifiers']
    },
    async execute(args) {
      const res = await runBridge('text_harvest', args)
      if (res.error) return `Harvest error: ${res.error}`
      const done = (res.harvested || []).map(h => `  - ${h.identifier} (${h.chars} chars, ${h.format})`).join('\n')
      const failed = (res.failed || []).map(f => `  - ${f.identifier}: ${f.reason}`).join('\n')
      return `### Harvested ${(res.harvested || []).length}\n${done}\n- Already stored: ${(res.already_stored || []).length}\n${failed ? `- Failed:\n${failed}\n` : ''}- Store: ${JSON.stringify(res.store)}`
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
}
