/**
 * Test suite for @martinm86867-ops/dsh-archive-suite
 *
 * Verifies:
 *   1. All 43 tools register with valid JSON Schema parameters.
 *   2. Python interpreter and bridge resolution never return bare non-executable strings.
 *   3. `archive_fact_check` executes in BOTH standalone mode (without Jina) and
 *      hybrid mode (with Jina), formatting primary records, stance counts,
 *      numeric discrepancy cautions, and local DuckDB warehouse evidence cards.
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  apply,
  resolvePython,
  archiveAvailable,
  formatUnifiedFactCheck,
  parseBundledSkill,
  ARCHIVIST_SKILL_NAME,
  ARCHIVIST_SKILL_PATH,
  runBridge,
} from '../index.js'
import { readFileSync } from 'node:fs'

test('dsh-archive-suite registers all 44 tools with valid JSON Schema objects', () => {
  const registered = new Map()
  apply({
    tools: {
      register(def) {
        registered.set(def.name, def)
      },
    },
  })

  assert.equal(registered.size, 44, `Expected 44 tools, got ${registered.size}`)
  for (const requiredTool of [
    'archive_epistemic_dossier',
    'archive_cache_manage',
    'archive_fact_check',
    'archive_forensic_compare',
    'archive_claim_trace',
    'archive_quote_from_index',
    'wayback_read',
    'archive_iiif_image',
    'archive_lending_borrow',
    'archive_item_manage',
    'archive_research_timeline',
  ]) {
    assert.ok(registered.has(requiredTool), `${requiredTool} must be registered`)
  }

  for (const [name, def] of registered) {
    assert.equal(def.parameters.type, 'object', `${name} parameters must be type: "object"`)
    assert.equal(typeof def.execute, 'function', `${name} must have an execute function`)
  }
})

test('resolvePython and archiveAvailable resolve to a real executable interpreter', () => {
  const py = resolvePython()
  assert.ok(py.includes('/') || py.includes('\\'), `Expected absolute path, got ${py}`)
  assert.equal(archiveAvailable(), true, 'Archivist backend must be available')
})

test('formatUnifiedFactCheck renders standalone Archive-Only mode (without Jina)', () => {
  const mockArchive = {
    ok: true,
    value: {
      claim: '"second order cybernetics"',
      recorded: {
        documents: 1388,
        languages: [{ key: 'English', documents: 1200 }],
      },
      primary_record: {
        documents: 33,
        collections: [{ key: 'dticarchive', documents: 18 }, { key: 'government-documents', documents: 15 }],
      },
      year_span: { earliest: 1996, latest: 2026 },
      stance_counts: { states: 3, reports: 1, denies: 0, mentions: 0, sampled: 4 },
      figures_missing: [],
      denial_phrasing: [],
      evidence: [
        {
          identifier: 'DTIC_ADA612230',
          title: 'Adaptation, Learning, and the Art of War',
          page: 65,
          date: '2018-09-27',
          collections: ['dticarchive', 'usgovernmentmirrors'],
          url: 'https://archive.org/details/DTIC_ADA612230/page/n65',
          quote: 'Cybernetics and Second Order Cybernetics',
          sha1: '922b39c33877',
          provenance: { tier: 'T1', basis: 'primary-source collection: dticarchive' },
          stance: 'states',
        },
      ],
      local_warehouse: [
        {
          id: 'card_observingsystems0000vonf_p2',
          identifier: 'observingsystems0000vonf',
          title: 'Observing Systems (Heinz von Foerster)',
          source_type: 'restricted_fts_quote',
          url: 'https://archive.org/details/observingsystems0000vonf',
          digest: '234a3d9e50cdf2d9',
          quote: 'Second order cybernetics: the cybernetics of observing systems',
        },
      ],
    },
  }

  const out = formatUnifiedFactCheck(
    '"second order cybernetics"',
    mockArchive,
    { mode: 'archive_standalone' }
  )

  assert.match(out, /1388 documents carry this phrasing/)
  assert.match(out, /Primary Record \(T1 government\/military\/declassified\): 33 documents/)
  assert.match(out, /3 state · 1 report · 0 deny/)
  assert.match(out, /DTIC_ADA612230/)
  assert.match(out, /card_observingsystems0000vonf_p2/)
  assert.match(out, /Operating in standalone Archive-First mode/)
})

test('formatUnifiedFactCheck renders hybrid Archive + Jina Live Web mode and flags missing figures', () => {
  const mockArchive = {
    ok: true,
    value: {
      claim: 'Water boils at 50 degrees Celsius at sea level',
      recorded: { documents: 12, languages: [{ key: 'English' }] },
      primary_record: { documents: 2, collections: [{ key: 'usgovernmentdocuments' }] },
      year_span: { earliest: 1950, latest: 2020 },
      stance_counts: { states: 1, reports: 0, denies: 1, mentions: 0, sampled: 2 },
      figures_missing: ['50'],
      denial_phrasing: [{ identifier: 'chem101', denial_phrasing: 'is false' }],
      evidence: [],
      local_warehouse: [],
    },
  }

  const mockLive = {
    mode: 'jina',
    map: {
      summary: '0 source(s) state the claim · 0 report it · 1 deny it · 0 mention only · of 1 retrieved',
      termCoverage: 0.85,
      caution: "The claim's figure(s) 50 appear in none of the sources that carry it.",
      figuresMissing: ['50'],
      evidence: [
        {
          index: 1,
          stance: 'denies',
          title: 'Boiling point of water',
          url: 'https://en.wikipedia.org/wiki/Boiling_point',
          snippet: 'The claim that water boils at 50 degrees Celsius is false; it boils at 100 degrees Celsius.',
        },
      ],
    },
  }

  const out = formatUnifiedFactCheck('Water boils at 50 degrees Celsius at sea level', mockArchive, mockLive)
  assert.match(out, /Caution \(Numeric Discrepancy\): figure\(s\) 50/)
  assert.match(out, /\[1\] DENIES — Boiling point of water/)
})

test('runBridge executes offline corpus_stats and keys_status via the bundled Python venv', async () => {
  const stats = await runBridge('corpus_stats', {})
  assert.equal(typeof stats.documents, 'number')
  assert.ok(stats.documents >= 1)

  const keys = await runBridge('keys_status', {})
  assert.equal(typeof keys.total_keys, 'number')
  assert.ok(keys.total_keys >= 1)
})

test('formatUnifiedFactCheck renders Multi-Axis Triangulation and Canonical Archive Texts', () => {
  const mockArchive = {
    ok: true,
    value: {
      claim: 'Does "The Protocols of the Elders of Zion" hold any merit?',
      subject_core: '"The Protocols of the Elders of Zion"',
      query: '"The Protocols of the Elders of Zion"',
      triangulation: {
        subject_core: '"The Protocols of the Elders of Zion"',
        caller_framing_stripped: true,
        corroboration_axis: {
          documents: 812,
          samples: [{ identifier: 'doc_a', title: 'Sample A', tier: 'T2', url: 'https://archive.org/details/doc_a', quote: 'corroboration quote' }],
        },
        critique_axis: {
          documents: 1640,
          samples: [{ identifier: 'doc_b', title: 'Sample B', tier: 'T1', url: 'https://archive.org/details/doc_b', quote: 'critique quote' }],
        },
        substance_axis: {
          documents: 2150,
          samples: [{ identifier: 'doc_c', title: 'Sample C', tier: 'T2', url: 'https://archive.org/details/doc_c', quote: 'substance quote' }],
        },
        canonical_editions: [
          { identifier: 'protocols_edition_1', downloads: 12000 },
        ],
      },
      recorded: { documents: 5400, languages: [{ key: 'English' }] },
      primary_record: { documents: 42, collections: [{ key: 'usgovernmentdocuments' }] },
      year_span: { earliest: 1920, latest: 2025 },
      stance_counts: { states: 4, reports: 0, denies: 0, mentions: 0, sampled: 4 },
      figures_missing: [],
      denial_phrasing: [],
      evidence: [],
      local_warehouse: [],
    },
  }

  const out = formatUnifiedFactCheck(
    'Does "The Protocols of the Elders of Zion" hold any merit?',
    mockArchive,
    { mode: 'archive_standalone' }
  )

  assert.match(out, /Neutral Subject Core \(framing bias stripped\): "The Protocols of the Elders of Zion"/)
  assert.match(out, /Multi-Axis Triangulation \(Anti-Confirmation-Bias Sweep\):/)
  assert.match(out, /Corroboration \/ Authenticity Axis: 812 documents/)
  assert.match(out, /Critique \/ Forgery \/ Refutation Axis: 1640 documents/)
  assert.match(out, /Substance & Historical Impact Axis: 2150 documents/)
  assert.match(out, /Canonical Archive Texts for Direct Substance Inspection.*protocols_edition_1/)
})

test('archivist-research-agent skill is packaged with valid frontmatter and registered via skills service', async () => {
  const raw = readFileSync(ARCHIVIST_SKILL_PATH, 'utf8')
  const parsed = parseBundledSkill(raw)
  assert.ok(parsed.description.length > 20 && parsed.description.length <= 500)
  assert.match(parsed.content, /archive_epistemic_dossier/)
  assert.match(parsed.content, /Separate Provenance from Substance/)

  let registeredProvider = null
  apply({
    tools: { register() {} },
    inject(deps, fn) {
      if (deps.includes('skills')) {
        fn({
          skills: {
            registerProvider(factory) {
              registeredProvider = factory()
            },
          },
        })
      }
    },
  })

  assert.ok(registeredProvider, 'Skill provider must be registered when skills service is injected')
  const list = await registeredProvider.list()
  assert.equal(list.length, 1)
  assert.equal(list[0].name, ARCHIVIST_SKILL_NAME)
  const loaded = await registeredProvider.get(list[0])
  assert.match(loaded.content, /archive_epistemic_dossier/)
})

test('observatory telemetry records tool runs, extracts sources, and mounts webServer routes', async () => {
  const { recordTelemetryRun, readTelemetryRuns, renderObservatoryHtml } = await import('../observatory.js')
  const recorded = recordTelemetryRun({
    command: 'epistemic_dossier',
    args: { statement: 'Does "Report from Iron Mountain" hold any merit?' },
    durationMs: 1420,
    res: {
      trace: {
        recorded: { documents: 2116 },
        primary_record: { documents: 42, examples: [{ identifier: 'cia-rdp88-01350r000200370012-1', title: 'CIA Iron Mountain File', tier: 'T1', url: 'https://archive.org/details/cia-rdp88' }] },
        triangulation: {
          subject_core: '"Report from Iron Mountain"',
          caller_framing_stripped: true,
          corroboration_axis: { documents: 1170 },
          critique_axis: { documents: 465 },
          substance_axis: { documents: 1819 },
        },
        evidence: [
          {
            identifier: 'peacewarindustry0000unse_r1z0',
            title: 'Peace and the War Industry',
            page: 184,
            page_scope: 'item_aggregate',
            provenance: { tier: 'T2' },
            stance: 'states',
            url: 'https://archive.org/details/peacewarindustry0000unse_r1z0',
            quote: 'Report from Iron Mountain remains a model of policy analysis.',
          },
        ],
      },
      forensic_report: {
        identifier_a: 'pdfy-A5uQx1ByqfwWuHma',
        identifier_b: 'lewin-l.-c.-report-from-iron-mountain-and-nyt-article-1967',
        exact_8gram_shingles: { observed: 13858, shuffled_null: 0, excess_over_null: 13858 },
        conceptual_window_alignments: [{ shared_distinctive_terms: 207, jaccard: 0.4652, terms: ['polemology', 'bouthoul', 'disarmament'] }],
      },
    },
  })

  assert.ok(recorded && recorded.id, 'Telemetry run must return a record id')
  assert.equal(recorded.triangulation.total_circulation, 2116)
  assert.equal(recorded.forensics.excess_8grams, 13858)
  assert.ok(recorded.sources.length >= 2)

  const recent = readTelemetryRuns(5)
  assert.ok(recent.some((r) => r.id === recorded.id))

  const html = renderObservatoryHtml()
  assert.match(html, /Archivist Epistemic & Forensic Observatory/)

  const routes = []
  apply({
    tools: { register() {} },
    inject(deps, fn) {
      if (deps.includes('webServer')) {
        fn({
          webServer: {
            register(route) { routes.push(route.path) },
          },
        })
      }
    },
  })
  assert.deepEqual(routes, [
    '/api/dsh-archive/observatory',
    '/api/dsh-archive/telemetry',
    '/api/dsh-archive/run',
    '/api/dsh-archive/cache',
  ])

  const cacheStats = await runBridge('cache_manage', { operation: 'stats' }, 15000, { skipTelemetry: true })
  assert.equal(typeof cacheStats.texts_cached, 'number')
  assert.equal(typeof cacheStats.metadata_cached, 'number')
  assert.equal(typeof cacheStats.artifacts_cached, 'number')
})
