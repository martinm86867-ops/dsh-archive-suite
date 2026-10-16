/**
 * Per-source stance classification for `jina_evidence_map` (pure data, zero deps).
 *
 * WHY THIS MODULE EXISTS
 * ----------------------
 * The previous implementation derived a verdict by measuring word overlap
 * between the claim and search snippets and calling that "grounding". That
 * proxy inverts specificity: a claim full of dates, figures and proper nouns
 * shares few *tokens* with any snippet, so the best-documented claims scored
 * lowest, while vague or definitional statements scored highest. It also
 * triggered a FALSE verdict on the mere *presence* of a negation word
 * ("false", "myth", "debunk") anywhere in a snippet — without checking what
 * was being negated — which is why uncontroversial and contested claims alike
 * came back FALSE.
 *
 * This module replaced that with a scoring contract. It has since gone further:
 * it no longer renders a verdict at all, because no snippet scan can establish
 * truth. It reports what each source DOES with the claim — state / report /
 * deny / mention — and lets the reader judge. What remains true of scoring:
 *
 *   1. Relevance gate  — a source only counts if it is *about* the claim.
 *   2. Refutation      — FALSE requires an explicit refutation pattern found
 *                        in a snippet. Absence of corroboration is NOT
 *                        refutation.
 *   3. Abstention      — no sufficiently relevant evidence yields UNKNOWN,
 *                        never a fabricated verdict. Abstention is the default;
 *                        TRUE and FALSE must be earned.
 *   4. No bare constants — confidence is computed from term coverage, so the
 *                        score is reproducible from the evidence.
 *
 * LIMITATIONS (stated, not hidden)
 * --------------------------------
 * This is still extraction over search snippets, not the Grounding API. It is
 * deliberately conservative: it will abstain more often than a true grounding
 * call, and that is the point — an honest UNKNOWN beats a fabricated verdict.
 * The durable fix is to route to `g.jina.ai`; this module defines the
 * evaluation contract that such a client must satisfy.
 */

/** Minimum share of the claim's remaining (non-stopword) terms that a source
 *  must contain before it is treated as *about* the claim at all. */
export const RELEVANCE_FLOOR = 0.2

/** Minimum share of the claim's remaining terms across all evidence before the
 *  claim is considered corroborated. High on purpose: SUPPORTED is a strong
 *  claim and must be earned, while UNKNOWN stays cheap. */
export const CORROBORATION_MIN = 0.85

/** Negation words are ignored: every claim is tokenised after removing them, so
 *  they can never drive term coverage the way they used to. */
export const NEGATIONS = new Set([
  'not', 'no', 'never', 'none', 'cannot', "can't", "don't", "doesn't", "isn't",
  "aren't", "wasn't", "weren't", "nor", 'without', 'nobody', 'nothing', 'neither',
])

export const STOPWORDS = new Set([
  'this', 'that', 'these', 'those', 'with', 'from', 'have', 'has', 'had', 'were',
  'was', 'are', 'what', 'when', 'where', 'which', 'their', 'there', 'about', 'would',
  'could', 'should', 'then', 'than', 'them', 'they', 'into', 'over', 'such', 'been',
  'being', 'also', 'more', 'most', 'some', 'only', 'very', 'just', 'like', 'made',
  // short function words — structural noise for coverage purposes
  'the', 'and', 'for', 'its', 'his', 'her', 'our', 'your', 'per', 'via', 'not',
])

/**
 * Refutation detection is two-tiered, because bare falsity words are
 * unreliable evidence of refutation: "the claim that TPM 2.0 is optional is
 * false" contains "is false" while *confirming* an unrelated claim.
 *
 *  - STRONG patterns are unambiguous on their own.
 *  - NEGATING patterns only count when the clause they negate actually refers
 *    to the claim under test (see findRefutation's content check).
 */
export const STRONG_REFUTATION_PATTERNS = [
  'debunked', 'has been debunked', 'disproven', 'disproved', 'discredited',
  'demonstrably false', 'patently false',
]

export const NEGATING_REFUTATION_PATTERNS = [
  'is false', 'are false', 'was false', 'were false',
  'is not true', 'are not true', 'was not true', 'were not true',
  'is untrue', 'are untrue',
  'false claim', 'false allegation', 'false assertion', 'false statement',
  'is a myth', 'are a myth', 'myth that', 'is a misconception',
  'is a hoax', 'falsehood', 'proven false', 'proved false',
  'not genocide', 'no genocide', 'not a genocide', 'was not genocide', 'not a famine',
]

// Kept as the exported union for callers that only want pattern inspection.
export const REFUTATION_PATTERNS = [...STRONG_REFUTATION_PATTERNS, ...NEGATING_REFUTATION_PATTERNS]

/** A FRESH regex per call: a shared /g literal carries lastIndex between
 *  .split() calls and silently drops words depending on call order. */
const everySeparator = () => /[^a-z0-9]+/g

/** Tokenise into the meaningful terms used for coverage. */
export function termsOf(text) {
  return String(text)
    .toLowerCase()
    .split(everySeparator())
    .filter((t) => t.length >= 4 && !STOPWORDS.has(t) && !NEGATIONS.has(t))
}

/** Minimum characters that must coincide before two terms count as the same
 *  lexical item. Catches morphological variants (inaugurated / inauguration,
 *  president / presidency) without letting short words fuzzy-match. */
const PREFIX_MATCH_MIN = 6

function wordsOf(text) {
  return String(text).toLowerCase().split(everySeparator()).filter((w) => w.length > 0)
}

/** Does any source word share a long enough prefix with `term`? */
function termPresent(term, sourceWords) {
  for (const w of sourceWords) {
    if (w === term) return true
    const n = Math.min(term.length, w.length)
    if (n >= PREFIX_MATCH_MIN && term.slice(0, n) === w.slice(0, n)) return true
  }
  return false
}

/** Share of a claim's distinct terms present in `haystackText`. */
export function coverageOf(claimTerms, haystackText) {
  const distinct = [...new Set(claimTerms)]
  if (distinct.length === 0) return 0
  const sourceWords = wordsOf(haystackText)
  if (sourceWords.length === 0) return 0
  const hits = distinct.filter((t) => termPresent(t, sourceWords)).length
  return hits / distinct.length
}

/**
 * Is the content a negation targets actually *this* claim?
 *
 * Scored against the claim, not against the clause: the clause may be padded
 * with arbitrary prose ("the claim that ..., according to the review"), and
 * measuring the clause's own words lets that padding dilute the signal. What
 * matters is how much of the claim is present.
 */
function negationIsAboutClaim(content, claimTerms) {
  if (claimTerms.length === 0) return false
  const distinct = [...new Set(claimTerms)]
  const words = new Set(String(content).toLowerCase().split(everySeparator()).filter((w) => w.length >= 2))
  if (words.size === 0) return false
  const hits = distinct.filter((t) => words.has(t)).length
  // Short claims ("water boils 50c") cannot supply more than one or two terms,
  // so require one shared term; longer claims must land most of themselves.
  const required = distinct.length <= 2 ? 1 : Math.min(2, Math.ceil(distinct.length * 0.6))
  return hits >= required
}

/**
 * Find a refutation of `claimText` in `text`.
 *
 * Strong patterns are accepted outright. Negating patterns must additionally
 * negate a clause that shares content with the claim — otherwise the snippet is
 * refuting some other proposition and says nothing about this claim.
 */
export function findRefutation(text, claimText = '') {
  const hay = normalizedForMatch(text)
  if (!hay) return null
  const claimTerms = termsOf(claimText)

  // EVERY pattern — including the strong list — must pass the content test.
  // "no evidence" and "conspiracy theory" were previously accepted outright, so
  // an unrelated article containing the phrase refuted any claim whose words it
  // happened to share. A deictic marker alone says nothing about what is being
  // denied.
  const patterns = [...STRONG_REFUTATION_PATTERNS, ...NEGATING_REFUTATION_PATTERNS]
  for (const p of patterns) {
    let at = hay.indexOf(p)
    while (at !== -1) {
      // The negated content sits BEFORE the falsity marker ("the claim that
      // X is false"), so bound the clause to the marker's own sentence and
      // read only up to the marker itself.
      const from = Math.max(0, at - 160)
      const before = hay.slice(from, at)
      let cut = -1
      for (let i = before.length - 1; i >= 0; i -= 1) {
        if (before[i] === '.' || before[i] === ';') { cut = i; break }
      }
      const clause = (cut >= 0 ? before.slice(cut + 1) : before).trim()
      if (negationIsAboutClaim(clause, claimTerms)) return p
      at = hay.indexOf(p, at + 1)
    }
  }
  return null
}

/**
 * Lowercase and strip quotation marks so a refutation survives the way it is
 * actually typeset: titles like `Are Not "Genocide"` use typographic quotes
 * (" " ' '), which would otherwise hide the phrase from pattern matching.
 */
function normalizedForMatch(text) {
  return String(text).toLowerCase().replace(/[\u2018\u2019\u201c\u201d"'`\u2032\u2033]/g, '')
}

/**
 * Attribution cues. A source that says "a commission found X" is reporting
 * someone else's assertion, not stating X; that is materially weaker evidence
 * than a source asserting X directly, and it must not be enough to settle a
 * contested claim.
 */
const ATTRIBUTION_CUES = [
  'says', 'said', 'according to', 'reported', 'claims', 'claimed', 'alleged',
  'alleges', 'allegation', 'accused', 'accuses', 'argues', 'argued', 'asserts',
  'asserted', 'contends', 'contended', 'found that', 'concluded that', 'ruled that',
  'denies', 'denied', 'disputes', 'disputed', 'report says',
]

/** Is this snippet reporting third-party attribution rather than stating a fact? */
function isAttributedReporting(text) {
  const hay = String(text).toLowerCase()
  return ATTRIBUTION_CUES.some((c) => hay.includes(c))
}

/**
 * Numeric values asserted by the claim (years, quantities, measurements).
 * Term overlap cannot distinguish "water boils at 50C" from a page that says
 * 100C — both contain the same words. A claim whose figures never appear in
 * the evidence is not corroborated, however well its words overlap.
 */
export function numericValuesOf(text) {
  const matches = String(text).match(/\d[\d,]*(?:\.\d+)?/g) || []
  return [...new Set(matches.map((n) => n.replace(/,/g, '')))]
}

/** Does every quantity in the claim appear somewhere in the evidence? */
function allFiguresPresent(claimNumbers, haystackText) {
  if (claimNumbers.length === 0) return true
  const hay = String(haystackText).replace(/,/g, '')
  const words = new Set(hay.toLowerCase().split(everySeparator()).filter((w) => w.length > 0))
  return claimNumbers.every((n) => words.has(n) || hay.includes(n))
}

/**
 * Normalise any of the shapes the SEARCH gateway returns into flat records:
 * a bare array, `{ results: [...] }`, or the Jina envelope
 * `{ code, status, data: { results: [...] } }`.
 */
export function normalizeResults(data) {
  if (Array.isArray(data)) return data.map(toRecord)
  if (!data || typeof data !== 'object') return []
  if (Array.isArray(data.results)) return data.results.map(toRecord)
  if (data.data && Array.isArray(data.data.results)) return data.data.results.map(toRecord)
  if (Array.isArray(data.data)) return data.data.map(toRecord)
  return []
}

function toRecord(r) {
  return {
    title: String((r && r.title) || ''),
    url: String((r && (r.url || r.link)) || ''),
    snippet: String((r && (r.snippet || r.description || r.content)) || ''),
  }
}

/**
 * Count how specific a claim is: digits, years, dates, quantities and
 * capitalised proper nouns. Used as the regression diagnostic for the old
 * specificity inversion — a good extractor covers these terms *better*, not
 * worse, so this must correlate positively with coverage.
 */
export function claimSpecificity(statement) {
  const text = String(statement)
  const numbers = (text.match(/\d+(?:[.,]\d+)*/g) || []).length
  const units = (text.match(/\b(?:percent|per cent|km|kg|mi|mb|gb|tb|hours?|days?|years?|degrees?|celsius|fahrenheit|january|february|march|april|may|june|july|august|september|october|november|december)\b/gi) || []).length
  const proper = (text.match(/(?<!^)(?<![.!?]\s)\b[A-Z][a-zA-Z]{2,}\b/g) || []).length
  const dates = (text.match(/\b\d{1,2}\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}\b/gi) || []).length
  return numbers + units + proper + dates * 2
}

/**
 * Derive a verdict from retrieved search sources.
 *
 * @param {string} statement the claim under test
 * @param {Array}  rawResults SEARCH results (normalised internally)
 * @returns {{verdict:'SUPPORTED'|'REFUTED'|'MIXED'|'UNKNOWN', confidence:number,
 *            reasoning:string, evidence:Array, coverage:number, refutation:string|null}}
 */
export function evaluateGroundedResults(statement, rawResults) {
  const claim = String(statement || '').trim()
  const claimTerms = termsOf(claim)
  const claimNumbers = numericValuesOf(claim)
  const results = normalizeResults(rawResults)

  const evidence = []
  for (let i = 0; i < results.length; i += 1) {
    const r = results[i]
    const text = r.snippet || r.title
    // Coverage is measured PER SOURCE. Summing vocabulary across sources was
    // the central defect: five unrelated pages that each mention a word or two
    // collectively "covered" the claim and produced SUPPORTED at 95%.
    const coverage = coverageOf(claimTerms, text)
    const refutation = findRefutation(r.snippet, claim) || findRefutation(r.title, claim)
    const attributed = refutation === null && isAttributedReporting(text)
    // A source carries the claim only if IT ALONE contains the claim's content.
    const carries = refutation === null && coverage >= CORROBORATION_MIN
    const relevant = refutation !== null || coverage >= RELEVANCE_FLOOR
    if (!relevant) continue
    evidence.push({
      index: i + 1,
      title: r.title,
      url: r.url,
      snippet: r.snippet,
      coverage: Number(coverage.toFixed(4)),
      refutation,
      attributed,
      // Four stances, because collapsing them is what made the tool misleading:
      //  denies   explicitly denies the claim
      //  states   carries the claim's content directly
      //  reports  carries it as someone's assertion ("a commission concluded")
      //  mentions shares vocabulary only — NOT support
      stance: refutation !== null ? 'denies'
        : (carries ? (attributed ? 'reports' : 'states') : 'mentions'),
    })
  }

  const denies = evidence.filter((e) => e.stance === 'denies')
  const statesList = evidence.filter((e) => e.stance === 'states')
  const reports = evidence.filter((e) => e.stance === 'reports')
  const mentions = evidence.filter((e) => e.stance === 'mentions')
  const carrying = statesList.length + reports.length

  // Reported as what it is: how much of the claim's vocabulary appears anywhere
  // in the retrieved text. This is NOT a probability that the claim is true,
  // and it is no longer printed as one.
  const termCoverage = Number(
    coverageOf(claimTerms, results.map((r) => r.snippet || r.title).join(' ')).toFixed(4),
  )
  const counts = {
    denies: denies.length, states: statesList.length, reports: reports.length,
    mentions: mentions.length, retrieved: results.length,
  }

  // No verdict. A snippet scan cannot establish whether a claim is true, and
  // four verdict words over a lexical score implied an authority this cannot
  // have. What it CAN do is report, per source, what that source does with the
  // claim — and let the reader judge.
  const classify = (stance) => evidence.filter((e) => e.stance === stance)
  const summary = [
    `${counts.states} source(s) state the claim`,
    `${counts.reports} report it as someone's assertion`,
    `${counts.denies} deny it`,
    `${counts.mentions} mention the subject only`,
    `of ${counts.retrieved} retrieved`,
  ].join(' · ')

  let caution = null
  // Figures the claim asserts that appear in none of the sources carrying it.
  // The old design swallowed this into an UNKNOWN verdict; reporting it keeps
  // the finding — the wording matches but the quantity does not — visible.
  const carrierText = [...statesList, ...reports].map((e) => e.snippet || e.title).join(' ')
  const figuresMissing = (claimNumbers.length > 0 && [...statesList, ...reports].length > 0)
    ? claimNumbers.filter((n) => !allFiguresPresent([n], carrierText))
    : []

  if (figuresMissing.length > 0) {
    caution = `The claim's figure(s) ${figuresMissing.join(', ')} appear in none of the sources that `
      + 'carry it: the wording matches, the quantity does not.'
  } else if (counts.mentions > 0 && counts.states === 0 && counts.denies === 0 && counts.reports === 0) {
    caution = 'Every relevant source shares vocabulary with the claim without asserting it. '
      + 'Topical overlap is not corroboration.'
  } else if (counts.denies > 0 && (counts.states > 0 || counts.reports > 0)) {
    caution = 'Retrieved sources disagree with one another; this evidence does not settle the claim.'
  } else if (counts.states === 0 && counts.reports > 0) {
    caution = 'The claim appears only as someone else\'s assertion, which is evidence about who '
      + 'asserts what, not that the assertion holds.'
  } else if (counts.states > 0 && counts.denies === 0) {
    caution = 'Sources carrying the claim\'s wording is not verification that the claim is true. '
      + 'Confirm against the primary record before relying on it.'
  }

  return {
    claim,
    evidence,
    termCoverage,
    counts,
    figuresMissing,
    denials: classify('denies'),
    statements: classify('states'),
    reports: classify('reports'),
    mentionsOnly: classify('mentions'),
    summary,
    caution,
    refutation: denies.length ? denies[0].refutation : null,
  }
}

/** Render the model-facing evidence map. Deliberately contains no verdict. */
export function formatGroundedVerdict(v, statement) {
  const c = v.counts || {}
  const lines = []
  lines.push('Evidence map — what the retrieved sources do with this claim:')
  lines.push('  ' + v.summary)
  // Term coverage is stated with its meaning. It was previously rendered
  // "Confidence: 95.0%", which turned a lexical overlap ratio into an apparent
  // probability that the claim was true.
  lines.push(`  term coverage: ${v.termCoverage} — share of the claim's vocabulary appearing in the`)
  lines.push('  retrieved text. Not a probability and not a verification.')
  if (v.caution) lines.push('  ' + v.caution)
  lines.push('')
  lines.push('Statement: ' + statement)
  const groups = [
    ['DENY', v.denials], ['STATE', v.statements],
    ['REPORT', v.reports], ['MENTION ONLY', v.mentionsOnly],
  ]
  for (const [label, group] of groups) {
    for (const e of (group || []).slice(0, 3)) {
      lines.push(`  [${e.index}] ${label} — ${e.title || 'Source'} (${e.url || ''})`)
      if (e.snippet) lines.push(`      Quote: "${e.snippet}"`)
    }
  }
  if (!v.evidence || v.evidence.length === 0) {
    lines.push('  No retrieved source was sufficiently about the claim to classify.')
  }
  return lines.join('\n')
}
