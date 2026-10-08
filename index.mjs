/**
 * Compatibility shim.
 *
 * This file was an earlier draft of the plugin (9 tools, published under the
 * old name `dsh-archive-org`). It is not the entry point — package.json points
 * at index.js — but it shipped in the package and would have gone stale
 * silently. Re-exporting the real module means a consumer that still imports
 * index.mjs gets the current tool set instead of a 1.0-era subset.
 */
export * from './index.js'
