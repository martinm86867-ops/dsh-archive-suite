"""Bridge script connecting DSH JavaScript plugin to Archivist Python Engine."""

import sys
import os
import json
import asyncio
from pathlib import Path

# Ensure archivist is in python path
WORKSPACE_ARCHIVE = Path(os.environ.get("DSH_ARCHIVE_HOME", "/home/martin/archive"))
if str(WORKSPACE_ARCHIVE) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ARCHIVE))

from archivist.config import ArchivistConfig
from archivist.client import ArchiveClient, ArchiveError
from archivist.search import SearchEngine, QueryBuilder
from archivist.wayback import WaybackEngine
from archivist.item import ItemManager
from archivist.research.warehouse import DuckDBWarehouse
from archivist.research.context import LLMContextFormatter
from archivist.pipeline.schema import PipelineConfig
from archivist.pipeline.engine import PipelineEngine


async def handle_search(args: dict) -> dict:
    qb = QueryBuilder()
    if args.get("query"):
        qb.raw(args["query"])
    if args.get("mediatype"):
        qb.mediatype(args["mediatype"])
    if args.get("collection"):
        qb.collection(args["collection"])

    query = qb.build()
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        res = await client.search(
            query=query,
            rows=int(args.get("rows", 10)),
            page=int(args.get("page", 1)),
            fields=["identifier", "title", "mediatype", "downloads", "date", "publicdate"]
        )
        return {"query": query, "num_found": res.get("numFound", 0), "docs": res.get("docs", [])}


async def handle_item_info(args: dict) -> dict:
    ident = args["identifier"]
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        meta = await client.get_metadata(ident)
        files = meta.get("files", [])
        m = meta.get("metadata", {})
        return {
            "identifier": ident,
            "title": m.get("title"),
            "mediatype": m.get("mediatype"),
            "collection": m.get("collection"),
            "server": meta.get("server"),
            "item_size": meta.get("item_size", 0),
            "files_count": len(files),
            "url": f"https://archive.org/details/{ident}",
            "sample_files": files[:10]
        }


async def handle_item_upload(args: dict) -> dict:
    ident = args["identifier"]
    file_path = Path(args["file_path"])
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        res = await client.upload_file(
            identifier=ident,
            filename=file_path.name,
            file_path=file_path
        )
        return res


async def handle_wayback_cdx(args: dict) -> dict:
    url = args["url"]
    limit = int(args.get("limit", 20))
    match_type = args.get("match_type", "prefix")
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        snapshots = await client.cdx_search(
            url=url,
            match_type=match_type,
            limit=limit,
            from_ts=args.get("from_ts"),
            to_ts=args.get("to_ts")
        )
        return {"url": url, "count": len(snapshots), "snapshots": snapshots}


async def handle_wayback_save(args: dict) -> dict:
    url = args["url"]
    wait = args.get("wait", True)
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        res = await client.save_page_now(url, capture_all=True)
        job_id = res.get("job_id")
        if wait and job_id:
            status = await client.wait_spn_completion(job_id, timeout=float(args.get("timeout", 45.0)))
            return {**res, **status}
        return res


async def handle_wayback_diff(args: dict) -> dict:
    url = args["url"]
    ts_a = args["timestamp_a"]
    ts_b = args["timestamp_b"]
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        wb = WaybackEngine(client)
        return await wb.diff_snapshots(url, ts_a, ts_b)


async def handle_pipeline_run(args: dict) -> dict:
    config_input = args.get("config")
    if isinstance(config_input, str):
        p = Path(config_input)
        if not p.is_absolute():
            p = WORKSPACE_ARCHIVE / p
        import yaml
        content = yaml.safe_load(p.read_text(encoding="utf-8"))
        pipe_cfg = PipelineConfig.model_validate(content)
    else:
        pipe_cfg = PipelineConfig.model_validate(config_input)

    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        engine = PipelineEngine(pipe_cfg, client)
        res = await engine.run(resume_run_id=args.get("resume_id"))
        return res


async def handle_research_search(args: dict) -> dict:
    query = args["query"]
    limit = int(args.get("limit", 5))
    wh_path = Path(args.get("warehouse") or (WORKSPACE_ARCHIVE / "data" / "research_warehouse.duckdb"))
    wh = DuckDBWarehouse(wh_path)
    results = wh.search_bm25(query, limit=limit)
    stats = wh.get_stats()
    wh.close()
    return {"query": query, "count": len(results), "warehouse_stats": stats, "results": results}


async def handle_research_prompt(args: dict) -> dict:
    query = args["query"]
    limit = int(args.get("limit", 3))
    wh_path = Path(args.get("warehouse") or (WORKSPACE_ARCHIVE / "data" / "research_warehouse.duckdb"))
    wh = DuckDBWarehouse(wh_path)
    results = wh.search_bm25(query, limit=limit)
    wh.close()
    prompt = LLMContextFormatter.build_research_prompt(query, results)
    return {"query": query, "evidence_cards_count": len(results), "prompt": prompt}


async def handle_media_manifest(args: dict) -> dict:
    from archivist.media import MediaAccessEngine
    ident = args["identifier"]
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        engine = MediaAccessEngine(client)
        return await engine.get_multimedia_manifest(ident)


async def handle_media_read_text(args: dict) -> dict:
    from archivist.media import MediaAccessEngine
    ident = args["identifier"]
    max_chars = int(args.get("max_chars", 5000))
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        engine = MediaAccessEngine(client)
        return await engine.read_full_text(ident, max_chars=max_chars)


async def handle_media_book_search(args: dict) -> dict:
    """Search inside one document: service first, local scan as fallback.

    Previously called MediaAccessEngine.search_inside_book, which no longer
    exists (the analysis layer moved out of media/fulltext). The service path
    returns page numbers and word boxes; the fallback scans the local text
    derivative and reports its coverage.
    """
    from archivist.fulltext import FullTextEngine
    ident = args["identifier"]
    query = args["query"]
    limit = int(args.get("limit", 10))
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        engine = FullTextEngine(client)
        try:
            return await engine.search_inside(ident, query, limit=limit)
        except ArchiveError:
            return await engine.search_inside_local(ident, query, limit=limit)


async def handle_media_download(args: dict) -> dict:
    ident = args["identifier"]
    filename = args["filename"]
    dest_dir = Path(args.get("destination_dir", "./downloads"))
    dest_file = dest_dir / filename
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        path = await client.download_file(ident, filename, dest_file)
        return {
            "identifier": ident,
            "filename": filename,
            "saved_path": str(path.resolve()),
            "size": path.stat().st_size
        }


async def handle_quota(args: dict) -> dict:
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        user_q = await client.get_user_spn_quota()
        sys_q = await client.get_system_spn_status()
        return {"user": user_q, "system": sys_q}


async def handle_views(args: dict) -> dict:
    ident = args["identifier"]
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        return await client.get_item_views(ident)


async def handle_keys_status(args: dict) -> dict:
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        statuses = client.rotation_manager.get_pool_status()
        return {"total_keys": len(statuses), "keys": statuses}


async def handle_keys_rotate(args: dict) -> dict:
    reason = args.get("reason", "dsh_tool_rotation")
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        old_creds = client.rotation_manager.current_credentials
        new_creds = await client.rotation_manager.rotate_to_next(reason=reason)
        return {
            "previous_key": old_creds.label if old_creds else "None",
            "active_key": new_creds.label if new_creds else "None",
            "access_key": f"{new_creds.access_key[:4]}...{new_creds.access_key[-4:]}" if new_creds else "None",
            "reason": reason
        }


def _corpus_db(args: dict):
    from archivist.corpus import CorpusDB
    path = args.get("db") or (WORKSPACE_ARCHIVE / "data" / "corpus.duckdb")
    return CorpusDB(Path(path))


async def handle_search_hits(args: dict) -> dict:
    """Bulk full-text hits with snippets, identifiers, pages and dates.

    The primitive a ranking pass needs: one call returns many candidates with
    the matched text, so a candidate set can be ordered without downloading a
    single document. Harvest only the winners.
    """
    from archivist.fulltext import FullTextEngine
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        res = await FullTextEngine(client).search(
            args["query"], size=int(args.get("size", 40)), scope=args.get("scope")
        )
        hits = []
        for h in res.get("hits", []):
            from archivist.fulltext import _provenance_tier
            hits.append({
                "identifier": h.get("identifier"),
                "title": h.get("title"),
                "page": h.get("page"),
                "date": h.get("date"),
                "collections": (h.get("collections") or [])[:3],
                "url": h.get("evidence_url"),
                "sha1": h.get("sha1"),
                "provenance": _provenance_tier(h.get("collections")),
                "quote": (h["snippets"][0]["text"] if h.get("snippets") else "")[:600],
            })
        return {"query": args["query"], "total": res.get("total"), "returned": len(hits), "hits": hits}


async def handle_claim_trace(args: dict) -> dict:
    """Map a claim's documentary footprint in the archive (not a verdict)."""
    from archivist.fulltext import FullTextEngine
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        return await FullTextEngine(client).claim_trace(
            args["statement"],
            size=int(args.get("size", 10)),
            primary_only=bool(args.get("primary_only", False)),
        )


async def handle_readable_copies(args: dict) -> dict:
    """Which copies of a work can actually be read."""
    from archivist.fulltext import FullTextEngine
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        return await FullTextEngine(client).readable_copies(
            query=args.get("query"),
            identifier=args.get("identifier"),
            limit=int(args.get("limit", 15)),
            check=int(args.get("check", 40)),
        )


async def handle_reference_drift(args: dict) -> dict:
    """Live vs archived status of every reference a page cites.

    The archivist owns the archived side here; the live side is a plain status
    check, not a content fetch. Jina refuses web.archive.org, so content
    comparison cannot share a fetcher between the two sides.
    """
    from archivist.references import ReferenceDrift, ReferenceCache
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    cache = ReferenceCache(Path(cfg.data_dir) / "reference_cache.sqlite")
    async with ArchiveClient(cfg) as client:
        return await ReferenceDrift(client, cache).drift(
            args["url"],
            timestamp=args.get("timestamp"),
            max_refs=int(args.get("max_refs", 12)),
            check_live=bool(args.get("check_live", True)),
            cache_ttl_hours=float(args.get("cache_ttl_hours", 24)),
            refresh=bool(args.get("refresh", False)),
        )


async def handle_scrape(args: dict) -> dict:
    """Bulk metadata harvest, cursor-based.

    ``search`` is capped at rows/page; this is the endpoint for pulling large
    candidate sets (thousands of identifiers) in one sweep. Pass the returned
    cursor back in to continue.
    """
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        res = await client.scrape(
            args["query"], fields=args.get("fields"), cursor=args.get("cursor")
        )
        items = res.get("items", []) or []
        return {
            "query": args["query"],
            "count": len(items),
            "cursor": res.get("cursor"),
            "items": items,
        }


async def handle_fulltext_search(args: dict) -> dict:
    """Cross-corpus search over document text (not metadata)."""
    from archivist.fulltext import FullTextEngine
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        return await FullTextEngine(client).search(
            args["query"], size=int(args.get("size", 10)), scope=args.get("scope")
        )


async def handle_search_inside(args: dict) -> dict:
    """Page-anchored search inside one item, with a local fallback."""
    from archivist.fulltext import FullTextEngine
    ident = args["identifier"]
    limit = int(args.get("limit", 10))
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        engine = FullTextEngine(client)
        try:
            return await engine.search_inside(ident, args["query"], limit=limit)
        except ArchiveError:
            return await engine.search_inside_local(ident, args["query"], limit=limit)


async def handle_text_availability(args: dict) -> dict:
    """Whether an item's text can be searched, and why not."""
    from archivist.fulltext import FullTextEngine
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        return await FullTextEngine(client).availability(args["identifier"])


async def handle_text_harvest(args: dict) -> dict:
    """Download text into the local store, once, politely."""
    from archivist.fulltext import FullTextEngine
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        return await FullTextEngine(client).harvest_text(
            args["identifiers"], pause=float(args.get("pause", 1.0)),
            max_docs=args.get("max_docs"),
        )


async def handle_text_store(args: dict) -> dict:
    """What is held locally for offline analysis."""
    from archivist.fulltext import FullTextEngine
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        store = FullTextEngine(client).store
        return {"stats": store.stats(), "entries": store.entries()}


async def handle_text_reuse(args: dict) -> dict:
    """Documents sharing phrasing with a passage or document (with null model)."""
    from archivist.analysis import CorpusAnalysis
    analysis = CorpusAnalysis(_corpus_db(args))
    return analysis.reuse(
        text=args.get("text"), identifier=args.get("identifier"),
        min_tokens=int(args.get("min_tokens", 20)),
        candidates=int(args.get("candidates", 8)),
    )


async def handle_text_variants(args: dict) -> dict:
    """Sibling uploads of the same work."""
    from archivist.analysis import CorpusAnalysis
    from archivist.fulltext import FullTextEngine
    offline = bool(args.get("offline", False))
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        analysis = CorpusAnalysis(_corpus_db(args), FullTextEngine(client))
        return await analysis.variants(
            args["identifier"], confirm_tokens=int(args.get("confirm_tokens", 40)),
            offline=offline,
        )


async def handle_text_reconcile(args: dict) -> dict:
    """Compare the copies of one work and report what each adds or omits."""
    from archivist.analysis import CorpusAnalysis
    from archivist.fulltext import FullTextEngine
    offline = bool(args.get("offline", False))
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        analysis = CorpusAnalysis(_corpus_db(args), FullTextEngine(client))
        return await analysis.reconcile(
            args["identifier"], confirm_tokens=int(args.get("confirm_tokens", 40)),
            min_run=int(args.get("min_run", 25)), offline=offline,
        )


async def handle_corpus_build(args: dict) -> dict:
    """Materialise the local text store into the columnar corpus."""
    from archivist.textstore import TextStore
    store = TextStore(Path(args.get("store") or (WORKSPACE_ARCHIVE / "data" / "text_cache")))
    return _corpus_db(args).build_from_store(store, indexes=bool(args.get("indexes", False)))


async def handle_corpus_stats(args: dict) -> dict:
    return _corpus_db(args).stats()


async def handle_corpus_similar(args: dict) -> dict:
    db = _corpus_db(args)
    return {"identifier": args["identifier"],
            "results": db.similar(args["identifier"], limit=int(args.get("limit", 10)))}


async def handle_corpus_shared(args: dict) -> dict:
    db = _corpus_db(args)
    return {"a": args["a"], "b": args["b"],
            "runs": db.shared_runs(args["a"], args["b"],
                                   min_tokens=int(args.get("min_tokens", 50)),
                                   limit=int(args.get("limit", 10)))}


async def handle_corpus_diff(args: dict) -> dict:
    db = _corpus_db(args)
    return {"base": args["base"], "other": args["other"],
            "runs": db.diff_runs(args["base"], args["other"],
                                 min_run=int(args.get("min_run", 25)),
                                 limit=int(args.get("limit", 10)))}


async def handle_corpus_phrase(args: dict) -> dict:
    db = _corpus_db(args)
    return {"phrase": args["phrase"],
            "results": db.phrase_search(args["phrase"], limit=int(args.get("limit", 20)))}


async def handle_corpus_sql(args: dict) -> dict:
    """Read-only queries against the local corpus."""
    query = (args.get("query") or "").strip()
    first = query.split(None, 1)[0].lower() if query else ""
    if first not in ("select", "with", "describe", "summarize", "show"):
        return {"error": "only read-only queries are allowed (SELECT / WITH / DESCRIBE / SUMMARIZE)"}
    rows = _corpus_db(args).sql(query)
    return {"row_count": len(rows), "rows": rows[:500]}


async def handle_quote_from_index(args: dict) -> dict:
    from archivist.fulltext import FullTextEngine
    ident = args["identifier"]
    terms = args["terms"]
    delay = float(args.get("delay", 1.0))
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        engine = FullTextEngine(client)
        return await engine.quote_from_index(ident, terms, delay=delay)


async def handle_query_diagnostics(args: dict) -> dict:
    from archivist.fulltext import FullTextEngine
    query = args["query"]
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        engine = FullTextEngine(client)
        return await engine.query_diagnostics(query)


async def handle_lending_status(args: dict) -> dict:
    from archivist.fulltext import FullTextEngine
    ident = args["identifier"]
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        engine = FullTextEngine(client)
        return await engine.lending_status(ident)


async def handle_agent_ingest(args: dict) -> dict:
    from archivist.research.agent import AgentArchiveBridge
    ident = args["identifier"]
    terms = args.get("terms")
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        bridge = AgentArchiveBridge(client)
        return await bridge.ingest_work(ident, terms=terms)


async def handle_agent_query(args: dict) -> dict:
    from archivist.research.agent import AgentArchiveBridge
    query = args["query"]
    limit = int(args.get("limit", 5))
    tier = args.get("tier")
    bridge = AgentArchiveBridge()
    cards = bridge.query_agent_cards(query, limit=limit, require_tier=tier)
    return {
        "query": query,
        "card_count": len(cards),
        "cards": cards,
        "formatted_prompt": bridge.format_agent_prompt(query, cards)
    }


COMMANDS = {
    "search": handle_search,
    "scrape": handle_scrape,
    "reference_drift": handle_reference_drift,
    "readable_copies": handle_readable_copies,
    "claim_trace": handle_claim_trace,
    "search_hits": handle_search_hits,
    "item_info": handle_item_info,
    "item_upload": handle_item_upload,
    "wayback_cdx": handle_wayback_cdx,
    "wayback_save": handle_wayback_save,
    "wayback_diff": handle_wayback_diff,
    "pipeline_run": handle_pipeline_run,
    "research_search": handle_research_search,
    "research_prompt": handle_research_prompt,
    "agent_ingest": handle_agent_ingest,
    "agent_query": handle_agent_query,
    "media_manifest": handle_media_manifest,
    "media_read_text": handle_media_read_text,
    "media_book_search": handle_media_book_search,
    "media_download": handle_media_download,
    "quota": handle_quota,
    "views": handle_views,
    "keys_status": handle_keys_status,
    "keys_rotate": handle_keys_rotate,
    # full-text retrieval (transport)
    "fulltext_search": handle_fulltext_search,
    "search_inside": handle_search_inside,
    "text_availability": handle_text_availability,
    "text_harvest": handle_text_harvest,
    "text_store": handle_text_store,
    "quote_from_index": handle_quote_from_index,
    "query_diagnostics": handle_query_diagnostics,
    "lending_status": handle_lending_status,
    # analysis over the local corpus
    "text_reuse": handle_text_reuse,
    "text_variants": handle_text_variants,
    "text_reconcile": handle_text_reconcile,
    # columnar corpus
    "corpus_build": handle_corpus_build,
    "corpus_stats": handle_corpus_stats,
    "corpus_similar": handle_corpus_similar,
    "corpus_shared": handle_corpus_shared,
    "corpus_diff": handle_corpus_diff,
    "corpus_phrase": handle_corpus_phrase,
    "corpus_sql": handle_corpus_sql,
}


async def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Missing command argument"}))
        sys.exit(1)

    cmd = sys.argv[1]
    handler = COMMANDS.get(cmd)
    if not handler:
        print(json.dumps({"error": f"Unknown command '{cmd}'"}))
        sys.exit(1)

    try:
        raw_input = sys.stdin.read().strip()
        args = json.loads(raw_input) if raw_input else {}
        res = await handler(args)
        print(json.dumps(res, default=str))
    except Exception as e:
        print(json.dumps({"error": str(e), "type": type(e).__name__}))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
