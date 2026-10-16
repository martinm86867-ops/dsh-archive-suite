"""Bridge script connecting DSH JavaScript plugin to Archivist Python Engine."""

import sys
import os
import json
import asyncio
from pathlib import Path

# Ensure archivist is in python path (prefer local vendored checkout, overridable via DSH_ARCHIVE_HOME)
_DEFAULT_HOME = Path(__file__).resolve().parent
WORKSPACE_ARCHIVE = Path(os.environ.get("DSH_ARCHIVE_HOME") or (
    _DEFAULT_HOME if (_DEFAULT_HOME / "archivist").is_dir() else Path("/home/martin/archive")
))
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
    if args.get("creator"):
        qb.creator(args["creator"])
    if args.get("title"):
        qb.title(args["title"])
    if args.get("subject"):
        qb.subject(args["subject"])
    if args.get("year"):
        qb.year(args["year"])
    if args.get("date_from") and args.get("date_to"):
        qb.date_range(args["date_from"], args["date_to"])
    if args.get("format"):
        qb.format(args["format"])

    query = qb.build()
    sorts = None
    if args.get("sort"):
        sorts = [args["sort"]] if isinstance(args["sort"], str) else list(args["sort"])

    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        res = await client.search(
            query=query,
            rows=int(args.get("rows", 10)),
            page=int(args.get("page", 1)),
            fields=args.get("fields") or ["identifier", "title", "mediatype", "collection", "creator", "downloads", "date", "publicdate"],
            sorts=sorts,
        )
        return {"query": query, "num_found": res.get("numFound", 0), "docs": res.get("docs", [])}


async def handle_item_info(args: dict) -> dict:
    from archivist.fulltext import FullTextEngine
    ident = args["identifier"]
    max_files = int(args.get("max_files", 25))
    format_filter = (args.get("format_filter") or "").lower().strip()
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        ft = FullTextEngine(client)
        meta = await ft._metadata(ident)
        files = meta.get("files", [])
        if format_filter:
            files = [f for f in files if format_filter in str(f.get("format", "")).lower() or format_filter in str(f.get("name", "")).lower()]
        m = meta.get("metadata", {})
        return {
            "identifier": ident,
            "title": m.get("title"),
            "mediatype": m.get("mediatype"),
            "collection": m.get("collection"),
            "creator": m.get("creator"),
            "date": m.get("date"),
            "description": str(m.get("description") or "")[:600],
            "server": meta.get("server"),
            "dir": meta.get("dir"),
            "item_size": meta.get("item_size", 0),
            "files_count": len(meta.get("files", [])),
            "filtered_files_count": len(files),
            "url": f"https://archive.org/details/{ident}",
            "sample_files": files[:max_files],
            "cached_in_store": True,
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
    limit = int(args.get("limit", 15))
    match_type = args.get("match_type", "exact")
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
    with DuckDBWarehouse(wh_path) as wh:
        results = wh.search_bm25(query, limit=limit)
        stats = wh.get_stats()
    return {"query": query, "count": len(results), "warehouse_stats": stats, "results": results}


async def handle_research_prompt(args: dict) -> dict:
    query = args["query"]
    limit = int(args.get("limit", 3))
    wh_path = Path(args.get("warehouse") or (WORKSPACE_ARCHIVE / "data" / "research_warehouse.duckdb"))
    with DuckDBWarehouse(wh_path) as wh:
        results = wh.search_bm25(query, limit=limit)
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
    """Read text slice from an item, with local cache, offset pagination, and regex grep."""
    import re
    from archivist.fulltext import FullTextEngine
    from archivist.media import MediaAccessEngine
    ident = args["identifier"]
    offset = int(args.get("offset", 0))
    max_chars = int(args.get("max_chars", 6000))
    pattern = args.get("grep") or args.get("pattern")
    context_chars = int(args.get("context_chars", 240))

    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        ft = FullTextEngine(client)
        try:
            fetched = await ft.fetch_text(ident, max_chars=max(offset + max_chars, 500_000) if pattern else (offset + max_chars))
        except ArchiveError:
            fetched = {"found": False}

        if fetched.get("found") and fetched.get("text"):
            full_text = fetched["text"]
            total_chars = fetched.get("chars", len(full_text))
            filename = fetched.get("file", f"{ident}_djvu.txt")
            file_url = fetched.get("source_url", f"https://archive.org/download/{ident}/{filename}")
        else:
            engine = MediaAccessEngine(client)
            try:
                res = await engine.read_full_text(ident, max_chars=offset + max_chars)
            except ArchiveError:
                res = {"found": False}
            if not res.get("found"):
                # Auto-borrow & cache when item is lending-restricted
                borrowed = await ft.auto_borrow_and_cache(ident, terms=[pattern] if pattern else None)
                if borrowed.get("text"):
                    full_text = borrowed["text"]
                    total_chars = len(full_text)
                    filename = f"{ident}_fts_borrowed.txt"
                    file_url = f"https://archive.org/details/{ident}"
                else:
                    return res
            else:
                full_text = res["text"]
                total_chars = len(full_text)
                filename = res["filename"]
                file_url = res["full_file_url"]

        if pattern:
            matches = []
            try:
                rx = re.compile(pattern, re.IGNORECASE)
            except re.error:
                rx = re.compile(re.escape(pattern), re.IGNORECASE)
            for m in rx.finditer(full_text):
                s = max(0, m.start() - context_chars)
                e = min(len(full_text), m.end() + context_chars)
                matches.append({
                    "offset": m.start(),
                    "match": m.group(0),
                    "excerpt": full_text[s:e].replace("\n", " ").strip(),
                })
                if len(matches) >= int(args.get("max_matches", 15)):
                    break
            return {
                "identifier": ident,
                "filename": filename,
                "found": True,
                "mode": "grep",
                "pattern": pattern,
                "total_chars": total_chars,
                "match_count": len(matches),
                "matches": matches,
                "full_file_url": file_url,
            }

        sliced = full_text[offset:offset + max_chars]
        return {
            "identifier": ident,
            "filename": filename,
            "found": True,
            "mode": "slice",
            "offset": offset,
            "next_offset": (offset + len(sliced)) if (offset + len(sliced)) < total_chars else None,
            "total_chars": total_chars,
            "length_read": len(sliced),
            "text": sliced,
            "full_file_url": file_url,
        }


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
    import hashlib
    from archivist.fulltext import FullTextEngine
    ident = args["identifier"]
    filename = args["filename"]
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    default_dest = cfg.data_dir / "text_cache" / "artifacts" / ident
    dest_dir = Path(args.get("destination_dir") or default_dest)
    dest_file = dest_dir / filename
    async with ArchiveClient(cfg) as client:
        ft = FullTextEngine(client)
        meta = await ft._metadata(ident)
        path = await client.download_file(ident, filename, dest_file)
        size = path.stat().st_size
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(512 * 1024):
                h.update(chunk)
        rec = ft.store.record_artifact(
            identifier=ident,
            kind="media_download",
            file_name=filename,
            fmt=Path(filename).suffix.lstrip(".") or "binary",
            size_bytes=size,
            sha256=h.hexdigest(),
            source_url=f"https://archive.org/download/{ident}/{filename}",
            local_path=str(path.resolve()),
            loan_status="open",
            metadata={"title": (meta.get("metadata") or {}).get("title", ident)},
        )
        return {
            "identifier": ident,
            "filename": filename,
            "saved_path": str(path.resolve()),
            "size": size,
            "sha256": rec["sha256"],
            "artifact_id": rec["id"],
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
        creds = client.rotation_manager.current_credentials
        if creds:
            try:
                await client.get_user_spn_quota()
            except Exception:
                pass
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
    """Download text into the local store, and auto-build the DuckDB corpus by default."""
    from archivist.fulltext import FullTextEngine
    from archivist.textstore import TextStore
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        report = await FullTextEngine(client).harvest_text(
            args["identifiers"], pause=float(args.get("pause", 1.0)),
            max_docs=args.get("max_docs"),
        )
        if args.get("auto_build", True):
            store = TextStore(Path(args.get("store") or (WORKSPACE_ARCHIVE / "data" / "text_cache")))
            corpus_res = _corpus_db(args).build_from_store(store, replace=False)
            report["corpus_built"] = corpus_res.get("documents", 0)
            report["corpus_stats"] = corpus_res.get("stats", {})
        return report


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
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    wh_path = Path(args.get("warehouse") or (cfg.data_dir / "research_warehouse.duckdb"))
    bridge = AgentArchiveBridge(warehouse_path=wh_path)
    cards = bridge.query_agent_cards(query, limit=limit, require_tier=tier)
    bridge.warehouse.close()
    return {
        "query": query,
        "card_count": len(cards),
        "cards": cards,
        "formatted_prompt": bridge.format_agent_prompt(query, cards)
    }


async def handle_wayback_read(args: dict) -> dict:
    """Fetch a historical Wayback Machine snapshot and extract clean readable text (or raw HTML)."""
    from bs4 import BeautifulSoup
    url = args["url"]
    timestamp = args.get("timestamp")
    max_chars = int(args.get("max_chars", 8000))
    offset = int(args.get("offset", 0))
    raw_html = bool(args.get("raw_html", False))

    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        if not timestamp:
            rows = await client.cdx_search(
                url=url, match_type="exact", limit=-1, status_codes=[200], fl=["timestamp", "original", "statuscode"]
            )
            if not rows:
                return {"url": url, "found": False, "error": f"No archived snapshot found on Wayback Machine for {url}"}
            timestamp = rows[-1].get("timestamp")

        raw_bytes = await client.get_wayback_snapshot(url, str(timestamp), raw=True)
        html_str = raw_bytes.decode("utf-8", errors="ignore")
        playback_url = f"https://web.archive.org/web/{timestamp}/{url}"

        if raw_html:
            sliced = html_str[offset:offset + max_chars]
            return {
                "url": url,
                "timestamp": str(timestamp),
                "playback_url": playback_url,
                "found": True,
                "mode": "raw_html",
                "total_chars": len(html_str),
                "offset": offset,
                "text": sliced,
            }

        soup = BeautifulSoup(html_str, "html.parser")
        title = soup.title.string.strip() if (soup.title and soup.title.string) else url
        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()
        clean_text = soup.get_text(separator="\n", strip=True)
        sliced = clean_text[offset:offset + max_chars]
        return {
            "url": url,
            "title": title,
            "timestamp": str(timestamp),
            "playback_url": playback_url,
            "found": True,
            "mode": "clean_text",
            "total_chars": len(clean_text),
            "offset": offset,
            "next_offset": (offset + len(sliced)) if (offset + len(sliced)) < len(clean_text) else None,
            "text": sliced,
        }


async def handle_iiif_image(args: dict) -> dict:
    """Extract canonical IIIF Image API 3.0 URLs and optional cropped figure downloads for digitized book pages."""
    from archivist.iiif import IIIFEngine
    ident = args["identifier"]
    page_number = int(args.get("page_number", 1))
    region = args.get("region", "full")
    size = args.get("size", "max")
    rotation = int(args.get("rotation", 0))
    fmt = args.get("format", "jpg")
    download_path = args.get("download_path")

    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        iiif = IIIFEngine(client)
        info = await iiif.get_page_image_url(
            identifier=ident, page_number=page_number, region=region, size=size, rotation=rotation, fmt=fmt
        )
        if download_path:
            dest = Path(download_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            session = await client.get_session()
            r = await session.get(info["iiif_url"])
            if r.status_code != 200:
                return {**info, "downloaded": False, "error": f"IIIF download failed (HTTP {r.status_code})"}
            dest.write_bytes(r.content)
            info["downloaded"] = True
            info["saved_path"] = str(dest.resolve())
            info["bytes"] = len(r.content)
            import hashlib
            from archivist.fulltext import FullTextEngine
            ft = FullTextEngine(client)
            ft.store.record_artifact(
                identifier=ident,
                kind="iiif_image",
                file_name=dest.name,
                fmt=fmt,
                size_bytes=len(r.content),
                sha256=hashlib.sha256(r.content).hexdigest(),
                source_url=info["iiif_url"],
                local_path=str(dest.resolve()),
                loan_status="open",
                metadata={"page_number": page_number, "region": region, "size": size},
            )
        return info


async def handle_lending_borrow(args: dict) -> dict:
    """Borrow, browse (1-hour loan), or return a lending-restricted item, and cache its content locally."""
    from archivist.fulltext import FullTextEngine
    ident = args["identifier"]
    action = args.get("action", "browse_book")
    terms = args.get("terms")
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    cookie = args.get("patron_cookie") or os.environ.get("IA_PATRON_COOKIE")
    if cookie:
        from archivist.config import PatronSession
        cfg.patron_session = PatronSession(cookie=cookie)
    async with ArchiveClient(cfg) as client:
        engine = FullTextEngine(client)
        if action == "return_loan":
            return await engine.return_item(ident)
        if args.get("auto_cache", True):
            return await engine.auto_borrow_and_cache(ident, terms=terms, action=action)
        return await engine.borrow_item(ident, action=action)


async def handle_item_manage(args: dict) -> dict:
    """Create items, patch metadata, delete files, or inspect background derivation tasks."""
    op = args["operation"]
    ident = args["identifier"]
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        if op == "tasks":
            return await client.get_tasks(ident)
        if op == "create":
            return await client.create_item(
                identifier=ident,
                title=args.get("title", ident),
                mediatype=args.get("mediatype"),
                collection=args.get("collection"),
                description=args.get("description"),
                creator=args.get("creator"),
                date=args.get("date"),
                subject=args.get("subject"),
                extra_metadata=args.get("extra_metadata"),
            )
        if op == "patch_metadata":
            patches = args.get("patches") or []
            return await client.modify_metadata(ident, patches)
        if op == "delete_file":
            filename = args["filename"]
            ok = await client.delete_file(ident, filename)
            return {"identifier": ident, "filename": filename, "deleted": ok}
        if op == "server_nodes":
            srv = await client.get_item_subpath(ident, "server")
            workable = await client.get_item_subpath(ident, "workable_servers")
            return {"identifier": ident, "server": srv, "workable_servers": workable}
        return {"error": f"Unknown item_manage operation: {op}"}


async def handle_research_timeline(args: dict) -> dict:
    """Retrieve temporal timeline of captures for a URL or entity in the local DuckDB warehouse."""
    source_key = args["source_key"]
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    wh_path = Path(args.get("warehouse") or (cfg.data_dir / "research_warehouse.duckdb"))
    with DuckDBWarehouse(wh_path) as wh:
        timeline = wh.get_timeline(source_key)
    return {"source_key": source_key, "count": len(timeline), "timeline": timeline}



async def handle_forensic_compare(args: dict) -> dict:
    """Direct primary-text forensic comparison between two documents (auto-harvesting if needed)."""
    from archivist.analysis import CorpusAnalysis
    from archivist.fulltext import FullTextEngine
    ident_a = args["identifier_a"]
    ident_b = args["identifier_b"]
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        ft = FullTextEngine(client)
        db = _corpus_db(args)
        analysis = CorpusAnalysis(db, ft)
        await analysis.ensure_local([ident_a, ident_b], pause=0.2)
        return analysis.forensic_compare(
            ident_a,
            ident_b,
            window_tokens=int(args.get("window_tokens", 250)),
            step_tokens=int(args.get("step_tokens", 125)),
            top_passages=int(args.get("top_passages", 8)),
        )


async def handle_epistemic_dossier(args: dict) -> dict:
    """One-shot autonomous 5-stage epistemic & primary-text forensic dossier."""
    from archivist.analysis import CorpusAnalysis
    from archivist.fulltext import FullTextEngine
    statement = args["statement"]
    compare_with = args.get("compare_with")
    focus_terms = args.get("focus_terms")
    max_harvest = int(args.get("max_harvest", 2))
    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    async with ArchiveClient(cfg) as client:
        ft = FullTextEngine(client)
        db = _corpus_db(args)
        analysis = CorpusAnalysis(db, ft)
        return await analysis.epistemic_dossier(
            statement=statement,
            compare_with=compare_with,
            focus_terms=focus_terms,
            max_harvest=max_harvest,
        )


async def handle_cache_manage(args: dict) -> dict:
    """Unified local cache & downloaded content manager (stats, list, inspect, borrow_and_cache, delete, prune, sync_corpus)."""
    from archivist.fulltext import FullTextEngine
    from archivist.textstore import TextStore
    op = args.get("operation", "stats")
    ident = args.get("identifier")
    kind = args.get("kind")
    limit = int(args.get("limit", 50))

    cfg = ArchivistConfig.load(search_dir=WORKSPACE_ARCHIVE)
    store = TextStore(Path(args.get("store") or (cfg.data_dir / "text_cache")))

    if op == "stats":
        summary = store.cache_summary()
        try:
            summary["corpus"] = _corpus_db(args).stats()
        except Exception:
            pass
        return summary

    if op == "list":
        return {
            "summary": store.cache_summary(),
            "texts": store.entries()[:limit],
            "metadata": store.metadata_entries(limit=limit),
            "artifacts": store.artifacts(kind=kind, identifier=ident, limit=limit),
        }

    if op == "inspect":
        if not ident:
            return {"error": "identifier is required for operation='inspect'"}
        base = ident.split("|")[0]
        meta = store.get_metadata(base, ttl_hours=0)
        text = store.get(base)
        arts = store.artifacts(identifier=base)
        return {
            "identifier": base,
            "has_cached_metadata": meta is not None,
            "metadata_summary": {
                "title": (meta.get("metadata") or {}).get("title") if meta else None,
                "mediatype": (meta.get("metadata") or {}).get("mediatype") if meta else None,
                "files_count": len(meta.get("files") or []) if meta else 0,
            } if meta else None,
            "has_cached_text": text is not None,
            "cached_text_chars": len(text) if text else 0,
            "cached_text_preview": text[:1200] if text else None,
            "artifacts": arts,
        }

    if op == "borrow_and_cache":
        if not ident:
            return {"error": "identifier is required for operation='borrow_and_cache'"}
        cookie = args.get("patron_cookie")
        if cookie:
            from archivist.config import PatronSession
            cfg.patron_session = PatronSession(cookie=cookie)
        async with ArchiveClient(cfg) as client:
            ft = FullTextEngine(client)
            res = await ft.auto_borrow_and_cache(ident, terms=args.get("terms"))
            if res.get("text"):
                _corpus_db(args).add_document(
                    ident.split("|")[0],
                    res["text"],
                    fmt="Borrowed / FTS Cached",
                    source_url=f"https://archive.org/details/{ident.split('|')[0]}",
                )
            return res

    if op == "delete":
        if not ident:
            return {"error": "identifier (or artifact id) is required for operation='delete'"}
        base = ident.split("|")[0]
        existed_text = store.discard(base)
        deleted_arts = store.delete_artifact(ident, delete_files=True)
        try:
            _corpus_db(args).remove_document(base)
        except Exception:
            pass
        return {
            "identifier": ident,
            "deleted_text": existed_text,
            "deleted_artifacts": deleted_arts,
            "removed_from_corpus": True,
            "summary": store.cache_summary(),
        }

    if op == "sync_corpus":
        built = _corpus_db(args).build_from_store(store, replace=bool(args.get("replace", False)))
        return {"operation": "sync_corpus", **built}

    return {"error": f"Unknown cache_manage operation: {op}"}


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
    # zero-code agent extensions
    "wayback_read": handle_wayback_read,
    "iiif_image": handle_iiif_image,
    "lending_borrow": handle_lending_borrow,
    "item_manage": handle_item_manage,
    "research_timeline": handle_research_timeline,
    "forensic_compare": handle_forensic_compare,
    "epistemic_dossier": handle_epistemic_dossier,
    "cache_manage": handle_cache_manage,
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
