"""Command-line interface for Archivist Engine using Click and Rich."""

from __future__ import annotations
import asyncio
import json
import logging
from pathlib import Path
from typing import Optional, List

import click
import yaml
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.syntax import Syntax

from .config import ArchivistConfig
from .client import ArchiveClient, ArchiveError
from .search import SearchEngine, QueryBuilder
from .wayback import WaybackEngine
from .item import ItemManager
from .pipeline.schema import PipelineConfig
from .pipeline.engine import PipelineEngine
from .pipeline.state import PipelineStateManager

console = Console()
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose debug logging.")
def main(verbose: bool):
    """🏛️ Archivist Engine — Unified Archive.org & Wayback Machine Pipeline System."""
    if verbose:
        logging.getLogger().setLevel(logging.INFO)
        logging.getLogger("archivist").setLevel(logging.DEBUG)


# ==========================================
# Config Command
# ==========================================

@main.command("config")
def cmd_config():
    """Inspect detected S3 credentials and operational configuration."""
    cfg = ArchivistConfig.load()
    table = Table(title="Archivist Engine Configuration", border_style="blue")
    table.add_column("Parameter", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Credentials Loaded", str(len(cfg.credentials)))
    for idx, c in enumerate(cfg.credentials):
        status = "[bold green]ACTIVE[/bold green]" if idx == cfg.active_credential_index else "inactive"
        table.add_row(
            f"Key #{idx + 1} ({c.label})",
            f"{c.access_key[:4]}...{c.access_key[-4:]} ({status})"
        )

    table.add_row("User Agent", cfg.user_agent)
    table.add_row("Default Collection", cfg.default_collection)
    table.add_row("Default Mediatype", cfg.default_mediatype)
    table.add_row("S3 Rate Limit", f"{cfg.rate_limits.s3_requests_per_sec} req/s")
    table.add_row("Wayback Rate Limit", f"{cfg.rate_limits.wayback_requests_per_sec} req/s")
    table.add_row("State DB Path", str(cfg.state_db_path))

    console.print(table)


@main.command("quota")
def cmd_quota():
    """Inspect authenticated Save Page Now 2 (SPN2) capture quotas and slot availability."""
    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            try:
                user_q = await client.get_user_spn_quota()
                sys_q = await client.get_system_spn_status()
            except ArchiveError as e:
                console.print(f"[red]Error fetching quota:[/red] {e}")
                return

            t = Table(title="Archive.org Authenticated SPN2 Quotas", border_style="cyan")
            t.add_column("Parameter", style="cyan")
            t.add_column("Value", style="green")

            t.add_row("Daily Capture Limit", f"{user_q.get('daily_captures_limit', 0):,} URLs/day")
            t.add_row("Daily Captures Used", f"{user_q.get('daily_captures', 0):,} URLs")
            t.add_row("Available Concurrent Slots", str(user_q.get("available", 0)))
            t.add_row("Active Processing Jobs", str(user_q.get("processing", 0)))
            t.add_row("Daily Status Checks Limit", f"{user_q.get('daily_status_limit', 0):,}")
            t.add_row("Global Cluster Health", sys_q.get("status", "ok"))
            t.add_row("Global Recent Captures", f"{sys_q.get('recent_captures', 0):,}")

            console.print(t)

    asyncio.run(run())


@main.command("views")
@click.argument("identifier")
def cmd_views(identifier: str):
    """Retrieve real-time viewership analytics from unpublicized views API."""
    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            try:
                v = await client.get_item_views(identifier)
            except ArchiveError as e:
                console.print(f"[red]Error:[/red] {e}")
                return

            t = Table(title=f"Traffic Analytics: {identifier}", border_style="cyan")
            t.add_column("Period", style="cyan")
            t.add_column("View Count", justify="right", style="green")

            t.add_row("All Time", f"{v.get('all_time', 0):,}")
            t.add_row("Last 30 Days", f"{v.get('last_30day', 0):,}")
            t.add_row("Last 7 Days", f"{v.get('last_7day', 0):,}")
            console.print(t)

    asyncio.run(run())


@main.command("server")
@click.argument("identifier")
def cmd_server(identifier: str):
    """Inspect physical cluster storage server nodes and writable nodes."""
    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            srv = await client.get_item_subpath(identifier, "server")
            workable = await client.get_item_subpath(identifier, "workable_servers")

            t = Table(title=f"Cluster Node Routing: {identifier}", border_style="cyan")
            t.add_column("Property", style="cyan")
            t.add_column("Node Address", style="yellow")

            t.add_row("Primary Storage Node", str(srv))
            if isinstance(workable, list):
                for idx, w in enumerate(workable):
                    t.add_row(f"Workable Node #{idx+1}", str(w))
            else:
                t.add_row("Workable Node", str(workable))
            console.print(t)

    asyncio.run(run())


# ==========================================
# Search Command
# ==========================================

@main.command("search")
@click.argument("query")
@click.option("-n", "--rows", default=20, help="Number of results to display.")
@click.option("-p", "--page", default=1, help="Page number.")
@click.option("-m", "--mediatype", help="Filter by mediatype (e.g. texts, audio, video).")
@click.option("-c", "--collection", help="Filter by collection.")
def cmd_search(query: str, rows: int, page: int, mediatype: Optional[str], collection: Optional[str]):
    """Search the Archive.org catalog with optional filters."""
    qb = QueryBuilder()
    if query:
        qb.raw(query)
    if mediatype:
        qb.mediatype(mediatype)
    if collection:
        qb.collection(collection)

    full_query = qb.build()
    console.print(f"[dim]Executing query: {full_query}[/dim]")

    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            res = await client.search(
                query=full_query,
                rows=rows,
                page=page,
                fields=["identifier", "title", "mediatype", "downloads", "date"]
            )
            docs = res.get("docs", [])
            num_found = res.get("numFound", 0)

            table = Table(title=f"Archive.org Search Results ({num_found} total hits)", border_style="cyan")
            table.add_column("Identifier", style="bold yellow")
            table.add_column("Title", style="white", max_width=45)
            table.add_column("Type", style="magenta")
            table.add_column("Date", style="blue")
            table.add_column("Downloads", justify="right", style="green")

            for d in docs:
                table.add_row(
                    d.get("identifier", ""),
                    str(d.get("title", ""))[:45],
                    d.get("mediatype", ""),
                    str(d.get("date", ""))[:10],
                    str(d.get("downloads", 0))
                )
            console.print(table)

    asyncio.run(run())


# ==========================================
# Item Commands
# ==========================================

@main.group("item")
def item_group():
    """Manage Archive.org items, files, and catalog tasks."""
    pass


@item_group.command("info")
@click.argument("identifier")
def cmd_item_info(identifier: str):
    """Display comprehensive metadata for an item."""
    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            try:
                meta = await client.get_metadata(identifier)
            except ArchiveError as e:
                console.print(f"[red]Error:[/red] {e}")
                return

            m = meta.get("metadata", {})
            files = meta.get("files", [])
            table = Table(title=f"Item: {identifier}", border_style="cyan")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="white")

            table.add_row("Title", str(m.get("title", "N/A")))
            table.add_row("Mediatype", str(m.get("mediatype", "N/A")))
            table.add_row("Collection", str(m.get("collection", "N/A")))
            table.add_row("Files Count", str(len(files)))
            table.add_row("Item Size", f"{meta.get('item_size', 0) / (1024*1024):.2f} MB")
            table.add_row("Server", str(meta.get("server", "N/A")))
            table.add_row("URL", f"https://archive.org/details/{identifier}")

            console.print(table)

    asyncio.run(run())


@item_group.command("files")
@click.argument("identifier")
def cmd_item_files(identifier: str):
    """List files in an Archive.org item."""
    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            mgr = ItemManager(client)
            files = await mgr.list_files(identifier)

            table = Table(title=f"Files in {identifier} ({len(files)} files)", border_style="cyan")
            table.add_column("Filename", style="bold yellow")
            table.add_column("Size", justify="right", style="green")
            table.add_column("Format", style="magenta")
            table.add_column("MD5 / SHA1", style="dim")

            for f in files:
                size_mb = f"{f.size / (1024*1024):.2f} MB" if f.size > 1024*1024 else f"{f.size / 1024:.1f} KB"
                checksum = f.md5 or f.sha1 or ""
                table.add_row(f.name, size_mb, f.format or "", checksum[:16])

            console.print(table)

    asyncio.run(run())


@item_group.command("upload")
@click.argument("identifier")
@click.argument("path", type=click.Path(exists=True))
@click.option("-t", "--title", help="Item title (if creating new item).")
@click.option("-m", "--mediatype", default="data", help="Item mediatype.")
@click.option("-c", "--collection", default="opensource", help="Item collection.")
def cmd_item_upload(identifier: str, path: str, title: Optional[str], mediatype: str, collection: str):
    """Upload a file or directory tree to an Archive.org item via IAS3."""
    src = Path(path)

    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            mgr = ItemManager(client)
            if src.is_file():
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TimeRemainingColumn(),
                    console=console
                ) as progress:
                    task = progress.add_task(f"Uploading {src.name}...", total=src.stat().st_size)

                    def update(cur, tot):
                        progress.update(task, completed=cur, total=tot)

                    res = await client.upload_file(
                        identifier=identifier,
                        filename=src.name,
                        file_path=src,
                        progress_callback=update
                    )
                console.print(f"[bold green]Upload complete:[/bold green] {res['url']}")
            else:
                console.print(f"Uploading directory {src} to {identifier}...")
                res = await mgr.upload_directory(
                    identifier=identifier,
                    local_dir=src,
                    item_title=title,
                    mediatype=mediatype,
                    collection=collection
                )
                console.print(f"[bold green]Directory uploaded successfully ({res['uploaded_files_count']} files).[/bold green]")

    asyncio.run(run())


@item_group.command("download")
@click.argument("identifier")
@click.argument("destination", type=click.Path())
@click.option("-f", "--format", "fmt", help="Filter by file format (e.g. 'Text PDF', 'VBR MP3').")
def cmd_item_download(identifier: str, destination: str, fmt: Optional[str]):
    """Download files from an item with resume support."""
    dest = Path(destination)

    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            mgr = ItemManager(client)
            formats = [fmt] if fmt else None
            paths = await mgr.download_item(identifier, dest, include_formats=formats)
            console.print(f"[bold green]Downloaded {len(paths)} files to {dest}[/bold green]")

    asyncio.run(run())


# ==========================================
# Wayback Commands
# ==========================================

@main.group("wayback")
def wayback_group():
    """Preserve URLs to Wayback Machine, search CDX, and build mirrors."""
    pass


@wayback_group.command("save")
@click.argument("url")
@click.option("--wait/--no-wait", default=True, help="Wait for capture job completion.")
def cmd_wayback_save(url: str, wait: bool):
    """Save a URL to Wayback Machine (Save Page Now 2)."""
    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            console.print(f"Submitting [bold cyan]{url}[/bold cyan] to Save Page Now...")
            res = await client.save_page_now(url, capture_all=True)
            if res.get("status") == "error":
                console.print(f"[yellow]Archive Notice:[/yellow] {res.get('message', res.get('status_ext'))}")
                return

            job_id = res.get("job_id")
            console.print(f"[dim]Job ID: {job_id}[/dim]")

            if wait and job_id:
                with console.status("Waiting for snapshot capture to finalize..."):
                    status = await client.wait_spn_completion(job_id)
                if status.get("status") == "success":
                    console.print(f"[bold green]Successfully archived![/bold green]")
                    console.print(f"Timestamp: {status.get('timestamp')}")
                    console.print(f"Playback: https://web.archive.org/web/{status.get('timestamp')}/{url}")
                else:
                    console.print(f"[yellow]Status:[/yellow] {status}")
            else:
                console.print("[green]Submission queued successfully.[/green]")

    asyncio.run(run())


@wayback_group.command("cdx")
@click.argument("url")
@click.option("-m", "--match-type", default="prefix", type=click.Choice(["exact", "prefix", "host", "domain"]))
@click.option("-n", "--limit", default=25, help="Maximum snapshots to display.")
@click.option("--from", "from_ts", help="Start timestamp (YYYYMMDD).")
@click.option("--to", "to_ts", help="End timestamp (YYYYMMDD).")
def cmd_wayback_cdx(url: str, match_type: str, limit: int, from_ts: Optional[str], to_ts: Optional[str]):
    """Query Wayback CDX Server for historical captures."""
    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            snapshots = await client.cdx_search(
                url=url,
                match_type=match_type,
                limit=limit,
                from_ts=from_ts,
                to_ts=to_ts
            )

            table = Table(title=f"Wayback CDX Snapshots for '{url}' ({len(snapshots)} found)", border_style="cyan")
            table.add_column("Timestamp", style="bold yellow")
            table.add_column("Status", style="magenta")
            table.add_column("Mime Type", style="blue")
            table.add_column("Original URL", style="white", max_width=40)
            table.add_column("Playback URL", style="dim")

            for s in snapshots:
                ts = s.get("timestamp", "")
                orig = s.get("original", "")
                playback = f"https://web.archive.org/web/{ts}/{orig}"
                table.add_row(ts, s.get("statuscode", ""), s.get("mimetype", ""), orig[:40], playback)

            console.print(table)

    asyncio.run(run())


@wayback_group.command("mirror")
@click.argument("url")
@click.argument("output_dir", type=click.Path())
@click.option("-n", "--max-pages", default=30, help="Maximum snapshot pages to download.")
def cmd_wayback_mirror(url: str, output_dir: str, max_pages: int):
    """Crawl CDX and download an offline snapshot mirror of a site."""
    out = Path(output_dir)

    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            wb = WaybackEngine(client)
            console.print(f"Building mirror for [bold cyan]{url}[/bold cyan] -> {out}...")
            paths = await wb.mirror_site(url, out, max_pages=max_pages)
            console.print(f"[bold green]Mirror complete: {len(paths)} assets saved in {out}[/bold green]")

    asyncio.run(run())


@wayback_group.command("diff")
@click.argument("url")
@click.argument("timestamp_a")
@click.argument("timestamp_b")
def cmd_wayback_diff(url: str, timestamp_a: str, timestamp_b: str):
    """Compare content of a webpage between two Wayback snapshot dates."""
    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            wb = WaybackEngine(client)
            diff_res = await wb.diff_snapshots(url, timestamp_a, timestamp_b)
            lines = diff_res["diff_lines"]

            if not lines:
                console.print("[green]No textual differences between snapshots.[/green]")
            else:
                console.print(f"[bold cyan]Diff between {timestamp_a} and {timestamp_b} ({len(lines)} changed lines):[/bold cyan]")
                for l in lines[:50]:
                    if l.startswith("+"):
                        console.print(f"[green]{l}[/green]")
                    elif l.startswith("-"):
                        console.print(f"[red]{l}[/red]")
                    else:
                        console.print(f"[dim]{l}[/dim]")
                if len(lines) > 50:
                    console.print(f"[dim]... and {len(lines) - 50} more lines.[/dim]")

    asyncio.run(run())


# ==========================================
# Pipeline Commands
# ==========================================

@main.group("pipeline")
def pipeline_group():
    """Run and monitor multi-stage asynchronous processing pipelines."""
    pass


@pipeline_group.command("run")
@click.argument("config_file", type=click.Path(exists=True))
@click.option("-r", "--resume", "resume_id", help="Resume an existing pipeline run by ID.")
def cmd_pipeline_run(config_file: str, resume_id: Optional[str]):
    """Execute a pipeline defined in YAML or JSON."""
    cfg_path = Path(config_file)
    content = cfg_path.read_text(encoding="utf-8")
    data = yaml.safe_load(content)
    pipe_cfg = PipelineConfig.model_validate(data)

    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            engine = PipelineEngine(pipe_cfg, client)

            with Progress(
                SpinnerColumn(),
                TextColumn("[bold cyan]{task.fields[name]}[/bold cyan]"),
                BarColumn(),
                TextColumn("[bold green]Produced: {task.fields[produced]}[/bold green]"),
                TextColumn("[bold yellow]Processed: {task.fields[transformed]}[/bold yellow]"),
                TextColumn("[bold blue]Sunk: {task.fields[consumed]}[/bold blue]"),
                TextColumn("[red]Errors: {task.fields[failed]}[/red]"),
                console=console
            ) as progress:
                task = progress.add_task(
                    "Executing",
                    name=pipe_cfg.name,
                    produced=0,
                    transformed=0,
                    consumed=0,
                    failed=0,
                    total=pipe_cfg.max_items or 100
                )

                def update_prog(stats):
                    progress.update(
                        task,
                        produced=stats["produced"],
                        transformed=stats["transformed"],
                        consumed=stats["consumed"],
                        failed=stats["failed"]
                    )

                engine.set_progress_callback(update_prog)
                res = await engine.run(resume_run_id=resume_id)

            console.print(Panel(
                f"[bold green]Pipeline {res['pipeline_name']} finished![/bold green]\n"
                f"Run ID: [cyan]{res['run_id']}[/cyan]\n"
                f"Status: [yellow]{res['status']}[/yellow]\n"
                f"Produced: {res['stats']['produced']} | Transformed: {res['stats']['transformed']} | Consumed: {res['stats']['consumed']} | Errors: {res['stats']['failed']}",
                title="Execution Summary"
            ))

    asyncio.run(run())


@pipeline_group.command("status")
@click.argument("run_id")
@click.option("--db", "db_path", default=None, help="Path to state SQLite database.")
def cmd_pipeline_status(run_id: str, db_path: Optional[str]):
    """Inspect status of a past or running pipeline run."""
    async def run():
        cfg = ArchivistConfig.load()
        target_db = Path(db_path) if db_path else cfg.state_db_path
        state = PipelineStateManager(target_db)
        await state.connect()
        stats = await state.get_run_stats(run_id)
        await state.close()

        table = Table(title=f"Pipeline Run: {run_id}", border_style="cyan")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Status", stats.get("status", "N/A"))
        table.add_row("Duration", f"{stats.get('duration_sec', 0):.2f}s")
        table.add_row("Error", str(stats.get("error", "None")))

        for stage, data in stats.get("stages", {}).items():
            table.add_row(f"Stage '{stage}'", f"Completed: {data['completed']}, Failed: {data['failed']}")

        console.print(table)

    asyncio.run(run())


# ==========================================
# Web Server Command
# ==========================================

@main.command("serve")
@click.option("--host", default="127.0.0.1", help="Host interface to bind.")
@click.option("--port", default=8080, help="Port to listen on.")
def cmd_serve(host: str, port: int):
    """Launch the Web Dashboard and REST API."""
    import uvicorn
    console.print(f"[bold green]Starting Archivist Engine Dashboard at http://{host}:{port}[/bold green]")
    uvicorn.run("archivist.server.app:app", host=host, port=port, log_level="info")


# ==========================================
# Research Commands (DuckDB + LLM Grounding)
# ==========================================

@main.group("research")
def research_group():
    """Analytical research warehouse and LLM-friendly retrieval tools."""
    pass


@research_group.command("ingest-snapshots")
@click.argument("sqlite_path", default="./data/wayback_snapshots.db", type=click.Path(exists=True))
@click.option("--warehouse", default="./data/research_warehouse.duckdb", help="DuckDB warehouse path.")
def cmd_research_ingest_snapshots(sqlite_path: str, warehouse: str):
    """Ingest extracted snapshots into DuckDB warehouse."""
    from .research.warehouse import DuckDBWarehouse
    wh = DuckDBWarehouse(Path(warehouse))
    count = wh.ingest_sqlite_snapshots(Path(sqlite_path))
    wh.close()
    console.print(f"[bold green]Ingested {count} snapshots into DuckDB warehouse ({warehouse}).[/bold green]")


@research_group.command("ingest-catalog")
@click.argument("sqlite_path", default="./data/harvested_catalog.db", type=click.Path(exists=True))
@click.option("--warehouse", default="./data/research_warehouse.duckdb", help="DuckDB warehouse path.")
def cmd_research_ingest_catalog(sqlite_path: str, warehouse: str):
    """Ingest harvested catalog items into DuckDB warehouse."""
    from .research.warehouse import DuckDBWarehouse
    wh = DuckDBWarehouse(Path(warehouse))
    count = wh.ingest_sqlite_catalog(Path(sqlite_path))
    wh.close()
    console.print(f"[bold green]Ingested {count} catalog items into DuckDB warehouse ({warehouse}).[/bold green]")


@research_group.command("search")
@click.argument("query")
@click.option("-n", "--limit", default=5, help="Maximum evidence cards.")
@click.option("--warehouse", default="./data/research_warehouse.duckdb", help="DuckDB warehouse path.")
def cmd_research_search(query: str, limit: int, warehouse: str):
    """Execute sub-millisecond BM25 full-text query across warehouse."""
    from .research.warehouse import DuckDBWarehouse
    wh = DuckDBWarehouse(Path(warehouse))
    results = wh.search_bm25(query, limit=limit)
    wh.close()

    table = Table(title=f"DuckDB Research Warehouse Search: '{query}' ({len(results)} hits)", border_style="cyan")
    table.add_column("ID", style="bold yellow")
    table.add_column("Type", style="magenta")
    table.add_column("Timestamp", style="blue")
    table.add_column("Title / Source", style="white", max_width=35)
    table.add_column("Score", justify="right", style="green")

    for r in results:
        table.add_row(
            r["id"],
            r["source_type"],
            r["timestamp"] or "N/A",
            r["title"][:35],
            f"{r['score']:.3f}" if r.get("score") is not None else "N/A"
        )
    console.print(table)


@research_group.command("prompt")
@click.argument("query")
@click.option("-n", "--limit", default=3, help="Maximum evidence cards.")
@click.option("--warehouse", default="./data/research_warehouse.duckdb", help="DuckDB warehouse path.")
def cmd_research_prompt(query: str, limit: int, warehouse: str):
    """Generate LLM-ready research prompt with citation-backed evidence cards."""
    from .research.warehouse import DuckDBWarehouse
    from .research.context import LLMContextFormatter
    wh = DuckDBWarehouse(Path(warehouse))
    results = wh.search_bm25(query, limit=limit)
    wh.close()

    prompt = LLMContextFormatter.build_research_prompt(query, results)
    console.print(Panel(prompt, title="LLM Citation & Grounded Research Prompt", border_style="green"))


@research_group.command("stats")
@click.option("--warehouse", default="./data/research_warehouse.duckdb", help="DuckDB warehouse path.")
def cmd_research_stats(warehouse: str):
    """Inspect DuckDB research warehouse statistics."""
    from .research.warehouse import DuckDBWarehouse
    wh = DuckDBWarehouse(Path(warehouse))
    stats = wh.get_stats()
    wh.close()

    table = Table(title="DuckDB Research Warehouse Stats", border_style="cyan")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Total Records", str(stats["total_records"]))
    table.add_row("Total Estimated Tokens", f"{stats['total_tokens']:,}")
    for st, count in stats.get("sources", {}).items():
        table.add_row(f"Source: {st}", str(count))

    console.print(table)


# ==========================================
# Media Commands (Text, Audio, Video, Software)
# ==========================================

@main.group("media")
def media_group():
    """Access archived multimedia: full text/OCR, audio, video, and software."""
    pass


@media_group.command("inspect")
@click.argument("identifier")
def cmd_media_inspect(identifier: str):
    """Deep inspect multimedia assets, video streams, audio tracks, and emulation."""
    from .media import MediaAccessEngine

    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            engine = MediaAccessEngine(client)
            m = await engine.get_multimedia_manifest(identifier)

            console.print(Panel(
                f"[bold cyan]{m['title']}[/bold cyan] ({m['mediatype']})\n"
                f"URL: {m['details_url']}\n"
                f"Interactive Embed / Player: [bold green]{m['embed_url']}[/bold green]",
                title=f"Archive Multimedia Item: {identifier}",
                border_style="cyan"
            ))

            # Emulation
            if "emulation" in m:
                emu = m["emulation"]
                console.print(f"[bold magenta]🎮 In-Browser Emulation:[/bold magenta] Emulator: `{emu.get('emulator')}` | Play: {emu.get('play_url')}")

            # Videos
            if m["video"]:
                t = Table(title=f"Video Streams ({len(m['video'])})", border_style="magenta")
                t.add_column("Filename", style="bold yellow")
                t.add_column("Format", style="cyan")
                t.add_column("Resolution", style="green")
                t.add_column("Size", justify="right", style="blue")
                t.add_column("Stream URL", style="dim")
                for v in m["video"]:
                    res = f"{v.get('width')}x{v.get('height')}" if v.get('width') else "N/A"
                    t.add_row(v["name"], v["format"], res, f"{v['size_mb']} MB", v["download_url"])
                console.print(t)

            # Audio
            if m["audio"]:
                t = Table(title=f"Audio Tracks ({len(m['audio'])})", border_style="blue")
                t.add_column("Track Title", style="bold yellow")
                t.add_column("Format", style="cyan")
                t.add_column("Bitrate", style="green")
                t.add_column("Size", justify="right", style="blue")
                t.add_column("Audio URL", style="dim")
                for a in m["audio"]:
                    t.add_row(a["title"][:30], a["format"], str(a.get("bitrate") or "N/A"), f"{a['size_mb']} MB", a["download_url"])
                console.print(t)

            # Texts
            if m["texts"]:
                t = Table(title=f"Texts & Documents ({len(m['texts'])})", border_style="green")
                t.add_column("Document", style="bold yellow")
                t.add_column("Format", style="cyan")
                t.add_column("Size", justify="right", style="blue")
                t.add_column("Download URL", style="dim")
                for doc in m["texts"]:
                    t.add_row(doc["name"], doc["format"], f"{doc['size_mb']} MB", doc["download_url"])
                console.print(t)

            # Software
            if m["software"]:
                t = Table(title=f"Software Packages & Disks ({len(m['software'])})", border_style="yellow")
                t.add_column("Package Name", style="bold yellow")
                t.add_column("Format", style="cyan")
                t.add_column("Size", justify="right", style="blue")
                t.add_column("Download URL", style="dim")
                for s in m["software"]:
                    t.add_row(s["name"], s["format"], f"{s['size_mb']} MB", s["download_url"])
                console.print(t)

    asyncio.run(run())


@media_group.command("text")
@click.argument("identifier")
@click.option("-n", "--max-chars", default=3000, help="Maximum characters to read.")
def cmd_media_text(identifier: str, max_chars: int):
    """Read OCR or transcribed text content directly from a book/document."""
    from .media import MediaAccessEngine

    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            engine = MediaAccessEngine(client)
            res = await engine.read_full_text(identifier, max_chars=max_chars)
            if not res.get("found"):
                console.print(f"[red]{res.get('message')}[/red]")
            else:
                console.print(Panel(
                    res["text"],
                    title=f"OCR Text: {identifier} (file: {res['filename']} | {res['length_read']} chars)",
                    border_style="green"
                ))

    asyncio.run(run())


@media_group.command("book-search")
@click.argument("identifier")
@click.argument("query")
def cmd_media_book_search(identifier: str, query: str):
    """Search inside the full text of a book/document on Archive.org."""
    from .fulltext import FullTextEngine

    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            try:
                res = await FullTextEngine(client).search_inside(identifier, query)
            except ArchiveError:
                res = await FullTextEngine(client).search_inside_local(identifier, query)
            matches = res.get("matches", [])
            if not matches:
                console.print(f"[yellow]No matches found for '{query}' inside {identifier}.[/yellow]")
            else:
                table = Table(title=f"Book Search: '{query}' inside {identifier} ({res['total_matches']} hits)", border_style="cyan")
                table.add_column("Page", style="bold yellow", width=10)
                table.add_column("Matched Snippet", style="white")
                table.add_column("Page Viewer URL", style="dim")
                for m in matches:
                    table.add_row(str(m.get("page")), m.get("text", "")[:80], m.get("page_url"))
                console.print(table)

    asyncio.run(run())


# ==========================================
# Key Management & Automatic Rotation Commands
# ==========================================

@main.group("keys")
def keys_group():
    """Manage S3 access keys, monitor quotas, and control auto-rotation."""
    pass


@keys_group.command("list")
def cmd_keys_list():
    """List all S3 credentials in the pool with live quota exhaustion status."""
    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            statuses = client.rotation_manager.get_pool_status()
            t = Table(title=f"S3 Key Pool & Quota Status ({len(statuses)} keys)", border_style="cyan")
            t.add_column("#", style="dim", width=4)
            t.add_column("Label", style="cyan")
            t.add_column("Access Key", style="yellow")
            t.add_column("Active", justify="center")
            t.add_column("Status", justify="center")
            t.add_column("Daily Captures", justify="right", style="green")
            t.add_column("Available Slots", justify="right", style="blue")

            for s in statuses:
                active_str = "[bold green]YES[/bold green]" if s["is_active"] else "[dim]no[/dim]"
                if s["is_exhausted"]:
                    status_str = f"[bold red]EXHAUSTED[/bold red] ({s['exhaustion_reason'] or 'limit'})"
                else:
                    status_str = "[bold green]HEALTHY[/bold green]"

                t.add_row(
                    str(s["index"] + 1),
                    s["label"],
                    s["access_key"],
                    active_str,
                    status_str,
                    f"{s['daily_captures']:,} / {s['daily_limit']:,}",
                    str(s["available_slots"])
                )
            console.print(t)

    asyncio.run(run())


@keys_group.command("rotate")
@click.option("-r", "--reason", default="manual_rotation", help="Rotation justification reason.")
def cmd_keys_rotate(reason: str):
    """Manually rotate active credentials to the next available key in the pool."""
    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            old_creds = client.rotation_manager.current_credentials
            new_creds = await client.rotation_manager.rotate_to_next(reason=reason)
            console.print(f"[bold green]Rotated credentials successfully:[/bold green]")
            console.print(f"Previous Key: [yellow]{old_creds.label if old_creds else 'None'}[/yellow]")
            console.print(f"Active Key:   [bold cyan]{new_creds.label} ({new_creds.access_key[:4]}...{new_creds.access_key[-4:]})[/bold cyan]")

    asyncio.run(run())


@keys_group.command("refresh")
def cmd_keys_refresh():
    """Query live SPN2 capture quotas from /save/status/user for every key in the pool."""
    async def run():
        cfg = ArchivistConfig.load()
        async with ArchiveClient(cfg) as client:
            console.print("Refreshing live quotas for all pool keys...")
            for k in client.rotation_manager._pool:
                headers = {"Authorization": k.credentials.auth_header, "User-Agent": cfg.user_agent}
                try:
                    session = await client.get_session()
                    r = await session.get("https://web.archive.org/save/status/user", headers=headers, timeout=10.0)
                    if r.status_code == 200:
                        data = r.json()
                        await client.rotation_manager.update_key_quota(k.credentials.access_key, data)
                except Exception as err:
                    console.print(f"[yellow]Failed to refresh {k.credentials.label}: {err}[/yellow]")

            statuses = client.rotation_manager.get_pool_status()
            t = Table(title="Updated Key Pool Quotas", border_style="cyan")
            t.add_column("Label", style="cyan")
            t.add_column("Access Key", style="yellow")
            t.add_column("Used Today", justify="right", style="green")
            t.add_column("Daily Limit", justify="right", style="blue")
            t.add_column("Status", justify="center")

            for s in statuses:
                st = "[bold red]EXHAUSTED[/bold red]" if s["is_exhausted"] else "[bold green]HEALTHY[/bold green]"
                t.add_row(s["label"], s["access_key"], f"{s['daily_captures']:,}", f"{s['daily_limit']:,}", st)
            console.print(t)

    asyncio.run(run())


@keys_group.command("add")
@click.argument("access_key")
@click.argument("secret_key")
@click.option("-l", "--label", default=None, help="Descriptive label for this keypair.")
@click.option("--save/--no-save", default=True, help="Persist keypair to credentials file.")
def cmd_keys_add(access_key: str, secret_key: str, label: Optional[str], save: bool):
    """Add a new S3 access key and secret keypair to the rotation pool."""
    cfg = ArchivistConfig.load()
    lbl = label or f"key_{len(cfg.credentials) + 1}"
    creds = S3Credentials(access_key=access_key.strip(), secret_key=secret_key.strip(), label=lbl)
    cfg.credentials.append(creds)
    if save:
        cfg.save_credentials()
        console.print(f"[bold green]Saved key '{lbl}' to credentials file.[/bold green]")
    console.print(f"[bold cyan]Added key '{lbl}' ({access_key[:4]}...{access_key[-4:]}) to rotation pool.[/bold cyan]")


@main.group("text")
def text_group():
    """Full-text retrieval: cross-corpus discovery, pin-point, diagnostics."""
    pass


@text_group.command("find")
@click.argument("query")
@click.option("-n", "--size", default=10, help="Number of hits (default: 10).")
@click.option("-s", "--scope", default=None, help="Scope/filter applied by the FTS service.")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
def cmd_text_find(query: str, size: int, scope: Optional[str], as_json: bool):
    """Search the text of documents across the archive (not just metadata)."""
    from .fulltext import FullTextEngine

    async def run():
        async with ArchiveClient(ArchivistConfig.load()) as client:
            res = await FullTextEngine(client).search(query, size=size, scope=scope)
            if as_json:
                console.print_json(json.dumps(res, default=str))
                return
            console.print(f"[bold]Total matches:[/bold] {res['total']}  "
                          f"[dim](took {res['took']} ms)[/dim]")
            for hit in res["hits"]:
                body = hit["snippets"][0]["text"][:400] if hit["snippets"] else "[dim]no snippet[/dim]"
                console.print(Panel(body,
                                    title=f"{hit['identifier']}  score={hit['score']:.2f}",
                                    border_style="cyan"))

    asyncio.run(run())


@text_group.command("inside")
@click.argument("identifier")
@click.argument("query")
@click.option("-n", "--limit", default=5, help="Maximum matches to show.")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
def cmd_text_inside(identifier: str, query: str, limit: int, as_json: bool):
    """Locate a passage inside one item, with page numbers and boxes."""
    from .fulltext import FullTextEngine

    async def run():
        async with ArchiveClient(ArchivistConfig.load()) as client:
            try:
                res = await FullTextEngine(client).search_inside(identifier, query, limit=limit)
            except ArchiveError as err:
                console.print(f"[bold red]{err}[/bold red]")
                return
            if as_json:
                console.print_json(json.dumps(res, default=str))
                return
            console.print(f"[bold]{res['identifier']}[/bold] - {res['total_matches']} matches "
                          f"(showing {len(res['matches'])}) via {res['derivative']['format']}")
            if res.get("warning"):
                console.print(f"[yellow]warning:[/yellow] {res['warning']}")
            for m in res["matches"]:
                console.print(Panel(m["text"][:400],
                                    title=f"page {m['page']}  {m['page_url']}",
                                    border_style="green"))

    asyncio.run(run())


@text_group.command("availability")
@click.argument("identifier")
def cmd_text_availability(identifier: str):
    """Report whether an item's text can be searched, and why not."""
    from .fulltext import FullTextEngine

    async def run():
        async with ArchiveClient(ArchivistConfig.load()) as client:
            info = await FullTextEngine(client).availability(identifier)
            console.print_json(json.dumps(info, default=str))

    asyncio.run(run())


@text_group.command("lending-status")
@click.argument("identifier")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
def cmd_text_lending_status(identifier: str, as_json: bool):
    """Query live digital lending/borrowing status from BookReaderJSIA.php."""
    from .fulltext import FullTextEngine

    async def run():
        async with ArchiveClient(ArchivistConfig.load()) as client:
            res = await FullTextEngine(client).lending_status(identifier)
            if as_json:
                console.print_json(json.dumps(res, default=str))
                return

            t = Table(title=f"Lending Status: {identifier} (Observed as: {res.get('observed_as')})", border_style="cyan")
            t.add_column("Property", style="cyan")
            t.add_column("Value", style="green")

            t.add_row("Is Lendable", str(res.get("is_lendable")))
            t.add_row("Is Readable", str(res.get("is_readable")))
            t.add_row("Available to Browse (1-hr)", str(res.get("available_to_browse")))
            t.add_row("Available to Borrow (14-day)", str(res.get("available_to_borrow")))
            t.add_row("Available on Waitlist", str(res.get("available_to_waitlist")))
            t.add_row("Available Copies", f"{res.get('available_lendable_copies')} / {res.get('max_borrowable_copies')}")
            t.add_row("Users on Waitlist", str(res.get("users_on_waitlist")))
            t.add_row("Print-Disabled Only", str(res.get("is_printdisabled")))
            t.add_row("Login Required", str(res.get("is_login_required")))
            t.add_row("Active Loan Held", str(res.get("loan_held")))
            if res.get("loans_url"):
                t.add_row("Loans Action URL", str(res.get("loans_url")))
            console.print(t)

    asyncio.run(run())


@text_group.command("quote")
@click.argument("identifier")
@click.argument("terms", nargs=-1, required=True)
@click.option("--delay", default=1.2, help="Seconds between index queries (default: 1.2).")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
def cmd_text_quote(identifier: str, terms: tuple, delay: float, as_json: bool):
    """Quote an item's text -- including lending-restricted items.

    The per-item search endpoint cannot serve a restricted item: it fetches the
    OCR file internally and is refused. The full-text index is a different
    service and can. Passages are verbatim, and the page is reported with its
    scope: for a restricted item it is one aggregate page for the whole item,
    not the passage's own.
    """
    from .fulltext import FullTextEngine

    async def run():
        async with ArchiveClient(ArchivistConfig.load()) as client:
            try:
                res = await FullTextEngine(client).quote_from_index(
                    identifier, list(terms), delay=delay
                )
            except ArchiveError as err:
                console.print(f"[bold red]{err}[/bold red]")
                return
            if as_json:
                console.print_json(json.dumps(res, default=str))
                return
            item = res["item"] or {}
            scope = item.get("page_scope")
            caveat = (
                "  [yellow](item-level aggregate page, not this passage's)[/yellow]"
                if scope == "item_aggregate"
                else ""
            )
            console.print(
                f"[bold]{identifier}[/bold] - {res['passage_count']} passages  "
                f"tier={item.get('tier')}  page={item.get('page')}{caveat}"
            )
            console.print(f"[dim]{item.get('tier_basis')}[/dim]")
            for p in res["passages"]:
                console.print(Panel(p["text"][:400],
                                    title=f"[{p['term']}]",
                                    border_style="green"))

    asyncio.run(run())


@text_group.command("diagnose")
@click.argument("query")
def cmd_text_diagnose(query: str):
    """Show which terms match, and which one kills a zero-result query."""
    from .fulltext import FullTextEngine

    async def run():
        async with ArchiveClient(ArchivistConfig.load()) as client:
            report = await FullTextEngine(client).query_diagnostics(query)
            console.print_json(json.dumps(report, default=str))

    asyncio.run(run())


@text_group.command("harvest")
@click.argument("identifiers", nargs=-1, required=True)
@click.option("--pause", default=1.0, help="Seconds between downloads (default: 1.0).")
@click.option("--max-docs", default=None, type=int, help="Stop after N downloads.")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
def cmd_text_harvest(identifiers: tuple, pause: float, max_docs: Optional[int], as_json: bool):
    """Download text into the local store, once, politely. Then analyse offline."""
    from .fulltext import FullTextEngine

    async def run():
        async with ArchiveClient(ArchivistConfig.load()) as client:
            report = await FullTextEngine(client).harvest_text(
                list(identifiers), pause=pause, max_docs=max_docs
            )
            if as_json:
                console.print_json(json.dumps(report, default=str))
                return
            console.print(f"[bold green]harvested {len(report['harvested'])}[/bold green]  "
                          f"[dim]already stored {len(report['already_stored'])}, "
                          f"failed {len(report['failed'])}[/dim]")
            for h in report["harvested"]:
                console.print(f"  {h['identifier']}  {h['chars']} chars  ({h['format']})")
            for f in report["failed"][:10]:
                console.print(f"  [red]failed[/red] {f['identifier']}: {f['reason']}")
            console.print(f"[dim]store: {report['store']}[/dim]")

    asyncio.run(run())


@text_group.command("store")
def cmd_text_store():
    """Show what is held locally for offline analysis."""
    from .fulltext import FullTextEngine

    async def run():
        async with ArchiveClient(ArchivistConfig.load()) as client:
            store = FullTextEngine(client).store
            stats = store.stats()
            console.print(f"[bold]{stats['documents']}[/bold] documents, "
                          f"{stats['total_chars']:,} chars at [cyan]{stats['root']}[/cyan]")
            table = Table(show_lines=False)
            table.add_column("identifier", overflow="fold")
            table.add_column("chars", justify="right")
            table.add_column("format")
            table.add_column("fetched")
            for e in store.entries():
                table.add_row(e["identifier"], f"{e['chars']:,}", e.get("format") or "",
                              (e.get("fetched_at") or "")[:19])
            console.print(table)

    asyncio.run(run())


@text_group.command("reuse")
@click.option("-i", "--identifier", default=None, help="Source item (must be in the corpus).")
@click.option("-f", "--file", "text_file", default=None, type=click.Path(exists=True),
              help="Read the source passage from a local text file.")
@click.option("-n", "--candidates", default=6, help="Maximum candidates to score.")
@click.option("--min-tokens", default=20, help="Minimum shared run in tokens (default: 20).")
@click.option("--db", "db_path", default=None, help="Corpus database path.")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
def cmd_text_reuse(identifier: Optional[str], text_file: Optional[str], candidates: int,
                   min_tokens: int, db_path: Optional[str], as_json: bool):
    """Find documents sharing phrasing with a passage or item (with null model)."""
    from .analysis import CorpusAnalysis
    from .corpus import CorpusDB

    if not identifier and not text_file:
        console.print("[bold red]Provide --identifier or --file[/bold red]")
        return

    source = Path(text_file).read_text(encoding="utf-8", errors="ignore") if text_file else None
    analysis = CorpusAnalysis(CorpusDB(db_path or "./data/corpus.duckdb"))
    try:
        res = analysis.reuse(text=source, identifier=identifier,
                             min_tokens=min_tokens, candidates=candidates)
    except ValueError as err:
        console.print(f"[bold red]{err}[/bold red]")
        return

    if as_json:
        console.print_json(json.dumps(res, default=str))
        return

    table = Table(title=f"Shared text ({res['mode']})")
    table.add_column("identifier", overflow="fold")
    table.add_column("longest run", justify="right")
    table.add_column("shared", justify="right")
    table.add_column("null", justify="right")
    for r in res["results"]:
        table.add_row(r["identifier"], str(r["longest_run_tokens"]),
                      str(r.get("shared_shingles", "-")), str(r.get("null_shingles", "-")))
    console.print(table)
    for r in res["results"][:3]:
        console.print(Panel(r["shared_passage"][:400],
                            title=f"{r['identifier']} - {r['longest_run_tokens']} tokens",
                            border_style="magenta"))


@text_group.command("variants")
@click.argument("identifier")
@click.option("--confirm-tokens", default=40, help="Shared-run threshold to accept a sibling.")
@click.option("--offline", is_flag=True, help="Local corpus only (no requests).")
@click.option("--db", "db_path", default=None, help="Corpus database path.")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
def cmd_text_variants(identifier: str, confirm_tokens: int, offline: bool,
                      db_path: Optional[str], as_json: bool):
    """List sibling uploads of a document (same work, different files)."""
    from .analysis import CorpusAnalysis
    from .corpus import CorpusDB
    from .fulltext import FullTextEngine

    async def run():
        async with ArchiveClient(ArchivistConfig.load()) as client:
            analysis = CorpusAnalysis(CorpusDB(db_path or "./data/corpus.duckdb"),
                                      FullTextEngine(client))
            try:
                res = await analysis.variants(identifier, confirm_tokens=confirm_tokens,
                                              offline=offline)
            except ValueError as err:
                console.print(f"[bold red]{err}[/bold red]")
                return
            if as_json:
                console.print_json(json.dumps(res, default=str))
                return
            console.print(f"[bold]{identifier}[/bold]  [dim](discovery: {res['discovery']})[/dim]")
            for v in res["variants"]:
                console.print(f"  {v['identifier']}  run={v['longest_run_tokens']} tok  "
                              f"containment={v['containment']}")

    asyncio.run(run())


@text_group.command("reconcile")
@click.argument("identifier")
@click.option("--confirm-tokens", default=40, help="Shared-run threshold to accept a sibling.")
@click.option("--min-run", default=25, help="Minimum differing run to report.")
@click.option("--offline", is_flag=True, help="Local corpus only (no requests).")
@click.option("--db", "db_path", default=None, help="Corpus database path.")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
def cmd_text_reconcile(identifier: str, confirm_tokens: int, min_run: int, offline: bool,
                       db_path: Optional[str], as_json: bool):
    """Compare the copies of one document and report what each adds."""
    from .analysis import CorpusAnalysis
    from .corpus import CorpusDB
    from .fulltext import FullTextEngine

    async def run():
        async with ArchiveClient(ArchivistConfig.load()) as client:
            analysis = CorpusAnalysis(CorpusDB(db_path or "./data/corpus.duckdb"),
                                      FullTextEngine(client))
            try:
                res = await analysis.reconcile(identifier, confirm_tokens=confirm_tokens,
                                               min_run=min_run, offline=offline)
            except ValueError as err:
                console.print(f"[bold red]{err}[/bold red]")
                return
            if as_json:
                console.print_json(json.dumps(res, default=str))
                return
            if "error" in res:
                console.print(f"[bold red]{res['error']}[/bold red]")
                return

            table = Table(title=f"Copies of {identifier}")
            table.add_column("identifier", overflow="fold")
            table.add_column("chars", justify="right")
            table.add_column("ocr", justify="right")
            for cp in res["copies"]:
                table.add_row(cp["identifier"], str(cp["chars"]), str(cp["ocr_quality"]))
            console.print(table)
            console.print(f"[bold]base:[/bold] {res['base']}  [dim]({res['base_reason']})[/dim]")
            for d in res["differences"]:
                console.print(f"[bold]vs {d['against']}[/bold]  base-only={d['base_only_tokens']} tok  "
                              f"variant-only={d['variant_only_tokens']} tok")
                for r in d["candidate_excisions"]:
                    console.print(Panel(r["snippet"][:300],
                                        title=f"candidate excision - {r['length_tokens']} tokens @{r['start_token']}",
                                        border_style="yellow"))
                for r in d["candidate_additions"]:
                    console.print(Panel(r["snippet"][:300],
                                        title=f"candidate addition - {r['length_tokens']} tokens @{r['start_token']}",
                                        border_style="cyan"))

    asyncio.run(run())

@main.group("corpus")
def corpus_group():
    """Local columnar corpus: build once, then query at memory speed."""
    pass


def _corpus(path: Optional[str] = None):
    from .corpus import CorpusDB
    return CorpusDB(path or str(Path("./data/corpus.duckdb")))


@corpus_group.command("build")
@click.option("--store", "store_dir", default="./data/text_cache", help="Text store root.")
@click.option("--db", "db_path", default=None, help="Corpus database path.")
@click.option("--indexes", is_flag=True,
              help="Also create ART indexes. Measured 6x larger and slower; off by default.")
def cmd_corpus_build(store_dir: str, db_path: Optional[str], indexes: bool):
    """Materialise the local text store into the columnar corpus."""
    from .textstore import TextStore

    store = TextStore(Path(store_dir))
    db = _corpus(db_path)
    result = db.build_from_store(store, indexes=indexes)
    console.print(f"[bold green]indexed {result['documents']} documents[/bold green]  "
                  f"[dim]{result['stats']}[/dim]")
    for d in result["detail"]:
        console.print(f"  {d['identifier'][:52]:<54} tok={d['tokens']:>8} "
                      f"shingles={d['shingles']:>8} fps={d['fingerprints']:>7}")


@corpus_group.command("stats")
@click.option("--db", "db_path", default=None)
def cmd_corpus_stats(db_path: Optional[str]):
    """Row counts and size of the local corpus."""
    db = _corpus(db_path)
    console.print_json(json.dumps(db.stats(), default=str))


@corpus_group.command("similar")
@click.argument("identifier")
@click.option("-n", "--limit", default=10)
@click.option("--db", "db_path", default=None)
def cmd_corpus_similar(identifier: str, limit: int, db_path: Optional[str]):
    """Documents whose text overlaps this one (fingerprint containment)."""
    db = _corpus(db_path)
    table = Table(title=f"Similar to {identifier}")
    table.add_column("identifier", overflow="fold")
    table.add_column("containment", justify="right")
    for row in db.similar(identifier, limit=limit):
        table.add_row(row["identifier"], str(row["containment"]))
    console.print(table)


@corpus_group.command("shared")
@click.argument("a")
@click.argument("b")
@click.option("--min-tokens", default=50)
@click.option("--db", "db_path", default=None)
def cmd_corpus_shared(a: str, b: str, min_tokens: int, db_path: Optional[str]):
    """Longest verbatim passages two documents share."""
    db = _corpus(db_path)
    for run in db.shared_runs(a, b, min_tokens=min_tokens):
        console.print(Panel(run["snippet"],
                            title=f"{run['length_tokens']} tokens  a@{run['start_a']} b@{run['start_b']}",
                            border_style="magenta"))


@corpus_group.command("diff")
@click.argument("base")
@click.argument("other")
@click.option("--min-run", default=25)
@click.option("--db", "db_path", default=None)
def cmd_corpus_diff(base: str, other: str, min_run: int, db_path: Optional[str]):
    """Regions present in BASE and absent from OTHER."""
    db = _corpus(db_path)
    for run in db.diff_runs(base, other, min_run=min_run):
        console.print(Panel(run["snippet"],
                            title=f"{run['length_tokens']} tokens @{run['start_token']}",
                            border_style="yellow"))


@corpus_group.command("phrase")
@click.argument("phrase")
@click.option("-n", "--limit", default=10)
@click.option("--db", "db_path", default=None)
def cmd_corpus_phrase(phrase: str, limit: int, db_path: Optional[str]):
    """Exact consecutive phrase search across the local corpus."""
    db = _corpus(db_path)
    for row in db.phrase_search(phrase, limit=limit):
        console.print(f"  {row['identifier']}  x{row['occurrences']}")


@corpus_group.command("sql")
@click.argument("query")
@click.option("--db", "db_path", default=None)
def cmd_corpus_sql(query: str, db_path: Optional[str]):
    """Run an arbitrary read query against the corpus (escape hatch)."""
    db = _corpus(db_path)
    rows = db.sql(query)
    if rows:
        table = Table()
        for col in db.con.description:
            table.add_column(str(col[0]))
        for r in rows[:500]:
            table.add_row(*[str(v)[:80] for v in r])
        console.print(table)
    else:
        console.print("[dim]no rows[/dim]")


if __name__ == "__main__":
    main()
