"""Pipeline stage abstractions and built-in source, transform, and sink stages."""

from __future__ import annotations
import asyncio
import fnmatch
import hashlib
import json
import logging
from pathlib import Path
from typing import AsyncIterator, Callable, Dict, Any, List, Optional, Set, Union
from urllib.parse import urlparse, unquote

import aiosqlite
from bs4 import BeautifulSoup

from ..client import ArchiveClient, ArchiveError
from .state import PipelineStateManager
from .schema import StageConfig

logger = logging.getLogger("archivist.pipeline.stages")


class PipelineItem:
    """Carries record payload and metadata through pipeline stages."""

    def __init__(self, key: str, data: Dict[str, Any], raw_bytes: Optional[bytes] = None):
        self.key = key
        self.data = data
        self.raw_bytes = raw_bytes
        self.tags: Set[str] = set()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "data": self.data,
            "tags": list(self.tags)
        }


class BaseStage:
    def __init__(
        self,
        config: StageConfig,
        client: ArchiveClient,
        state: PipelineStateManager,
        run_id: str
    ):
        self.config = config
        self.name = config.name or config.type
        self.params = config.params
        self.client = client
        self.state = state
        self.run_id = run_id


class SourceStage(BaseStage):
    """Generates initial stream of pipeline items."""

    async def produce(self) -> AsyncIterator[PipelineItem]:
        raise NotImplementedError


class TransformStage(BaseStage):
    """Processes or filters an incoming pipeline item."""

    async def process(self, item: PipelineItem) -> Optional[PipelineItem]:
        raise NotImplementedError


class SinkStage(BaseStage):
    """Consumes an incoming pipeline item to storage or external system."""

    async def consume(self, item: PipelineItem) -> None:
        raise NotImplementedError

    async def finalize(self) -> None:
        pass


# ==========================================
# Sources
# ==========================================

class SearchSource(SourceStage):
    """Queries Archive.org catalog and emits item records."""

    async def produce(self) -> AsyncIterator[PipelineItem]:
        query = self.params.get("query", "*:*")
        fields = self.params.get("fields", ["identifier", "title", "mediatype", "collection", "date", "downloads"])
        max_items = self.params.get("max_items", 100)
        use_scrape = self.params.get("use_scrape", False)

        from ..search import SearchEngine
        searcher = SearchEngine(self.client)

        completed = await self.state.get_completed_keys(self.run_id, self.name)
        count = 0

        if use_scrape:
            async for item in searcher.scrape_all(query=query, fields=fields, max_results=max_items):
                ident = item.get("identifier", f"item_{count}")
                if ident in completed:
                    continue
                yield PipelineItem(key=ident, data=item)
                count += 1
                if max_items and count >= max_items:
                    break
        else:
            async for doc in searcher.iterate_search(query=query, fields=fields, max_results=max_items):
                ident = doc.get("identifier", f"item_{count}")
                if ident in completed:
                    continue
                yield PipelineItem(key=ident, data=doc)
                count += 1
                if max_items and count >= max_items:
                    break


class CDXSource(SourceStage):
    """Queries Wayback Machine CDX server and emits snapshot capture records."""

    async def produce(self) -> AsyncIterator[PipelineItem]:
        url = self.params.get("url")
        if not url:
            raise ValueError("CDXSource requires 'url' parameter.")

        match_type = self.params.get("match_type", "prefix")
        limit = self.params.get("limit", 1000)
        status_codes = self.params.get("status_codes", [200])
        from_ts = self.params.get("from_ts")
        to_ts = self.params.get("to_ts")
        collapse = self.params.get("collapse", "digest")

        completed = await self.state.get_completed_keys(self.run_id, self.name)
        records = await self.client.cdx_search(
            url=url,
            match_type=match_type,
            limit=limit,
            status_codes=status_codes,
            from_ts=from_ts,
            to_ts=to_ts,
            collapse=collapse
        )

        for rec in records:
            orig = rec.get("original", "")
            ts = rec.get("timestamp", "")
            key = f"{orig}@{ts}"
            if key in completed:
                continue
            yield PipelineItem(key=key, data=rec)


class UrlListSource(SourceStage):
    """Emits URLs from inline config or external text file."""

    async def produce(self) -> AsyncIterator[PipelineItem]:
        urls = list(self.params.get("urls", []))
        file_path = self.params.get("file")
        if file_path:
            p = Path(file_path)
            if p.is_file():
                urls.extend([line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()])

        completed = await self.state.get_completed_keys(self.run_id, self.name)
        for url in urls:
            if not url or url.startswith("#"):
                continue
            if url in completed:
                continue
            yield PipelineItem(key=url, data={"url": url})


class LocalFilesSource(SourceStage):
    """Scans local filesystem path and emits file records."""

    async def produce(self) -> AsyncIterator[PipelineItem]:
        dir_path = Path(self.params.get("path", "."))
        pattern = self.params.get("pattern", "*")
        compute_hashes = self.params.get("compute_hashes", True)

        completed = await self.state.get_completed_keys(self.run_id, self.name)

        for p in dir_path.rglob("*"):
            if not p.is_file():
                continue
            rel = str(p.relative_to(dir_path))
            if not fnmatch.fnmatch(rel, pattern):
                continue
            if rel in completed:
                continue

            file_info: Dict[str, Any] = {
                "local_path": str(p.resolve()),
                "relative_path": rel,
                "size": p.stat().st_size,
                "mtime": p.stat().st_mtime
            }

            if compute_hashes:
                h_md5 = hashlib.md5()
                h_sha1 = hashlib.sha1()
                with open(p, "rb") as f:
                    while chunk := f.read(512 * 1024):
                        h_md5.update(chunk)
                        h_sha1.update(chunk)
                file_info["md5"] = h_md5.hexdigest()
                file_info["sha1"] = h_sha1.hexdigest()

            yield PipelineItem(key=rel, data=file_info)


class CSVSource(SourceStage):
    """Streams records directly from a CSV dataset file (e.g. lending-browsable.csv)."""

    async def produce(self) -> AsyncIterator[PipelineItem]:
        file_path_raw = self.params.get("file_path") or self.params.get("file")
        if not file_path_raw:
            raise ValueError("CSVSource requires 'file_path' or 'file' parameter.")

        file_path = Path(file_path_raw)
        if not file_path.is_file():
            raise ValueError(f"CSVSource file not found: {file_path}")

        key_column = self.params.get("key_column", "identifier")
        delimiter = self.params.get("delimiter", ",")
        max_rows = self.params.get("max_rows") or self.params.get("max_items")
        filter_col = self.params.get("filter_column")
        filter_val = self.params.get("filter_value")

        import csv
        completed = await self.state.get_completed_keys(self.run_id, self.name)
        yielded = 0

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            for row in reader:
                if filter_col and filter_val:
                    if str(row.get(filter_col)) != str(filter_val):
                        continue
                key = row.get(key_column) or f"row_{yielded}"
                if key in completed:
                    continue
                yield PipelineItem(key=key, data=row)
                yielded += 1
                if max_rows and yielded >= max_rows:
                    break


# ==========================================
# Transforms
# ==========================================

class FilterTransform(TransformStage):
    """Filters items based on predicates."""

    async def process(self, item: PipelineItem) -> Optional[PipelineItem]:
        predicates = self.params.get("predicates", {})
        for field, expected in predicates.items():
            val = item.data.get(field)
            if isinstance(expected, list):
                if val not in expected:
                    return None
            elif str(val) != str(expected):
                return None

        # Size range filter
        min_size = self.params.get("min_size")
        max_size = self.params.get("max_size")
        size = item.data.get("size")
        if size is not None:
            if min_size is not None and int(size) < min_size:
                return None
            if max_size is not None and int(size) > max_size:
                return None

        return item


class DeduplicateTransform(TransformStage):
    """Deduplicates items on specified field key."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._seen: Set[str] = set()

    async def process(self, item: PipelineItem) -> Optional[PipelineItem]:
        key_field = self.params.get("field", "key")
        val = str(item.data.get(key_field, item.key))
        if val in self._seen:
            return None
        self._seen.add(val)
        return item


class DownloadTransform(TransformStage):
    """Downloads remote content for item."""

    async def process(self, item: PipelineItem) -> Optional[PipelineItem]:
        dest_dir = self.params.get("destination_dir")

        # 1. Wayback snapshot download
        if "original" in item.data and "timestamp" in item.data:
            orig = item.data["original"]
            ts = item.data["timestamp"]
            raw = self.params.get("raw", True)
            try:
                content = await self.client.get_wayback_snapshot(orig, ts, raw=raw)
                item.raw_bytes = content
                item.data["download_size"] = len(content)
                if dest_dir:
                    parsed = urlparse(orig)
                    path = unquote(parsed.path).lstrip("/") or "index.html"
                    if path.endswith("/"):
                        path += "index.html"
                    dest_file = Path(dest_dir) / parsed.netloc / path
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    dest_file.write_bytes(content)
                    item.data["saved_path"] = str(dest_file)
            except Exception as e:
                logger.error(f"Failed to download snapshot {item.key}: {e}")
                return None

        # 2. Archive.org item file download
        elif "identifier" in item.data and "filename" in item.data:
            ident = item.data["identifier"]
            fn = item.data["filename"]
            if dest_dir:
                dest_file = Path(dest_dir) / ident / fn
                try:
                    await self.client.download_file(ident, fn, dest_file)
                    item.data["saved_path"] = str(dest_file)
                except Exception as e:
                    logger.error(f"Failed to download {ident}/{fn}: {e}")
                    return None

        return item


class HtmlTextTransform(TransformStage):
    """Extracts clean plain-text from HTML snapshots."""

    async def process(self, item: PipelineItem) -> Optional[PipelineItem]:
        html_bytes = item.raw_bytes
        if not html_bytes and "saved_path" in item.data:
            p = Path(item.data["saved_path"])
            if p.is_file():
                html_bytes = p.read_bytes()

        if html_bytes:
            soup = BeautifulSoup(html_bytes, "html.parser")
            for tag in soup(["script", "style", "nav", "footer"]):
                tag.decompose()
            text = soup.get_text(separator="\n", strip=True)
            item.data["extracted_text"] = text
            item.data["text_length"] = len(text)

        return item


# ==========================================
# Sinks
# ==========================================

class SPNSink(SinkStage):
    """Preserves URLs to Wayback Machine Save Page Now 2 (SPN2)."""

    async def consume(self, item: PipelineItem) -> None:
        url = item.data.get("url") or item.data.get("original") or item.key
        wait_completion = self.params.get("wait_completion", True)

        res = await self.client.save_page_now(
            url=url,
            capture_all=self.params.get("capture_all", True),
            capture_outlinks=self.params.get("capture_outlinks", False),
            capture_screenshot=self.params.get("capture_screenshot", False)
        )

        job_id = res.get("job_id")
        if wait_completion and job_id:
            status_res = await self.client.wait_spn_completion(job_id, timeout=self.params.get("timeout", 60.0))
            item.data["spn_result"] = {**res, **status_res}
        else:
            item.data["spn_result"] = res


class IAS3UploadSink(SinkStage):
    """Uploads local files into Archive.org item via IAS3."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._created_items: Set[str] = set()

    async def consume(self, item: PipelineItem) -> None:
        ident = self.params.get("identifier") or item.data.get("target_identifier")
        if not ident:
            raise ValueError("IAS3UploadSink requires 'identifier' in params or target_identifier in item.")

        if ident not in self._created_items:
            try:
                await self.client.get_metadata(ident)
            except ArchiveError as e:
                if e.status_code == 404:
                    title = self.params.get("title", f"Archive {ident}")
                    await self.client.create_item(
                        identifier=ident,
                        title=title,
                        mediatype=self.params.get("mediatype"),
                        collection=self.params.get("collection")
                    )
                else:
                    raise
            self._created_items.add(ident)

        local_path = item.data.get("local_path") or item.data.get("saved_path")
        if not local_path:
            raise ValueError(f"No local file path found in item {item.key}")

        remote_name = item.data.get("relative_path") or Path(local_path).name
        res = await self.client.upload_file(
            identifier=ident,
            filename=remote_name,
            file_path=Path(local_path)
        )
        item.data["upload_result"] = res


class SQLiteSink(SinkStage):
    """Persists pipeline items into an indexed SQLite table."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db: Optional[aiosqlite.Connection] = None

    async def _get_conn(self) -> aiosqlite.Connection:
        if self._db is None:
            db_path = Path(self.params.get("db_path", "./data/pipeline_output.db"))
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._db = await aiosqlite.connect(str(db_path))
            table = self.params.get("table", "records")
            await self._db.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    key TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL,
                    extracted_text TEXT,
                    created_at REAL NOT NULL
                )
            """)
            await self._db.commit()
        return self._db

    async def consume(self, item: PipelineItem) -> None:
        db = await self._get_conn()
        table = self.params.get("table", "records")
        text = item.data.get("extracted_text")
        import time
        await db.execute(
            f"INSERT OR REPLACE INTO {table} (key, data_json, extracted_text, created_at) VALUES (?, ?, ?, ?)",
            (item.key, json.dumps(item.data), text, time.time())
        )
        await db.commit()

    async def finalize(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None


class JSONLSink(SinkStage):
    """Appends pipeline records to a JSON Lines file."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._file = None

    async def consume(self, item: PipelineItem) -> None:
        out_path = Path(self.params.get("file_path", "./data/output.jsonl"))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(item.to_dict()) + "\n")


class ConsoleSink(SinkStage):
    """Prints processed items to standard output/logs."""

    async def consume(self, item: PipelineItem) -> None:
        logger.info(f"[STAGE: {self.name}] Item {item.key}: {json.dumps(item.data, default=str)[:160]}...")


class DuckDBSink(SinkStage):
    """Streams pipeline records directly into the analytical DuckDB research warehouse."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from ..research.warehouse import DuckDBWarehouse
        db_path = Path(self.params.get("db_path", "./data/research_warehouse.duckdb"))
        self._warehouse = DuckDBWarehouse(db_path)

    async def consume(self, item: PipelineItem) -> None:
        source_type = self.params.get("source_type") or ("wayback_snapshot" if "original" in item.data else "catalog_item")
        source_key = item.data.get("original") or item.data.get("identifier") or item.key
        text = item.data.get("extracted_text") or item.data.get("description") or json.dumps(item.data)
        title = item.data.get("title") or source_key
        ts = item.data.get("timestamp") or str(item.data.get("date", "")).replace("-", "")
        archive_url = item.data.get("archive_url")
        if not archive_url:
            if "timestamp" in item.data and "original" in item.data:
                archive_url = f"https://web.archive.org/web/{ts}/{item.data['original']}"
            elif "identifier" in item.data:
                archive_url = f"https://archive.org/details/{item.data['identifier']}"

        self._warehouse.insert_record(
            record_id=item.key,
            source_type=source_type,
            source_key=source_key,
            content=text,
            title=title,
            timestamp=ts,
            archive_url=archive_url,
            metadata=item.data
        )

    async def finalize(self) -> None:
        self._warehouse.rebuild_fts_index()
        self._warehouse.close()


STAGE_REGISTRY: Dict[str, type] = {
    # Sources
    "search": SearchSource,
    "cdx": CDXSource,
    "url_list": UrlListSource,
    "local_files": LocalFilesSource,
    "csv": CSVSource,
    # Transforms
    "filter": FilterTransform,
    "deduplicate": DeduplicateTransform,
    "download": DownloadTransform,
    "html_text": HtmlTextTransform,
    # Sinks
    "spn": SPNSink,
    "ias3_upload": IAS3UploadSink,
    "sqlite": SQLiteSink,
    "duckdb": DuckDBSink,
    "jsonl": JSONLSink,
    "console": ConsoleSink
}


def create_stage(
    stage_config: StageConfig,
    client: ArchiveClient,
    state: PipelineStateManager,
    run_id: str
) -> Union[SourceStage, TransformStage, SinkStage]:
    stage_cls = STAGE_REGISTRY.get(stage_config.type)
    if not stage_cls:
        raise ValueError(f"Unknown stage type '{stage_config.type}'. Available: {list(STAGE_REGISTRY.keys())}")
    return stage_cls(stage_config, client, state, run_id)
