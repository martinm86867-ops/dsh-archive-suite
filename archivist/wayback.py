"""Wayback Machine preservation, CDX query, and mirror generation engine."""

from __future__ import annotations
import asyncio
import difflib
import logging
import re
from pathlib import Path
from typing import AsyncIterator, Callable, Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse, unquote

from bs4 import BeautifulSoup

from .client import ArchiveClient, ArchiveError

logger = logging.getLogger("archivist.wayback")


class WaybackEngine:
    """Orchestrates Wayback Machine preservation, CDX crawling, and offline mirror reconstruction."""

    def __init__(self, client: ArchiveClient):
        self.client = client

    async def save_urls(
        self,
        urls: List[str],
        wait_completion: bool = True,
        concurrency: int = 2,
        progress_callback: Optional[Callable[[str, str, Dict[str, Any]], None]] = None
    ) -> List[Dict[str, Any]]:
        """Submit a batch of URLs to Save Page Now 2 (SPN2) with concurrency control."""
        sem = asyncio.Semaphore(concurrency)
        results: List[Dict[str, Any]] = []

        async def worker(url: str):
            async with sem:
                try:
                    res = await self.client.save_page_now(url, capture_all=True)
                    job_id = res.get("job_id")
                    if wait_completion and job_id:
                        final_status = await self.client.wait_spn_completion(job_id)
                        combined = {**res, **final_status}
                        results.append(combined)
                        if progress_callback:
                            progress_callback(url, combined.get("status", "unknown"), combined)
                    else:
                        results.append(res)
                        if progress_callback:
                            progress_callback(url, res.get("status", "submitted"), res)
                except Exception as e:
                    err_res = {"url": url, "status": "failed", "error": str(e)}
                    results.append(err_res)
                    if progress_callback:
                        progress_callback(url, "failed", err_res)

        tasks = [worker(u) for u in urls]
        await asyncio.gather(*tasks)
        return results

    async def crawl_cdx(
        self,
        url_prefix: str,
        match_type: str = "prefix",
        limit: int = 1000,
        status_codes: Optional[List[int]] = None,
        from_ts: Optional[str] = None,
        to_ts: Optional[str] = None,
        dedup_digest: bool = True
    ) -> List[Dict[str, str]]:
        """Retrieve index of all historical captures for a domain or path."""
        collapse = "digest" if dedup_digest else None
        return await self.client.cdx_search(
            url=url_prefix,
            match_type=match_type,
            limit=limit,
            status_codes=status_codes or [200],
            from_ts=from_ts,
            to_ts=to_ts,
            collapse=collapse
        )

    async def mirror_site(
        self,
        target_url: str,
        output_dir: Path,
        timestamp: Optional[str] = None,
        max_pages: int = 50,
        concurrency: int = 3,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> List[Path]:
        """Download historical snapshots of a domain/path and build an offline directory mirror."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Querying CDX for {target_url} (max {max_pages} pages)...")
        snapshots = await self.crawl_cdx(
            url_prefix=target_url,
            match_type="prefix",
            limit=max_pages,
            to_ts=timestamp
        )

        if not snapshots:
            logger.warning(f"No CDX snapshots found for {target_url}")
            return []

        # Map each original URL to its most recent snapshot timestamp
        latest_snapshots: Dict[str, Dict[str, str]] = {}
        for snap in snapshots:
            orig = snap.get("original", "")
            ts = snap.get("timestamp", "")
            if not orig or not ts:
                continue
            if orig not in latest_snapshots or ts > latest_snapshots[orig]["timestamp"]:
                latest_snapshots[orig] = snap

        sem = asyncio.Semaphore(concurrency)
        downloaded: List[Path] = []
        total = len(latest_snapshots)
        completed = 0

        async def download_snapshot(orig_url: str, snap: Dict[str, str]):
            nonlocal completed
            ts = snap["timestamp"]
            parsed = urlparse(orig_url)
            path = unquote(parsed.path).lstrip("/")
            if not path or path.endswith("/"):
                path += "index.html"

            # Avoid path traversal
            dest_file = (output_dir / parsed.netloc / path).resolve()
            if not str(dest_file).startswith(str(output_dir.resolve())):
                return

            dest_file.parent.mkdir(parents=True, exist_ok=True)

            async with sem:
                try:
                    content = await self.client.get_wayback_snapshot(orig_url, ts, raw=True)
                    dest_file.write_bytes(content)
                    downloaded.append(dest_file)
                except Exception as e:
                    logger.error(f"Failed to fetch {orig_url}@{ts}: {e}")
                finally:
                    completed += 1
                    if progress_callback:
                        progress_callback(orig_url, completed, total)

        tasks = [download_snapshot(u, s) for u, s in latest_snapshots.items()]
        await asyncio.gather(*tasks)
        return downloaded

    async def diff_snapshots(
        self,
        url: str,
        timestamp_a: str,
        timestamp_b: str
    ) -> Dict[str, Any]:
        """Fetch two snapshots of a URL and calculate text-level differences across time."""
        raw_a = await self.client.get_wayback_snapshot(url, timestamp_a, raw=True)
        raw_b = await self.client.get_wayback_snapshot(url, timestamp_b, raw=True)

        text_a = BeautifulSoup(raw_a, "html.parser").get_text(separator="\n", strip=True)
        text_b = BeautifulSoup(raw_b, "html.parser").get_text(separator="\n", strip=True)

        lines_a = text_a.splitlines()
        lines_b = text_b.splitlines()

        diff = list(difflib.unified_diff(
            lines_a, lines_b,
            fromfile=f"{url} ({timestamp_a})",
            tofile=f"{url} ({timestamp_b})",
            lineterm=""
        ))

        return {
            "url": url,
            "timestamp_a": timestamp_a,
            "timestamp_b": timestamp_b,
            "diff_lines": diff,
            "diff_text": "\n".join(diff),
            "chars_a": len(text_a),
            "chars_b": len(text_b),
            "diff_count": len(diff)
        }
