"""Archive.org item and file operations manager."""

from __future__ import annotations
import asyncio
import fnmatch
import logging
from pathlib import Path
from typing import Callable, Dict, Any, List, Optional, Set
from dataclasses import dataclass

from .client import ArchiveClient, ArchiveError

logger = logging.getLogger("archivist.item")


@dataclass
class ArchiveFileInfo:
    name: str
    size: int
    format: Optional[str] = None
    md5: Optional[str] = None
    sha1: Optional[str] = None
    crc32: Optional[str] = None
    mtime: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ArchiveFileInfo":
        return cls(
            name=data.get("name", ""),
            size=int(data.get("size", 0)),
            format=data.get("format"),
            md5=data.get("md5"),
            sha1=data.get("sha1"),
            crc32=data.get("crc32"),
            mtime=data.get("mtime")
        )


class ItemManager:
    """High-level item manager for bulk uploads, folder syncing, and metadata inspection."""

    def __init__(self, client: ArchiveClient):
        self.client = client

    async def get_item(self, identifier: str) -> Dict[str, Any]:
        """Fetch item metadata and files structure."""
        return await self.client.get_metadata(identifier)

    async def list_files(self, identifier: str) -> List[ArchiveFileInfo]:
        """List all files in an item with typed metadata."""
        meta = await self.client.get_metadata(identifier)
        files = meta.get("files", [])
        return [ArchiveFileInfo.from_dict(f) for f in files]

    async def upload_directory(
        self,
        identifier: str,
        local_dir: Path,
        item_title: Optional[str] = None,
        mediatype: Optional[str] = None,
        collection: Optional[str] = None,
        concurrency: int = 2,
        include_pattern: Optional[str] = None,
        exclude_pattern: Optional[str] = None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> Dict[str, Any]:
        """Upload an entire local directory tree to an Archive.org item."""
        local_dir = Path(local_dir)
        if not local_dir.is_dir():
            raise ArchiveError(f"Directory not found: {local_dir}")

        # Check if item exists, create if not
        try:
            await self.client.get_metadata(identifier)
            logger.info(f"Item {identifier} exists. Uploading files into existing item.")
        except ArchiveError as e:
            if e.status_code == 404:
                title = item_title or f"Archive of {local_dir.name}"
                logger.info(f"Creating new item: {identifier} ('{title}')")
                await self.client.create_item(
                    identifier=identifier,
                    title=title,
                    mediatype=mediatype,
                    collection=collection
                )
            else:
                raise

        # Discover files to upload
        files_to_upload: List[Path] = []
        for p in local_dir.rglob("*"):
            if not p.is_file():
                continue
            rel_path = str(p.relative_to(local_dir))
            if include_pattern and not fnmatch.fnmatch(rel_path, include_pattern):
                continue
            if exclude_pattern and fnmatch.fnmatch(rel_path, exclude_pattern):
                continue
            files_to_upload.append(p)

        sem = asyncio.Semaphore(concurrency)
        results: List[Dict[str, Any]] = []

        async def upload_task(file_path: Path):
            remote_name = str(file_path.relative_to(local_dir)).replace("\\", "/")
            async with sem:
                def prog(cur: int, tot: int):
                    if progress_callback:
                        progress_callback(remote_name, cur, tot)

                res = await self.client.upload_file(
                    identifier=identifier,
                    filename=remote_name,
                    file_path=file_path,
                    progress_callback=prog
                )
                results.append(res)

        tasks = [upload_task(f) for f in files_to_upload]
        await asyncio.gather(*tasks)

        return {
            "identifier": identifier,
            "uploaded_files_count": len(results),
            "files": results
        }

    async def download_item(
        self,
        identifier: str,
        destination_dir: Path,
        concurrency: int = 4,
        include_formats: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> List[Path]:
        """Download all or filtered files from an Archive.org item."""
        destination_dir = Path(destination_dir)
        destination_dir.mkdir(parents=True, exist_ok=True)

        files = await self.list_files(identifier)
        if include_formats:
            formats_set = {f.lower() for f in include_formats}
            files = [f for f in files if (f.format and f.format.lower() in formats_set)]

        sem = asyncio.Semaphore(concurrency)
        downloaded_paths: List[Path] = []

        async def download_task(file_info: ArchiveFileInfo):
            dest = destination_dir / file_info.name
            async with sem:
                def prog(cur: int, tot: int):
                    if progress_callback:
                        progress_callback(file_info.name, cur, tot)

                path = await self.client.download_file(
                    identifier=identifier,
                    filename=file_info.name,
                    destination=dest,
                    expected_size=file_info.size,
                    progress_callback=prog
                )
                downloaded_paths.append(path)

        tasks = [download_task(f) for f in files]
        await asyncio.gather(*tasks)
        return downloaded_paths
