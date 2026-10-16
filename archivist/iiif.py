"""IIIF Image and Canvas extraction engine for digitized books and manuscripts."""

from __future__ import annotations
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import quote

from .client import ArchiveClient, ArchiveError

logger = logging.getLogger("archivist.iiif")


class IIIFEngine:
    """Extracts high-resolution book pages, figures, and cropped diagrams via IIIF API."""

    BASE_IIIF_URL = "https://iiif.archive.org/image/iiif/3"

    def __init__(self, client: ArchiveClient):
        self.client = client

    async def get_page_image_url(
        self,
        identifier: str,
        page_number: int = 1,
        region: str = "full",
        size: str = "max",
        rotation: int = 0,
        fmt: str = "jpg"
    ) -> Dict[str, Any]:
        """Construct the canonical IIIF Image API 3.0 URL for a digitized book page."""
        meta = await self.client.get_metadata(identifier)
        files = meta.get("files", [])

        # Look for JP2 tar/zip or individual page scans
        leaf_pattern = f"n{page_number - 1:04d}" if page_number > 0 else "n0000"
        target_file = None
        for f in files:
            name = f.get("name", "")
            if name.endswith(".jp2") or name.endswith(".jpg"):
                if leaf_pattern in name or f"_{page_number:04d}" in name:
                    target_file = name
                    break

        if not target_file:
            # Standard Archive.org single-page naming convention
            target_file = f"{identifier}_{page_number - 1:04d}.jp2"

        encoded_id = quote(f"{identifier}/{target_file}", safe="")
        iiif_url = f"{self.BASE_IIIF_URL}/{encoded_id}/{region}/{size}/{rotation}/default.{fmt}"

        return {
            "identifier": identifier,
            "page_number": page_number,
            "target_file": target_file,
            "iiif_url": iiif_url,
            "region": region,
            "size": size,
            "details_page": f"https://archive.org/details/{identifier}/page/n{page_number - 1}"
        }

    async def download_figure_crop(
        self,
        identifier: str,
        page_number: int,
        x: int,
        y: int,
        width: int,
        height: int,
        destination_path: Path
    ) -> Path:
        """Download an exact bounding-box cropped figure or diagram from a book page."""
        region = f"{x},{y},{width},{height}"
        info = await self.get_page_image_url(identifier, page_number=page_number, region=region)
        url = info["iiif_url"]

        dest = Path(destination_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        session = await self.client.get_session()
        response = await session.get(url)
        if response.status_code != 200:
            raise ArchiveError(f"IIIF crop failed: HTTP {response.status_code}")

        dest.write_bytes(response.content)
        return dest
