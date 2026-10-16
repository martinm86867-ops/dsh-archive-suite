"""Multi-modal media access engine for Archive.org texts, audio, video, and software."""

from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from urllib.parse import quote

from .client import ArchiveClient, ArchiveError

logger = logging.getLogger("archivist.media")

TEXT_FORMATS = {"Text PDF", "DjVuTXT", "DjVu", "EPUB", "Kindle", "Daisy", "Plain Text"}
AUDIO_FORMATS = {"VBR MP3", "MP3", "128Kbps MP3", "64Kbps MP3", "Flac", "Ogg Vorbis", "Shorten", "WAV"}
VIDEO_FORMATS = {"512Kb MPEG4", "h.264", "MPEG4", "WebM", "Ogg Video", "Flash Video", "Windows Media"}
SUBTITLE_FORMATS = {"SubRip", "WebVTT"}
SOFTWARE_FORMATS = {"ZIP", "ISO", "ROM", "Floppy Disk Image", "Archive BitTorrent", "Commodore 64", "Amiga", "Apple II"}


class MediaAccessEngine:
    """Provides specialized extraction and access to texts, audio, video, and software."""

    def __init__(self, client: ArchiveClient):
        self.client = client

    async def get_multimedia_manifest(self, identifier: str) -> Dict[str, Any]:
        """Categorize an item's files into texts, audio, video, software, and emulation streams."""
        meta = await self.client.get_metadata(identifier)
        m = meta.get("metadata", {})
        raw_files = meta.get("files", [])
        mediatype = m.get("mediatype", "unknown")
        server = meta.get("server", "archive.org")

        manifest: Dict[str, Any] = {
            "identifier": identifier,
            "title": m.get("title", identifier),
            "mediatype": mediatype,
            "collection": m.get("collection"),
            "date": m.get("date"),
            "creator": m.get("creator"),
            "description": m.get("description", ""),
            "embed_url": f"https://archive.org/embed/{identifier}",
            "details_url": f"https://archive.org/details/{identifier}",
            "texts": [],
            "audio": [],
            "video": [],
            "software": [],
            "subtitles": []
        }

        # Emulation detection
        emulator = m.get("emulator")
        emulator_ext = m.get("emulator_ext")
        emulator_start = m.get("emulator_start")
        if emulator or mediatype == "software":
            manifest["emulation"] = {
                "supported": bool(emulator or mediatype == "software"),
                "emulator": emulator or "in-browser",
                "emulator_ext": emulator_ext,
                "emulator_start": emulator_start,
                "play_url": f"https://archive.org/embed/{identifier}"
            }

        base_dl = f"https://archive.org/download/{identifier}"

        for f in raw_files:
            fmt = f.get("format", "")
            name = f.get("name", "")
            size = int(f.get("size", 0))
            dl_url = f"{base_dl}/{quote(name)}"

            file_entry = {
                "name": name,
                "format": fmt,
                "size_bytes": size,
                "size_mb": round(size / (1024 * 1024), 2),
                "download_url": dl_url,
                "md5": f.get("md5"),
                "sha1": f.get("sha1")
            }

            if fmt in TEXT_FORMATS or name.endswith(("_djvu.txt", ".pdf", ".epub", ".txt")):
                manifest["texts"].append(file_entry)

            if fmt in AUDIO_FORMATS or name.endswith((".mp3", ".flac", ".ogg", ".wav")):
                audio_entry = {
                    **file_entry,
                    "title": f.get("title") or name,
                    "artist": f.get("artist") or m.get("creator"),
                    "album": f.get("album"),
                    "length_sec": float(f.get("length", 0)) if f.get("length") else None,
                    "bitrate": f.get("bitrate")
                }
                manifest["audio"].append(audio_entry)

            if fmt in VIDEO_FORMATS or name.endswith((".mp4", ".webm", ".ogv")):
                video_entry = {
                    **file_entry,
                    "height": f.get("height"),
                    "width": f.get("width"),
                    "length_sec": float(f.get("length", 0)) if f.get("length") else None
                }
                manifest["video"].append(video_entry)

            if fmt in SUBTITLE_FORMATS or name.endswith((".srt", ".vtt")):
                manifest["subtitles"].append(file_entry)

            if mediatype == "software" or fmt in SOFTWARE_FORMATS or name.endswith((".iso", ".rom", ".dsk", ".img", ".bin")):
                manifest["software"].append(file_entry)

        return manifest

    async def read_full_text(self, identifier: str, max_chars: int = 8000) -> Dict[str, Any]:
        """Read OCR or transcribed text content directly from a document or book item."""
        meta = await self.client.get_metadata(identifier)
        files = meta.get("files", [])

        # Find best text candidate: _djvu.txt > .txt > XML
        text_filename = None
        for f in files:
            name = f.get("name", "")
            if name.endswith("_djvu.txt"):
                text_filename = name
                break
            elif name.endswith(".txt") and not name.endswith("_meta.txt"):
                text_filename = name

        if not text_filename:
            return {
                "identifier": identifier,
                "found": False,
                "message": f"No plain text or OCR file found for {identifier}. Check PDF/DjVu files list."
            }

        url = f"https://archive.org/download/{identifier}/{quote(text_filename)}"
        session = await self.client.get_session()
        headers = self.client._get_auth_header()

        # Stream first max_chars
        content = ""
        async with session.stream("GET", url, headers=headers) as response:
            if response.status_code != 200:
                raise ArchiveError(f"Failed to fetch text: HTTP {response.status_code}")
            async for chunk in response.aiter_text():
                content += chunk
                if len(content) >= max_chars:
                    content = content[:max_chars]
                    break

        return {
            "identifier": identifier,
            "filename": text_filename,
            "found": True,
            "length_read": len(content),
            "text": content,
            "full_file_url": url
        }
