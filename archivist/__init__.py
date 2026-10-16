"""Archivist Engine — Unified Archive.org & Wayback Machine Pipeline System."""

from .config import ArchivistConfig, S3Credentials
from .client import ArchiveClient, ArchiveError, ArchiveRateLimitError
from .search import SearchEngine, QueryBuilder
from .wayback import WaybackEngine
from .item import ItemManager, ArchiveFileInfo
from .media import MediaAccessEngine
from .pipeline import (
    PipelineConfig, StageConfig, PipelineEngine, PipelineStateManager, PipelineItem
)

__version__ = "1.0.0"
__all__ = [
    "ArchivistConfig",
    "S3Credentials",
    "ArchiveClient",
    "ArchiveError",
    "ArchiveRateLimitError",
    "SearchEngine",
    "QueryBuilder",
    "WaybackEngine",
    "ItemManager",
    "ArchiveFileInfo",
    "MediaAccessEngine",
    "PipelineConfig",
    "StageConfig",
    "PipelineEngine",
    "PipelineStateManager",
    "PipelineItem",
]
