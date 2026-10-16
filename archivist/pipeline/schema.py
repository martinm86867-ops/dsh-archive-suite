"""Pipeline configuration schemas using Pydantic."""

from __future__ import annotations
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel, Field


class StageConfig(BaseModel):
    """Specification for a single pipeline stage."""
    type: str
    name: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    """Specification for a complete pipeline execution graph."""
    name: str = "archive-pipeline"
    description: Optional[str] = None
    concurrency: int = 4
    batch_size: int = 20
    max_items: Optional[int] = None
    resume: bool = True
    state_db: Optional[str] = "./data/pipeline_state.db"
    source: StageConfig
    transforms: List[StageConfig] = Field(default_factory=list)
    sinks: List[StageConfig] = Field(default_factory=list)
