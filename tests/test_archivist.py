"""Automated test suite for Archivist Engine."""

import asyncio
import os
import tempfile
from pathlib import Path
import pytest

from archivist.config import ArchivistConfig, S3Credentials
from archivist.search import QueryBuilder
from archivist.client import TokenBucketRateLimiter
from archivist.pipeline.state import PipelineStateManager
from archivist.pipeline.schema import PipelineConfig, StageConfig
from archivist.pipeline.engine import PipelineEngine


def test_config_credentials():
    cfg = ArchivistConfig.load()
    assert len(cfg.credentials) >= 1
    current = cfg.current_credentials
    assert current is not None
    assert current.access_key.isalnum()
    assert current.auth_header.startswith("LOW ")


def test_query_builder():
    qb = QueryBuilder()
    q = qb.collection("nasa").mediatype("image").year(2023).build()
    assert "collection:(nasa)" in q
    assert "mediatype:(image)" in q
    assert "year:2023" in q


@pytest.mark.asyncio
async def test_token_bucket_rate_limiter():
    limiter = TokenBucketRateLimiter(rate=20.0, capacity=2.0)
    # Acquire tokens without error
    await limiter.acquire(1.0)
    await limiter.acquire(1.0)
    assert limiter.tokens <= 1.0


@pytest.mark.asyncio
async def test_pipeline_state_manager(tmp_path):
    db_file = tmp_path / "test_state.db"
    mgr = PipelineStateManager(db_file)
    await mgr.connect()

    run_id = "test_run_123"
    await mgr.create_run(run_id, "test_pipeline", {"foo": "bar"})

    # Record item success
    await mgr.record_item_success(run_id, "item_1", "stage_filter", {"status": "ok"})
    completed = await mgr.get_completed_keys(run_id, "stage_filter")
    assert "item_1" in completed

    # Record item failure
    await mgr.record_item_failure(run_id, "item_2", "stage_filter", "error details")

    stats = await mgr.get_run_stats(run_id)
    assert stats["status"] == "running"
    assert stats["stages"]["stage_filter"]["completed"] == 1
    assert stats["stages"]["stage_filter"]["failed"] == 1

    await mgr.finish_run(run_id, status="completed")
    stats_finished = await mgr.get_run_stats(run_id)
    assert stats_finished["status"] == "completed"

    await mgr.close()


@pytest.mark.asyncio
async def test_pipeline_engine_execution(tmp_path):
    db_state = tmp_path / "state.db"
    out_jsonl = tmp_path / "out.jsonl"

    cfg = PipelineConfig(
        name="test_engine_pipeline",
        concurrency=2,
        state_db=str(db_state),
        source=StageConfig(
            type="url_list",
            name="test_urls",
            params={"urls": ["https://example.com/1", "https://example.com/2", "https://example.com/1"]}
        ),
        transforms=[
            StageConfig(type="deduplicate", name="dedup", params={"field": "url"})
        ],
        sinks=[
            StageConfig(type="jsonl", name="sink_file", params={"file_path": str(out_jsonl)})
        ]
    )

    engine = PipelineEngine(cfg)
    res = await engine.run()

    assert res["status"] == "completed"
    assert res["stats"]["produced"] == 3
    # After deduplication: exactly 2 items transformed and consumed
    assert res["stats"]["transformed"] == 2
    assert res["stats"]["consumed"] == 2

    # Check output jsonl file
    lines = out_jsonl.read_text().splitlines()
    assert len(lines) == 2


def test_duckdb_research_warehouse(tmp_path):
    import sqlite3
    from archivist.research.warehouse import DuckDBWarehouse
    from archivist.research.context import LLMContextFormatter

    db_path = tmp_path / "research.duckdb"
    with DuckDBWarehouse(db_path) as wh:
        wh.insert_record(
            record_id="rec_1",
            source_type="catalog_item",
            source_key="apollo_11",
            content="Neil Armstrong and Buzz Aldrin lunar module descent transcripts and flight data.",
            title="Apollo 11 Flight Log",
            timestamp="19690720"
        )
        wh.rebuild_fts_index()

        stats = wh.get_stats()
        assert stats["total_records"] == 1

        results = wh.search_bm25("Armstrong lunar")
        assert len(results) == 1
        assert results[0]["id"] == "rec_1"

        prompt = LLMContextFormatter.build_research_prompt("Tell me about the lunar module", results)
        assert "[EVIDENCE CARD: rec_1]" in prompt
        assert "Neil Armstrong" in prompt

        # Verify SQLite table name validation rejects nonexistent/injected table names
        sqlite_path = tmp_path / "src.db"
        conn = sqlite3.connect(str(sqlite_path))
        conn.execute("CREATE TABLE catalog_items (key TEXT, data_json TEXT)")
        conn.commit()
        conn.close()

        with pytest.raises(ValueError, match="does not exist"):
            wh.ingest_sqlite_catalog(sqlite_path, table="catalog_items; DROP TABLE catalog_items;--")

    assert wh._con is None


@pytest.mark.asyncio
async def test_media_access_engine():
    from archivist.media import MediaAccessEngine
    from archivist.client import ArchiveClient

    async with ArchiveClient() as client:
        media = MediaAccessEngine(client)
        # Test software manifest parsing
        soft_m = await media.get_multimedia_manifest("msdos_NetHack--_1992")
        assert soft_m["mediatype"] == "software"
        assert soft_m["emulation"]["supported"] is True
        assert soft_m["emulation"]["emulator"] == "dosbox"
        assert len(soft_m["software"]) >= 1

        # Test video manifest parsing
        vid_m = await media.get_multimedia_manifest("jadeed19")
        assert vid_m["mediatype"] == "movies"
        assert len(vid_m["video"]) >= 1
        assert vid_m["video"][0]["format"] in ("Ogg Video", "512Kb MPEG4", "Windows Media")


@pytest.mark.asyncio
async def test_key_rotation_manager(tmp_path):
    from archivist.rotation import KeyRotationManager
    from archivist.config import ArchivistConfig, S3Credentials

    cfg = ArchivistConfig(
        data_dir=tmp_path,
        credentials=[
            S3Credentials(access_key="KEY1", secret_key="SEC1", label="k1"),
            S3Credentials(access_key="KEY2", secret_key="SEC2", label="k2"),
        ]
    )

    mgr = KeyRotationManager(cfg)
    assert mgr.current_credentials.access_key == "KEY1"

    # Rotate to next key
    next_creds = await mgr.rotate_to_next(reason="quota_test")
    assert next_creds.access_key == "KEY2"
    assert mgr.current_credentials.access_key == "KEY2"

    statuses = mgr.get_pool_status()
    assert len(statuses) == 2
    assert statuses[0]["is_exhausted"] is True
    assert statuses[1]["is_active"] is True



