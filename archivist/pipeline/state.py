"""SQLite-backed persistent state manager for pipeline checkpointing and recovery."""

from __future__ import annotations
import json
import logging
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Set
import aiosqlite

logger = logging.getLogger("archivist.pipeline.state")


class PipelineStateManager:
    """Manages persistent SQLite state for multi-stage pipelines to guarantee fault-tolerant resumption."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        if self._db is None:
            self._db = await aiosqlite.connect(str(self.db_path))
            await self._db.execute("PRAGMA journal_mode = WAL;")
            await self._db.execute("PRAGMA synchronous = NORMAL;")
            await self._init_schema()

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    async def _init_schema(self) -> None:
        assert self._db is not None
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                config_json TEXT,
                start_time REAL NOT NULL,
                end_time REAL,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS pipeline_items (
                run_id TEXT NOT NULL,
                item_key TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                status TEXT NOT NULL,
                data_json TEXT,
                result_json TEXT,
                error TEXT,
                updated_at REAL NOT NULL,
                PRIMARY KEY (run_id, item_key, stage_name)
            );

            CREATE INDEX IF NOT EXISTS idx_items_lookup ON pipeline_items(run_id, stage_name, status);
        """)
        await self._db.commit()

    async def create_run(self, run_id: str, name: str, config_dict: Dict[str, Any]) -> None:
        assert self._db is not None
        await self._db.execute(
            """INSERT OR REPLACE INTO pipeline_runs (run_id, name, status, config_json, start_time)
               VALUES (?, ?, 'running', ?, ?)""",
            (run_id, name, json.dumps(config_dict), time.time())
        )
        await self._db.commit()

    async def finish_run(self, run_id: str, status: str = "completed", error: Optional[str] = None) -> None:
        assert self._db is not None
        await self._db.execute(
            """UPDATE pipeline_runs SET status = ?, end_time = ?, error = ? WHERE run_id = ?""",
            (status, time.time(), error, run_id)
        )
        await self._db.commit()

    async def get_completed_keys(self, run_id: str, stage_name: str) -> Set[str]:
        """Fetch set of item keys already completed for this stage in this run."""
        assert self._db is not None
        async with self._db.execute(
            """SELECT item_key FROM pipeline_items
               WHERE run_id = ? AND stage_name = ? AND status = 'completed'""",
            (run_id, stage_name)
        ) as cursor:
            rows = await cursor.fetchall()
            return {row[0] for row in rows}

    async def record_item_success(
        self,
        run_id: str,
        item_key: str,
        stage_name: str,
        result_data: Optional[Dict[str, Any]] = None
    ) -> None:
        assert self._db is not None
        res_str = json.dumps(result_data) if result_data else None
        await self._db.execute(
            """INSERT OR REPLACE INTO pipeline_items
               (run_id, item_key, stage_name, status, result_json, error, updated_at)
               VALUES (?, ?, ?, 'completed', ?, NULL, ?)""",
            (run_id, item_key, stage_name, res_str, time.time())
        )
        await self._db.commit()

    async def record_item_failure(
        self,
        run_id: str,
        item_key: str,
        stage_name: str,
        error_msg: str,
        data: Optional[Dict[str, Any]] = None
    ) -> None:
        assert self._db is not None
        data_str = json.dumps(data) if data else None
        await self._db.execute(
            """INSERT OR REPLACE INTO pipeline_items
               (run_id, item_key, stage_name, status, data_json, error, updated_at)
               VALUES (?, ?, ?, 'failed', ?, ?, ?)""",
            (run_id, item_key, stage_name, data_str, error_msg, time.time())
        )
        await self._db.commit()

    async def get_run_stats(self, run_id: str) -> Dict[str, Any]:
        """Aggregate execution statistics for a run."""
        assert self._db is not None
        stats: Dict[str, Any] = {"stages": {}}

        async with self._db.execute(
            "SELECT status, start_time, end_time, error FROM pipeline_runs WHERE run_id = ?",
            (run_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                stats["status"] = row[0]
                stats["start_time"] = row[1]
                stats["end_time"] = row[2]
                stats["duration_sec"] = (row[2] or time.time()) - row[1]
                stats["error"] = row[3]

        async with self._db.execute(
            """SELECT stage_name, status, COUNT(*)
               FROM pipeline_items WHERE run_id = ? GROUP BY stage_name, status""",
            (run_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            for stage, status, count in rows:
                if stage not in stats["stages"]:
                    stats["stages"][stage] = {"completed": 0, "failed": 0, "total": 0}
                if status == "completed":
                    stats["stages"][stage]["completed"] += count
                elif status == "failed":
                    stats["stages"][stage]["failed"] += count
                stats["stages"][stage]["total"] += count

        return stats
