"""Asynchronous high-throughput pipeline execution engine with backpressure and state management."""

from __future__ import annotations
import asyncio
import datetime
import logging
import time
import uuid
from pathlib import Path
from typing import Callable, Dict, Any, List, Optional

from ..client import ArchiveClient
from .schema import PipelineConfig
from .state import PipelineStateManager
from .stages import (
    BaseStage, SourceStage, TransformStage, SinkStage, PipelineItem, create_stage
)

logger = logging.getLogger("archivist.pipeline.engine")


class PipelineEngine:
    """Executes multi-stage asynchronous data pipelines with checkpointed resumption."""

    def __init__(self, config: PipelineConfig, client: Optional[ArchiveClient] = None):
        self.config = config
        self.client = client or ArchiveClient()
        self.state = PipelineStateManager(Path(config.state_db or "./data/pipeline_state.db"))
        self.run_id: str = f"run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        self._running = False
        self._stats: Dict[str, Any] = {
            "produced": 0,
            "transformed": 0,
            "consumed": 0,
            "failed": 0,
            "stage_metrics": {}
        }
        self._progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None

    def set_progress_callback(self, cb: Callable[[Dict[str, Any]], None]) -> None:
        self._progress_callback = cb

    def _notify_progress(self) -> None:
        if self._progress_callback:
            self._progress_callback(self._stats)

    async def run(self, resume_run_id: Optional[str] = None) -> Dict[str, Any]:
        """Execute the pipeline graph until source is exhausted and all queues are flushed."""
        if resume_run_id:
            self.run_id = resume_run_id

        await self.state.connect()
        await self.state.create_run(self.run_id, self.config.name, self.config.model_dump())

        # Instantiate stages
        source_stage: SourceStage = create_stage(self.config.source, self.client, self.state, self.run_id)  # type: ignore
        transforms: List[TransformStage] = [
            create_stage(t, self.client, self.state, self.run_id) for t in self.config.transforms  # type: ignore
        ]
        sinks: List[SinkStage] = [
            create_stage(s, self.client, self.state, self.run_id) for s in self.config.sinks  # type: ignore
        ]

        # Build intermediate queues between stages
        # Queue 0: Source -> Transform 0
        # Queue i: Transform i-1 -> Transform i
        # Queue last: Transform N-1 -> Sinks
        num_queues = len(transforms) + 1
        queues: List[asyncio.Queue[Optional[PipelineItem]]] = [
            asyncio.Queue(maxsize=self.config.batch_size * 2) for _ in range(num_queues)
        ]

        self._running = True
        worker_tasks: List[asyncio.Task] = []

        # 1. Source producer coroutine
        async def source_producer():
            try:
                count = 0
                async for item in source_stage.produce():
                    if not self._running:
                        break
                    await queues[0].put(item)
                    count += 1
                    self._stats["produced"] = count
                    self._notify_progress()
                    if self.config.max_items and count >= self.config.max_items:
                        break
            except Exception as e:
                logger.error(f"Source stage failed: {e}", exc_info=True)
                await self.state.record_item_failure(self.run_id, "SOURCE", source_stage.name, str(e))
            finally:
                # Signal completion to next queue
                for _ in range(self.config.concurrency):
                    await queues[0].put(None)

        producer_task = asyncio.create_task(source_producer())
        worker_tasks.append(producer_task)

        # 2. Transform stage workers
        for stage_idx, transform_stage in enumerate(transforms):
            in_q = queues[stage_idx]
            out_q = queues[stage_idx + 1]

            async def transform_worker(stage=transform_stage, in_queue=in_q, out_queue=out_q):
                while self._running:
                    item = await in_queue.get()
                    if item is None:
                        in_queue.task_done()
                        break

                    try:
                        processed_item = await stage.process(item)
                        if processed_item is not None:
                            await self.state.record_item_success(self.run_id, item.key, stage.name, processed_item.data)
                            await out_queue.put(processed_item)
                            self._stats["transformed"] += 1
                        else:
                            # Filtered out
                            pass
                    except Exception as err:
                        logger.error(f"Error in {stage.name} on item {item.key}: {err}")
                        self._stats["failed"] += 1
                        await self.state.record_item_failure(self.run_id, item.key, stage.name, str(err), item.data)
                    finally:
                        in_queue.task_done()
                        self._notify_progress()

            # Spawn concurrent workers for this transform stage
            stage_workers = [asyncio.create_task(transform_worker()) for _ in range(self.config.concurrency)]

            async def sentinel_propagator(workers=stage_workers, out_queue=out_q):
                await asyncio.gather(*workers)
                for _ in range(self.config.concurrency):
                    await out_queue.put(None)

            worker_tasks.extend(stage_workers)
            worker_tasks.append(asyncio.create_task(sentinel_propagator()))

        # 3. Sink stage consumers
        sink_in_queue = queues[-1]

        async def sink_worker():
            while self._running:
                item = await sink_in_queue.get()
                if item is None:
                    sink_in_queue.task_done()
                    break

                try:
                    for sink in sinks:
                        await sink.consume(item)
                        await self.state.record_item_success(self.run_id, item.key, sink.name, item.data)
                    self._stats["consumed"] += 1
                except Exception as err:
                    logger.error(f"Error in sink on item {item.key}: {err}")
                    self._stats["failed"] += 1
                    await self.state.record_item_failure(self.run_id, item.key, "SINKS", str(err), item.data)
                finally:
                    sink_in_queue.task_done()
                    self._notify_progress()

        sink_tasks = [asyncio.create_task(sink_worker()) for _ in range(self.config.concurrency)]
        worker_tasks.extend(sink_tasks)

        # Wait for all queues and tasks to drain
        error_msg = None
        try:
            await producer_task
            for q in queues:
                await q.join()
            await asyncio.gather(*sink_tasks)
            for sink in sinks:
                await sink.finalize()
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Pipeline execution error: {e}", exc_info=True)
        finally:
            self._running = False
            status = "failed" if error_msg else "completed"
            await self.state.finish_run(self.run_id, status=status, error=error_msg)
            run_stats = await self.state.get_run_stats(self.run_id)
            await self.state.close()

        return {
            "run_id": self.run_id,
            "pipeline_name": self.config.name,
            "status": "failed" if error_msg else "completed",
            "stats": self._stats,
            "persistent_stats": run_stats
        }

    def stop(self) -> None:
        """Signal pipeline to stop processing further items."""
        self._running = False
