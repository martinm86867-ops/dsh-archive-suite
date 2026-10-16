"""FastAPI Web Dashboard and REST API for Archivist Engine."""

from __future__ import annotations
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional
import json

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from ..config import ArchivistConfig
from ..client import ArchiveClient
from ..search import SearchEngine
from ..wayback import WaybackEngine
from ..item import ItemManager
from ..pipeline.schema import PipelineConfig
from ..pipeline.engine import PipelineEngine
from ..pipeline.state import PipelineStateManager

config = ArchivistConfig.load()
client = ArchiveClient(config)
search_engine = SearchEngine(client)
wayback_engine = WaybackEngine(client)
item_manager = ItemManager(client)
state_manager = PipelineStateManager(config.state_db_path)

active_pipelines: Dict[str, PipelineEngine] = {}


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await state_manager.connect()
    try:
        yield
    finally:
        await client.close()
        await state_manager.close()


app = FastAPI(title="Archivist Engine Dashboard", version="1.0.0", lifespan=lifespan)


@app.get("/api/status")
async def get_status():
    creds = config.current_credentials
    return {
        "authenticated": creds is not None,
        "access_key_preview": f"{creds.access_key[:4]}...{creds.access_key[-4:]}" if creds else None,
        "active_credential_label": creds.label if creds else None,
        "rate_limits": {
            "s3": config.rate_limits.s3_requests_per_sec,
            "wayback": config.rate_limits.wayback_requests_per_sec,
            "search": config.rate_limits.search_requests_per_sec
        },
        "active_jobs_count": len(active_pipelines)
    }


@app.get("/api/search")
async def search_items(q: str = "*:*", rows: int = 20, page: int = 1):
    try:
        res = await client.search(query=q, rows=rows, page=page)
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/wayback/cdx")
async def query_cdx(url: str, limit: int = 50, match_type: str = "exact"):
    try:
        snaps = await client.cdx_search(url=url, limit=limit, match_type=match_type)
        return snaps
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class SaveUrlRequest(BaseModel):
    url: str
    capture_all: bool = True
    capture_screenshot: bool = False


@app.post("/api/wayback/save")
async def save_url(req: SaveUrlRequest):
    try:
        res = await client.save_page_now(
            url=req.url,
            capture_all=req.capture_all,
            capture_screenshot=req.capture_screenshot
        )
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/item/{identifier}")
async def get_item_meta(identifier: str):
    try:
        return await client.get_metadata(identifier)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/api/pipeline/run")
async def run_pipeline_endpoint(pipe_config: PipelineConfig, background_tasks: BackgroundTasks):
    engine = PipelineEngine(pipe_config, client)
    run_id = engine.run_id
    active_pipelines[run_id] = engine

    async def execute():
        try:
            await engine.run()
        finally:
            active_pipelines.pop(run_id, None)

    background_tasks.add_task(execute)
    return {"run_id": run_id, "status": "started", "name": pipe_config.name}


@app.get("/api/pipeline/active")
async def list_active_pipelines():
    return {
        run_id: {
            "name": engine.config.name,
            "stats": engine._stats
        } for run_id, engine in active_pipelines.items()
    }


@app.get("/api/pipeline/stats/{run_id}")
async def get_pipeline_stats(run_id: str):
    if run_id in active_pipelines:
        return {
            "run_id": run_id,
            "status": "running",
            "stats": active_pipelines[run_id]._stats
        }
    stats = await state_manager.get_run_stats(run_id)
    return {"run_id": run_id, **stats}


@app.get("/", response_class=HTMLResponse)
async def dashboard_view():
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Archivist Engine — Archive.org Power Suite</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #0d1117;
      --card-bg: #161b22;
      --border: #30363d;
      --primary: #2f81f7;
      --accent: #238636;
      --text: #c9d1d9;
      --text-bright: #f0f6fc;
      --text-dim: #8b949e;
      --danger: #f85149;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Inter', -apple-system, sans-serif;
      line-height: 1.5;
      padding: 24px;
    }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 20px;
      border-bottom: 1px solid var(--border);
      margin-bottom: 24px;
    }
    h1 { color: var(--text-bright); font-size: 24px; display: flex; align-items: center; gap: 10px; }
    .badge {
      background: #238636;
      color: #fff;
      font-size: 12px;
      padding: 3px 8px;
      border-radius: 12px;
      font-weight: 600;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 20px;
      margin-bottom: 24px;
    }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 20px;
    }
    .card h2 {
      font-size: 16px;
      color: var(--text-bright);
      margin-bottom: 12px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    input, textarea, select, button {
      font-family: inherit;
      border-radius: 6px;
      border: 1px solid var(--border);
      background: #090d13;
      color: var(--text-bright);
      padding: 8px 12px;
      font-size: 14px;
      width: 100%;
      margin-bottom: 10px;
    }
    input:focus, textarea:focus, select:focus {
      outline: none;
      border-color: var(--primary);
    }
    button {
      background: var(--primary);
      border: none;
      font-weight: 600;
      cursor: pointer;
      transition: background 0.2s;
    }
    button:hover { background: #388bfd; }
    button.btn-accent { background: var(--accent); }
    button.btn-accent:hover { background: #2ea043; }
    pre {
      font-family: 'JetBrains Mono', monospace;
      background: #090d13;
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 12px;
      font-size: 12px;
      max-height: 240px;
      overflow: auto;
      color: #7ee787;
    }
    .status-val { font-family: 'JetBrains Mono', monospace; color: #58a6ff; }
    .flex-row { display: flex; gap: 10px; }
  </style>
</head>
<body>
  <header>
    <h1>🏛️ Archivist Engine <span class="badge" id="authBadge">Checking...</span></h1>
    <div id="systemInfo" style="font-size: 13px; color: var(--text-dim);">Connecting...</div>
  </header>

  <div class="grid">
    <!-- Wayback Archiver Card -->
    <div class="card">
      <h2>🚀 Save Page Now (SPN2)</h2>
      <input type="text" id="spnUrl" placeholder="https://example.com/article" />
      <button class="btn-accent" onclick="submitSpn()">Archive URL to Wayback</button>
      <pre id="spnOutput">// SPN2 status will appear here...</pre>
    </div>

    <!-- CDX Historical Search -->
    <div class="card">
      <h2>🕰️ Wayback CDX Search</h2>
      <div class="flex-row">
        <input type="text" id="cdxUrl" placeholder="example.com" />
        <select id="cdxMatchType" style="width: 140px;">
          <option value="exact">Exact</option>
          <option value="prefix" selected>Prefix</option>
          <option value="domain">Domain</option>
        </select>
      </div>
      <button onclick="queryCdx()">Lookup Snapshots</button>
      <pre id="cdxOutput">// Historical snapshots will appear here...</pre>
    </div>

    <!-- Catalog Explorer -->
    <div class="card">
      <h2>🔍 Archive.org Search & Items</h2>
      <input type="text" id="catalogQuery" placeholder="collection:nasa AND mediatype:image" />
      <button onclick="searchCatalog()">Search Catalog</button>
      <pre id="catalogOutput">// Catalog results will appear here...</pre>
    </div>
  </div>

  <!-- Pipeline Runner Card -->
  <div class="card" style="margin-bottom: 24px;">
    <h2>⚡ Declarative Pipeline Runner</h2>
    <textarea id="pipelineJson" rows="8" style="font-family: 'JetBrains Mono', monospace; font-size: 13px;">{
  "name": "wayback-archiver-demo",
  "concurrency": 2,
  "source": {
    "type": "url_list",
    "params": {
      "urls": [
        "https://example.com",
        "https://example.org"
      ]
    }
  },
  "transforms": [],
  "sinks": [
    {
      "type": "spn",
      "params": {
        "capture_all": true,
        "wait_completion": false
      }
    },
    {
      "type": "console",
      "params": {}
    }
  ]
}</textarea>
    <div class="flex-row">
      <button class="btn-accent" style="width: 200px;" onclick="launchPipeline()">Run Pipeline</button>
      <button style="width: 200px;" onclick="refreshActive()">Refresh Status</button>
    </div>
    <pre id="pipelineOutput">// Live pipeline execution status...</pre>
  </div>

  <script>
    async function init() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        const badge = document.getElementById('authBadge');
        const sys = document.getElementById('systemInfo');
        if (data.authenticated) {
          badge.textContent = 'Authenticated (IAS3)';
          badge.style.background = '#238636';
          sys.textContent = `Active Key: ${data.access_key_preview} (${data.active_credential_label}) | S3: ${data.rate_limits.s3}/s`;
        } else {
          badge.textContent = 'Unauthenticated';
          badge.style.background = '#8b949e';
          sys.textContent = 'Read-only mode (No S3 credentials loaded)';
        }
      } catch(e) {
        console.error(e);
      }
    }

    async function submitSpn() {
      const url = document.getElementById('spnUrl').value.trim();
      if (!url) return;
      const pre = document.getElementById('spnOutput');
      pre.textContent = 'Submitting capture job to SPN2...';
      const res = await fetch('/api/wayback/save', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({url})
      });
      const data = await res.json();
      pre.textContent = JSON.stringify(data, null, 2);
    }

    async function queryCdx() {
      const url = document.getElementById('cdxUrl').value.trim();
      const matchType = document.getElementById('cdxMatchType').value;
      if (!url) return;
      const pre = document.getElementById('cdxOutput');
      pre.textContent = 'Querying CDX index...';
      const res = await fetch(`/api/wayback/cdx?url=${encodeURIComponent(url)}&match_type=${matchType}&limit=25`);
      const data = await res.json();
      pre.textContent = JSON.stringify(data, null, 2);
    }

    async function searchCatalog() {
      const q = document.getElementById('catalogQuery').value.trim();
      if (!q) return;
      const pre = document.getElementById('catalogOutput');
      pre.textContent = 'Searching Archive.org...';
      const res = await fetch(`/api/search?q=${encodeURIComponent(q)}&rows=10`);
      const data = await res.json();
      pre.textContent = JSON.stringify(data, null, 2);
    }

    async function launchPipeline() {
      const pre = document.getElementById('pipelineOutput');
      try {
        const payload = JSON.parse(document.getElementById('pipelineJson').value);
        pre.textContent = 'Submitting pipeline...';
        const res = await fetch('/api/pipeline/run', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(payload)
        });
        const data = await res.json();
        pre.textContent = JSON.stringify(data, null, 2);
        setTimeout(refreshActive, 1000);
      } catch (err) {
        pre.textContent = 'JSON Parse Error: ' + err.message;
      }
    }

    async function refreshActive() {
      const pre = document.getElementById('pipelineOutput');
      const res = await fetch('/api/pipeline/active');
      const data = await res.json();
      pre.textContent = JSON.stringify(data, null, 2);
    }

    init();
    setInterval(refreshActive, 3000);
  </script>
</body>
</html>
"""
    return HTMLResponse(content=html_content)
