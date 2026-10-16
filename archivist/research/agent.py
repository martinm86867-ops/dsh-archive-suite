"""AI Agent Ingestion and Retrieval Engine for Open and Restricted Archival Works."""

from __future__ import annotations
import asyncio
import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from ..client import ArchiveClient, ArchiveError, ArchiveAccessDeniedError
from ..fulltext import FullTextEngine, grade_item, PAGE_EXACT, PAGE_ITEM_AGGREGATE
from .warehouse import DuckDBWarehouse
from .context import LLMContextFormatter

logger = logging.getLogger("archivist.research.agent")


class AgentArchiveBridge:
    """Seamless bridge for AI agents to ingest and query any Archive.org work in DuckDB."""

    def __init__(
        self,
        client: Optional[ArchiveClient] = None,
        warehouse: Optional[DuckDBWarehouse] = None,
        warehouse_path: Optional[Path] = None
    ):
        self.client = client or ArchiveClient()
        default_wh = Path(self.client.config.data_dir) / "research_warehouse.duckdb"
        db_path = warehouse_path or default_wh
        self.warehouse = warehouse or DuckDBWarehouse(db_path)
        self.engine = FullTextEngine(self.client)

    async def ingest_work(
        self,
        identifier: str,
        terms: Optional[List[str]] = None,
        chunk_size: int = 1500,
        chunk_overlap: int = 200
    ) -> Dict[str, Any]:
        """Ingest any Archive.org work into DuckDB, automatically handling open vs restricted routes.

        If the item is open: downloads text/PDF, chunks into semantic blocks, and stores with digests.
        If the item is lending-restricted: mines verbatim passages via the public FTS API with honest
        page scopes and stores them as indexed evidence cards.
        """
        ident = identifier.split("|")[0].strip()
        avail = await self.engine.availability(ident)
        state = avail.get("state")
        access = avail.get("access", {})
        meta = await self.engine._metadata(ident)
        m = meta.get("metadata", {})
        title = m.get("title") or ident
        date_str = str(m.get("date") or m.get("publicdate") or "").replace("-", "")[:14]
        collections = m.get("collection") or []
        if isinstance(collections, str):
            collections = [collections]
        tier, tier_basis = grade_item(collections)

        # -------------------------------------------------------------
        # Branch 1: Lending-Restricted Work -> Auto-Borrow & Cache + Mine via FTS Index
        # -------------------------------------------------------------
        if access.get("lending") or state == "restricted":
            query_terms = terms
            if not query_terms:
                # Extract subject / title keywords as probe terms if none provided
                cand_terms = []
                if "title" in m:
                    words = re.findall(r"[A-Za-z]{4,}", m["title"])
                    cand_terms.extend(words[:3])
                subjs = m.get("subject", [])
                if isinstance(subjs, str):
                    subjs = [subjs]
                for s in subjs:
                    words = re.findall(r"[A-Za-z]{4,}", s)
                    cand_terms.extend(words[:2])
                query_terms = list(dict.fromkeys(cand_terms))[:4] or ["theory", "system"]

            logger.info(f"Auto-borrowing and caching restricted work {ident} for terms: {query_terms}")
            try:
                res = await self.engine.auto_borrow_and_cache(ident, terms=query_terms)
                if res.get("status") == "unavailable":
                    res = await self.engine.quote_from_index(ident, query_terms, delay=0.8)
            except Exception as e:
                return {
                    "identifier": ident,
                    "status": "error",
                    "reason": f"Auto-borrow / FTS index query failed: {e}",
                    "access": access
                }

            item_info = res.get("item", {})
            passages = res.get("passages", [])
            evidence_url = item_info.get("evidence_url") or f"https://archive.org/details/{ident}"
            page_scope = item_info.get("page_scope") or "item_aggregate"
            page = item_info.get("page")

            ingested_cards = []
            for idx, p in enumerate(passages, 1):
                card_id = f"card_{ident}_p{idx}"
                text_content = p.get("text", "").strip()
                if not text_content:
                    continue

                card_meta = {
                    "term": p.get("term"),
                    "page": page,
                    "page_scope": page_scope,
                    "tier": tier,
                    "tier_basis": tier_basis,
                    "spans": p.get("spans"),
                    "lending_restricted": True
                }

                self.warehouse.insert_record(
                    record_id=card_id,
                    source_type="restricted_fts_quote",
                    source_key=ident,
                    content=text_content,
                    title=f"{title} (Term: {p.get('term')})",
                    timestamp=date_str,
                    archive_url=evidence_url,
                    metadata=card_meta
                )
                ingested_cards.append(card_id)

            self.warehouse.rebuild_fts_index()
            return {
                "identifier": ident,
                "status": "ingested",
                "mode": "restricted_fts_mine",
                "title": title,
                "tier": tier,
                "tier_basis": tier_basis,
                "evidence_url": evidence_url,
                "page_scope": page_scope,
                "cards_ingested": len(ingested_cards),
                "terms_mined": query_terms,
                "caveat": res.get("caveat")
            }

        # -------------------------------------------------------------
        # Branch 2: Searchable / Open Work -> Ingest Full Text
        # -------------------------------------------------------------
        text_data = None
        try:
            text_res = await self.engine.fetch_text(ident)
            if text_res.get("found"):
                text_data = text_res.get("text", "")
        except ArchiveAccessDeniedError:
            # Fallback to Branch 1 if denied
            return await self.ingest_work(ident, terms=terms or ["system", "data"])

        if not text_data:
            # If no derivative, try PDF or fall back to FTS mine
            return await self.ingest_work(ident, terms=terms or ["system", "data"])

        # Chunk the text into sliding-window evidence blocks
        chunks = []
        start = 0
        text_len = len(text_data)
        while start < text_len:
            end = min(text_len, start + chunk_size)
            chunk_slice = text_data[start:end].strip()
            if len(chunk_slice) >= 80:
                chunks.append(chunk_slice)
            start += (chunk_size - chunk_overlap)

        evidence_url = f"https://archive.org/details/{ident}"
        for idx, chunk in enumerate(chunks, 1):
            card_id = f"card_{ident}_c{idx}"
            card_meta = {
                "chunk_index": idx,
                "total_chunks": len(chunks),
                "tier": tier,
                "tier_basis": tier_basis,
                "page_scope": "exact",
                "lending_restricted": False
            }
            self.warehouse.insert_record(
                record_id=card_id,
                source_type="open_text_derivative",
                source_key=ident,
                content=chunk,
                title=f"{title} (Part {idx}/{len(chunks)})",
                timestamp=date_str,
                archive_url=evidence_url,
                metadata=card_meta
            )

        self.warehouse.rebuild_fts_index()
        return {
            "identifier": ident,
            "status": "ingested",
            "mode": "open_derivative",
            "title": title,
            "tier": tier,
            "tier_basis": tier_basis,
            "total_chars": text_len,
            "cards_ingested": len(chunks),
            "evidence_url": evidence_url
        }

    def query_agent_cards(
        self,
        query: str,
        limit: int = 5,
        require_tier: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Search the warehouse and format results into LLM-optimized agent context cards."""
        raw_hits = self.warehouse.search_bm25(query, limit=limit * 2)
        cards = []

        for h in raw_hits:
            meta = {}
            if h.get("metadata_json"):
                try:
                    meta = json.loads(h["metadata_json"])
                except Exception:
                    pass

            item_tier = meta.get("tier") or "T2"
            if require_tier and item_tier != require_tier:
                continue

            digest = h.get("digest", "")[:16]
            source_key = h.get("source_key", "")
            title = h.get("title", source_key)
            page_scope = meta.get("page_scope") or "exact"
            page_num = meta.get("page")
            archive_url = h.get("archive_url") or f"https://archive.org/details/{source_key}"
            is_restricted = meta.get("lending_restricted", False)

            card_summary = {
                "card_id": h.get("id"),
                "identifier": source_key,
                "title": title,
                "tier": item_tier,
                "tier_basis": meta.get("tier_basis", "catalogued collection"),
                "source_type": h.get("source_type"),
                "is_restricted": is_restricted,
                "page": page_num,
                "page_scope": page_scope,
                "evidence_url": archive_url,
                "digest": digest,
                "score": round(float(h.get("score") or 0.0), 3),
                "verbatim_text": h.get("content", "").strip()
            }
            cards.append(card_summary)
            if len(cards) >= limit:
                break

        return cards

    def format_agent_prompt(
        self,
        query: str,
        cards: List[Dict[str, Any]],
        system_rules: Optional[str] = None
    ) -> str:
        """Render agent cards into a dense, verified citation prompt block."""
        blocks = []
        for c in cards:
            page_info = f"p. {c['page']} (scope: {c['page_scope']})" if c['page'] else f"scope: {c['page_scope']}"
            restr_flag = " [LENDING COPY / FTS QUOTE]" if c["is_restricted"] else ""
            block = f"""#### [{c['card_id']} | {c['tier']} | {c['identifier']}]{restr_flag}
- **Source**: *{c['title']}*
- **Permanent Link**: {c['evidence_url']}
- **Page / Scope**: {page_info}
- **Integrity Digest**: `{c['digest']}...`
```text
{c['verbatim_text'][:800]}
```"""
            blocks.append(block)

        evidence_str = "\n\n".join(blocks) if blocks else "_No archival evidence matched query in warehouse._"
        rules = system_rules or (
            "You are an archival research agent. Base your answer strictly on the verbatim evidence "
            "cards provided below. Every factual assertion MUST cite its [CARD_ID] and note whether "
            "the source is a primary record (T1), institutional publication (T2), or contemporaneous work (T3)."
        )

        return f"""# VERIFIED ARCHIVAL EVIDENCE CONTEXT ({len(cards)} cards)
{rules}

## EVIDENCE CARDS
{evidence_str}

---
## AGENT RESEARCH QUERY
{query}

## SYNTHESIS & AUDITABLE CITATIONS:
"""
