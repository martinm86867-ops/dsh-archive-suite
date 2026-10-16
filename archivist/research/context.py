"""LLM Context Card Formatter with verifiable provenance and token budgeting."""

from __future__ import annotations
from typing import Dict, Any, List, Optional


class LLMContextFormatter:
    """Formats retrieved archival records into token-bounded, citation-dense LLM context cards."""

    @staticmethod
    def format_citation_card(record: Dict[str, Any], snippet_chars: int = 1200) -> str:
        """Render a single research record as an auditable context card."""
        rec_id = record.get("id", "UNKNOWN")
        source = record.get("source_key", "UNKNOWN")
        ts = record.get("timestamp", "N/A")
        archive_url = record.get("archive_url", "N/A")
        digest = record.get("digest", "")[:16]
        content = record.get("content", "").strip()

        if len(content) > snippet_chars:
            content = content[:snippet_chars] + f"\n... [Truncated: Total length {len(record.get('content', ''))} chars]"

        return f"""### [EVIDENCE CARD: {rec_id}]
- **Source**: `{source}`
- **Captured At**: `{ts}`
- **Permanent URL**: {archive_url}
- **Integrity Digest**: `{digest}...`
- **Extracted Content**:
```text
{content}
```
"""

    @classmethod
    def build_research_prompt(
        cls,
        query: str,
        records: List[Dict[str, Any]],
        max_total_tokens: int = 4000,
        system_instructions: Optional[str] = None
    ) -> str:
        """Compose a structured LLM research prompt with explicit grounding rules."""
        cards: List[str] = []
        accumulated_tokens = 0

        for r in records:
            card = cls.format_citation_card(r)
            est_tokens = len(card) // 4
            if accumulated_tokens + est_tokens > max_total_tokens:
                break
            cards.append(card)
            accumulated_tokens += est_tokens

        evidence_section = "\n".join(cards) if cards else "_No relevant archival evidence found._"

        default_instructions = (
            "You are a rigorous archival research assistant. Synthesize the provided historical "
            "evidence cards to answer the user query. Every factual assertion MUST cite its evidence "
            "source using the exact [EVIDENCE CARD: ...] identifier and note the historical capture timestamp."
        )
        instructions = system_instructions or default_instructions

        return f"""# RESEARCH CONTEXT & VERIFIABLE ARCHIVAL EVIDENCE
{instructions}

## AVAILABLE HISTORICAL EVIDENCE ({len(cards)} cards | ~{accumulated_tokens} tokens)
{evidence_section}

---
## RESEARCH QUERY
{query}

## ANSWER SYNTHESIS (Ground assertions strictly in the evidence above):
"""
